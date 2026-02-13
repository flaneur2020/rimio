use crate::error::{MetaError, Result};
use crate::gossip_internal_transport::{
    GossipInternalHttpTransport, GossipInternalTransportIngress, GossipInternalTransportOptions,
    clear_global_ingress, install_global_ingress, new_transport_channels,
};
use bytes::Bytes;
use memberlist::delegate::{AliveDelegate, CompositeDelegate, EventDelegate, MergeDelegate};
use memberlist::net::NetTransportOptions;
use memberlist::net::stream_layer::tcp::Tcp;
use memberlist::proto::{MaybeResolvedAddress, Meta, NodeState, State};
use memberlist::tokio::{TokioNetTransport, TokioRuntime};
use memberlist::transport::Node;
use memberlist::{Memberlist, Options};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetaMemberState {
    Alive,
    Suspect,
    Other,
}

#[derive(Debug, Clone)]
pub struct MetaMember {
    pub node_id: String,
    pub namespace: String,
    pub address: String,
    pub state: MetaMemberState,
}

#[derive(Debug, Clone)]
pub struct MetaKvOptions {
    pub namespace: String,
    pub node_id: String,
    pub bind_addr: String,
    pub advertise_addr: Option<String>,
    pub seeds: Vec<String>,
    pub transport: Option<String>,
}

#[derive(Default, Clone, Serialize, Deserialize)]
struct KvStore {
    entries: HashMap<String, KvRecord>,
}

impl KvStore {
    fn merge_from(&mut self, remote: KvStore) {
        for (key, remote_record) in remote.entries {
            match self.entries.get(&key) {
                None => {
                    self.entries.insert(key, remote_record);
                }
                Some(local_record) => {
                    let replace = remote_record.updated_at_ms > local_record.updated_at_ms
                        || (remote_record.updated_at_ms == local_record.updated_at_ms
                            && remote_record.writer > local_record.writer);
                    if replace {
                        self.entries.insert(key, remote_record);
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct KvRecord {
    value: Vec<u8>,
    updated_at_ms: i64,
    writer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireGossipMeta {
    namespace: String,
    node_id: String,
    address: String,
}

#[derive(Clone)]
struct MetaKvDelegate {
    namespace: String,
    local_meta: Arc<RwLock<WireGossipMeta>>,
    store: Arc<RwLock<KvStore>>,
}

#[derive(Debug, thiserror::Error)]
enum DelegateError {
    #[error("{0}")]
    Message(String),
}

impl MetaKvDelegate {
    fn new(
        namespace: String,
        local_meta: Arc<RwLock<WireGossipMeta>>,
        store: Arc<RwLock<KvStore>>,
    ) -> Self {
        Self {
            namespace,
            local_meta,
            store,
        }
    }

    fn encode_meta(meta: &WireGossipMeta) -> std::result::Result<Meta, DelegateError> {
        let encoded = serde_json::to_vec(meta).map_err(|error| {
            DelegateError::Message(format!("encode gossip meta failed: {}", error))
        })?;

        Meta::try_from(encoded)
            .map_err(|error| DelegateError::Message(format!("gossip meta too large: {}", error)))
    }

    fn decode_meta(meta: &Meta) -> std::result::Result<Option<WireGossipMeta>, DelegateError> {
        if meta.is_empty() {
            return Ok(None);
        }

        let parsed: WireGossipMeta = serde_json::from_slice(meta.as_bytes()).map_err(|error| {
            DelegateError::Message(format!("decode gossip meta failed: {}", error))
        })?;
        Ok(Some(parsed))
    }

    fn validate_peer(
        &self,
        peer: &NodeState<SmolStr, SocketAddr>,
    ) -> std::result::Result<(), DelegateError> {
        if let Some(meta) = Self::decode_meta(peer.meta())?
            && meta.namespace != self.namespace
        {
            return Err(DelegateError::Message(format!(
                "namespace mismatch from peer '{}': expected='{}', got='{}'",
                peer.id(),
                self.namespace,
                meta.namespace
            )));
        }

        Ok(())
    }
}

impl memberlist::delegate::NodeDelegate for MetaKvDelegate {
    async fn node_meta(&self, _limit: usize) -> Meta {
        let current_meta = self.local_meta.read().await.clone();
        Self::encode_meta(&current_meta).unwrap_or_else(|error| {
            tracing::warn!(error = %error, "failed to build memberlist node meta");
            Meta::empty()
        })
    }

    async fn local_state(&self, _join: bool) -> Bytes {
        let snapshot = self.store.read().await.clone();
        match serde_json::to_vec(&snapshot) {
            Ok(bytes) => Bytes::from(bytes),
            Err(error) => {
                tracing::warn!(error = %error, "failed to encode metakv local_state");
                Bytes::new()
            }
        }
    }

    async fn merge_remote_state(&self, buf: &[u8], _join: bool) {
        if buf.is_empty() {
            return;
        }

        let remote: KvStore = match serde_json::from_slice(buf) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(error = %error, "failed to decode metakv remote_state");
                return;
            }
        };

        let mut store = self.store.write().await;
        store.merge_from(remote);
    }
}

impl AliveDelegate for MetaKvDelegate {
    type Error = DelegateError;
    type Id = SmolStr;
    type Address = SocketAddr;

    async fn notify_alive(
        &self,
        peer: Arc<NodeState<Self::Id, Self::Address>>,
    ) -> std::result::Result<(), Self::Error> {
        self.validate_peer(&peer)
    }
}

impl MergeDelegate for MetaKvDelegate {
    type Error = DelegateError;
    type Id = SmolStr;
    type Address = SocketAddr;

    async fn notify_merge(
        &self,
        peers: Arc<[NodeState<Self::Id, Self::Address>]>,
    ) -> std::result::Result<(), Self::Error> {
        for peer in peers.iter() {
            self.validate_peer(peer)?;
        }

        Ok(())
    }
}

impl EventDelegate for MetaKvDelegate {
    type Id = SmolStr;
    type Address = SocketAddr;

    async fn notify_join(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
        tracing::info!(node = %node, "memberlist peer joined");
    }

    async fn notify_leave(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
        tracing::info!(node = %node, "memberlist peer left");
    }

    async fn notify_update(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
        tracing::debug!(node = %node, "memberlist peer updated");
    }
}

fn parse_socket_addr(value: &str, field: &str) -> Result<SocketAddr> {
    value
        .parse::<SocketAddr>()
        .map_err(|error| MetaError::Config(format!("invalid {} '{}': {}", field, value, error)))
}

fn parse_transport_name(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("memberlist_net")
        .to_ascii_lowercase()
}

fn map_member_state(state: State) -> MetaMemberState {
    match state {
        State::Alive => MetaMemberState::Alive,
        State::Suspect => MetaMemberState::Suspect,
        _ => MetaMemberState::Other,
    }
}

fn now_timestamp_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis() as i64)
        .unwrap_or_default()
}

type NetMemberlist = Memberlist<
    TokioNetTransport<SmolStr, memberlist::tokio::TokioSocketAddrResolver, Tcp<TokioRuntime>>,
    CompositeDelegate<
        SmolStr,
        SocketAddr,
        MetaKvDelegate,
        memberlist::delegate::VoidDelegate<SmolStr, SocketAddr>,
        MetaKvDelegate,
        MetaKvDelegate,
        MetaKvDelegate,
    >,
>;

type InternalMemberlist = Memberlist<
    GossipInternalHttpTransport,
    CompositeDelegate<
        SmolStr,
        SocketAddr,
        MetaKvDelegate,
        memberlist::delegate::VoidDelegate<SmolStr, SocketAddr>,
        MetaKvDelegate,
        MetaKvDelegate,
        MetaKvDelegate,
    >,
>;

enum MemberlistHandle {
    Net(NetMemberlist),
    Internal(InternalMemberlist),
}

impl MemberlistHandle {
    async fn members(&self) -> memberlist::proto::SmallVec<Arc<NodeState<SmolStr, SocketAddr>>> {
        match self {
            Self::Net(memberlist) => memberlist.members().await,
            Self::Internal(memberlist) => memberlist.members().await,
        }
    }

    async fn update_node(&self, timeout: Duration) -> std::result::Result<(), String> {
        match self {
            Self::Net(memberlist) => memberlist
                .update_node(timeout)
                .await
                .map_err(|error| error.to_string()),
            Self::Internal(memberlist) => memberlist
                .update_node(timeout)
                .await
                .map_err(|error| error.to_string()),
        }
    }
}

static LOCAL_KV_STORES: OnceLock<StdMutex<HashMap<String, Arc<RwLock<KvStore>>>>> = OnceLock::new();

fn shared_store_for_namespace(namespace: &str) -> Arc<RwLock<KvStore>> {
    let stores = LOCAL_KV_STORES.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut guard = stores.lock().expect("metakv local stores mutex poisoned");

    guard
        .entry(namespace.to_string())
        .or_insert_with(|| Arc::new(RwLock::new(KvStore::default())))
        .clone()
}

#[derive(Clone)]
pub struct MetaKv {
    namespace: String,
    memberlist: Arc<MemberlistHandle>,
    store: Arc<RwLock<KvStore>>,
    local_meta: Arc<RwLock<WireGossipMeta>>,
}

impl MetaKv {
    pub async fn new(options: MetaKvOptions) -> Result<Self> {
        let namespace = options.namespace.trim().to_string();
        if namespace.is_empty() {
            return Err(MetaError::Config(
                "registry namespace cannot be empty".to_string(),
            ));
        }

        let node_id = options.node_id.trim();
        if node_id.is_empty() {
            return Err(MetaError::Config(
                "gossip node_id cannot be empty".to_string(),
            ));
        }

        let bind = parse_socket_addr(&options.bind_addr, "gossip bind_addr")?;
        let advertise = match options.advertise_addr.as_deref() {
            Some(value) if !value.trim().is_empty() => {
                Some(parse_socket_addr(value, "gossip advertise_addr")?)
            }
            _ => None,
        };

        let local_node_id = SmolStr::new(node_id);
        let local_advertise_addr = advertise.unwrap_or(bind);
        let local_advertise = local_advertise_addr.to_string();
        let transport_name = parse_transport_name(options.transport.as_deref());
        let store = shared_store_for_namespace(&namespace);
        let local_meta = Arc::new(RwLock::new(WireGossipMeta {
            namespace: namespace.clone(),
            node_id: node_id.to_string(),
            address: local_advertise,
        }));

        let delegate_core = MetaKvDelegate::new(
            namespace.clone(),
            Arc::clone(&local_meta),
            Arc::clone(&store),
        );
        let delegate = CompositeDelegate::new()
            .with_alive_delegate(delegate_core.clone())
            .with_event_delegate(delegate_core.clone())
            .with_merge_delegate(delegate_core.clone())
            .with_node_delegate(delegate_core);

        let memberlist = if transport_name == "internal_http" {
            let (packet_producer, packet_subscriber, stream_producer, stream_subscriber) =
                new_transport_channels();
            install_global_ingress(GossipInternalTransportIngress::new(
                packet_producer,
                stream_producer,
                Duration::from_secs(5),
            ));

            let transport_options = GossipInternalTransportOptions {
                local_id: local_node_id,
                bind_addr: bind,
                advertise_addr: local_advertise_addr,
                request_timeout: Duration::from_secs(5),
                packet_subscriber,
                stream_subscriber,
            };

            let memberlist = Memberlist::with_delegate(delegate, transport_options, Options::lan())
                .await
                .map_err(|error| {
                    clear_global_ingress();
                    MetaError::Internal(format!("failed to start gossip metakv: {}", error))
                })?;

            MemberlistHandle::Internal(memberlist)
        } else {
            if transport_name != "memberlist_net" && transport_name != "net" {
                return Err(MetaError::Config(format!(
                    "unsupported gossip transport '{}': expected memberlist_net|internal_http",
                    transport_name
                )));
            }

            let mut transport_options = NetTransportOptions::<
                SmolStr,
                memberlist::tokio::TokioSocketAddrResolver,
                Tcp<TokioRuntime>,
            >::with_stream_layer_options(local_node_id, ());

            transport_options.add_bind_address(bind);
            if let Some(addr) = advertise {
                transport_options = transport_options.with_advertise_address(addr);
            }

            let memberlist = Memberlist::with_delegate(delegate, transport_options, Options::lan())
                .await
                .map_err(|error| {
                    MetaError::Internal(format!("failed to start gossip metakv: {}", error))
                })?;

            MemberlistHandle::Net(memberlist)
        };

        let parsed_seeds = options
            .seeds
            .into_iter()
            .map(|seed| parse_socket_addr(seed.trim(), "gossip seed"))
            .collect::<Result<Vec<_>>>()?;

        if !parsed_seeds.is_empty() {
            match &memberlist {
                MemberlistHandle::Net(net) => {
                    let join_targets = parsed_seeds.iter().cloned().map(|seed| {
                        Node::new(
                            SmolStr::new(seed.to_string()),
                            MaybeResolvedAddress::resolved(seed),
                        )
                    });

                    match net.join_many(join_targets).await {
                        Ok(joined) => {
                            tracing::info!(joined = joined.len(), "gossip metakv joined seeds");
                        }
                        Err((joined, error)) => {
                            if joined.is_empty() {
                                return Err(MetaError::Internal(format!(
                                    "failed to join gossip seeds: {}",
                                    error
                                )));
                            }

                            tracing::warn!(
                                joined = joined.len(),
                                error = %error,
                                "gossip metakv partially joined seeds"
                            );
                        }
                    }
                }
                MemberlistHandle::Internal(internal) => {
                    let join_targets = parsed_seeds.iter().cloned().map(|seed| {
                        Node::new(
                            SmolStr::new(seed.to_string()),
                            MaybeResolvedAddress::resolved(seed),
                        )
                    });

                    match internal.join_many(join_targets).await {
                        Ok(joined) => {
                            tracing::info!(joined = joined.len(), "gossip metakv joined seeds");
                        }
                        Err((joined, error)) => {
                            if joined.is_empty() {
                                return Err(MetaError::Internal(format!(
                                    "failed to join gossip seeds: {}",
                                    error
                                )));
                            }

                            tracing::warn!(
                                joined = joined.len(),
                                error = %error,
                                "gossip metakv partially joined seeds"
                            );
                        }
                    }
                }
            }
        }

        Ok(Self {
            namespace,
            memberlist: Arc::new(memberlist),
            store,
            local_meta,
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub async fn set_local_node(&self, node_id: &str, address: &str) -> Result<()> {
        {
            let mut local_meta = self.local_meta.write().await;
            local_meta.node_id = node_id.to_string();
            local_meta.address = address.to_string();
        }

        self.sync_once().await
    }

    pub async fn sync_once(&self) -> Result<()> {
        self.memberlist
            .update_node(Duration::from_secs(1))
            .await
            .map_err(|error| {
                MetaError::Internal(format!("memberlist update_node failed: {}", error))
            })
    }

    pub async fn members(&self) -> Result<Vec<MetaMember>> {
        self.sync_once().await?;

        let peers = self.memberlist.members().await;
        let mut members = Vec::new();

        for peer in peers {
            let maybe_meta = MetaKvDelegate::decode_meta(peer.meta())
                .map_err(|error| MetaError::Internal(error.to_string()))?;

            if let Some(meta) = maybe_meta {
                if meta.namespace != self.namespace {
                    continue;
                }

                members.push(MetaMember {
                    node_id: meta.node_id,
                    namespace: meta.namespace,
                    address: meta.address,
                    state: map_member_state(peer.state()),
                });
            } else {
                members.push(MetaMember {
                    node_id: peer.id().to_string(),
                    namespace: self.namespace.clone(),
                    address: peer.address().to_string(),
                    state: map_member_state(peer.state()),
                });
            }
        }

        members.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        members.dedup_by(|left, right| left.node_id == right.node_id);

        Ok(members)
    }

    pub async fn put(&self, key: &str, value: &[u8]) -> Result<()> {
        let writer = {
            let local = self.local_meta.read().await;
            local.node_id.clone()
        };

        let mut store = self.store.write().await;
        store.entries.insert(
            key.to_string(),
            KvRecord {
                value: value.to_vec(),
                updated_at_ms: now_timestamp_ms(),
                writer,
            },
        );
        Ok(())
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let store = self.store.read().await;
        Ok(store.entries.get(key).map(|record| record.value.clone()))
    }

    pub async fn list_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let store = self.store.read().await;
        let mut items = store
            .entries
            .iter()
            .filter_map(|(key, record)| {
                if key.starts_with(prefix) {
                    Some((key.clone(), record.value.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        items.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(items)
    }

    pub async fn put_if_absent(&self, key: &str, value: &[u8]) -> Result<bool> {
        let writer = {
            let local = self.local_meta.read().await;
            local.node_id.clone()
        };

        let mut store = self.store.write().await;
        if store.entries.contains_key(key) {
            return Ok(false);
        }

        store.entries.insert(
            key.to_string(),
            KvRecord {
                value: value.to_vec(),
                updated_at_ms: now_timestamp_ms(),
                writer,
            },
        );
        Ok(true)
    }
}
