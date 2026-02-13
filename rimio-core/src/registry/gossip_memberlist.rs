use crate::error::{Result, RimError};
use crate::node::{NodeInfo, NodeStatus};
use crate::registry::Registry;
use crate::registry::gossip_internal_transport::{
    GossipInternalHttpTransport, GossipInternalTransportIngress, GossipInternalTransportOptions,
    clear_global_ingress, install_global_ingress, new_transport_channels,
};
use crate::slot_manager::{ReplicaStatus, SlotHealth, SlotInfo};
use async_trait::async_trait;
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
use tokio::sync::RwLock;

static LOCAL_REGISTRY_STORES: OnceLock<StdMutex<HashMap<String, Arc<RwLock<RegistryStore>>>>> =
    OnceLock::new();

fn shared_store_for_namespace(namespace: &str) -> Arc<RwLock<RegistryStore>> {
    let stores = LOCAL_REGISTRY_STORES.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut guard = stores
        .lock()
        .expect("gossip local registry stores mutex poisoned");

    guard
        .entry(namespace.to_string())
        .or_insert_with(|| Arc::new(RwLock::new(RegistryStore::default())))
        .clone()
}

#[derive(Default, Clone, Serialize, Deserialize)]
struct RegistryStore {
    slots: HashMap<u16, SlotInfo>,
    healths: HashMap<(u16, String), SlotHealth>,
    bootstrap_state: Option<Vec<u8>>,
}

impl RegistryStore {
    fn merge_from(&mut self, remote: RegistryStore) {
        for (slot_id, remote_slot) in remote.slots {
            match self.slots.get(&slot_id) {
                Some(local_slot) => {
                    if remote_slot.latest_seq > local_slot.latest_seq {
                        self.slots.insert(slot_id, remote_slot);
                    }
                }
                None => {
                    self.slots.insert(slot_id, remote_slot);
                }
            }
        }

        for (key, remote_health) in remote.healths {
            match self.healths.get(&key) {
                Some(local_health) => {
                    if remote_health.last_updated > local_health.last_updated {
                        self.healths.insert(key, remote_health);
                    }
                }
                None => {
                    self.healths.insert(key, remote_health);
                }
            }
        }

        if self.bootstrap_state.is_none() {
            self.bootstrap_state = remote.bootstrap_state;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireGossipMeta {
    namespace: String,
    node_id: String,
    address: String,
}

#[derive(Clone)]
struct RegistryMemberlistDelegate {
    namespace: String,
    local_meta: Arc<RwLock<WireGossipMeta>>,
    store: Arc<RwLock<RegistryStore>>,
}

#[derive(Debug, thiserror::Error)]
enum DelegateError {
    #[error("{0}")]
    Message(String),
}

impl RegistryMemberlistDelegate {
    fn new(
        namespace: String,
        local_meta: Arc<RwLock<WireGossipMeta>>,
        store: Arc<RwLock<RegistryStore>>,
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

impl memberlist::delegate::NodeDelegate for RegistryMemberlistDelegate {
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
                tracing::warn!(error = %error, "failed to encode registry local_state");
                Bytes::new()
            }
        }
    }

    async fn merge_remote_state(&self, buf: &[u8], _join: bool) {
        if buf.is_empty() {
            return;
        }

        let remote: RegistryStore = match serde_json::from_slice(buf) {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(error = %error, "failed to decode registry remote_state");
                return;
            }
        };

        let mut store = self.store.write().await;
        store.merge_from(remote);
    }
}

impl AliveDelegate for RegistryMemberlistDelegate {
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

impl MergeDelegate for RegistryMemberlistDelegate {
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

impl EventDelegate for RegistryMemberlistDelegate {
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
        .map_err(|error| RimError::Config(format!("invalid {} '{}': {}", field, value, error)))
}

fn parse_transport_name(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("memberlist_net")
        .to_ascii_lowercase()
}

fn state_to_node_status(state: State) -> NodeStatus {
    match state {
        State::Alive => NodeStatus::Healthy,
        State::Suspect => NodeStatus::Degraded,
        _ => NodeStatus::Unhealthy,
    }
}

fn is_health_expired(last_updated: chrono::DateTime<chrono::Utc>) -> bool {
    chrono::Utc::now().signed_duration_since(last_updated) > chrono::Duration::seconds(60)
}

type MemberlistType = Memberlist<
    TokioNetTransport<SmolStr, memberlist::tokio::TokioSocketAddrResolver, Tcp<TokioRuntime>>,
    CompositeDelegate<
        SmolStr,
        SocketAddr,
        RegistryMemberlistDelegate,
        memberlist::delegate::VoidDelegate<SmolStr, SocketAddr>,
        RegistryMemberlistDelegate,
        RegistryMemberlistDelegate,
        RegistryMemberlistDelegate,
    >,
>;

type InternalMemberlistType = Memberlist<
    GossipInternalHttpTransport,
    CompositeDelegate<
        SmolStr,
        SocketAddr,
        RegistryMemberlistDelegate,
        memberlist::delegate::VoidDelegate<SmolStr, SocketAddr>,
        RegistryMemberlistDelegate,
        RegistryMemberlistDelegate,
        RegistryMemberlistDelegate,
    >,
>;

enum RegistryMemberlistHandle {
    Net(MemberlistType),
    Internal(InternalMemberlistType),
}

impl RegistryMemberlistHandle {
    async fn join_many(
        &self,
        join_targets: impl Iterator<Item = Node<SmolStr, MaybeResolvedAddress<SocketAddr, SocketAddr>>>,
    ) -> std::result::Result<
        memberlist::proto::SmallVec<Node<SmolStr, SocketAddr>>,
        (
            memberlist::proto::SmallVec<Node<SmolStr, SocketAddr>>,
            String,
        ),
    > {
        match self {
            Self::Net(memberlist) => memberlist
                .join_many(join_targets)
                .await
                .map_err(|(joined, error)| (joined, error.to_string())),
            Self::Internal(memberlist) => memberlist
                .join_many(join_targets)
                .await
                .map_err(|(joined, error)| (joined, error.to_string())),
        }
    }

    async fn members(&self) -> memberlist::proto::SmallVec<Arc<NodeState<SmolStr, SocketAddr>>> {
        match self {
            Self::Net(memberlist) => memberlist.members().await,
            Self::Internal(memberlist) => memberlist.members().await,
        }
    }

    async fn update_node(&self, timeout: std::time::Duration) -> std::result::Result<(), String> {
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

pub struct GossipMemberlistRegistry {
    namespace: String,
    memberlist: RegistryMemberlistHandle,
    store: Arc<RwLock<RegistryStore>>,
    local_meta: Arc<RwLock<WireGossipMeta>>,
}

impl GossipMemberlistRegistry {
    pub async fn new(
        namespace: &str,
        node_id: &str,
        bind_addr: &str,
        advertise_addr: Option<&str>,
        seeds: Vec<String>,
        transport: Option<&str>,
    ) -> Result<Self> {
        let namespace = namespace.trim().to_string();
        if namespace.is_empty() {
            return Err(RimError::Config(
                "registry namespace cannot be empty".to_string(),
            ));
        }

        let node_id = node_id.trim();
        if node_id.is_empty() {
            return Err(RimError::Config(
                "gossip node_id cannot be empty".to_string(),
            ));
        }

        let bind = parse_socket_addr(bind_addr, "gossip bind_addr")?;
        let advertise = match advertise_addr {
            Some(value) => Some(parse_socket_addr(value, "gossip advertise_addr")?),
            None => None,
        };

        let local_node_id = SmolStr::new(node_id);
        let local_advertise_addr = advertise.unwrap_or(bind);
        let local_advertise = local_advertise_addr.to_string();
        let transport_name = parse_transport_name(transport);
        let store = shared_store_for_namespace(&namespace);
        let local_meta = Arc::new(RwLock::new(WireGossipMeta {
            namespace: namespace.clone(),
            node_id: node_id.to_string(),
            address: local_advertise,
        }));
        let delegate_core = RegistryMemberlistDelegate::new(
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
                std::time::Duration::from_secs(5),
            ));

            let options = GossipInternalTransportOptions {
                local_id: local_node_id,
                bind_addr: bind,
                advertise_addr: local_advertise_addr,
                request_timeout: std::time::Duration::from_secs(5),
                packet_subscriber,
                stream_subscriber,
            };

            let memberlist = Memberlist::with_delegate(delegate, options, Options::lan())
                .await
                .map_err(|error| {
                    clear_global_ingress();
                    RimError::Internal(format!("failed to start gossip registry: {}", error))
                })?;

            RegistryMemberlistHandle::Internal(memberlist)
        } else {
            if transport_name != "memberlist_net" && transport_name != "net" {
                return Err(RimError::Config(format!(
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
                    RimError::Internal(format!("failed to start gossip registry: {}", error))
                })?;

            RegistryMemberlistHandle::Net(memberlist)
        };

        let parsed_seeds = seeds
            .into_iter()
            .map(|seed| parse_socket_addr(seed.trim(), "gossip seed"))
            .collect::<Result<Vec<_>>>()?;

        if !parsed_seeds.is_empty() {
            let join_targets = parsed_seeds.into_iter().map(|seed| {
                Node::new(
                    SmolStr::new(seed.to_string()),
                    MaybeResolvedAddress::resolved(seed),
                )
            });

            match memberlist.join_many(join_targets).await {
                Ok(joined) => {
                    tracing::info!(joined = joined.len(), "gossip registry joined seeds");
                }
                Err((joined, error)) => {
                    if joined.is_empty() {
                        return Err(RimError::Internal(format!(
                            "failed to join gossip seeds: {}",
                            error
                        )));
                    }

                    tracing::warn!(
                        joined = joined.len(),
                        error = %error,
                        "gossip registry partially joined seeds"
                    );
                }
            }
        }

        Ok(Self {
            namespace,
            memberlist,
            store,
            local_meta,
        })
    }

    fn decode_meta(meta: &Meta) -> Result<Option<WireGossipMeta>> {
        if meta.is_empty() {
            return Ok(None);
        }

        let parsed: WireGossipMeta = serde_json::from_slice(meta.as_bytes())
            .map_err(|error| RimError::Internal(format!("decode gossip meta failed: {}", error)))?;
        Ok(Some(parsed))
    }

    async fn nodes_from_memberlist(&self) -> Result<Vec<NodeInfo>> {
        let members = self.memberlist.members().await;
        let mut nodes = Vec::new();

        for member in members {
            let meta = Self::decode_meta(member.meta())?;
            if let Some(meta) = meta {
                if meta.namespace != self.namespace {
                    continue;
                }

                nodes.push(NodeInfo {
                    node_id: meta.node_id,
                    group_id: meta.namespace,
                    address: meta.address,
                    status: state_to_node_status(member.state()),
                    slots: Vec::new(),
                });
            } else {
                nodes.push(NodeInfo {
                    node_id: member.id().to_string(),
                    group_id: self.namespace.clone(),
                    address: member.address().to_string(),
                    status: state_to_node_status(member.state()),
                    slots: Vec::new(),
                });
            }
        }

        nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        nodes.dedup_by(|left, right| left.node_id == right.node_id);
        Ok(nodes)
    }

    async fn ensure_joined_snapshot(&self) -> Result<()> {
        self.memberlist
            .update_node(std::time::Duration::from_secs(1))
            .await
            .map_err(|error| {
                RimError::Internal(format!("memberlist update_node failed: {}", error))
            })
    }
}

#[async_trait]
impl Registry for GossipMemberlistRegistry {
    async fn register_node(&self, node: &NodeInfo) -> Result<()> {
        if node.group_id.trim() != self.namespace {
            return Err(RimError::Config(format!(
                "node group_id mismatch: expected='{}', got='{}'",
                self.namespace, node.group_id
            )));
        }

        {
            let mut local_meta = self.local_meta.write().await;
            local_meta.node_id = node.node_id.clone();
            local_meta.address = node.address.clone();
        }

        self.memberlist
            .update_node(std::time::Duration::from_secs(1))
            .await
            .map_err(|error| {
                RimError::Internal(format!("memberlist update_node failed: {}", error))
            })?;

        Ok(())
    }

    async fn get_slot(&self, slot_id: u16) -> Result<Option<SlotInfo>> {
        let store = self.store.read().await;
        Ok(store.slots.get(&slot_id).cloned())
    }

    async fn set_slot(&self, info: &SlotInfo) -> Result<()> {
        let mut store = self.store.write().await;
        store.slots.insert(info.slot_id, info.clone());
        Ok(())
    }

    async fn get_all_slots(&self) -> Result<HashMap<u16, SlotInfo>> {
        let store = self.store.read().await;
        Ok(store.slots.clone())
    }

    async fn report_health(&self, health: &SlotHealth) -> Result<()> {
        let mut store = self.store.write().await;
        store
            .healths
            .insert((health.slot_id, health.node_id.clone()), health.clone());
        Ok(())
    }

    async fn get_slot_health(&self, slot_id: u16) -> Result<Vec<SlotHealth>> {
        let store = self.store.read().await;
        let mut healths = Vec::new();

        for ((current_slot, _), health) in &store.healths {
            if *current_slot == slot_id && !is_health_expired(health.last_updated) {
                healths.push(health.clone());
            }
        }

        Ok(healths)
    }

    async fn get_healthy_replicas(&self, slot_id: u16) -> Result<Vec<(String, String)>> {
        let healths = self.get_slot_health(slot_id).await?;

        let healthy: Vec<(String, String)> = healths
            .into_iter()
            .filter(|health| health.status == ReplicaStatus::Healthy)
            .map(|health| (health.node_id, health.seq))
            .collect();

        if healthy.is_empty() {
            return Ok(Vec::new());
        }

        let latest_seq = healthy
            .iter()
            .map(|(_, seq)| seq.clone())
            .max()
            .unwrap_or_default();

        Ok(healthy
            .into_iter()
            .filter(|(_, seq)| seq == &latest_seq)
            .collect())
    }

    async fn get_nodes(&self) -> Result<Vec<NodeInfo>> {
        self.nodes_from_memberlist().await
    }

    async fn get_bootstrap_state(&self) -> Result<Option<Vec<u8>>> {
        self.ensure_joined_snapshot().await?;
        let store = self.store.read().await;
        Ok(store.bootstrap_state.clone())
    }

    async fn set_bootstrap_state_if_absent(&self, payload: &[u8]) -> Result<bool> {
        let mut store = self.store.write().await;
        if store.bootstrap_state.is_some() {
            return Ok(false);
        }

        store.bootstrap_state = Some(payload.to_vec());
        drop(store);
        self.ensure_joined_snapshot().await?;
        Ok(true)
    }
}
