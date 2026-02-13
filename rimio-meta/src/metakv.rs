use crate::error::{MetaError, Result};
use openraft::BasicNode;
use openraft::Config as RaftConfig;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::LogState;
use openraft::Raft;
use openraft::RaftLogId;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::RaftTypeConfig;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;
use openraft::Vote;
use openraft::error::ForwardToLeader;
use openraft::error::InstallSnapshotError;
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::ClientWriteResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftStateMachine;
use rusqlite::Connection;
use rusqlite::OptionalExtension;
use rusqlite::params;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::Digest;
use sha2::Sha256;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::RwLock;

const URI_RAFT_VOTE: &str = "/internal/v1/meta/raft-vote";
const URI_RAFT_APPEND: &str = "/internal/v1/meta/raft-append";
const URI_RAFT_SNAPSHOT: &str = "/internal/v1/meta/raft-snapshot";
const URI_ADD_LEARNER: &str = "/internal/v1/meta/add-learner";
const URI_CLIENT_WRITE: &str = "/internal/v1/meta/write";

const META_KEY_LAST_APPLIED_LOG: &str = "__meta:last_applied_log";
const META_KEY_LAST_MEMBERSHIP: &str = "__meta:last_membership";

pub type MetaNodeId = u64;

openraft::declare_raft_types!(
    pub MetaTypeConfig:
        D = MetaWriteRequest,
        R = MetaWriteResponse,
        Node = BasicNode,
);

type MetaRaft = Raft<MetaTypeConfig>;

pub type MetaRaftError<E = openraft::error::Infallible> = openraft::error::RaftError<MetaNodeId, E>;
pub type MetaClientWriteError = openraft::error::ClientWriteError<MetaNodeId, BasicNode>;

pub type MetaVoteRequest = VoteRequest<MetaNodeId>;
pub type MetaVoteResponse = VoteResponse<MetaNodeId>;
pub type MetaAppendEntriesRequest = AppendEntriesRequest<MetaTypeConfig>;
pub type MetaAppendEntriesResponse = AppendEntriesResponse<MetaNodeId>;
pub type MetaInstallSnapshotRequest = InstallSnapshotRequest<MetaTypeConfig>;
pub type MetaInstallSnapshotResponse = InstallSnapshotResponse<MetaNodeId>;

pub type MetaVoteResult = std::result::Result<MetaVoteResponse, MetaRaftError>;
pub type MetaAppendEntriesResult = std::result::Result<MetaAppendEntriesResponse, MetaRaftError>;
pub type MetaInstallSnapshotResult =
    std::result::Result<MetaInstallSnapshotResponse, MetaRaftError<InstallSnapshotError>>;
pub type MetaClientWriteResult =
    std::result::Result<ClientWriteResponse<MetaTypeConfig>, MetaRaftError<MetaClientWriteError>>;
pub type MetaAddLearnerResult =
    std::result::Result<ClientWriteResponse<MetaTypeConfig>, MetaRaftError<MetaClientWriteError>>;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum MetaWriteRequest {
    Put { key: String, value: Vec<u8> },
    PutIfAbsent { key: String, value: Vec<u8> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaWriteResponse {
    pub created: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaAddLearnerRequest {
    pub node_id: String,
    pub address: String,
    #[serde(default)]
    pub blocking: bool,
}

#[derive(Debug, Clone)]
struct LocalNodeMeta {
    node_id: String,
    address: String,
}

#[derive(Debug, Clone, Deserialize)]
struct NodeRecord {
    node_id: String,
    #[serde(default)]
    group_id: String,
    #[serde(default)]
    address: String,
}

fn canonical_transport(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("internal_http")
        .to_ascii_lowercase()
}

fn validate_transport(value: Option<&str>) -> Result<()> {
    let transport = canonical_transport(value);
    if transport == "internal_http" || transport == "openraft_http" || transport == "openraft" {
        return Ok(());
    }

    Err(MetaError::Config(format!(
        "unsupported gossip transport '{}': expected internal_http",
        transport
    )))
}

fn normalize_address(value: &str, field: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(MetaError::Config(format!("{} cannot be empty", field)));
    }

    if !trimmed.contains(':') {
        return Err(MetaError::Config(format!(
            "{} '{}' is invalid: expected host:port",
            field, value
        )));
    }

    Ok(trimmed.to_string())
}

fn normalize_seed_list(seeds: Vec<String>) -> Result<Vec<String>> {
    let mut unique = Vec::new();
    let mut seen = HashSet::new();

    for seed in seeds {
        let addr = normalize_address(seed.as_str(), "gossip seed")?;
        if seen.insert(addr.clone()) {
            unique.push(addr);
        }
    }

    Ok(unique)
}

fn sanitize_filename_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>()
}

fn metakv_db_path(namespace: &str, node_id: &str, bind_addr: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push("rimio");
    path.push("meta");

    let file_name = format!(
        "{}_{}_{}.sqlite",
        sanitize_filename_component(namespace),
        sanitize_filename_component(node_id),
        sanitize_filename_component(bind_addr)
    );

    path.push(file_name);
    path
}

fn node_name_to_raft_id(node_id: &str) -> MetaNodeId {
    let digest = Sha256::digest(node_id.as_bytes());
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[0..8]);
    let mut value = u64::from_be_bytes(bytes);

    if value == 0 {
        value = 1;
    }

    value
}

async fn post_json<Request, Response>(
    client: &reqwest::Client,
    addr: &str,
    uri: &str,
    payload: &Request,
) -> Result<Response>
where
    Request: Serialize + ?Sized,
    Response: DeserializeOwned,
{
    let url = format!("http://{}{}", addr.trim_end_matches('/'), uri);
    let response = client.post(url).json(payload).send().await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(MetaError::Http(format!(
            "remote returned non-success status {}: {}",
            status, body
        )));
    }

    response
        .json::<Response>()
        .await
        .map_err(|error| MetaError::Http(error.to_string()))
}

#[derive(Clone)]
struct MetaNetwork {
    client: reqwest::Client,
}

impl MetaNetwork {
    fn new() -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self { client }
    }

    async fn send_rpc<Req, Resp, Err>(
        &self,
        target: MetaNodeId,
        target_node: &BasicNode,
        uri: &str,
        request: Req,
    ) -> std::result::Result<Resp, openraft::error::RPCError<MetaNodeId, BasicNode, Err>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        Err: std::error::Error + DeserializeOwned,
    {
        let url = format!("http://{}{}", target_node.addr, uri);

        let response = self
            .client
            .post(url)
            .json(&request)
            .send()
            .await
            .map_err(|error| {
                if error.is_connect() {
                    return openraft::error::RPCError::Unreachable(
                        openraft::error::Unreachable::new(&error),
                    );
                }

                openraft::error::RPCError::Network(openraft::error::NetworkError::new(&error))
            })?;

        let payload: std::result::Result<Resp, Err> = response.json().await.map_err(|error| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(&error))
        })?;

        payload.map_err(|error| {
            openraft::error::RPCError::RemoteError(openraft::error::RemoteError::new(target, error))
        })
    }
}

impl RaftNetworkFactory<MetaTypeConfig> for MetaNetwork {
    type Network = MetaNetworkConnection;

    async fn new_client(&mut self, target: MetaNodeId, node: &BasicNode) -> Self::Network {
        MetaNetworkConnection {
            owner: self.clone(),
            target,
            target_node: node.clone(),
        }
    }
}

struct MetaNetworkConnection {
    owner: MetaNetwork,
    target: MetaNodeId,
    target_node: BasicNode,
}

impl RaftNetwork<MetaTypeConfig> for MetaNetworkConnection {
    async fn append_entries(
        &mut self,
        request: MetaAppendEntriesRequest,
        _option: RPCOption,
    ) -> std::result::Result<
        MetaAppendEntriesResponse,
        openraft::error::RPCError<MetaNodeId, BasicNode, MetaRaftError>,
    > {
        self.owner
            .send_rpc(self.target, &self.target_node, URI_RAFT_APPEND, request)
            .await
    }

    async fn install_snapshot(
        &mut self,
        request: MetaInstallSnapshotRequest,
        _option: RPCOption,
    ) -> std::result::Result<
        MetaInstallSnapshotResponse,
        openraft::error::RPCError<MetaNodeId, BasicNode, MetaRaftError<InstallSnapshotError>>,
    > {
        self.owner
            .send_rpc(self.target, &self.target_node, URI_RAFT_SNAPSHOT, request)
            .await
    }

    async fn vote(
        &mut self,
        request: MetaVoteRequest,
        _option: RPCOption,
    ) -> std::result::Result<
        MetaVoteResponse,
        openraft::error::RPCError<MetaNodeId, BasicNode, MetaRaftError>,
    > {
        self.owner
            .send_rpc(self.target, &self.target_node, URI_RAFT_VOTE, request)
            .await
    }
}

#[derive(Clone, Debug, Default)]
struct MetaLogStore<C: RaftTypeConfig> {
    inner: Arc<Mutex<MetaLogStoreInner<C>>>,
}

#[derive(Debug)]
struct MetaLogStoreInner<C: RaftTypeConfig> {
    last_purged_log_id: Option<LogId<C::NodeId>>,
    log: BTreeMap<u64, C::Entry>,
    committed: Option<LogId<C::NodeId>>,
    vote: Option<Vote<C::NodeId>>,
}

impl<C: RaftTypeConfig> Default for MetaLogStoreInner<C> {
    fn default() -> Self {
        Self {
            last_purged_log_id: None,
            log: BTreeMap::new(),
            committed: None,
            vote: None,
        }
    }
}

impl<C: RaftTypeConfig> MetaLogStoreInner<C> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> std::result::Result<Vec<C::Entry>, StorageError<C::NodeId>>
    where
        C::Entry: Clone,
    {
        Ok(self
            .log
            .range(range)
            .map(|(_index, entry)| entry.clone())
            .collect())
    }

    async fn get_log_state(&mut self) -> std::result::Result<LogState<C>, StorageError<C::NodeId>> {
        let last = self
            .log
            .iter()
            .next_back()
            .map(|(_index, entry)| entry.get_log_id().clone())
            .or_else(|| self.last_purged_log_id.clone());

        Ok(LogState {
            last_purged_log_id: self.last_purged_log_id.clone(),
            last_log_id: last,
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<C::NodeId>>,
    ) -> std::result::Result<(), StorageError<C::NodeId>> {
        self.committed = committed;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> std::result::Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
        Ok(self.committed.clone())
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<C::NodeId>,
    ) -> std::result::Result<(), StorageError<C::NodeId>> {
        self.vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> std::result::Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
        Ok(self.vote.clone())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<C>,
    ) -> std::result::Result<(), StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry>,
    {
        for entry in entries {
            self.log.insert(entry.get_log_id().index, entry);
        }

        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(
        &mut self,
        log_id: LogId<C::NodeId>,
    ) -> std::result::Result<(), StorageError<C::NodeId>> {
        let keys = self
            .log
            .range(log_id.index..)
            .map(|(index, _entry)| *index)
            .collect::<Vec<_>>();

        for key in keys {
            self.log.remove(&key);
        }

        Ok(())
    }

    async fn purge(
        &mut self,
        log_id: LogId<C::NodeId>,
    ) -> std::result::Result<(), StorageError<C::NodeId>> {
        self.last_purged_log_id = Some(log_id.clone());

        let keys = self
            .log
            .range(..=log_id.index)
            .map(|(index, _entry)| *index)
            .collect::<Vec<_>>();

        for key in keys {
            self.log.remove(&key);
        }

        Ok(())
    }
}

impl<C: RaftTypeConfig> RaftLogReader<C> for MetaLogStore<C>
where
    C::Entry: Clone,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> std::result::Result<Vec<C::Entry>, StorageError<C::NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.try_get_log_entries(range).await
    }
}

impl<C: RaftTypeConfig> RaftLogStorage<C> for MetaLogStore<C>
where
    C::Entry: Clone,
{
    type LogReader = Self;

    async fn get_log_state(&mut self) -> std::result::Result<LogState<C>, StorageError<C::NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.get_log_state().await
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<C::NodeId>>,
    ) -> std::result::Result<(), StorageError<C::NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.save_committed(committed).await
    }

    async fn read_committed(
        &mut self,
    ) -> std::result::Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.read_committed().await
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<C::NodeId>,
    ) -> std::result::Result<(), StorageError<C::NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.save_vote(vote).await
    }

    async fn read_vote(
        &mut self,
    ) -> std::result::Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.read_vote().await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<C>,
    ) -> std::result::Result<(), StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry>,
    {
        let mut inner = self.inner.lock().await;
        inner.append(entries, callback).await
    }

    async fn truncate(
        &mut self,
        log_id: LogId<C::NodeId>,
    ) -> std::result::Result<(), StorageError<C::NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.truncate(log_id).await
    }

    async fn purge(
        &mut self,
        log_id: LogId<C::NodeId>,
    ) -> std::result::Result<(), StorageError<C::NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.purge(log_id).await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

#[derive(Debug)]
struct StoredSnapshot {
    meta: SnapshotMeta<MetaNodeId, BasicNode>,
    data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotData {
    last_applied_log: Option<LogId<MetaNodeId>>,
    last_membership: StoredMembership<MetaNodeId, BasicNode>,
    entries: Vec<(String, Vec<u8>)>,
}

#[derive(Debug)]
struct MetaStateMachineStore {
    db_path: PathBuf,
    snapshot_idx: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
}

impl MetaStateMachineStore {
    fn new(db_path: PathBuf) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let this = Self {
            db_path,
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(None),
        };

        this.init_schema()?;
        Ok(this)
    }

    fn open_connection(&self) -> std::result::Result<Connection, rusqlite::Error> {
        let conn = Connection::open(&self.db_path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.busy_timeout(Duration::from_millis(500))?;
        Ok(conn)
    }

    fn init_schema(&self) -> Result<()> {
        let conn = self.open_connection()?;
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS kv (
              k TEXT PRIMARY KEY,
              v BLOB NOT NULL
            );

            CREATE TABLE IF NOT EXISTS meta (
              k TEXT PRIMARY KEY,
              v BLOB NOT NULL
            );
            ",
        )?;
        Ok(())
    }

    fn write_meta_value<T: Serialize>(
        tx: &rusqlite::Transaction<'_>,
        key: &str,
        value: &T,
    ) -> std::result::Result<(), rusqlite::Error> {
        let encoded = serde_json::to_vec(value).map_err(|error| {
            rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(
                error.to_string(),
            )))
        })?;

        tx.execute(
            "INSERT INTO meta(k, v) VALUES (?1, ?2) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            params![key, encoded],
        )?;

        Ok(())
    }

    fn read_meta_value<T: DeserializeOwned>(
        conn: &Connection,
        key: &str,
    ) -> std::result::Result<Option<T>, rusqlite::Error> {
        let raw = conn
            .query_row("SELECT v FROM meta WHERE k=?1", params![key], |row| {
                row.get::<_, Vec<u8>>(0)
            })
            .optional()?;

        match raw {
            Some(bytes) => {
                let parsed = serde_json::from_slice::<T>(&bytes).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        bytes.len(),
                        rusqlite::types::Type::Blob,
                        Box::new(std::io::Error::other(error.to_string())),
                    )
                })?;

                Ok(Some(parsed))
            }
            None => Ok(None),
        }
    }

    fn read_applied_state(
        &self,
    ) -> std::result::Result<
        (
            Option<LogId<MetaNodeId>>,
            StoredMembership<MetaNodeId, BasicNode>,
        ),
        rusqlite::Error,
    > {
        let conn = self.open_connection()?;
        let last_applied_log =
            Self::read_meta_value::<Option<LogId<MetaNodeId>>>(&conn, META_KEY_LAST_APPLIED_LOG)?
                .flatten();

        let last_membership = Self::read_meta_value::<StoredMembership<MetaNodeId, BasicNode>>(
            &conn,
            META_KEY_LAST_MEMBERSHIP,
        )?
        .unwrap_or_default();

        Ok((last_applied_log, last_membership))
    }

    fn list_all_entries(&self) -> std::result::Result<Vec<(String, Vec<u8>)>, rusqlite::Error> {
        let conn = self.open_connection()?;
        let mut stmt = conn.prepare("SELECT k, v FROM kv ORDER BY k ASC")?;

        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(rows)
    }

    fn get_value(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let conn = self.open_connection()?;
        conn.query_row("SELECT v FROM kv WHERE k=?1", params![key], |row| {
            row.get::<_, Vec<u8>>(0)
        })
        .optional()
        .map_err(MetaError::from)
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let conn = self.open_connection()?;
        let pattern = format!("{}%", prefix);

        let mut stmt = conn.prepare("SELECT k, v FROM kv WHERE k LIKE ?1 ORDER BY k ASC")?;
        let rows = stmt
            .query_map(params![pattern], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(rows)
    }
}

impl RaftSnapshotBuilder<MetaTypeConfig> for Arc<MetaStateMachineStore> {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<Snapshot<MetaTypeConfig>, StorageError<MetaNodeId>> {
        let entries = self
            .list_all_entries()
            .map_err(|error| StorageIOError::read_state_machine(&error))?;
        let (last_applied_log, last_membership) = self
            .read_applied_state()
            .map_err(|error| StorageIOError::read_state_machine(&error))?;

        let payload = SnapshotData {
            last_applied_log,
            last_membership: last_membership.clone(),
            entries,
        };

        let data = serde_json::to_vec(&payload)
            .map_err(|error| StorageIOError::read_state_machine(&error))?;

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = payload.last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: payload.last_applied_log,
            last_membership,
            snapshot_id,
        };

        {
            let mut current = self.current_snapshot.write().await;
            *current = Some(StoredSnapshot {
                meta: meta.clone(),
                data: data.clone(),
            });
        }

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<MetaTypeConfig> for Arc<MetaStateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> std::result::Result<
        (
            Option<LogId<MetaNodeId>>,
            StoredMembership<MetaNodeId, BasicNode>,
        ),
        StorageError<MetaNodeId>,
    > {
        let state = self
            .read_applied_state()
            .map_err(|error| StorageIOError::read_state_machine(&error))?;
        Ok(state)
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> std::result::Result<Vec<MetaWriteResponse>, StorageError<MetaNodeId>>
    where
        I: IntoIterator<Item = Entry<MetaTypeConfig>> + Send,
    {
        let mut responses = Vec::new();
        let mut conn = self
            .open_connection()
            .map_err(|error| StorageIOError::write_state_machine(&error))?;
        let tx = conn
            .transaction()
            .map_err(|error| StorageIOError::write_state_machine(&error))?;

        let mut last_applied_log: Option<LogId<MetaNodeId>> = None;
        let mut last_membership: Option<StoredMembership<MetaNodeId, BasicNode>> = None;

        for entry in entries {
            last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => responses.push(MetaWriteResponse { created: false }),
                EntryPayload::Normal(request) => match request {
                    MetaWriteRequest::Put { key, value } => {
                        tx.execute(
                            "INSERT INTO kv(k, v) VALUES (?1, ?2) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                            params![key, value],
                        )
                        .map_err(|error| StorageIOError::write_state_machine(&error))?;

                        responses.push(MetaWriteResponse { created: true });
                    }
                    MetaWriteRequest::PutIfAbsent { key, value } => {
                        let existing = tx
                            .query_row("SELECT 1 FROM kv WHERE k=?1", params![key], |row| {
                                row.get::<_, i64>(0)
                            })
                            .optional()
                            .map_err(|error| StorageIOError::write_state_machine(&error))?;

                        if existing.is_some() {
                            responses.push(MetaWriteResponse { created: false });
                        } else {
                            tx.execute("INSERT INTO kv(k, v) VALUES (?1, ?2)", params![key, value])
                                .map_err(|error| StorageIOError::write_state_machine(&error))?;
                            responses.push(MetaWriteResponse { created: true });
                        }
                    }
                },
                EntryPayload::Membership(membership) => {
                    last_membership = Some(StoredMembership::new(Some(entry.log_id), membership));
                    responses.push(MetaWriteResponse { created: true });
                }
            }
        }

        MetaStateMachineStore::write_meta_value(&tx, META_KEY_LAST_APPLIED_LOG, &last_applied_log)
            .map_err(|error| StorageIOError::write_state_machine(&error))?;

        if let Some(last_membership) = last_membership {
            MetaStateMachineStore::write_meta_value(
                &tx,
                META_KEY_LAST_MEMBERSHIP,
                &last_membership,
            )
            .map_err(|error| StorageIOError::write_state_machine(&error))?;
        }

        tx.commit()
            .map_err(|error| StorageIOError::write_state_machine(&error))?;
        Ok(responses)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> std::result::Result<
        Box<<MetaTypeConfig as RaftTypeConfig>::SnapshotData>,
        StorageError<MetaNodeId>,
    > {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<MetaNodeId, BasicNode>,
        snapshot: Box<<MetaTypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> std::result::Result<(), StorageError<MetaNodeId>> {
        let raw = snapshot.into_inner();

        let payload = serde_json::from_slice::<SnapshotData>(&raw)
            .map_err(|error| StorageIOError::read_snapshot(Some(meta.signature()), &error))?;

        {
            let mut conn = self
                .open_connection()
                .map_err(|error| StorageIOError::write_snapshot(None, &error))?;
            let tx = conn
                .transaction()
                .map_err(|error| StorageIOError::write_snapshot(None, &error))?;

            tx.execute("DELETE FROM kv", [])
                .map_err(|error| StorageIOError::write_snapshot(None, &error))?;

            for (key, value) in payload.entries {
                tx.execute("INSERT INTO kv(k, v) VALUES (?1, ?2)", params![key, value])
                    .map_err(|error| StorageIOError::write_snapshot(None, &error))?;
            }

            MetaStateMachineStore::write_meta_value(
                &tx,
                META_KEY_LAST_APPLIED_LOG,
                &meta.last_log_id,
            )
            .map_err(|error| StorageIOError::write_snapshot(None, &error))?;
            MetaStateMachineStore::write_meta_value(
                &tx,
                META_KEY_LAST_MEMBERSHIP,
                &meta.last_membership,
            )
            .map_err(|error| StorageIOError::write_snapshot(None, &error))?;

            tx.commit()
                .map_err(|error| StorageIOError::write_snapshot(None, &error))?;
        }

        {
            let mut current = self.current_snapshot.write().await;
            *current = Some(StoredSnapshot {
                meta: meta.clone(),
                data: raw,
            });
        }

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<Snapshot<MetaTypeConfig>>, StorageError<MetaNodeId>> {
        let current = self.current_snapshot.read().await;
        match &*current {
            Some(snapshot) => Ok(Some(Snapshot {
                meta: snapshot.meta.clone(),
                snapshot: Box::new(Cursor::new(snapshot.data.clone())),
            })),
            None => Ok(None),
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

#[derive(Clone)]
struct GlobalMetaNode {
    raft: MetaRaft,
}

static GLOBAL_META_NODE: OnceLock<StdMutex<Option<Arc<GlobalMetaNode>>>> = OnceLock::new();

fn global_meta_node_lock() -> &'static StdMutex<Option<Arc<GlobalMetaNode>>> {
    GLOBAL_META_NODE.get_or_init(|| StdMutex::new(None))
}

fn install_global_node(node: Arc<GlobalMetaNode>) {
    let lock = global_meta_node_lock();
    let mut guard = lock.lock().expect("global meta node lock poisoned");
    *guard = Some(node);
}

fn get_global_node() -> Result<Arc<GlobalMetaNode>> {
    let lock = global_meta_node_lock();
    let guard = lock.lock().expect("global meta node lock poisoned");

    guard.clone().ok_or_else(|| {
        MetaError::Internal("meta raft global node is not initialized yet".to_string())
    })
}

pub fn clear_global_node() {
    if let Some(lock) = GLOBAL_META_NODE.get() {
        let mut guard = lock.lock().expect("global meta node lock poisoned");
        *guard = None;
    }
}

pub async fn handle_global_vote(request: MetaVoteRequest) -> Result<MetaVoteResult> {
    let node = get_global_node()?;
    Ok(node.raft.vote(request).await)
}

pub async fn handle_global_append_entries(
    request: MetaAppendEntriesRequest,
) -> Result<MetaAppendEntriesResult> {
    let node = get_global_node()?;
    Ok(node.raft.append_entries(request).await)
}

pub async fn handle_global_install_snapshot(
    request: MetaInstallSnapshotRequest,
) -> Result<MetaInstallSnapshotResult> {
    let node = get_global_node()?;
    Ok(node.raft.install_snapshot(request).await)
}

pub async fn handle_global_add_learner(
    request: MetaAddLearnerRequest,
) -> Result<MetaAddLearnerResult> {
    let node = get_global_node()?;

    let node_name = request.node_id.trim();
    if node_name.is_empty() {
        return Err(MetaError::Config(
            "add_learner requires non-empty node_id".to_string(),
        ));
    }

    let address = normalize_address(request.address.as_str(), "add_learner address")?;
    let raft_id = node_name_to_raft_id(node_name);

    Ok(node
        .raft
        .add_learner(raft_id, BasicNode::new(address), request.blocking)
        .await)
}

pub async fn handle_global_client_write(
    request: MetaWriteRequest,
) -> Result<MetaClientWriteResult> {
    let node = get_global_node()?;
    Ok(node.raft.client_write(request).await)
}

#[derive(Clone)]
pub struct MetaKv {
    namespace: String,
    raft: MetaRaft,
    state_machine: Arc<MetaStateMachineStore>,
    local_node: Arc<RwLock<LocalNodeMeta>>,
    local_raft_id: MetaNodeId,
    seeds: Vec<String>,
    client: reqwest::Client,
}

impl MetaKv {
    pub async fn new(options: MetaKvOptions) -> Result<Self> {
        validate_transport(options.transport.as_deref())?;

        let namespace = options.namespace.trim().to_string();
        if namespace.is_empty() {
            return Err(MetaError::Config(
                "registry namespace cannot be empty".to_string(),
            ));
        }

        let node_id = options.node_id.trim().to_string();
        if node_id.is_empty() {
            return Err(MetaError::Config(
                "gossip node_id cannot be empty".to_string(),
            ));
        }

        let bind_addr = normalize_address(options.bind_addr.as_str(), "gossip bind_addr")?;
        let advertise_addr = match options.advertise_addr.as_deref() {
            Some(raw) if !raw.trim().is_empty() => normalize_address(raw, "gossip advertise_addr")?,
            _ => bind_addr.clone(),
        };

        let seeds = normalize_seed_list(options.seeds)?;
        let local_raft_id = node_name_to_raft_id(node_id.as_str());

        let db_path = metakv_db_path(namespace.as_str(), node_id.as_str(), bind_addr.as_str());
        let state_machine = Arc::new(MetaStateMachineStore::new(db_path)?);
        let log_store = MetaLogStore::<MetaTypeConfig>::default();
        let network = MetaNetwork::new();

        let raft_config = Arc::new(
            RaftConfig {
                cluster_name: format!("rimio-meta-{}", namespace),
                heartbeat_interval: 500,
                election_timeout_min: 1500,
                election_timeout_max: 3000,
                ..Default::default()
            }
            .validate()
            .map_err(|error| MetaError::Config(format!("invalid raft config: {}", error)))?,
        );

        let raft = Raft::new(
            local_raft_id,
            raft_config,
            network,
            log_store,
            state_machine.clone(),
        )
        .await
        .map_err(|error| MetaError::Internal(format!("failed to start openraft: {}", error)))?;

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        let metakv = Self {
            namespace,
            raft: raft.clone(),
            state_machine,
            local_node: Arc::new(RwLock::new(LocalNodeMeta {
                node_id,
                address: advertise_addr,
            })),
            local_raft_id,
            seeds: seeds.clone(),
            client,
        };

        install_global_node(Arc::new(GlobalMetaNode { raft }));

        metakv.bootstrap_or_join(seeds).await?;
        Ok(metakv)
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub async fn set_local_node(&self, node_id: &str, address: &str) -> Result<()> {
        let mut local = self.local_node.write().await;

        if local.node_id != node_id {
            return Err(MetaError::Config(format!(
                "set_local_node mismatch: expected '{}', got '{}'",
                local.node_id, node_id
            )));
        }

        local.address = normalize_address(address, "node address")?;
        Ok(())
    }

    pub async fn sync_once(&self) -> Result<()> {
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(())
    }

    pub async fn members(&self) -> Result<Vec<MetaMember>> {
        let active = self.active_raft_member_ids();
        let mut members = Vec::new();

        for (_key, value) in self.list_prefix("nodes/").await? {
            let record: NodeRecord = match serde_json::from_slice(&value) {
                Ok(record) => record,
                Err(_error) => continue,
            };

            let raft_id = node_name_to_raft_id(record.node_id.as_str());
            let state = if active.contains(&raft_id) {
                MetaMemberState::Alive
            } else {
                MetaMemberState::Other
            };

            members.push(MetaMember {
                node_id: record.node_id,
                namespace: if record.group_id.trim().is_empty() {
                    self.namespace.clone()
                } else {
                    record.group_id
                },
                address: record.address,
                state,
            });
        }

        members.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        members.dedup_by(|left, right| left.node_id == right.node_id);
        Ok(members)
    }

    pub async fn put(&self, key: &str, value: &[u8]) -> Result<()> {
        self.client_write(MetaWriteRequest::Put {
            key: key.to_string(),
            value: value.to_vec(),
        })
        .await
        .map(|_response| ())
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.state_machine.get_value(key)
    }

    pub async fn list_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        self.state_machine.list_prefix(prefix)
    }

    pub async fn put_if_absent(&self, key: &str, value: &[u8]) -> Result<bool> {
        self.client_write(MetaWriteRequest::PutIfAbsent {
            key: key.to_string(),
            value: value.to_vec(),
        })
        .await
        .map(|response| response.created)
    }

    async fn bootstrap_or_join(&self, seeds: Vec<String>) -> Result<()> {
        if seeds.is_empty() {
            self.initialize_single_node().await?;
            return Ok(());
        }

        self.join_existing_cluster(seeds).await
    }

    async fn initialize_single_node(&self) -> Result<()> {
        let local_addr = {
            let local = self.local_node.read().await;
            local.address.clone()
        };

        let members = BTreeMap::from([(self.local_raft_id, BasicNode::new(local_addr))]);

        match self.raft.initialize(members).await {
            Ok(()) => {
                tracing::info!("metakv raft initialized as single-node cluster");
                Ok(())
            }
            Err(openraft::error::RaftError::APIError(
                openraft::error::InitializeError::NotAllowed(_),
            )) => {
                tracing::info!("metakv raft already initialized, skipping initialize");
                Ok(())
            }
            Err(error) => Err(MetaError::Internal(format!(
                "failed to initialize metakv raft: {}",
                error
            ))),
        }
    }

    async fn join_existing_cluster(&self, seeds: Vec<String>) -> Result<()> {
        let request = {
            let local = self.local_node.read().await;
            MetaAddLearnerRequest {
                node_id: local.node_id.clone(),
                address: local.address.clone(),
                blocking: false,
            }
        };

        let mut candidates = seeds;
        let mut last_error = String::new();

        for _attempt in 0..20 {
            let mut leader_hint: Option<String> = None;

            for candidate in candidates.clone() {
                match self
                    .send_remote_add_learner(candidate.as_str(), &request)
                    .await
                {
                    Ok(()) => {
                        tracing::info!(target = %candidate, "metakv raft joined cluster");
                        return Ok(());
                    }
                    Err(AddLearnerError::ForwardToLeader(Some(addr))) => {
                        leader_hint = Some(addr);
                    }
                    Err(error) => {
                        last_error = error.to_string();
                    }
                }
            }

            if let Some(leader) = leader_hint {
                if !candidates.iter().any(|candidate| candidate == &leader) {
                    candidates.insert(0, leader);
                }
            }

            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        Err(MetaError::Internal(format!(
            "failed to join metakv raft via seeds: {}",
            if last_error.is_empty() {
                "unknown error"
            } else {
                last_error.as_str()
            }
        )))
    }

    fn active_raft_member_ids(&self) -> HashSet<MetaNodeId> {
        let metrics = self.raft.metrics();
        let snapshot = metrics.borrow().clone();

        snapshot
            .membership_config
            .membership()
            .nodes()
            .map(|(id, _node)| *id)
            .collect()
    }

    async fn client_write(&self, request: MetaWriteRequest) -> Result<MetaWriteResponse> {
        let mut last_forward: Option<ForwardToLeader<MetaNodeId, BasicNode>> = None;

        for _attempt in 0..10 {
            match self.raft.client_write(request.clone()).await {
                Ok(response) => return Ok(response.data),
                Err(openraft::error::RaftError::APIError(
                    MetaClientWriteError::ForwardToLeader(forward),
                )) => {
                    last_forward = Some(forward);
                    tokio::time::sleep(Duration::from_millis(120)).await;
                }
                Err(error) => {
                    return Err(MetaError::Internal(format!(
                        "meta raft client_write failed: {}",
                        error
                    )));
                }
            }
        }

        let forward = last_forward.unwrap_or_else(ForwardToLeader::empty);
        self.forward_client_write(request, forward).await
    }

    async fn forward_client_write(
        &self,
        request: MetaWriteRequest,
        forward: ForwardToLeader<MetaNodeId, BasicNode>,
    ) -> Result<MetaWriteResponse> {
        let mut candidates = self.write_forward_candidates(Some(&forward)).await;
        if candidates.is_empty() {
            candidates = self.write_forward_candidates(None).await;
        }

        let mut visited = HashSet::new();
        let mut last_error = String::new();

        for _round in 0..8 {
            let mut next_leader_hint: Option<String> = None;

            for candidate in candidates.clone() {
                if !visited.insert(candidate.clone()) {
                    continue;
                }

                match self
                    .send_remote_client_write(candidate.as_str(), &request)
                    .await
                {
                    Ok(response) => return Ok(response),
                    Err(WriteForwardError::ForwardToLeader(Some(addr))) => {
                        next_leader_hint = Some(addr);
                    }
                    Err(error) => {
                        last_error = error.to_string();
                    }
                }
            }

            let mut refreshed = self.write_forward_candidates(None).await;
            if let Some(addr) = next_leader_hint {
                refreshed.insert(0, addr);
            }

            refreshed.retain(|candidate| !visited.contains(candidate));
            if refreshed.is_empty() {
                break;
            }

            candidates = refreshed;
        }

        Err(MetaError::Internal(format!(
            "failed to forward meta write to leader: {}",
            if last_error.is_empty() {
                "no reachable leader"
            } else {
                last_error.as_str()
            }
        )))
    }

    async fn write_forward_candidates(
        &self,
        hint: Option<&ForwardToLeader<MetaNodeId, BasicNode>>,
    ) -> Vec<String> {
        let mut candidates = Vec::new();

        if let Some(hint) = hint {
            if let Some(node) = &hint.leader_node {
                candidates.push(node.addr.clone());
            }

            if let Some(leader_id) = hint.leader_id
                && let Some(addr) = self.lookup_node_addr_from_metrics(leader_id)
            {
                candidates.push(addr);
            }
        }

        if let Some(addr) = self.current_leader_addr_from_metrics() {
            candidates.push(addr);
        }

        candidates.extend(self.seeds.clone());

        let local_addr = self.local_node.read().await.address.clone();
        candidates.retain(|candidate| candidate != &local_addr);

        let mut unique = Vec::new();
        let mut seen = HashSet::new();
        for candidate in candidates {
            if seen.insert(candidate.clone()) {
                unique.push(candidate);
            }
        }

        unique
    }

    fn current_leader_addr_from_metrics(&self) -> Option<String> {
        let metrics = self.raft.metrics();
        let snapshot = metrics.borrow().clone();
        let leader = snapshot.current_leader?;

        snapshot
            .membership_config
            .membership()
            .get_node(&leader)
            .map(|node| node.addr.clone())
    }

    fn lookup_node_addr_from_metrics(&self, id: MetaNodeId) -> Option<String> {
        let metrics = self.raft.metrics();
        let snapshot = metrics.borrow().clone();

        snapshot
            .membership_config
            .membership()
            .get_node(&id)
            .map(|node| node.addr.clone())
    }

    async fn send_remote_add_learner(
        &self,
        target: &str,
        request: &MetaAddLearnerRequest,
    ) -> std::result::Result<(), AddLearnerError> {
        let payload: MetaAddLearnerResult =
            post_json(&self.client, target, URI_ADD_LEARNER, request)
                .await
                .map_err(AddLearnerError::Http)?;

        match payload {
            Ok(_response) => Ok(()),
            Err(MetaRaftError::APIError(MetaClientWriteError::ForwardToLeader(forward))) => Err(
                AddLearnerError::ForwardToLeader(forward.leader_node.map(|node| node.addr)),
            ),
            Err(error) => Err(AddLearnerError::Remote(error.to_string())),
        }
    }

    async fn send_remote_client_write(
        &self,
        target: &str,
        request: &MetaWriteRequest,
    ) -> std::result::Result<MetaWriteResponse, WriteForwardError> {
        let payload: MetaClientWriteResult =
            post_json(&self.client, target, URI_CLIENT_WRITE, request)
                .await
                .map_err(WriteForwardError::Http)?;

        match payload {
            Ok(response) => Ok(response.data),
            Err(MetaRaftError::APIError(MetaClientWriteError::ForwardToLeader(forward))) => Err(
                WriteForwardError::ForwardToLeader(forward.leader_node.map(|node| node.addr)),
            ),
            Err(error) => Err(WriteForwardError::Remote(error.to_string())),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum AddLearnerError {
    #[error("{0}")]
    Http(MetaError),

    #[error("forward to leader: {0:?}")]
    ForwardToLeader(Option<String>),

    #[error("{0}")]
    Remote(String),
}

#[derive(Debug, thiserror::Error)]
enum WriteForwardError {
    #[error("{0}")]
    Http(MetaError),

    #[error("forward to leader: {0:?}")]
    ForwardToLeader(Option<String>),

    #[error("{0}")]
    Remote(String),
}
