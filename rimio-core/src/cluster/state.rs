use super::types::{
    ClusterInitRequest, ClusterInitResult, ClusterInitScanConfig, ClusterInitScanEntry,
    ClusterNodeConfig, ClusterState,
};
use crate::{
    ArchiveStore, BlobMeta, MetadataStore, PartIndexState, RedisArchiveStore, RegistryBuilder,
    Result, RimError, SlotInfo, SlotManager, slot_for_key,
};
use chrono::Utc;
use ulid::Ulid;

#[derive(Clone)]
pub struct ClusterManager {
    registry_builder: RegistryBuilder,
}

impl ClusterManager {
    pub fn new(registry_builder: RegistryBuilder) -> Self {
        Self { registry_builder }
    }

    pub async fn init_if_needed(&self, request: ClusterInitRequest) -> Result<ClusterInitResult> {
        let bootstrap_store = self.registry_builder.build().await?;

        if let Some(existing) = bootstrap_store.get_bootstrap_state().await? {
            let state = decode_cluster_state(&existing)?;
            ensure_local_layout(&request.current_node, &state)?;
            return Ok(ClusterInitResult {
                bootstrap_state: state,
                won_bootstrap_race: false,
            });
        }

        let proposed = ClusterState {
            initialized_at: Utc::now().to_rfc3339(),
            current_node: request.current_node.clone(),
            nodes: request.nodes.clone(),
            replication: request.replication.clone(),
            archive: request.archive.clone(),
            initialized_by: request.current_node.clone(),
        };

        let payload = serde_json::to_vec(&proposed).map_err(|error| {
            RimError::Internal(format!("Failed to encode bootstrap state: {}", error))
        })?;

        let won_bootstrap_race = bootstrap_store
            .set_bootstrap_state_if_absent(&payload)
            .await?;

        let active_bytes = bootstrap_store
            .get_bootstrap_state()
            .await?
            .ok_or_else(|| {
                RimError::Internal(
                    "Bootstrap state is missing after initialization attempt".to_string(),
                )
            })?;

        let state = decode_cluster_state(&active_bytes)?;
        ensure_local_layout(&request.current_node, &state)?;

        if won_bootstrap_race {
            ensure_slot_assignments(&bootstrap_store, &state).await?;
            run_optional_init_scan(&request.current_node, &request.init_scan, &state).await?;
        }

        Ok(ClusterInitResult {
            bootstrap_state: state,
            won_bootstrap_race,
        })
    }
}

async fn ensure_slot_assignments(
    registry: &std::sync::Arc<dyn crate::Registry>,
    state: &ClusterState,
) -> Result<()> {
    let mut existing = registry.get_all_slots().await?;
    let total_slots = state.replication.total_slots;

    if existing.len() >= total_slots as usize {
        return Ok(());
    }

    let mut node_ids: Vec<String> = state
        .nodes
        .iter()
        .map(|node| node.node_id.clone())
        .collect();
    node_ids.sort();
    node_ids.dedup();

    if node_ids.is_empty() {
        return Err(RimError::Config(
            "initial cluster has no nodes for slot assignment".to_string(),
        ));
    }

    let replica_count = node_ids.len().min(3).max(1);
    let mut created = 0usize;

    for slot_id in 0..total_slots {
        if existing.contains_key(&slot_id) {
            continue;
        }

        let start = (slot_id as usize) % node_ids.len();
        let mut replicas = Vec::with_capacity(replica_count);
        for offset in 0..replica_count {
            replicas.push(node_ids[(start + offset) % node_ids.len()].clone());
        }

        let primary = replicas.first().cloned().ok_or_else(|| {
            RimError::Internal(format!(
                "slot {} assignment produced empty replica list",
                slot_id
            ))
        })?;

        let slot = SlotInfo {
            slot_id,
            replicas,
            primary,
            latest_seq: Ulid::new().to_string(),
        };

        registry.set_slot(&slot).await?;
        existing.insert(slot_id, slot);
        created += 1;
    }

    if created > 0 {
        tracing::info!(
            "initialized slot assignments in registry: created={} total_slots={} replica_count={}",
            created,
            total_slots,
            replica_count
        );
    }

    Ok(())
}

fn decode_cluster_state(payload: &[u8]) -> Result<ClusterState> {
    serde_json::from_slice(payload).map_err(|error| {
        RimError::Internal(format!(
            "Failed to decode bootstrap state from registry: {}",
            error
        ))
    })
}

fn ensure_local_layout(current_node: &str, state: &ClusterState) -> Result<ClusterNodeConfig> {
    let node = state
        .nodes
        .iter()
        .find(|node| node.node_id == current_node)
        .ok_or_else(|| {
            RimError::Config(format!(
                "current_node '{}' not found in initialized cluster",
                current_node
            ))
        })?
        .clone();

    for disk in &node.disks {
        let rimio_dir = disk.path.join("rimio");
        std::fs::create_dir_all(&rimio_dir)?;
        tracing::info!("Ensured directory exists: {:?}", rimio_dir);
    }

    Ok(node)
}

async fn run_optional_init_scan(
    current_node: &str,
    init_scan: &Option<ClusterInitScanConfig>,
    state: &ClusterState,
) -> Result<()> {
    let init_scan = match init_scan {
        Some(scan) if scan.enabled => scan,
        _ => return Ok(()),
    };

    let redis = &init_scan.redis;

    let node = state
        .nodes
        .iter()
        .find(|node| node.node_id == current_node)
        .ok_or_else(|| {
            RimError::Config(format!(
                "current_node '{}' not found in initialized cluster",
                current_node
            ))
        })?;

    let data_dir = node
        .disks
        .first()
        .map(|disk| disk.path.clone())
        .ok_or_else(|| RimError::Config("current node has no configured disks".to_string()))?;

    let slot_manager = SlotManager::new(current_node.to_string(), data_dir)?;

    let archive_store: Box<dyn ArchiveStore> =
        Box::new(RedisArchiveStore::new(redis.url.as_str())?);

    let page_size = redis.page_size.max(1);
    let mut cursor: Option<String> = None;
    let mut imported = 0usize;

    loop {
        let page = archive_store
            .list_blobs_page(&redis.list_key, cursor.as_deref(), page_size)
            .await?;

        if page.entries.is_empty() {
            if imported == 0 {
                tracing::info!(
                    "init_scan enabled but archive list '{}' is empty",
                    redis.list_key
                );
            }
            break;
        }

        for raw in page.entries {
            let entry: ClusterInitScanEntry = serde_json::from_str(&raw).map_err(|error| {
                RimError::Config(format!("invalid init_scan entry JSON: {} ({})", raw, error))
            })?;

            let normalized_path = normalize_blob_path(&entry.path)?;
            let slot_id = slot_for_key(&normalized_path, state.replication.total_slots);

            if !slot_manager.has_slot(slot_id).await {
                slot_manager.init_slot(slot_id).await?;
            }

            let slot = slot_manager.get_slot(slot_id).await?;
            let metadata_store = MetadataStore::new(slot)?;
            let generation = metadata_store.next_generation(&normalized_path)?;

            let part_size = entry.part_size.max(1);
            let part_count = if entry.size_bytes == 0 {
                0
            } else {
                entry.size_bytes.div_ceil(part_size) as u32
            };

            let updated_at = entry
                .updated_at
                .as_deref()
                .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
                .map(|value| value.with_timezone(&Utc))
                .unwrap_or_else(Utc::now);

            let meta = BlobMeta {
                path: normalized_path.clone(),
                slot_id,
                generation,
                version: generation,
                size_bytes: entry.size_bytes,
                etag: entry.etag.clone(),
                part_size,
                part_count,
                part_index_state: PartIndexState::None,
                archive_url: Some(entry.archive_url.clone()),
                updated_at,
            };

            let applied = metadata_store.upsert_meta(&meta)?;
            if applied {
                imported += 1;
            }

            tracing::info!(
                "init_scan imported path={} slot={} generation={} applied={}",
                normalized_path,
                slot_id,
                generation,
                applied
            );
        }

        match page.next_cursor {
            Some(next_cursor) => {
                if cursor.as_deref() == Some(next_cursor.as_str()) {
                    break;
                }
                cursor = Some(next_cursor);
            }
            None => break,
        }
    }

    tracing::info!("init_scan imported {} objects", imported);
    Ok(())
}

fn normalize_blob_path(path: &str) -> Result<String> {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return Err(RimError::InvalidRequest(
            "blob path cannot be empty".to_string(),
        ));
    }

    let mut components = Vec::new();
    for component in trimmed.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(RimError::InvalidRequest(format!(
                "invalid blob path component: {}",
                component
            )));
        }
        components.push(component);
    }

    Ok(components.join("/"))
}
