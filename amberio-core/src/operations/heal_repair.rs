use crate::operations::read_blob::ReadBlobOperation;
use crate::{AmberError, NodeInfo, Result};
use std::sync::Arc;

#[derive(Clone)]
pub struct HealRepairOperation {
    read_blob_operation: Arc<ReadBlobOperation>,
}

#[derive(Debug, Clone)]
pub struct HealRepairOperationRequest {
    pub slot_id: u16,
    pub source: NodeInfo,
    pub blob_paths: Vec<String>,
    pub dry_run: bool,
}

#[derive(Debug, Clone)]
pub struct HealRepairOperationResult {
    pub repaired_objects: usize,
    pub skipped_objects: usize,
    pub errors: Vec<String>,
}

impl HealRepairOperation {
    pub fn new(read_blob_operation: Arc<ReadBlobOperation>) -> Self {
        Self {
            read_blob_operation,
        }
    }

    pub async fn run(
        &self,
        request: HealRepairOperationRequest,
    ) -> Result<HealRepairOperationResult> {
        let HealRepairOperationRequest {
            slot_id,
            source,
            blob_paths,
            dry_run,
        } = request;

        let mut repaired_objects = 0usize;
        let mut skipped_objects = 0usize;
        let mut errors = Vec::new();

        for raw_path in blob_paths {
            let path = match normalize_blob_path(&raw_path) {
                Ok(path) => path,
                Err(error) => {
                    skipped_objects += 1;
                    errors.push(format!("{}: {}", raw_path, error));
                    continue;
                }
            };

            if dry_run {
                skipped_objects += 1;
                continue;
            }

            let remote_head = match self
                .read_blob_operation
                .fetch_remote_head(&source.address, slot_id, &path)
                .await
            {
                Ok(Some(head)) => head,
                Ok(None) => {
                    skipped_objects += 1;
                    errors.push(format!("{}: source has no head", path));
                    continue;
                }
                Err(error) => {
                    skipped_objects += 1;
                    errors.push(format!("{}: {}", path, error));
                    continue;
                }
            };

            match self
                .read_blob_operation
                .repair_path_from_head(&source, slot_id, &path, &remote_head)
                .await
            {
                Ok(_) => repaired_objects += 1,
                Err(error) => {
                    skipped_objects += 1;
                    errors.push(format!("{}: {}", path, error));
                }
            }
        }

        Ok(HealRepairOperationResult {
            repaired_objects,
            skipped_objects,
            errors,
        })
    }
}

fn normalize_blob_path(path: &str) -> Result<String> {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return Err(AmberError::InvalidRequest(
            "blob path cannot be empty".to_string(),
        ));
    }

    let mut components = Vec::new();
    for component in trimmed.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(AmberError::InvalidRequest(format!(
                "invalid blob path component: {}",
                component
            )));
        }
        components.push(component);
    }

    Ok(components.join("/"))
}
