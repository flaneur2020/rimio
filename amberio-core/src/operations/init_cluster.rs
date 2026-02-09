use crate::{ClusterInitRequest, ClusterInitResult, ClusterManager, RegistryBuilder, Result};

#[derive(Clone)]
pub struct InitClusterOperation {
    cluster_manager: ClusterManager,
}

impl InitClusterOperation {
    pub fn new(registry_builder: RegistryBuilder) -> Self {
        Self {
            cluster_manager: ClusterManager::new(registry_builder),
        }
    }

    pub async fn run(&self, request: ClusterInitRequest) -> Result<ClusterInitResult> {
        self.cluster_manager.init_if_needed(request).await
    }
}
