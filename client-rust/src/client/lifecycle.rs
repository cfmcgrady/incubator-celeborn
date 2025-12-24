// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Lifecycle manager for Celeborn client.
//!
//! Manages shuffle registration, heartbeats, and partition locations.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::config::CelebornConfig;
use crate::error::{CelebornError, Result, StatusCode};
use crate::network::TransportClient;
use crate::protocol::transport::*;
use crate::protocol::{PartitionLocation, PartitionMode, StorageInfo, WorkerInfo};

/// Shuffle state information.
#[derive(Debug)]
struct ShuffleState {
    /// Number of mappers
    num_mappers: i32,
    /// Number of partitions
    num_partitions: i32,
    /// Partition locations (partition_id -> locations)
    partition_locations: DashMap<i32, Vec<PartitionLocation>>,
    /// Whether the shuffle is registered
    registered: AtomicBool,
}

impl ShuffleState {
    fn new(num_mappers: i32, num_partitions: i32) -> Self {
        Self {
            num_mappers,
            num_partitions,
            partition_locations: DashMap::new(),
            registered: AtomicBool::new(false),
        }
    }
}

/// Lifecycle manager for managing shuffle lifecycle.
pub struct LifecycleManager {
    /// Configuration
    config: Arc<CelebornConfig>,
    /// Transport client
    transport_client: Arc<TransportClient>,
    /// Registered shuffles
    shuffles: DashMap<i32, Arc<ShuffleState>>,
    /// Total bytes written
    total_written: AtomicI64,
    /// Total file count
    file_count: AtomicI64,
    /// Heartbeat task handle
    heartbeat_handle: RwLock<Option<JoinHandle<()>>>,
    /// Whether the manager is running
    running: AtomicBool,
    /// Excluded workers
    excluded_workers: DashMap<String, WorkerInfo>,
}

impl LifecycleManager {
    /// Create a new lifecycle manager.
    pub fn new(config: Arc<CelebornConfig>, transport_client: Arc<TransportClient>) -> Self {
        Self {
            config,
            transport_client,
            shuffles: DashMap::new(),
            total_written: AtomicI64::new(0),
            file_count: AtomicI64::new(0),
            heartbeat_handle: RwLock::new(None),
            running: AtomicBool::new(true),
            excluded_workers: DashMap::new(),
        }
    }

    /// Start the heartbeat task.
    pub async fn start_heartbeat(&self) {
        let transport_client = self.transport_client.clone();
        let app_id = self.config.app_id.clone();
        let interval = self.config.heartbeat_interval;
        
        // Use Arc to share atomic values safely across threads
        let total_written = Arc::new(AtomicI64::new(0));
        let file_count = Arc::new(AtomicI64::new(0));
        let running = Arc::new(AtomicBool::new(true));
        
        let total_written_clone = total_written.clone();
        let file_count_clone = file_count.clone();
        let running_clone = running.clone();

        let handle = tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);

            while running_clone.load(Ordering::Relaxed) {
                interval_timer.tick().await;

                let request = PbHeartbeatFromApplication {
                    app_id: app_id.clone(),
                    total_written: total_written_clone.load(Ordering::Relaxed),
                    file_count: file_count_clone.load(Ordering::Relaxed),
                    request_id: Uuid::new_v4().to_string(),
                    need_checked_worker_list: vec![],
                    should_response: false,
                };

                match transport_client
                    .send_to_master::<_, PbHeartbeatFromApplicationResponse>(
                        TransportMessageType::HeartbeatFromApplication,
                        &request,
                    )
                    .await
                {
                    Ok(response) => {
                        debug!("Heartbeat sent successfully, status: {}", response.status);
                    }
                    Err(e) => {
                        warn!("Failed to send heartbeat: {}", e);
                    }
                }
            }
        });

        *self.heartbeat_handle.write().await = Some(handle);
    }

    /// Register a new shuffle.
    pub async fn register_shuffle(
        &self,
        shuffle_id: i32,
        num_mappers: i32,
        num_partitions: i32,
    ) -> Result<i32> {
        // Check if already registered
        if self.shuffles.contains_key(&shuffle_id) {
            return Ok(shuffle_id);
        }

        info!(
            "Registering shuffle {} with {} mappers and {} partitions",
            shuffle_id, num_mappers, num_partitions
        );

        let request = PbRegisterShuffle {
            shuffle_id,
            num_mappers,
            num_partitions,
        };

        let response: PbRegisterShuffleResponse = self
            .transport_client
            .send_to_master(TransportMessageType::RegisterShuffle, &request)
            .await?;

        let status = StatusCode::from(response.status);
        if !status.is_success() && status != StatusCode::ShuffleAlreadyRegistered {
            return Err(CelebornError::ServerError {
                status,
                message: format!("Failed to register shuffle {}", shuffle_id),
            });
        }

        // Create shuffle state
        let state = Arc::new(ShuffleState::new(num_mappers, num_partitions));

        // Store partition locations
        for pb_location in response.partition_locations {
            let location = self.convert_partition_location(&pb_location);
            state
                .partition_locations
                .entry(location.id)
                .or_insert_with(Vec::new)
                .push(location);
        }

        state.registered.store(true, Ordering::Relaxed);
        self.shuffles.insert(shuffle_id, state);

        info!("Shuffle {} registered successfully", shuffle_id);
        Ok(shuffle_id)
    }

    /// Get partition location for a shuffle.
    pub fn get_partition_location(
        &self,
        shuffle_id: i32,
        partition_id: i32,
    ) -> Result<Vec<PartitionLocation>> {
        let state = self.shuffles.get(&shuffle_id).ok_or_else(|| {
            CelebornError::ShuffleNotFound(shuffle_id)
        })?;

        state
            .partition_locations
            .get(&partition_id)
            .map(|v| v.clone())
            .ok_or_else(|| CelebornError::PartitionNotFound {
                shuffle_id,
                partition_id,
            })
    }

    /// Update partition location.
    pub fn update_partition_location(
        &self,
        shuffle_id: i32,
        partition_id: i32,
        location: PartitionLocation,
    ) {
        if let Some(state) = self.shuffles.get(&shuffle_id) {
            state
                .partition_locations
                .entry(partition_id)
                .or_insert_with(Vec::new)
                .push(location);
        }
    }

    /// Signal that a mapper has finished.
    pub async fn mapper_end(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
    ) -> Result<()> {
        debug!(
            "Mapper end: shuffle={}, map={}, attempt={}, num_mappers={}",
            shuffle_id, map_id, attempt_id, num_mappers
        );

        let request = PbMapperEnd {
            shuffle_id,
            map_id,
            attempt_id,
            num_mappers,
            partition_id: -1, // Not used for reduce partition mode
        };

        let response: PbMapperEndResponse = self
            .transport_client
            .send_to_master(TransportMessageType::MapperEnd, &request)
            .await?;

        let status = StatusCode::from(response.status);
        if !status.is_success() {
            return Err(CelebornError::ServerError {
                status,
                message: format!("Mapper end failed for shuffle {}", shuffle_id),
            });
        }

        Ok(())
    }

    /// Get reducer file groups.
    pub async fn get_reducer_file_group(
        &self,
        shuffle_id: i32,
    ) -> Result<HashMap<i32, Vec<PartitionLocation>>> {
        let request = PbGetReducerFileGroup { shuffle_id };

        let response: PbGetReducerFileGroupResponse = self
            .transport_client
            .send_to_master(TransportMessageType::GetReducerFileGroup, &request)
            .await?;

        let status = StatusCode::from(response.status);
        if !status.is_success() {
            return Err(CelebornError::ServerError {
                status,
                message: format!("Failed to get reducer file group for shuffle {}", shuffle_id),
            });
        }

        let mut result = HashMap::new();
        for (partition_id, file_group) in response.file_groups {
            let locations: Vec<PartitionLocation> = file_group
                .locations
                .iter()
                .map(|loc| self.convert_partition_location(loc))
                .collect();
            result.insert(partition_id, locations);
        }

        Ok(result)
    }

    /// Unregister a shuffle.
    pub async fn unregister_shuffle(&self, shuffle_id: i32) -> Result<()> {
        info!("Unregistering shuffle {}", shuffle_id);

        let request = PbUnregisterShuffle {
            app_id: self.config.app_id.clone(),
            shuffle_id,
            request_id: Uuid::new_v4().to_string(),
        };

        let response: PbUnregisterShuffleResponse = self
            .transport_client
            .send_to_master(TransportMessageType::UnregisterShuffle, &request)
            .await?;

        let status = StatusCode::from(response.status);
        if !status.is_success() {
            warn!(
                "Failed to unregister shuffle {}: {:?}",
                shuffle_id, status
            );
        }

        self.shuffles.remove(&shuffle_id);
        Ok(())
    }

    /// Revive a partition (request new location after failure).
    pub async fn revive_partition(
        &self,
        shuffle_id: i32,
        partition_id: i32,
        epoch: i32,
        old_location: Option<&PartitionLocation>,
    ) -> Result<PartitionLocation> {
        debug!(
            "Reviving partition: shuffle={}, partition={}, epoch={}",
            shuffle_id, partition_id, epoch
        );

        let partition_info = PbRevivePartitionInfo {
            partition_id,
            epoch,
            partition: old_location.map(|loc| self.convert_to_pb_location(loc)),
            status: StatusCode::Success as i32,
        };

        let request = PbRevive {
            shuffle_id,
            map_id: vec![],
            partition_info: vec![partition_info],
        };

        let response: PbChangeLocationResponse = self
            .transport_client
            .send_to_master(TransportMessageType::ChangeLocation, &request)
            .await?;

        // Find the new location for our partition
        for info in response.partition_info {
            if info.partition_id == partition_id {
                if let Some(pb_loc) = info.partition {
                    let location = self.convert_partition_location(&pb_loc);
                    self.update_partition_location(shuffle_id, partition_id, location.clone());
                    return Ok(location);
                }
            }
        }

        Err(CelebornError::PartitionNotFound {
            shuffle_id,
            partition_id,
        })
    }

    /// Add bytes written.
    pub fn add_bytes_written(&self, bytes: i64) {
        self.total_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Add file count.
    pub fn add_file_count(&self, count: i64) {
        self.file_count.fetch_add(count, Ordering::Relaxed);
    }

    /// Check if a worker is excluded.
    pub fn is_worker_excluded(&self, worker: &WorkerInfo) -> bool {
        self.excluded_workers.contains_key(&worker.to_unique_id())
    }

    /// Stop the lifecycle manager.
    pub async fn stop(&self) {
        info!("Stopping lifecycle manager");
        self.running.store(false, Ordering::Relaxed);

        // Cancel heartbeat task
        if let Some(handle) = self.heartbeat_handle.write().await.take() {
            handle.abort();
        }

        // Unregister all shuffles
        let shuffle_ids: Vec<i32> = self.shuffles.iter().map(|e| *e.key()).collect();
        for shuffle_id in shuffle_ids {
            if let Err(e) = self.unregister_shuffle(shuffle_id).await {
                warn!("Failed to unregister shuffle {}: {}", shuffle_id, e);
            }
        }

        self.transport_client.close();
    }

    /// Convert protobuf partition location to our type.
    fn convert_partition_location(&self, pb: &PbPartitionLocation) -> PartitionLocation {
        let mut location = PartitionLocation::new(
            pb.id,
            pb.epoch,
            pb.host.clone(),
            pb.rpc_port,
            pb.push_port,
            pb.fetch_port,
            pb.replicate_port,
        );

        location.mode = PartitionMode::from(pb.mode);

        if let Some(ref storage) = pb.storage_info {
            location.storage_info = Some(StorageInfo {
                storage_type: storage.r#type,
                mount_point: storage.mount_point.clone(),
                final_result: storage.final_result,
                file_path: storage.file_path.clone(),
                available_storage_types: storage.available_storage_types,
                file_size: storage.file_size,
                chunk_offsets: storage.chunk_offsets.clone(),
            });
        }

        if let Some(ref peer) = pb.peer {
            location.peer = Some(Box::new(self.convert_partition_location(peer)));
        }

        location
    }

    /// Convert our partition location to protobuf.
    fn convert_to_pb_location(&self, loc: &PartitionLocation) -> PbPartitionLocation {
        PbPartitionLocation {
            mode: loc.mode as i32,
            id: loc.id,
            epoch: loc.epoch,
            host: loc.host.clone(),
            rpc_port: loc.rpc_port,
            push_port: loc.push_port,
            fetch_port: loc.fetch_port,
            replicate_port: loc.replicate_port,
            peer: loc.peer.as_ref().map(|p| Box::new(self.convert_to_pb_location(p))),
            storage_info: loc.storage_info.as_ref().map(|s| PbStorageInfo {
                r#type: s.storage_type,
                mount_point: s.mount_point.clone(),
                final_result: s.final_result,
                file_path: s.file_path.clone(),
                available_storage_types: s.available_storage_types,
                file_size: s.file_size,
                chunk_offsets: s.chunk_offsets.clone(),
            }),
            map_id_bitmap: vec![],
        }
    }

    /// Get the shuffle key.
    pub fn shuffle_key(&self, shuffle_id: i32) -> String {
        format!("{}-{}", self.config.app_id, shuffle_id)
    }

    /// Get the configuration.
    pub fn config(&self) -> &CelebornConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shuffle_key() {
        let config = Arc::new(
            CelebornConfig::builder()
                .app_id("test-app")
                .master_endpoints(vec!["localhost:9097".to_string()])
                .build()
                .unwrap(),
        );
        let transport = Arc::new(TransportClient::new(config.clone()).unwrap());
        let manager = LifecycleManager::new(config, transport);

        assert_eq!(manager.shuffle_key(1), "test-app-1");
        assert_eq!(manager.shuffle_key(42), "test-app-42");
    }
}
