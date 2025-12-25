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

//! Revive manager for handling partition recovery after push failures.
//!
//! When a push operation fails (e.g., worker is down), the ReviveManager
//! batches revive requests and sends them to the Master to get new partition
//! locations.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use crate::error::{CelebornError, Result, StatusCode};
use crate::protocol::generated::{
    PbChangeLocationResponse, PbPartitionLocation, PbRevive, PbRevivePartitionInfo,
};
use crate::protocol::{PartitionLocation, TransportMessageType};
use crate::network::TransportClient;

/// A request to revive a partition.
#[derive(Debug)]
pub struct ReviveRequest {
    /// Shuffle ID
    pub shuffle_id: i32,
    /// Map ID
    pub map_id: i32,
    /// Attempt ID
    pub attempt_id: i32,
    /// Partition ID
    pub partition_id: i32,
    /// Current epoch
    pub epoch: i32,
    /// Old partition location (if available)
    pub old_location: Option<PartitionLocation>,
    /// Cause of the revive request
    pub cause: StatusCode,
    /// Revive status (set after revive completes)
    pub revive_status: AtomicI32,
}

impl ReviveRequest {
    /// Create a new revive request.
    pub fn new(
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        partition_id: i32,
        epoch: i32,
        old_location: Option<PartitionLocation>,
        cause: StatusCode,
    ) -> Self {
        Self {
            shuffle_id,
            map_id,
            attempt_id,
            partition_id,
            epoch,
            old_location,
            cause,
            revive_status: AtomicI32::new(StatusCode::Unknown as i32),
        }
    }

    /// Get the revive status.
    pub fn get_status(&self) -> StatusCode {
        StatusCode::from(self.revive_status.load(Ordering::Relaxed))
    }

    /// Set the revive status.
    pub fn set_status(&self, status: StatusCode) {
        self.revive_status.store(status as i32, Ordering::Relaxed);
    }
}

/// Result of a batch revive operation.
#[derive(Debug, Clone)]
pub struct ReviveResult {
    /// Partition ID to new location mapping
    pub new_locations: HashMap<i32, PartitionLocation>,
    /// Partition ID to status code mapping
    pub status_codes: HashMap<i32, StatusCode>,
    /// Map IDs that have ended
    pub ended_map_ids: HashSet<i32>,
}

/// Manager for batching and processing revive requests.
pub struct ReviveManager {
    /// Transport client for sending requests
    transport_client: Arc<TransportClient>,
    /// Request sender channel
    request_sender: Sender<Arc<ReviveRequest>>,
    /// Request receiver (wrapped in mutex for single consumer)
    request_receiver: Mutex<Receiver<Arc<ReviveRequest>>>,
    /// Batch size for revive requests
    batch_size: usize,
    /// Interval for processing batched requests
    batch_interval: Duration,
    /// Running flag
    running: AtomicBool,
    /// Excluded workers (worker_id -> timestamp)
    excluded_workers: DashMap<String, i64>,
    /// Callback for updating partition locations
    location_updater: Arc<dyn Fn(i32, i32, PartitionLocation) + Send + Sync>,
    /// Callback for checking if mapper has ended
    mapper_ended_checker: Arc<dyn Fn(i32, i32) -> bool + Send + Sync>,
    /// Callback for checking if newer partition exists
    newer_partition_checker: Arc<dyn Fn(i32, i32, i32) -> bool + Send + Sync>,
}

impl ReviveManager {
    /// Create a new ReviveManager.
    pub fn new(
        transport_client: Arc<TransportClient>,
        batch_size: usize,
        batch_interval: Duration,
        location_updater: Arc<dyn Fn(i32, i32, PartitionLocation) + Send + Sync>,
        mapper_ended_checker: Arc<dyn Fn(i32, i32) -> bool + Send + Sync>,
        newer_partition_checker: Arc<dyn Fn(i32, i32, i32) -> bool + Send + Sync>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(1024);
        
        Self {
            transport_client,
            request_sender: sender,
            request_receiver: Mutex::new(receiver),
            batch_size,
            batch_interval,
            running: AtomicBool::new(true),
            excluded_workers: DashMap::new(),
            location_updater,
            mapper_ended_checker,
            newer_partition_checker,
        }
    }

    /// Add a revive request to the queue.
    pub async fn add_request(&self, request: Arc<ReviveRequest>) -> Result<()> {
        // Exclude the worker that caused the failure
        if let Some(ref loc) = request.old_location {
            self.exclude_worker_by_cause(request.cause, loc);
        }

        self.request_sender
            .send(request)
            .await
            .map_err(|e| CelebornError::Internal(format!("Failed to queue revive request: {}", e)))
    }

    /// Exclude a worker based on the failure cause.
    fn exclude_worker_by_cause(&self, cause: StatusCode, location: &PartitionLocation) {
        let worker_id = format!("{}:{}", location.host, location.push_port);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        match cause {
            StatusCode::PushDataFailNonCriticalCause
            | StatusCode::PushDataFailPrimary
            | StatusCode::PushDataFailReplica
            | StatusCode::PushDataCreateConnectionFailPrimary
            | StatusCode::PushDataCreateConnectionFailReplica
            | StatusCode::PushDataConnectionExceptionPrimary
            | StatusCode::PushDataConnectionExceptionReplica
            | StatusCode::PushDataTimeoutPrimary
            | StatusCode::PushDataTimeoutReplica => {
                debug!("Excluding worker {} due to {:?}", worker_id, cause);
                self.excluded_workers.insert(worker_id, now);
            }
            _ => {
                // Don't exclude for other causes
            }
        }
    }

    /// Check if a worker is excluded.
    pub fn is_worker_excluded(&self, host: &str, port: i32) -> bool {
        let worker_id = format!("{}:{}", host, port);
        self.excluded_workers.contains_key(&worker_id)
    }

    /// Remove a worker from the excluded list.
    pub fn remove_excluded_worker(&self, host: &str, port: i32) {
        let worker_id = format!("{}:{}", host, port);
        self.excluded_workers.remove(&worker_id);
    }

    /// Start the background batch processing task.
    pub fn start_batch_processor(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let manager = self.clone();
        
        tokio::spawn(async move {
            let mut interval_timer = interval(manager.batch_interval);
            
            while manager.running.load(Ordering::Relaxed) {
                interval_timer.tick().await;
                
                // Collect requests from the channel
                let mut requests: Vec<Arc<ReviveRequest>> = Vec::new();
                {
                    let mut receiver = manager.request_receiver.lock().await;
                    while let Ok(req) = receiver.try_recv() {
                        requests.push(req);
                        if requests.len() >= manager.batch_size {
                            break;
                        }
                    }
                }

                if requests.is_empty() {
                    continue;
                }

                // Group requests by shuffle ID
                let mut shuffle_requests: HashMap<i32, Vec<Arc<ReviveRequest>>> = HashMap::new();
                for req in requests {
                    shuffle_requests
                        .entry(req.shuffle_id)
                        .or_insert_with(Vec::new)
                        .push(req);
                }

                // Process each shuffle's requests
                for (shuffle_id, reqs) in shuffle_requests {
                    if let Err(e) = manager.process_batch(shuffle_id, reqs).await {
                        error!("Failed to process revive batch for shuffle {}: {}", shuffle_id, e);
                    }
                }
            }
        })
    }

    /// Process a batch of revive requests for a single shuffle.
    async fn process_batch(
        &self,
        shuffle_id: i32,
        requests: Vec<Arc<ReviveRequest>>,
    ) -> Result<()> {
        debug!(
            "Processing revive batch for shuffle {}: {} requests",
            shuffle_id,
            requests.len()
        );

        // Filter requests: skip if mapper ended or newer partition exists
        let mut filtered_requests: Vec<Arc<ReviveRequest>> = Vec::new();
        let mut map_ids: HashSet<i32> = HashSet::new();
        let mut requests_to_send: HashMap<i32, Arc<ReviveRequest>> = HashMap::new();

        for req in &requests {
            // Check if newer partition exists or mapper has ended
            if (self.newer_partition_checker)(shuffle_id, req.partition_id, req.epoch)
                || (self.mapper_ended_checker)(shuffle_id, req.map_id)
            {
                req.set_status(StatusCode::Success);
            } else {
                filtered_requests.push(req.clone());
                map_ids.insert(req.map_id);
                
                // Keep only the request with the highest epoch for each partition
                if let Some(existing) = requests_to_send.get(&req.partition_id) {
                    if existing.epoch < req.epoch {
                        requests_to_send.insert(req.partition_id, req.clone());
                    }
                } else {
                    requests_to_send.insert(req.partition_id, req.clone());
                }
            }
        }

        if requests_to_send.is_empty() {
            return Ok(());
        }

        // Build the revive request
        let partition_infos: Vec<PbRevivePartitionInfo> = requests_to_send
            .values()
            .map(|req| {
                PbRevivePartitionInfo {
                    partition_id: req.partition_id,
                    epoch: req.epoch,
                    partition: req.old_location.as_ref().map(|loc| self.convert_to_pb_location(loc)),
                    status: req.cause as i32,
                }
            })
            .collect();

        let revive_request = PbRevive {
            shuffle_id,
            map_id: map_ids.iter().cloned().collect(),
            partition_info: partition_infos,
        };

        // Send to Master
        let response: PbChangeLocationResponse = self
            .transport_client
            .send_to_master(TransportMessageType::ChangeLocation, &revive_request)
            .await?;

        // Process response
        let ended_map_ids: HashSet<i32> = response.ended_map_id.iter().cloned().collect();

        // Build result map
        let mut results: HashMap<i32, StatusCode> = HashMap::new();
        
        for info in &response.partition_info {
            let partition_id = info.partition_id;
            let status = StatusCode::from(info.status);
            
            // If old location is still available, remove from excluded
            if info.old_available {
                if let Some(req) = requests_to_send.get(&partition_id) {
                    if let Some(ref loc) = req.old_location {
                        self.remove_excluded_worker(&loc.host, loc.push_port);
                    }
                }
            }

            if status == StatusCode::Success {
                if let Some(ref pb_loc) = info.partition {
                    let new_location = self.convert_partition_location(pb_loc);
                    
                    // Remove new location from excluded list
                    self.remove_excluded_worker(&new_location.host, new_location.push_port);
                    if let Some(ref peer) = new_location.peer {
                        self.remove_excluded_worker(&peer.host, peer.push_port);
                    }
                    
                    // Update partition location
                    (self.location_updater)(shuffle_id, partition_id, new_location);
                }
            } else if status == StatusCode::StageEnded {
                info!("Stage ended for shuffle {}", shuffle_id);
                return Ok(());
            } else if status == StatusCode::ShuffleNotRegistered {
                error!("Shuffle {} not registered!", shuffle_id);
                return Err(CelebornError::ShuffleNotFound(shuffle_id));
            }

            results.insert(partition_id, status);
        }

        // Update status for all filtered requests
        for req in &filtered_requests {
            if (self.mapper_ended_checker)(shuffle_id, req.map_id) {
                req.set_status(StatusCode::Success);
            } else if let Some(&status) = results.get(&req.partition_id) {
                req.set_status(status);
            } else {
                req.set_status(StatusCode::ReviveFailed);
            }
        }

        info!(
            "Revive batch completed for shuffle {}: {} partitions processed",
            shuffle_id,
            results.len()
        );

        Ok(())
    }

    /// Perform a synchronous single-partition revive.
    pub async fn revive_single(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        partition_id: i32,
        epoch: i32,
        old_location: Option<&PartitionLocation>,
        cause: StatusCode,
    ) -> Result<PartitionLocation> {
        debug!(
            "Reviving single partition: shuffle={}, partition={}, epoch={}, cause={:?}",
            shuffle_id, partition_id, epoch, cause
        );

        // Exclude the worker
        if let Some(loc) = old_location {
            self.exclude_worker_by_cause(cause, loc);
        }

        // Build request
        let partition_info = PbRevivePartitionInfo {
            partition_id,
            epoch,
            partition: old_location.map(|loc| self.convert_to_pb_location(loc)),
            status: cause as i32,
        };

        let request = PbRevive {
            shuffle_id,
            map_id: vec![map_id],
            partition_info: vec![partition_info],
        };

        // Send to Master
        let response: PbChangeLocationResponse = self
            .transport_client
            .send_to_master(TransportMessageType::ChangeLocation, &request)
            .await?;

        // Find the new location
        for info in response.partition_info {
            if info.partition_id == partition_id {
                let status = StatusCode::from(info.status);
                
                if status == StatusCode::Success {
                    if let Some(pb_loc) = info.partition {
                        let new_location = self.convert_partition_location(&pb_loc);
                        
                        // Remove from excluded list
                        self.remove_excluded_worker(&new_location.host, new_location.push_port);
                        
                        // Update location
                        (self.location_updater)(shuffle_id, partition_id, new_location.clone());
                        
                        return Ok(new_location);
                    }
                } else if status == StatusCode::StageEnded {
                    return Err(CelebornError::StageEnded(shuffle_id));
                } else if status == StatusCode::ShuffleNotRegistered {
                    return Err(CelebornError::ShuffleNotFound(shuffle_id));
                } else {
                    return Err(CelebornError::ReviveFailed {
                        shuffle_id,
                        partition_id,
                        status,
                    });
                }
            }
        }

        Err(CelebornError::PartitionNotFound {
            shuffle_id,
            partition_id,
        })
    }

    /// Stop the revive manager.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Convert protobuf partition location to our type.
    fn convert_partition_location(&self, pb: &PbPartitionLocation) -> PartitionLocation {
        use crate::protocol::{PartitionMode, StorageInfo};
        
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
        use crate::protocol::generated::PbStorageInfo;
        
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
            split_start: 0,
            split_end: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_revive_request_creation() {
        let req = ReviveRequest::new(
            1,
            0,
            0,
            0,
            0,
            None,
            StatusCode::PushDataFailPrimary,
        );
        
        assert_eq!(req.shuffle_id, 1);
        assert_eq!(req.partition_id, 0);
        assert_eq!(req.cause, StatusCode::PushDataFailPrimary);
        assert_eq!(req.get_status(), StatusCode::Unknown);
    }

    #[test]
    fn test_revive_request_status() {
        let req = ReviveRequest::new(
            1, 0, 0, 0, 0, None,
            StatusCode::PushDataFailPrimary,
        );
        
        req.set_status(StatusCode::Success);
        assert_eq!(req.get_status(), StatusCode::Success);
        
        req.set_status(StatusCode::ReviveFailed);
        assert_eq!(req.get_status(), StatusCode::ReviveFailed);
    }
}
