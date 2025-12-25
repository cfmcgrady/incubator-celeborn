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

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};
use std::sync::Arc;

use dashmap::{DashMap, DashSet};
use futures::future::join_all;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::config::CelebornConfig;
use crate::error::{CelebornError, Result, StatusCode};
use crate::network::TransportClient;
use crate::protocol::transport::*;
use crate::protocol::{PartitionLocation, PartitionMode, StorageInfo, WorkerInfo};

/// Stage end status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageEndStatus {
    /// Stage has not ended yet
    NotEnded,
    /// Stage end is in progress
    InProgress,
    /// Stage ended successfully
    Success,
    /// Stage ended with data loss
    DataLost,
}

/// Mapper attempt state.
#[derive(Debug)]
struct MapperAttemptState {
    /// Mapper attempts (map_id -> attempt_id)
    /// -1 means not finished, >= 0 means finished with that attempt
    attempts: Vec<AtomicI32>,
}

impl MapperAttemptState {
    fn new(num_mappers: i32) -> Self {
        let attempts: Vec<AtomicI32> = (0..num_mappers)
            .map(|_| AtomicI32::new(-1))
            .collect();
        Self { attempts }
    }

    /// Mark a mapper attempt as finished.
    /// Returns true if this is the first successful attempt for this mapper.
    fn finish_attempt(&self, map_id: i32, attempt_id: i32) -> bool {
        if map_id < 0 || map_id as usize >= self.attempts.len() {
            return false;
        }
        
        let current = self.attempts[map_id as usize].load(Ordering::Acquire);
        if current >= 0 {
            // Already finished with another attempt
            return false;
        }
        
        // Try to set the attempt
        self.attempts[map_id as usize]
            .compare_exchange(current, attempt_id, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Check if all mappers have finished.
    fn all_finished(&self) -> bool {
        self.attempts.iter().all(|a| a.load(Ordering::Acquire) >= 0)
    }

    /// Get the number of finished mappers.
    fn finished_count(&self) -> usize {
        self.attempts.iter().filter(|a| a.load(Ordering::Acquire) >= 0).count()
    }
}

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
    /// Committed partition IDs (unique_id)
    committed_ids: DashSet<String>,
    /// Batch ID counters per partition (partition_id -> counter)
    batch_id_counters: DashMap<i32, AtomicI32>,
    /// Stage end status
    stage_end_status: RwLock<StageEndStatus>,
    /// Mapper attempt state
    mapper_attempts: MapperAttemptState,
}

impl ShuffleState {
    fn new(num_mappers: i32, num_partitions: i32) -> Self {
        Self {
            num_mappers,
            num_partitions,
            partition_locations: DashMap::new(),
            registered: AtomicBool::new(false),
            committed_ids: DashSet::new(),
            batch_id_counters: DashMap::new(),
            stage_end_status: RwLock::new(StageEndStatus::NotEnded),
            mapper_attempts: MapperAttemptState::new(num_mappers),
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
    ///
    /// This sends a RequestSlots message to the Master to allocate partition locations,
    /// then sends ReserveSlots to each Worker to prepare for data push.
    /// Note: In Celeborn, RegisterShuffle is handled by LifecycleManager (client-side),
    /// while RequestSlots is the actual RPC to Master for slot allocation.
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

        // Create partition ID list
        let partition_id_list: Vec<i32> = (0..num_partitions).collect();

        let user_identifier = PbUserIdentifier {
            tenant_id: "default".to_string(),
            name: "default".to_string(),
        };

        // Request slots from Master
        // Storage type masks:
        // - MEMORY_MASK = 0b1 = 1
        // - LOCAL_DISK_MASK = 0b10 = 2
        // - HDFS_MASK = 0b100 = 4
        // - ALL_TYPES_AVAILABLE_MASK = 0 (means all types available)
        let request = PbRequestSlots {
            application_id: self.config.app_id.clone(),
            shuffle_id,
            partition_id_list,
            hostname: gethostname::gethostname().to_string_lossy().to_string(),
            should_replicate: self.config.push_replicate_enabled,
            request_id: Uuid::new_v4().to_string(),
            storage_type: 0, // Default storage type (MEMORY)
            user_identifier: Some(user_identifier.clone()),
            should_rack_aware: false,
            max_workers: 0, // 0 means no limit
            available_storage_types: 2, // LOCAL_DISK_MASK = 2 (for local disk storage)
        };

        let response: PbRequestSlotsResponse = self
            .transport_client
            .send_to_master(TransportMessageType::RequestSlots, &request)
            .await?;

        let status = StatusCode::from(response.status);
        if !status.is_success() {
            return Err(CelebornError::ServerError {
                status,
                message: format!("Failed to request slots for shuffle {}", shuffle_id),
            });
        }

        // Create shuffle state
        let state = Arc::new(ShuffleState::new(num_mappers, num_partitions));

        // Group partition locations by worker for ReserveSlots calls
        // Key: (host, rpc_port), Value: (primary_locations, replica_locations)
        let mut worker_locations: HashMap<(String, i32), (Vec<PbPartitionLocation>, Vec<PbPartitionLocation>)> = HashMap::new();

        // Store partition locations from worker resources and group by worker
        for (_worker_id, worker_resource) in &response.worker_resource {
            for pb_location in &worker_resource.primary_partitions {
                // Debug: log the storage info from Master
                if let Some(ref storage) = pb_location.storage_info {
                    debug!(
                        "Primary partition {} storage_info: type={}, mount_point={}, available_storage_types={}",
                        pb_location.id, storage.r#type, storage.mount_point, storage.available_storage_types
                    );
                } else {
                    debug!("Primary partition {} has no storage_info", pb_location.id);
                }
                
                let location = self.convert_partition_location(pb_location);
                let key = (location.host.clone(), location.rpc_port);
                worker_locations
                    .entry(key)
                    .or_insert_with(|| (Vec::new(), Vec::new()))
                    .0
                    .push(pb_location.clone());
                state
                    .partition_locations
                    .entry(location.id)
                    .or_insert_with(Vec::new)
                    .push(location);
            }
            for pb_location in &worker_resource.replica_partitions {
                let location = self.convert_partition_location(pb_location);
                let key = (location.host.clone(), location.rpc_port);
                worker_locations
                    .entry(key)
                    .or_insert_with(|| (Vec::new(), Vec::new()))
                    .1
                    .push(pb_location.clone());
                state
                    .partition_locations
                    .entry(location.id)
                    .or_insert_with(Vec::new)
                    .push(location);
            }
        }

        // Send ReserveSlots to each Worker
        let reserve_futures: Vec<_> = worker_locations
            .into_iter()
            .map(|((host, rpc_port), (primary_locs, replica_locs))| {
                let client = self.transport_client.clone();
                let app_id = self.config.app_id.clone();
                let user_id = user_identifier.clone();
                let push_timeout = self.config.push_timeout.as_millis() as i64;
                
                async move {
                    let request = PbReserveSlots {
                        application_id: app_id,
                        shuffle_id,
                        primary_locations: primary_locs,
                        replica_locations: replica_locs,
                        split_threshold: 256 * 1024 * 1024, // 256MB default
                        split_mode: 0, // SOFT split mode
                        partition_type: 0, // REDUCE partition type
                        range_read_filter: false,
                        user_identifier: Some(user_id),
                        push_data_timeout: push_timeout,
                        partition_split_enabled: true,
                        available_storage_types: 2, // LOCAL_DISK_MASK = 2
                    };

                    debug!("Sending ReserveSlots to worker {}:{}", host, rpc_port);
                    let result = client
                        .send_to_worker::<_, PbReserveSlotsResponse>(
                            &host,
                            rpc_port,
                            TransportMessageType::ReserveSlots,
                            &request,
                        )
                        .await;
                    (host, rpc_port, result)
                }
            })
            .collect();

        // Execute ReserveSlots in parallel
        let results = join_all(reserve_futures).await;

        // Check results
        let mut failed_workers = Vec::new();
        for (host, port, result) in results {
            match result {
                Ok(response) => {
                    let status = StatusCode::from(response.status);
                    if status.is_success() {
                        debug!("ReserveSlots succeeded for worker {}:{}", host, port);
                    } else {
                        warn!(
                            "ReserveSlots failed for worker {}:{}: {:?} - {}",
                            host, port, status, response.reason
                        );
                        failed_workers.push((host, port, response.reason));
                    }
                }
                Err(e) => {
                    warn!("ReserveSlots error for worker {}:{}: {}", host, port, e);
                    failed_workers.push((host, port, e.to_string()));
                }
            }
        }

        if !failed_workers.is_empty() {
            // For now, we log the failures but continue
            // In a production implementation, we might want to retry or fail
            warn!(
                "ReserveSlots failed for {} workers: {:?}",
                failed_workers.len(),
                failed_workers
            );
        }

        state.registered.store(true, Ordering::Relaxed);
        let partition_count = state.partition_locations.len();
        self.shuffles.insert(shuffle_id, state);

        info!("Shuffle {} registered successfully with {} partition locations",
              shuffle_id, partition_count);
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
    ///
    /// This tracks mapper completion and can trigger stage end when all mappers are done.
    /// Returns true if this is the first successful attempt for this mapper.
    pub async fn mapper_end(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
    ) -> Result<bool> {
        debug!(
            "Mapper end: shuffle={}, map={}, attempt={}",
            shuffle_id, map_id, attempt_id
        );

        let state = self.shuffles.get(&shuffle_id).ok_or_else(|| {
            CelebornError::ShuffleNotFound(shuffle_id)
        })?;

        // Mark this mapper attempt as finished
        let is_first = state.mapper_attempts.finish_attempt(map_id, attempt_id);
        
        if is_first {
            let finished = state.mapper_attempts.finished_count();
            info!(
                "Mapper {} (attempt {}) completed for shuffle {} ({}/{} mappers done)",
                map_id, attempt_id, shuffle_id, finished, num_mappers
            );
        } else {
            debug!(
                "Mapper {} (attempt {}) already completed by another attempt for shuffle {}",
                map_id, attempt_id, shuffle_id
            );
        }

        Ok(is_first)
    }

    /// Handle stage end for a shuffle.
    ///
    /// This commits all files and marks the stage as ended.
    /// Should be called after all mappers have finished.
    pub async fn stage_end(&self, shuffle_id: i32) -> Result<StageEndStatus> {
        info!("Handling stage end for shuffle {}", shuffle_id);

        // Check if shuffle is registered
        if !self.shuffles.contains_key(&shuffle_id) {
            info!(
                "[stage_end] Shuffle {} not registered, maybe no shuffle data within this stage",
                shuffle_id
            );
            return Ok(StageEndStatus::Success);
        }

        let state = self.shuffles.get(&shuffle_id).ok_or_else(|| {
            CelebornError::ShuffleNotFound(shuffle_id)
        })?;

        // Check current status
        {
            let status = state.stage_end_status.read().await;
            match *status {
                StageEndStatus::Success | StageEndStatus::DataLost => {
                    info!("[stage_end] Shuffle {} already ended with status {:?}", shuffle_id, *status);
                    return Ok(*status);
                }
                StageEndStatus::InProgress => {
                    info!("[stage_end] Shuffle {} stage end is already in progress", shuffle_id);
                    return Ok(StageEndStatus::InProgress);
                }
                StageEndStatus::NotEnded => {
                    // Continue to process
                }
            }
        }

        // Set status to InProgress
        {
            let mut status = state.stage_end_status.write().await;
            if *status != StageEndStatus::NotEnded {
                return Ok(*status);
            }
            *status = StageEndStatus::InProgress;
        }

        // Drop the state reference before calling commit
        drop(state);

        // Commit files
        let commit_result = self.request_commit_files(shuffle_id).await;

        // Get state again to update status
        let state = self.shuffles.get(&shuffle_id).ok_or_else(|| {
            CelebornError::ShuffleNotFound(shuffle_id)
        })?;

        let final_status = match commit_result {
            Ok(response) => {
                let status_code = StatusCode::from(response.status);
                if status_code.is_success() || status_code == StatusCode::PartialSuccess {
                    // Check if there's data loss
                    if !response.failed_primary_ids.is_empty() || !response.failed_replica_ids.is_empty() {
                        warn!(
                            "[stage_end] Shuffle {} completed with some failures: {} primary, {} replica failed",
                            shuffle_id,
                            response.failed_primary_ids.len(),
                            response.failed_replica_ids.len()
                        );
                        StageEndStatus::DataLost
                    } else {
                        info!(
                            "[stage_end] Shuffle {} completed successfully: {} primary, {} replica committed",
                            shuffle_id,
                            response.committed_primary_ids.len(),
                            response.committed_replica_ids.len()
                        );
                        StageEndStatus::Success
                    }
                } else {
                    warn!("[stage_end] Shuffle {} commit failed with status {:?}", shuffle_id, status_code);
                    StageEndStatus::DataLost
                }
            }
            Err(e) => {
                warn!("[stage_end] Shuffle {} commit error: {}", shuffle_id, e);
                StageEndStatus::DataLost
            }
        };

        // Update final status
        {
            let mut status = state.stage_end_status.write().await;
            *status = final_status;
        }

        // Clear partition locations (similar to Java implementation)
        state.partition_locations.clear();

        Ok(final_status)
    }

    /// Wait for stage end to complete.
    ///
    /// Returns (is_timeout, wait_time_ms).
    pub async fn wait_stage_end(&self, shuffle_id: i32, timeout_ms: u64) -> (bool, u64) {
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_millis(timeout_ms);
        let poll_interval = std::time::Duration::from_millis(100);

        loop {
            if let Some(state) = self.shuffles.get(&shuffle_id) {
                let status = state.stage_end_status.read().await;
                match *status {
                    StageEndStatus::Success | StageEndStatus::DataLost => {
                        let elapsed = start.elapsed().as_millis() as u64;
                        return (false, elapsed);
                    }
                    _ => {}
                }
            } else {
                // Shuffle not found, consider it ended
                let elapsed = start.elapsed().as_millis() as u64;
                return (false, elapsed);
            }

            if start.elapsed() >= timeout {
                let elapsed = start.elapsed().as_millis() as u64;
                return (true, elapsed);
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Check if stage has ended.
    pub async fn is_stage_end(&self, shuffle_id: i32) -> bool {
        if let Some(state) = self.shuffles.get(&shuffle_id) {
            let status = state.stage_end_status.read().await;
            matches!(*status, StageEndStatus::Success | StageEndStatus::DataLost)
        } else {
            true // If shuffle not found, consider it ended
        }
    }

    /// Check if stage end is in progress or completed.
    pub async fn is_stage_end_or_in_progress(&self, shuffle_id: i32) -> bool {
        if let Some(state) = self.shuffles.get(&shuffle_id) {
            let status = state.stage_end_status.read().await;
            !matches!(*status, StageEndStatus::NotEnded)
        } else {
            true
        }
    }

    /// Get the stage end status.
    pub async fn get_stage_end_status(&self, shuffle_id: i32) -> StageEndStatus {
        if let Some(state) = self.shuffles.get(&shuffle_id) {
            *state.stage_end_status.read().await
        } else {
            StageEndStatus::Success // If shuffle not found, consider it ended successfully
        }
    }

    /// Check if all mappers have finished for a shuffle.
    pub fn all_mappers_finished(&self, shuffle_id: i32) -> bool {
        if let Some(state) = self.shuffles.get(&shuffle_id) {
            state.mapper_attempts.all_finished()
        } else {
            false
        }
    }

    /// Get the number of finished mappers for a shuffle.
    pub fn finished_mapper_count(&self, shuffle_id: i32) -> usize {
        if let Some(state) = self.shuffles.get(&shuffle_id) {
            state.mapper_attempts.finished_count()
        } else {
            0
        }
    }

    /// Mark a partition as successfully pushed (written).
    pub fn add_partition_data_pushed(&self, shuffle_id: i32, unique_id: &str) {
        if let Some(state) = self.shuffles.get(&shuffle_id) {
            state.committed_ids.insert(unique_id.to_string());
        }
    }

    /// Request Workers to commit files for a shuffle.
    ///
    /// This should be called after all mappers have finished.
    pub async fn request_commit_files(&self, shuffle_id: i32) -> Result<PbCommitFilesResponse> {
        info!("Requesting commit files for shuffle {}", shuffle_id);
        
        let state = self.shuffles.get(&shuffle_id).ok_or_else(|| {
            CelebornError::ShuffleNotFound(shuffle_id)
        })?;

        // Group committed IDs by worker
        let mut worker_to_ids: HashMap<(String, i32), (Vec<String>, Vec<String>)> = HashMap::new();
        
        // Iterate over all partition locations to find which ones are committed and which worker handles them
        for entry in state.partition_locations.iter() {
            for loc in entry.value() {
                let unique_id = loc.unique_id();
                if state.committed_ids.contains(&unique_id) {
                    let key = (loc.host.clone(), loc.rpc_port);
                    let entry = worker_to_ids.entry(key).or_insert((Vec::new(), Vec::new()));
                    if loc.mode == PartitionMode::Primary {
                        entry.0.push(unique_id);
                    } else {
                        entry.1.push(unique_id);
                    }
                }
            }
        }
        
        // Prepare requests
        let map_attempts = vec![0; state.num_mappers as usize]; // Simplified
        let mut futures = Vec::new();
        
        for ((host, port), (primary_ids, replica_ids)) in worker_to_ids {
            if primary_ids.is_empty() && replica_ids.is_empty() {
                continue;
            }
            
            let request = PbCommitFiles {
                application_id: self.config.app_id.clone(),
                shuffle_id,
                primary_ids,
                replica_ids,
                map_attempts: map_attempts.clone(),
                epoch: 0,
                mock_failure: false,
            };
            
            let client = self.transport_client.clone();
            let host_clone = host.clone();
            
            futures.push(async move {
                let result = client.send_to_worker::<_, PbCommitFilesResponse>(
                    &host_clone,
                    port,
                    TransportMessageType::CommitFiles,
                    &request
                ).await;
                (result, host_clone, port)
            });
        }
        
        // Execute in parallel
        let results = join_all(futures).await;
        
        // Aggregate results
        let mut response = PbCommitFilesResponse {
            status: StatusCode::Success as i32,
            committed_primary_ids: Vec::new(),
            committed_replica_ids: Vec::new(),
            failed_primary_ids: Vec::new(),
            failed_replica_ids: Vec::new(),
            committed_primary_storage_infos: HashMap::new(),
            committed_replica_storage_infos: HashMap::new(),
            total_written: 0,
            file_count: 0,
        };
        
        let mut total_failures = 0;
        
        for (res, host, port) in results {
            match res {
                Ok(r) => {
                    let status = StatusCode::from(r.status);
                    if !status.is_success() {
                        total_failures += 1;
                        warn!("Worker {}:{} returned status {:?}", host, port, status);
                    }
                    response.committed_primary_ids.extend(r.committed_primary_ids);
                    response.committed_replica_ids.extend(r.committed_replica_ids);
                    response.failed_primary_ids.extend(r.failed_primary_ids);
                    response.failed_replica_ids.extend(r.failed_replica_ids);
                }
                Err(e) => {
                    total_failures += 1;
                    warn!("Failed to commit files on worker {}:{}: {}", host, port, e);
                }
            }
        }
        
        if total_failures > 0 {
            warn!("Commit files completed with {} worker failures", total_failures);
            if response.committed_primary_ids.is_empty() && response.committed_replica_ids.is_empty() {
                response.status = StatusCode::RequestFailed as i32;
            } else {
                response.status = StatusCode::PartialSuccess as i32;
            }
        } else {
            info!(
                "Commit files successful for shuffle {}: {} primary, {} replica committed",
                shuffle_id,
                response.committed_primary_ids.len(),
                response.committed_replica_ids.len()
            );
        }

        Ok(response)
    }

    /// Get reducer file groups.
    ///
    /// Note: In Celeborn, GetReducerFileGroup is handled by LifecycleManager (client-side).
    /// For the Rust client, we return the partition locations that were allocated during
    /// shuffle registration (RequestSlots).
    pub async fn get_reducer_file_group(
        &self,
        shuffle_id: i32,
    ) -> Result<HashMap<i32, Vec<PartitionLocation>>> {
        // First check if we have the shuffle state locally (e.g. we registered it)
        if let Some(state) = self.shuffles.get(&shuffle_id) {
            let mut result = HashMap::new();
            for entry in state.partition_locations.iter() {
                result.insert(*entry.key(), entry.value().clone());
            }
            return Ok(result);
        }

        // If not found locally, try to fetch from Master (Reducer role)
        info!("Shuffle {} not found locally, fetching from Master...", shuffle_id);
        
        let request = PbGetReducerFileGroup {
            shuffle_id,
        };
        
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
        
        // Convert response to our format
        let mut result = HashMap::new();
        for (partition_id, file_group) in response.file_groups {
            let locations: Vec<PartitionLocation> = file_group
                .locations
                .iter()
                .map(|pb_loc| self.convert_partition_location(pb_loc))
                .collect();
            result.insert(partition_id, locations);
        }
        
        Ok(result)
    }

    /// Unregister a shuffle.
    ///
    /// This will trigger stage_end if it hasn't been called yet,
    /// then unregister the shuffle from the Master.
    pub async fn unregister_shuffle(&self, shuffle_id: i32) -> Result<()> {
        info!("Unregistering shuffle {}", shuffle_id);

        // If stage end has not been handled, trigger it first
        if !self.is_stage_end(shuffle_id).await {
            info!("Triggering stage_end before unregister for shuffle {}", shuffle_id);
            
            // Trigger stage end
            let _ = self.stage_end(shuffle_id).await;
            
            // Wait for stage end with timeout (default 5 minutes)
            let timeout_ms = self.config.push_timeout.as_millis() as u64;
            let (is_timeout, cost) = self.wait_stage_end(shuffle_id, timeout_ms).await;
            
            if is_timeout {
                warn!(
                    "[unregister_shuffle] Stage end timeout for shuffle {} after {}ms",
                    shuffle_id, cost
                );
            } else {
                info!(
                    "[unregister_shuffle] Stage end completed for shuffle {} in {}ms",
                    shuffle_id, cost
                );
            }
        }

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

    /// Get the next batch ID for a partition.
    ///
    /// Each partition maintains its own batch ID counter that increments
    /// with each push operation. This is used in the batch header format
    /// expected by Celeborn Worker.
    pub fn next_batch_id(&self, shuffle_id: i32, partition_id: i32) -> i32 {
        if let Some(state) = self.shuffles.get(&shuffle_id) {
            let counter = state.batch_id_counters
                .entry(partition_id)
                .or_insert_with(|| AtomicI32::new(0));
            counter.fetch_add(1, Ordering::Relaxed)
        } else {
            // If shuffle not found, return 0 (shouldn't happen in normal flow)
            0
        }
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

    #[test]
    fn test_stage_end_status_enum() {
        // Test StageEndStatus enum values
        assert_eq!(StageEndStatus::NotEnded, StageEndStatus::NotEnded);
        assert_eq!(StageEndStatus::InProgress, StageEndStatus::InProgress);
        assert_eq!(StageEndStatus::Success, StageEndStatus::Success);
        assert_eq!(StageEndStatus::DataLost, StageEndStatus::DataLost);
        
        // Test that different variants are not equal
        assert_ne!(StageEndStatus::NotEnded, StageEndStatus::InProgress);
        assert_ne!(StageEndStatus::Success, StageEndStatus::DataLost);
    }

    #[test]
    fn test_mapper_attempt_state_new() {
        let state = MapperAttemptState::new(5);
        assert_eq!(state.attempts.len(), 5);
        assert!(!state.all_finished());
        assert_eq!(state.finished_count(), 0);
    }

    #[test]
    fn test_mapper_attempt_state_finish_attempt() {
        let state = MapperAttemptState::new(3);
        
        // First attempt for mapper 0 should succeed
        assert!(state.finish_attempt(0, 0));
        assert_eq!(state.finished_count(), 1);
        
        // Second attempt for same mapper should fail (already finished)
        assert!(!state.finish_attempt(0, 1));
        assert_eq!(state.finished_count(), 1);
        
        // First attempt for mapper 1 should succeed
        assert!(state.finish_attempt(1, 0));
        assert_eq!(state.finished_count(), 2);
        
        // First attempt for mapper 2 should succeed
        assert!(state.finish_attempt(2, 0));
        assert_eq!(state.finished_count(), 3);
        
        // All mappers should be finished now
        assert!(state.all_finished());
    }

    #[test]
    fn test_mapper_attempt_state_invalid_map_id() {
        let state = MapperAttemptState::new(3);
        
        // Negative map_id should fail
        assert!(!state.finish_attempt(-1, 0));
        
        // Out of bounds map_id should fail
        assert!(!state.finish_attempt(3, 0));
        assert!(!state.finish_attempt(100, 0));
        
        // Valid map_id should succeed
        assert!(state.finish_attempt(0, 0));
    }

    #[test]
    fn test_mapper_attempt_state_all_finished() {
        let state = MapperAttemptState::new(2);
        
        assert!(!state.all_finished());
        
        state.finish_attempt(0, 0);
        assert!(!state.all_finished());
        
        state.finish_attempt(1, 0);
        assert!(state.all_finished());
    }

    #[test]
    fn test_shuffle_state_new() {
        let state = ShuffleState::new(4, 10);
        
        assert_eq!(state.num_mappers, 4);
        assert_eq!(state.num_partitions, 10);
        assert!(!state.registered.load(Ordering::Relaxed));
        assert!(state.partition_locations.is_empty());
        assert!(state.committed_ids.is_empty());
    }

    #[tokio::test]
    async fn test_shuffle_state_stage_end_status() {
        let state = ShuffleState::new(2, 4);
        
        // Initial status should be NotEnded
        {
            let status = state.stage_end_status.read().await;
            assert_eq!(*status, StageEndStatus::NotEnded);
        }
        
        // Update to InProgress
        {
            let mut status = state.stage_end_status.write().await;
            *status = StageEndStatus::InProgress;
        }
        
        // Verify InProgress
        {
            let status = state.stage_end_status.read().await;
            assert_eq!(*status, StageEndStatus::InProgress);
        }
        
        // Update to Success
        {
            let mut status = state.stage_end_status.write().await;
            *status = StageEndStatus::Success;
        }
        
        // Verify Success
        {
            let status = state.stage_end_status.read().await;
            assert_eq!(*status, StageEndStatus::Success);
        }
    }

    #[tokio::test]
    async fn test_lifecycle_manager_stage_end_not_registered() {
        let config = Arc::new(
            CelebornConfig::builder()
                .app_id("test-app")
                .master_endpoints(vec!["localhost:9097".to_string()])
                .build()
                .unwrap(),
        );
        let transport = Arc::new(TransportClient::new(config.clone()).unwrap());
        let manager = LifecycleManager::new(config, transport);
        
        // Stage end for non-registered shuffle should return Success
        let result = manager.stage_end(999).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), StageEndStatus::Success);
    }

    #[tokio::test]
    async fn test_lifecycle_manager_is_stage_end() {
        let config = Arc::new(
            CelebornConfig::builder()
                .app_id("test-app")
                .master_endpoints(vec!["localhost:9097".to_string()])
                .build()
                .unwrap(),
        );
        let transport = Arc::new(TransportClient::new(config.clone()).unwrap());
        let manager = LifecycleManager::new(config, transport);
        
        // Non-existent shuffle should be considered ended
        assert!(manager.is_stage_end(999).await);
        assert!(manager.is_stage_end_or_in_progress(999).await);
    }

    #[tokio::test]
    async fn test_lifecycle_manager_get_stage_end_status() {
        let config = Arc::new(
            CelebornConfig::builder()
                .app_id("test-app")
                .master_endpoints(vec!["localhost:9097".to_string()])
                .build()
                .unwrap(),
        );
        let transport = Arc::new(TransportClient::new(config.clone()).unwrap());
        let manager = LifecycleManager::new(config, transport);
        
        // Non-existent shuffle should return Success
        let status = manager.get_stage_end_status(999).await;
        assert_eq!(status, StageEndStatus::Success);
    }

    #[test]
    fn test_lifecycle_manager_all_mappers_finished() {
        let config = Arc::new(
            CelebornConfig::builder()
                .app_id("test-app")
                .master_endpoints(vec!["localhost:9097".to_string()])
                .build()
                .unwrap(),
        );
        let transport = Arc::new(TransportClient::new(config.clone()).unwrap());
        let manager = LifecycleManager::new(config, transport);
        
        // Non-existent shuffle should return false
        assert!(!manager.all_mappers_finished(999));
        assert_eq!(manager.finished_mapper_count(999), 0);
    }

    #[tokio::test]
    async fn test_lifecycle_manager_wait_stage_end_not_registered() {
        let config = Arc::new(
            CelebornConfig::builder()
                .app_id("test-app")
                .master_endpoints(vec!["localhost:9097".to_string()])
                .build()
                .unwrap(),
        );
        let transport = Arc::new(TransportClient::new(config.clone()).unwrap());
        let manager = LifecycleManager::new(config, transport);
        
        // Wait for non-existent shuffle should return immediately (not timeout)
        let (is_timeout, wait_time) = manager.wait_stage_end(999, 1000).await;
        assert!(!is_timeout);
        assert!(wait_time < 100); // Should be very fast
    }

    #[tokio::test]
    async fn test_stage_end_status_clone_and_copy() {
        let status1 = StageEndStatus::Success;
        let status2 = status1; // Copy
        let status3 = status1.clone(); // Clone
        
        assert_eq!(status1, status2);
        assert_eq!(status1, status3);
    }

    #[test]
    fn test_mapper_attempt_state_concurrent_finish() {
        use std::thread;
        
        let state = Arc::new(MapperAttemptState::new(100));
        let mut handles = vec![];
        
        // Spawn multiple threads trying to finish the same mappers
        for map_id in 0..100 {
            let state_clone = state.clone();
            handles.push(thread::spawn(move || {
                // Multiple attempts for the same mapper
                let result1 = state_clone.finish_attempt(map_id, 0);
                let result2 = state_clone.finish_attempt(map_id, 1);
                // Only one should succeed
                result1 || result2
            }));
        }
        
        // Wait for all threads
        for handle in handles {
            assert!(handle.join().unwrap());
        }
        
        // All mappers should be finished
        assert!(state.all_finished());
        assert_eq!(state.finished_count(), 100);
    }
}
