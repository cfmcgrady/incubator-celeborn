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

//! Partition Split mechanism for Celeborn Rust client.
//!
//! This module implements the partition split functionality that handles:
//! - SOFT_SPLIT: Worker signals that partition is getting large, client should request new location
//! - HARD_SPLIT: Worker forces split, client must wait for new location before continuing
//!
//! The split mechanism is triggered by Worker responses during push operations.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

use crate::error::{CelebornError, Result};
use crate::protocol::PartitionLocation;

/// Status codes for split operations (matching Java StatusCode enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SplitStatus {
    /// Soft split - partition is getting large, request new location asynchronously
    SoftSplit = 22,
    /// Hard split - partition must be split, wait for new location
    HardSplit = 21,
}

impl TryFrom<u8> for SplitStatus {
    type Error = CelebornError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            21 => Ok(SplitStatus::HardSplit),
            22 => Ok(SplitStatus::SoftSplit),
            _ => Err(CelebornError::Protocol(format!(
                "Unknown split status: {}",
                value
            ))),
        }
    }
}

/// Split range information for a partition.
/// Format: "{partition_id}_{split_start}_{split_end}"
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SplitRange {
    /// Partition ID
    pub partition_id: i32,
    /// Split start index
    pub split_start: i32,
    /// Split end index
    pub split_end: i32,
}

impl SplitRange {
    /// Create a new split range.
    pub fn new(partition_id: i32, split_start: i32, split_end: i32) -> Self {
        Self {
            partition_id,
            split_start,
            split_end,
        }
    }

    /// Create a split range from a partition location.
    pub fn from_location(loc: &PartitionLocationWithSplit) -> Self {
        Self {
            partition_id: loc.location.id,
            split_start: loc.split_start,
            split_end: loc.split_end,
        }
    }

    /// Get the string representation (used as map key).
    pub fn to_string(&self) -> String {
        format!("{}_{}_{}",  self.partition_id, self.split_start, self.split_end)
    }
}

/// Extended partition location with split information.
#[derive(Debug, Clone)]
pub struct PartitionLocationWithSplit {
    /// Base partition location
    pub location: PartitionLocation,
    /// Split start index
    pub split_start: i32,
    /// Split end index
    pub split_end: i32,
    /// Parent location (for split partitions)
    pub parent: Option<Box<PartitionLocationWithSplit>>,
}

impl PartitionLocationWithSplit {
    /// Create a new partition location with split info.
    pub fn new(location: PartitionLocation) -> Self {
        Self {
            location,
            split_start: -1,
            split_end: -1,
            parent: None,
        }
    }

    /// Create with explicit split range.
    pub fn with_split_range(location: PartitionLocation, split_start: i32, split_end: i32) -> Self {
        Self {
            location,
            split_start,
            split_end,
            parent: None,
        }
    }

    /// Get the split range string.
    pub fn get_split_range(&self) -> String {
        format!("{}_{}_{}",  self.location.id, self.split_start, self.split_end)
    }

    /// Set the split range.
    pub fn set_split_range(&mut self, split_start: i32, split_end: i32) {
        self.split_start = split_start;
        self.split_end = split_end;
    }
}

/// Request to change partition location (due to split or failure).
#[derive(Debug)]
pub struct ChangePartitionRequest {
    /// Shuffle ID
    pub shuffle_id: i32,
    /// Partition ID
    pub partition_id: i32,
    /// Current epoch
    pub epoch: i32,
    /// Old partition location
    pub old_partition: Option<PartitionLocationWithSplit>,
    /// Cause of the change request
    pub cause: Option<SplitStatus>,
    /// Response channel
    pub response_tx: oneshot::Sender<Result<PartitionLocationWithSplit>>,
}

/// Manages partition locations and handles split operations.
///
/// This is the Rust equivalent of Java's ChangePartitionManager.
pub struct PartitionLocationManager {
    /// Shuffle ID -> (Partition ID -> PartitionLocationManager entry)
    partition_locations: RwLock<HashMap<i32, HashMap<i32, PartitionLocationEntry>>>,
    /// Pending change partition requests
    /// shuffleId -> (splitRange -> set of requests)
    pending_requests: RwLock<HashMap<i32, HashMap<String, Vec<ChangePartitionRequest>>>>,
    /// Partitions currently being processed
    in_batch_partitions: RwLock<HashMap<i32, HashSet<String>>>,
    /// Request sender for async processing
    request_tx: mpsc::Sender<ChangePartitionRequest>,
    /// Whether batch handling is enabled
    batch_handle_enabled: bool,
    /// Batch handle interval in milliseconds
    batch_handle_interval_ms: u64,
}

/// Entry for a single partition's location management.
#[derive(Debug)]
struct PartitionLocationEntry {
    /// Current epoch
    current_epoch: AtomicI32,
    /// All locations for this partition (epoch -> location)
    locations: RwLock<HashMap<i32, PartitionLocationWithSplit>>,
    /// Child locations (for split partitions)
    children: RwLock<Vec<PartitionLocationWithSplit>>,
}

impl PartitionLocationEntry {
    fn new() -> Self {
        Self {
            current_epoch: AtomicI32::new(0),
            locations: RwLock::new(HashMap::new()),
            children: RwLock::new(Vec::new()),
        }
    }

    /// Get the latest partition location.
    fn get_latest(&self) -> Option<PartitionLocationWithSplit> {
        let locations = self.locations.read();
        let current_epoch = self.current_epoch.load(Ordering::SeqCst);
        locations.get(&current_epoch).cloned()
    }

    /// Get location for a specific epoch.
    fn get_by_epoch(&self, epoch: i32) -> Option<PartitionLocationWithSplit> {
        self.locations.read().get(&epoch).cloned()
    }

    /// Add a new location.
    fn add_location(&self, location: PartitionLocationWithSplit) {
        let epoch = location.location.epoch;
        let mut locations = self.locations.write();
        locations.insert(epoch, location);
        
        // Update current epoch if this is newer
        let current = self.current_epoch.load(Ordering::SeqCst);
        if epoch > current {
            self.current_epoch.store(epoch, Ordering::SeqCst);
        }
    }

    /// Check if a newer partition exists.
    fn has_newer_partition(&self, epoch: i32) -> bool {
        self.current_epoch.load(Ordering::SeqCst) > epoch
    }

    /// Add a child location (for split partitions).
    fn add_child(&self, child: PartitionLocationWithSplit) {
        self.children.write().push(child);
    }

    /// Get a random child location.
    fn get_random_child(&self) -> Option<PartitionLocationWithSplit> {
        let children = self.children.read();
        if children.is_empty() {
            None
        } else {
            // Simple selection - use first child for deterministic behavior
            // In production, could use a proper random selection
            Some(children[0].clone())
        }
    }
}

impl PartitionLocationManager {
    /// Create a new partition location manager.
    pub fn new(batch_handle_enabled: bool, batch_handle_interval_ms: u64) -> (Self, mpsc::Receiver<ChangePartitionRequest>) {
        let (request_tx, request_rx) = mpsc::channel(1000);
        
        let manager = Self {
            partition_locations: RwLock::new(HashMap::new()),
            pending_requests: RwLock::new(HashMap::new()),
            in_batch_partitions: RwLock::new(HashMap::new()),
            request_tx,
            batch_handle_enabled,
            batch_handle_interval_ms,
        };
        
        (manager, request_rx)
    }

    /// Register a shuffle.
    pub fn register_shuffle(&self, shuffle_id: i32) {
        self.partition_locations.write().entry(shuffle_id).or_insert_with(HashMap::new);
        self.pending_requests.write().entry(shuffle_id).or_insert_with(HashMap::new);
        self.in_batch_partitions.write().entry(shuffle_id).or_insert_with(HashSet::new);
    }

    /// Unregister a shuffle.
    pub fn unregister_shuffle(&self, shuffle_id: i32) {
        self.partition_locations.write().remove(&shuffle_id);
        self.pending_requests.write().remove(&shuffle_id);
        self.in_batch_partitions.write().remove(&shuffle_id);
    }

    /// Add or update a partition location.
    pub fn update_partition_location(&self, shuffle_id: i32, location: PartitionLocationWithSplit) {
        let mut shuffles = self.partition_locations.write();
        let partitions = shuffles.entry(shuffle_id).or_insert_with(HashMap::new);
        let entry = partitions.entry(location.location.id).or_insert_with(PartitionLocationEntry::new);
        entry.add_location(location);
    }

    /// Get the latest partition location.
    pub fn get_latest_location(&self, shuffle_id: i32, partition_id: i32) -> Option<PartitionLocationWithSplit> {
        let shuffles = self.partition_locations.read();
        shuffles.get(&shuffle_id)
            .and_then(|partitions| partitions.get(&partition_id))
            .and_then(|entry| entry.get_latest())
    }

    /// Get partition location by epoch.
    pub fn get_location_by_epoch(&self, shuffle_id: i32, partition_id: i32, epoch: i32) -> Option<PartitionLocationWithSplit> {
        let shuffles = self.partition_locations.read();
        shuffles.get(&shuffle_id)
            .and_then(|partitions| partitions.get(&partition_id))
            .and_then(|entry| entry.get_by_epoch(epoch))
    }

    /// Check if a newer partition location exists.
    pub fn has_newer_partition(&self, shuffle_id: i32, partition_id: i32, epoch: i32) -> bool {
        let shuffles = self.partition_locations.read();
        shuffles.get(&shuffle_id)
            .and_then(|partitions| partitions.get(&partition_id))
            .map(|entry| entry.has_newer_partition(epoch))
            .unwrap_or(false)
    }

    /// Handle a split response from worker.
    ///
    /// This is called when a push operation receives a SOFT_SPLIT or HARD_SPLIT response.
    pub async fn handle_split(
        &self,
        shuffle_id: i32,
        partition_id: i32,
        epoch: i32,
        old_location: Option<PartitionLocationWithSplit>,
        split_status: SplitStatus,
    ) -> Result<Option<PartitionLocationWithSplit>> {
        debug!(
            "Handling {} for shuffle {} partition {} epoch {}",
            match split_status {
                SplitStatus::SoftSplit => "SOFT_SPLIT",
                SplitStatus::HardSplit => "HARD_SPLIT",
            },
            shuffle_id,
            partition_id,
            epoch
        );

        // Check if we already have a newer partition
        if self.has_newer_partition(shuffle_id, partition_id, epoch) {
            debug!(
                "Newer partition already exists for shuffle {} partition {} epoch {}",
                shuffle_id, partition_id, epoch
            );
            return Ok(self.get_latest_location(shuffle_id, partition_id));
        }

        match split_status {
            SplitStatus::SoftSplit => {
                // For soft split, we request new location asynchronously
                // The current push can continue with the existing location
                self.request_partition_change(shuffle_id, partition_id, epoch, old_location, Some(split_status)).await?;
                Ok(None) // Return None to indicate async handling
            }
            SplitStatus::HardSplit => {
                // For hard split, we must wait for new location
                let new_location = self.request_partition_change_sync(
                    shuffle_id, partition_id, epoch, old_location, Some(split_status)
                ).await?;
                Ok(Some(new_location))
            }
        }
    }

    /// Request a partition change asynchronously.
    async fn request_partition_change(
        &self,
        shuffle_id: i32,
        partition_id: i32,
        epoch: i32,
        old_partition: Option<PartitionLocationWithSplit>,
        cause: Option<SplitStatus>,
    ) -> Result<()> {
        let (response_tx, _response_rx) = oneshot::channel();
        
        let request = ChangePartitionRequest {
            shuffle_id,
            partition_id,
            epoch,
            old_partition,
            cause,
            response_tx,
        };

        self.request_tx.send(request).await.map_err(|e| {
            CelebornError::Internal(format!("Failed to send change partition request: {}", e))
        })?;

        Ok(())
    }

    /// Request a partition change synchronously (wait for result).
    async fn request_partition_change_sync(
        &self,
        shuffle_id: i32,
        partition_id: i32,
        epoch: i32,
        old_partition: Option<PartitionLocationWithSplit>,
        cause: Option<SplitStatus>,
    ) -> Result<PartitionLocationWithSplit> {
        let (response_tx, response_rx) = oneshot::channel();
        
        let request = ChangePartitionRequest {
            shuffle_id,
            partition_id,
            epoch,
            old_partition,
            cause,
            response_tx,
        };

        self.request_tx.send(request).await.map_err(|e| {
            CelebornError::Internal(format!("Failed to send change partition request: {}", e))
        })?;

        response_rx.await.map_err(|e| {
            CelebornError::Internal(format!("Failed to receive change partition response: {}", e))
        })?
    }

    /// Process pending change partition requests.
    ///
    /// This should be called periodically when batch handling is enabled,
    /// or immediately when batch handling is disabled.
    pub async fn process_pending_requests<F, Fut>(
        &self,
        shuffle_id: i32,
        allocate_new_location: F,
    ) -> Result<()>
    where
        F: Fn(i32, i32, i32, Option<PartitionLocationWithSplit>) -> Fut,
        Fut: std::future::Future<Output = Result<PartitionLocationWithSplit>>,
    {
        let requests_to_process: Vec<(String, Vec<ChangePartitionRequest>)> = {
            let mut pending = self.pending_requests.write();
            let mut in_batch = self.in_batch_partitions.write();
            
            if let Some(shuffle_requests) = pending.get_mut(&shuffle_id) {
                let in_batch_set = in_batch.entry(shuffle_id).or_insert_with(HashSet::new);
                
                // First, collect the keys to process
                let keys_to_process: Vec<String> = shuffle_requests.keys()
                    .filter(|split_range| !in_batch_set.contains(*split_range))
                    .cloned()
                    .collect();
                
                // Then, remove and collect the requests
                let mut result = Vec::new();
                for key in keys_to_process {
                    if let Some(requests) = shuffle_requests.remove(&key) {
                        in_batch_set.insert(key.clone());
                        result.push((key, requests));
                    }
                }
                result
            } else {
                Vec::new()
            }
        };

        for (split_range, requests) in requests_to_process {
            if requests.is_empty() {
                continue;
            }

            // Get the request with highest epoch
            let max_epoch_request = requests.iter()
                .max_by_key(|r| r.epoch)
                .unwrap();

            let shuffle_id = max_epoch_request.shuffle_id;
            let partition_id = max_epoch_request.partition_id;
            let epoch = max_epoch_request.epoch;
            let old_partition = max_epoch_request.old_partition.clone();

            // Allocate new location
            match allocate_new_location(shuffle_id, partition_id, epoch, old_partition).await {
                Ok(new_location) => {
                    // Update our location cache
                    self.update_partition_location(shuffle_id, new_location.clone());

                    // Reply to all waiting requests
                    for request in requests {
                        let _ = request.response_tx.send(Ok(new_location.clone()));
                    }

                    info!(
                        "Partition split successful for shuffle {} partition {} epoch {} -> {}",
                        shuffle_id, partition_id, epoch, new_location.location.epoch
                    );
                }
                Err(e) => {
                    error!(
                        "Failed to allocate new location for shuffle {} partition {}: {}",
                        shuffle_id, partition_id, e
                    );
                    
                    // Reply with error to all waiting requests
                    for request in requests {
                        let _ = request.response_tx.send(Err(CelebornError::Internal(
                            format!("Failed to allocate new partition location: {}", e)
                        )));
                    }
                }
            }

            // Remove from in-batch set
            if let Some(in_batch_set) = self.in_batch_partitions.write().get_mut(&shuffle_id) {
                in_batch_set.remove(&split_range);
            }
        }

        Ok(())
    }

    /// Add a change partition request to pending queue.
    pub fn add_pending_request(&self, request: ChangePartitionRequest) {
        let shuffle_id = request.shuffle_id;
        let split_range = request.old_partition.as_ref()
            .map(|p| p.get_split_range())
            .unwrap_or_else(|| request.partition_id.to_string());

        let mut pending = self.pending_requests.write();
        let shuffle_requests = pending.entry(shuffle_id).or_insert_with(HashMap::new);
        let range_requests = shuffle_requests.entry(split_range).or_insert_with(Vec::new);
        range_requests.push(request);
    }

    /// Get all partition locations for a shuffle.
    pub fn get_all_locations(&self, shuffle_id: i32) -> HashMap<i32, PartitionLocationWithSplit> {
        let shuffles = self.partition_locations.read();
        shuffles.get(&shuffle_id)
            .map(|partitions| {
                partitions.iter()
                    .filter_map(|(id, entry)| entry.get_latest().map(|loc| (*id, loc)))
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Split handler that integrates with the push flow.
pub struct SplitHandler {
    /// Partition location manager
    location_manager: Arc<PartitionLocationManager>,
    /// Maximum number of split retries
    max_split_retries: i32,
}

impl SplitHandler {
    /// Create a new split handler.
    pub fn new(location_manager: Arc<PartitionLocationManager>, max_split_retries: i32) -> Self {
        Self {
            location_manager,
            max_split_retries,
        }
    }

    /// Handle a push response that indicates split is needed.
    ///
    /// Returns the new partition location if hard split, or None if soft split.
    pub async fn handle_push_response(
        &self,
        shuffle_id: i32,
        partition_id: i32,
        epoch: i32,
        response_status: u8,
        current_location: Option<PartitionLocationWithSplit>,
    ) -> Result<Option<PartitionLocationWithSplit>> {
        let split_status = SplitStatus::try_from(response_status)?;
        
        self.location_manager.handle_split(
            shuffle_id,
            partition_id,
            epoch,
            current_location,
            split_status,
        ).await
    }

    /// Check if a response indicates split is needed.
    pub fn is_split_response(response_status: u8) -> bool {
        response_status == SplitStatus::SoftSplit as u8 || 
        response_status == SplitStatus::HardSplit as u8
    }

    /// Get the partition location manager.
    pub fn location_manager(&self) -> &Arc<PartitionLocationManager> {
        &self.location_manager
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::PartitionMode;

    fn create_test_location(id: i32, epoch: i32) -> PartitionLocation {
        PartitionLocation {
            id,
            epoch,
            host: "localhost".to_string(),
            rpc_port: 9097,
            push_port: 9098,
            fetch_port: 9099,
            replicate_port: 9100,
            mode: PartitionMode::Primary,
            peer: None,
            storage_info: None,
        }
    }

    #[test]
    fn test_split_range() {
        let range = SplitRange::new(1, 0, 10);
        assert_eq!(range.to_string(), "1_0_10");
    }

    #[test]
    fn test_partition_location_with_split() {
        let loc = create_test_location(1, 0);
        let mut loc_with_split = PartitionLocationWithSplit::new(loc);
        
        assert_eq!(loc_with_split.split_start, -1);
        assert_eq!(loc_with_split.split_end, -1);
        
        loc_with_split.set_split_range(0, 10);
        assert_eq!(loc_with_split.get_split_range(), "1_0_10");
    }

    #[test]
    fn test_partition_location_entry() {
        let entry = PartitionLocationEntry::new();
        
        // Add first location
        let loc1 = PartitionLocationWithSplit::new(create_test_location(1, 0));
        entry.add_location(loc1.clone());
        
        assert_eq!(entry.current_epoch.load(Ordering::SeqCst), 0);
        assert!(!entry.has_newer_partition(0));
        
        // Add newer location
        let loc2 = PartitionLocationWithSplit::new(create_test_location(1, 1));
        entry.add_location(loc2.clone());
        
        assert_eq!(entry.current_epoch.load(Ordering::SeqCst), 1);
        assert!(entry.has_newer_partition(0));
        assert!(!entry.has_newer_partition(1));
        
        // Get latest
        let latest = entry.get_latest().unwrap();
        assert_eq!(latest.location.epoch, 1);
        
        // Get by epoch
        let by_epoch = entry.get_by_epoch(0).unwrap();
        assert_eq!(by_epoch.location.epoch, 0);
    }

    #[tokio::test]
    async fn test_partition_location_manager() {
        let (manager, _rx) = PartitionLocationManager::new(false, 100);
        
        // Register shuffle
        manager.register_shuffle(1);
        
        // Add location
        let loc = PartitionLocationWithSplit::new(create_test_location(0, 0));
        manager.update_partition_location(1, loc);
        
        // Get latest
        let latest = manager.get_latest_location(1, 0).unwrap();
        assert_eq!(latest.location.epoch, 0);
        
        // Check newer partition
        assert!(!manager.has_newer_partition(1, 0, 0));
        
        // Add newer location
        let loc2 = PartitionLocationWithSplit::new(create_test_location(0, 1));
        manager.update_partition_location(1, loc2);
        
        assert!(manager.has_newer_partition(1, 0, 0));
        
        // Unregister shuffle
        manager.unregister_shuffle(1);
        assert!(manager.get_latest_location(1, 0).is_none());
    }

    #[test]
    fn test_split_status_conversion() {
        assert_eq!(SplitStatus::try_from(21).unwrap(), SplitStatus::HardSplit);
        assert_eq!(SplitStatus::try_from(22).unwrap(), SplitStatus::SoftSplit);
        assert!(SplitStatus::try_from(0).is_err());
    }

    #[test]
    fn test_is_split_response() {
        assert!(SplitHandler::is_split_response(21)); // HARD_SPLIT
        assert!(SplitHandler::is_split_response(22)); // SOFT_SPLIT
        assert!(!SplitHandler::is_split_response(0)); // SUCCESS
        assert!(!SplitHandler::is_split_response(1)); // Other status
    }
}
