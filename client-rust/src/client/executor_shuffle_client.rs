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

//! Executor-side ShuffleClient for Driver-Executor separation.
//!
//! This module provides a ShuffleClient implementation that runs in the Executor
//! process and communicates with the LifecycleManager in the Driver process via RPC.
//!
//! # Usage with Apache Spark Comet
//!
//! ```rust,no_run
//! use celeborn_client::{ExecutorShuffleClient, CelebornConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Configuration
//!     let config = CelebornConfig::builder()
//!         .app_id("spark-app-001")
//!         .master_endpoints(vec!["master:9097".to_string()])
//!         .build()?;
//!
//!     // Create executor shuffle client
//!     let client = ExecutorShuffleClient::new(config);
//!
//!     // Connect to LifecycleManager in Driver
//!     // (host and port are provided by Spark Driver)
//!     client.setup_lifecycle_manager_ref("driver-host", 9098).await?;
//!
//!     // Now use the client for shuffle operations
//!     let shuffle_id = 0;
//!     let data = b"shuffle data";
//!     client.push_data(shuffle_id, 0, 0, 0, data).await?;
//!
//!     Ok(())
//! }
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::client::input_stream::{
    CelebornInputStream, CelebornInputStreamBuilder, CelebornInputStreamConfig, MetricsCallback,
    NoOpMetricsCallback,
};
use crate::client::lifecycle_client::{
    LifecycleManagerClient, NettyLifecycleManagerClient, ReducerFileGroupResponse,
    RevivePartitionInfo, ReviveResponse,
};
use crate::config::{CelebornConfig, CompressionCodec};
use crate::error::{CelebornError, Result, StatusCode};
use crate::network::ConnectionPool;
use crate::network::codec::Frame;
use crate::protocol::{Encodable, PartitionLocation, PushData};
use crate::protocol::message::MessageType;

/// Shuffle state for tracking registration and partition locations.
struct ShuffleState {
    /// Number of mappers
    num_mappers: i32,
    /// Number of partitions
    num_partitions: i32,
    /// Partition locations (partition_id -> locations)
    partition_locations: DashMap<i32, Vec<PartitionLocation>>,
    /// Whether the shuffle is registered
    registered: AtomicBool,
    /// Batch ID counters per partition
    batch_id_counters: DashMap<i32, AtomicI32>,
}

impl ShuffleState {
    fn new(num_mappers: i32, num_partitions: i32) -> Self {
        Self {
            num_mappers,
            num_partitions,
            partition_locations: DashMap::new(),
            registered: AtomicBool::new(false),
            batch_id_counters: DashMap::new(),
        }
    }
}

/// Executor-side ShuffleClient for Driver-Executor separation.
///
/// This client is designed for compute engines like Apache Spark Comet where:
/// - The Driver runs in JVM with Java LifecycleManager
/// - The Executor runs Rust code (via JNI) with this ShuffleClient
///
/// The client communicates with the Driver's LifecycleManager via RPC for:
/// - Shuffle registration
/// - Partition location management
/// - Mapper end signaling
/// - Reducer file group retrieval
///
/// Data push/fetch operations go directly to Celeborn Workers.
pub struct ExecutorShuffleClient {
    /// Configuration
    config: Arc<CelebornConfig>,
    /// Application unique ID
    app_unique_id: String,
    /// LifecycleManager client (set via setup_lifecycle_manager_ref)
    lifecycle_client: RwLock<Option<Arc<dyn LifecycleManagerClient>>>,
    /// Registered shuffles
    shuffles: DashMap<i32, Arc<ShuffleState>>,
    /// Reducer file groups cache (shuffle_id -> file groups)
    reducer_file_groups: DashMap<i32, ReducerFileGroupResponse>,
    /// Excluded workers for fetch
    fetch_excluded_workers: Arc<DashMap<String, Instant>>,
    /// Connection pool for push/fetch
    connection_pool: Arc<ConnectionPool>,
    /// Whether the client is initialized
    initialized: AtomicBool,
}

impl ExecutorShuffleClient {
    /// Create a new ExecutorShuffleClient.
    ///
    /// After creation, call `setup_lifecycle_manager_ref` to connect to the
    /// Driver's LifecycleManager before using shuffle operations.
    pub fn new(config: CelebornConfig) -> Self {
        let config = Arc::new(config);
        let app_unique_id = config.app_id.clone();

        let connection_pool = Arc::new(ConnectionPool::new(
            config.connection_pool_size,
            config.max_in_flight_requests,
        ));

        Self {
            config,
            app_unique_id,
            lifecycle_client: RwLock::new(None),
            shuffles: DashMap::new(),
            reducer_file_groups: DashMap::new(),
            fetch_excluded_workers: Arc::new(DashMap::new()),
            connection_pool,
            initialized: AtomicBool::new(false),
        }
    }

    /// Setup connection to LifecycleManager in Driver.
    ///
    /// This must be called before any shuffle operations.
    ///
    /// # Arguments
    /// * `host` - LifecycleManager host (Driver host)
    /// * `port` - LifecycleManager port
    pub async fn setup_lifecycle_manager_ref(&self, host: &str, port: i32) -> Result<()> {
        info!(
            "Setting up LifecycleManager reference at {}:{}",
            host, port
        );

        let client = NettyLifecycleManagerClient::new(
            self.config.clone(),
            host.to_string(),
            port,
        );

        let client: Arc<dyn LifecycleManagerClient> = Arc::new(client);

        {
            let mut lc = self.lifecycle_client.write().await;
            *lc = Some(client);
        }

        self.initialized.store(true, Ordering::Release);

        info!("LifecycleManager reference setup complete");
        Ok(())
    }

    /// Check if the client is initialized.
    fn check_initialized(&self) -> Result<()> {
        if !self.initialized.load(Ordering::Acquire) {
            return Err(CelebornError::Internal(
                "ExecutorShuffleClient not initialized. Call setup_lifecycle_manager_ref first."
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// Get the LifecycleManager client.
    async fn get_lifecycle_client(&self) -> Result<Arc<dyn LifecycleManagerClient>> {
        self.check_initialized()?;
        let client = self.lifecycle_client.read().await;
        client.clone().ok_or_else(|| {
            CelebornError::Internal("LifecycleManager client not set".to_string())
        })
    }

    /// Get the application ID.
    pub fn app_id(&self) -> &str {
        &self.app_unique_id
    }

    /// Register a shuffle.
    ///
    /// This sends a RegisterShuffle RPC to the Driver's LifecycleManager.
    /// If the shuffle is already registered (by Java LifecycleManager), it will
    /// fetch the partition locations from the LifecycleManager instead.
    pub async fn register_shuffle(
        &self,
        shuffle_id: i32,
        num_mappers: i32,
        num_partitions: i32,
    ) -> Result<i32> {
        // Check if already registered locally
        if self.shuffles.contains_key(&shuffle_id) {
            return Ok(shuffle_id);
        }

        let client = self.get_lifecycle_client().await?;

        info!(
            "Registering shuffle {} with {} mappers and {} partitions",
            shuffle_id, num_mappers, num_partitions
        );

        let response = client
            .register_shuffle(shuffle_id, num_mappers, num_partitions)
            .await?;

        info!(
            "Received register_shuffle response for shuffle {}: status={:?}, partition_locations_count={}",
            shuffle_id, response.status, response.partition_locations.len()
        );

        // Store shuffle state
        let state = Arc::new(ShuffleState::new(num_mappers, num_partitions));

        if response.status.is_success() || response.status == StatusCode::ShuffleAlreadyRegistered {
            // Store partition locations from register response
            // Note: When shuffle is already registered, Java LifecycleManager returns SUCCESS
            // with partition locations, not ShuffleAlreadyRegistered
            for (partition_id, locations) in response.partition_locations {
                state.partition_locations.insert(partition_id, locations);
            }
            
            if response.status == StatusCode::ShuffleAlreadyRegistered {
                info!(
                    "Shuffle {} already registered, using partition locations from response",
                    shuffle_id
                );
            }
        } else {
            return Err(CelebornError::ServerError {
                status: response.status,
                message: format!("Failed to register shuffle {}", shuffle_id),
            });
        }

        state.registered.store(true, Ordering::Release);
        self.shuffles.insert(shuffle_id, state);

        info!("Shuffle {} registered successfully", shuffle_id);
        Ok(shuffle_id)
    }

    /// Push shuffle data.
    ///
    /// Data is pushed directly to Celeborn Workers.
    pub async fn push_data(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        partition_id: i32,
        data: &[u8],
    ) -> Result<()> {
        self.check_initialized()?;

        // Ensure shuffle is registered
        let state = self.shuffles.get(&shuffle_id).ok_or_else(|| {
            CelebornError::ShuffleNotFound(shuffle_id)
        })?;

        // Get partition location
        let locations = state
            .partition_locations
            .get(&partition_id)
            .map(|v| v.clone())
            .ok_or_else(|| CelebornError::PartitionNotFound {
                shuffle_id,
                partition_id,
            })?;

        if locations.is_empty() {
            return Err(CelebornError::PartitionNotFound {
                shuffle_id,
                partition_id,
            });
        }

        let location = &locations[0];
        let shuffle_key = self.shuffle_key(shuffle_id);
        let partition_unique_id = location.unique_id();

        // Compress data if needed
        let compressed_data = self.compress_data(data)?;

        // Build body with batch header: mapId (4) + attemptId (4) + batchId (4) + compressedTotalSize (4) + data
        // Note: Use little-endian to match Java client's Platform.putInt which uses native (little) endian on x86/x64
        let batch_id = self.next_batch_id(shuffle_id, partition_id);
        let compressed_size = compressed_data.len() as i32;

        let mut body_with_header = BytesMut::with_capacity(16 + compressed_data.len());
        body_with_header.put_i32_le(map_id);
        body_with_header.put_i32_le(attempt_id);
        body_with_header.put_i32_le(batch_id);
        body_with_header.put_i32_le(compressed_size);
        body_with_header.put_slice(&compressed_data);
        let body_bytes = body_with_header.freeze();
        
        // Debug: write to file for debugging
        use std::io::Write;
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("/tmp/celeborn_rust_debug.log")
        {
            let _ = writeln!(file, "[CELEBORN-RUST] push_data: map_id={}, attempt_id={}, batch_id={}, compressed_size={}",
                map_id, attempt_id, batch_id, compressed_size);
            let _ = writeln!(file, "[CELEBORN-RUST] Batch header (first 16 bytes): {:02x?}", &body_bytes[..16]);
        }

        // Send push data to worker
        self.send_push_data(&shuffle_key, &partition_unique_id, location, body_bytes)
            .await
    }

    /// Send push data to a worker and wait for response.
    ///
    /// This method sends PushData to the Worker and waits for an RpcResponse.
    /// The response contains a status code that indicates the result:
    /// - SUCCESS (0): Data was successfully written
    /// - SOFT_SPLIT (22): Data was written but partition needs revive
    /// - HARD_SPLIT (21): Data was not written, need to revive and retry
    /// - MAP_ENDED (15): Mapper has already ended
    /// - Other error codes indicate various failure conditions
    async fn send_push_data(
        &self,
        shuffle_key: &str,
        partition_unique_id: &str,
        location: &PartitionLocation,
        data: Bytes,
    ) -> Result<()> {
        use std::sync::atomic::AtomicI64;
        use std::time::Duration;
        static REQUEST_ID_COUNTER: AtomicI64 = AtomicI64::new(1);
        
        let request_id = REQUEST_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        let mode = location.mode as u8;

        let push_data = PushData::new(
            request_id,
            mode,
            shuffle_key.to_string(),
            partition_unique_id.to_string(),
            data.clone(),
        );

        // Get connection to worker
        let addr: SocketAddr = location
            .push_address()
            .parse()
            .map_err(|e| CelebornError::Connection(format!("Invalid address: {}", e)))?;

        let conn = self.connection_pool.get_connection(addr).await?;

        // Encode message and send
        let data_len = data.len();
        let message_buf = push_data.encode_to_bytes();
        let frame = Frame::with_body(MessageType::PushData, message_buf.freeze(), data);

        // Send and wait for response
        let timeout_duration = self.config.push_timeout;
        let response = conn.send_push_data(frame, request_id, timeout_duration).await?;

        // Parse response status code
        // Response format: RpcResponse with body containing status code (1 byte)
        let status_code = if !response.body.is_empty() {
            StatusCode::from(response.body[0] as i32)
        } else if response.message.len() > 12 {
            // Status might be in the message body after requestId (8) + bodySize (4)
            StatusCode::from(response.message[12] as i32)
        } else {
            StatusCode::Success
        };

        // Debug log
        debug!(
            "PushData response: request_id={}, status={:?}, partition={}",
            request_id, status_code, partition_unique_id
        );

        // Handle response status
        match status_code {
            StatusCode::Success => {
                debug!(
                    "Pushed {} bytes to partition {} on {}",
                    data_len,
                    partition_unique_id,
                    location.push_address()
                );
                Ok(())
            }
            StatusCode::SoftSplit => {
                // Data was written but partition needs revive
                // For now, we treat this as success and let the caller handle revive
                info!(
                    "PushData returned SOFT_SPLIT for partition {}, data was written",
                    partition_unique_id
                );
                Ok(())
            }
            StatusCode::HardSplit => {
                // Data was NOT written, need to revive and retry
                // Return an error so the caller can handle revive and retry
                Err(CelebornError::ServerError {
                    status: StatusCode::HardSplit,
                    message: format!(
                        "HARD_SPLIT for partition {}, need to revive and retry",
                        partition_unique_id
                    ),
                })
            }
            StatusCode::MapEnded => {
                // Mapper has already ended, this is expected for speculative tasks
                info!(
                    "PushData returned MAP_ENDED for partition {}, mapper already finished",
                    partition_unique_id
                );
                Ok(())
            }
            StatusCode::PushDataSuccessPrimaryCongested | StatusCode::PushDataSuccessReplicaCongested => {
                // Data was written but worker is congested
                // For now, treat as success but could implement backpressure
                debug!(
                    "PushData returned congested status {:?} for partition {}",
                    status_code, partition_unique_id
                );
                Ok(())
            }
            _ => {
                // Other error status codes
                Err(CelebornError::ServerError {
                    status: status_code,
                    message: format!(
                        "PushData failed with status {:?} for partition {}",
                        status_code, partition_unique_id
                    ),
                })
            }
        }
    }

    /// Compress data using the configured codec.
    fn compress_data(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.config.compression_codec {
            CompressionCodec::None => Ok(data.to_vec()),
            CompressionCodec::Lz4 => {
                #[cfg(feature = "compression-lz4")]
                {
                    Ok(lz4_flex::compress_prepend_size(data))
                }
                #[cfg(not(feature = "compression-lz4"))]
                {
                    Ok(data.to_vec())
                }
            }
            CompressionCodec::Zstd => {
                #[cfg(feature = "compression-zstd")]
                {
                    zstd::encode_all(data, 3).map_err(|e| {
                        CelebornError::Compression(format!("Zstd compression failed: {}", e))
                    })
                }
                #[cfg(not(feature = "compression-zstd"))]
                {
                    Ok(data.to_vec())
                }
            }
        }
    }

    /// Signal that a mapper has finished.
    ///
    /// This sends a MapperEnd RPC to the Driver's LifecycleManager.
    pub async fn mapper_end(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
    ) -> Result<bool> {
        let client = self.get_lifecycle_client().await?;

        debug!(
            "MapperEnd: shuffle={}, map={}, attempt={}",
            shuffle_id, map_id, attempt_id
        );

        let response = client
            .mapper_end(
                shuffle_id,
                map_id,
                attempt_id,
                num_mappers,
                -1, // partition_id for ReducePartition type
                HashMap::new(),
            )
            .await?;

        Ok(response.status.is_success())
    }

    /// Read partition data.
    ///
    /// This creates a CelebornInputStream for reading shuffle data.
    pub async fn read_partition(
        &self,
        shuffle_id: i32,
        partition_id: i32,
        attempt_number: i32,
        start_map_index: i32,
        end_map_index: i32,
    ) -> Result<CelebornInputStream> {
        self.read_partition_with_callback(
            shuffle_id,
            partition_id,
            attempt_number,
            start_map_index,
            end_map_index,
            Arc::new(NoOpMetricsCallback),
        )
        .await
    }

    /// Read partition data with metrics callback.
    pub async fn read_partition_with_callback(
        &self,
        shuffle_id: i32,
        partition_id: i32,
        attempt_number: i32,
        start_map_index: i32,
        end_map_index: i32,
        metrics_callback: Arc<dyn MetricsCallback>,
    ) -> Result<CelebornInputStream> {
        let client = self.get_lifecycle_client().await?;

        // Get reducer file groups (cached)
        let file_groups = if let Some(cached) = self.reducer_file_groups.get(&shuffle_id) {
            cached.clone()
        } else {
            let response = client.get_reducer_file_group(shuffle_id).await?;
            if !response.status.is_success() {
                return Err(CelebornError::ServerError {
                    status: response.status,
                    message: format!(
                        "Failed to get reducer file group for shuffle {}",
                        shuffle_id
                    ),
                });
            }
            self.reducer_file_groups.insert(shuffle_id, response.clone());
            response
        };

        // Get locations for this partition
        let locations = file_groups
            .file_groups
            .get(&partition_id)
            .cloned()
            .unwrap_or_default();

        if locations.is_empty() {
            // Return empty stream
            return Ok(CelebornInputStream::empty());
        }

        // Build CelebornInputStream
        let config = CelebornInputStreamConfig::from(self.config.as_ref());
        let shuffle_key = format!("{}-{}", self.app_unique_id, shuffle_id);

        let stream = CelebornInputStreamBuilder::new()
            .config(config)
            .connection_pool(self.connection_pool.clone())
            .shuffle_key(&shuffle_key)
            .locations(locations)
            .attempts(file_groups.attempts.clone())
            .attempt_number(attempt_number)
            .start_map_index(start_map_index)
            .end_map_index(end_map_index)
            .excluded_workers(self.fetch_excluded_workers.clone())
            .metrics_callback(metrics_callback)
            .build()
            .await?;

        Ok(stream)
    }

    /// Get reducer file groups.
    pub async fn get_reducer_file_group(
        &self,
        shuffle_id: i32,
    ) -> Result<HashMap<i32, Vec<PartitionLocation>>> {
        let client = self.get_lifecycle_client().await?;

        let response = client.get_reducer_file_group(shuffle_id).await?;

        if !response.status.is_success() {
            return Err(CelebornError::ServerError {
                status: response.status,
                message: format!(
                    "Failed to get reducer file group for shuffle {}",
                    shuffle_id
                ),
            });
        }

        Ok(response.file_groups)
    }

    /// Cleanup shuffle state.
    pub fn cleanup(&self, shuffle_id: i32, _map_id: i32, _attempt_id: i32) {
        // Remove local state
        // The actual cleanup is handled by LifecycleManager in Driver
        debug!("Cleanup called for shuffle {}", shuffle_id);
    }

    /// Cleanup shuffle.
    pub fn cleanup_shuffle(&self, shuffle_id: i32) -> bool {
        self.shuffles.remove(&shuffle_id);
        self.reducer_file_groups.remove(&shuffle_id);
        true
    }

    /// Get shuffle ID for app shuffle.
    pub async fn get_shuffle_id(
        &self,
        app_shuffle_id: i32,
        app_shuffle_identifier: &str,
        is_writer: bool,
    ) -> Result<i32> {
        let client = self.get_lifecycle_client().await?;
        client
            .get_shuffle_id(app_shuffle_id, app_shuffle_identifier, is_writer)
            .await
    }

    /// Report shuffle fetch failure.
    pub async fn report_shuffle_fetch_failure(
        &self,
        app_shuffle_id: i32,
        shuffle_id: i32,
        failure_type: i32,
    ) -> Result<bool> {
        let client = self.get_lifecycle_client().await?;
        client
            .report_shuffle_fetch_failure(app_shuffle_id, shuffle_id, failure_type)
            .await
    }

    /// Shutdown the client.
    pub async fn shutdown(&self) {
        info!("Shutting down ExecutorShuffleClient");

        // Clear all state
        self.shuffles.clear();
        self.reducer_file_groups.clear();
        self.fetch_excluded_workers.clear();

        self.initialized.store(false, Ordering::Release);
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

    /// Get the next batch ID for a partition.
    pub fn next_batch_id(&self, shuffle_id: i32, partition_id: i32) -> i32 {
        if let Some(state) = self.shuffles.get(&shuffle_id) {
            let counter = state
                .batch_id_counters
                .entry(partition_id)
                .or_insert_with(|| AtomicI32::new(0));
            counter.fetch_add(1, Ordering::Relaxed)
        } else {
            0
        }
    }

    /// Generate shuffle key.
    pub fn shuffle_key(&self, shuffle_id: i32) -> String {
        format!("{}-{}", self.app_unique_id, shuffle_id)
    }
}

/// ReviveManager for Executor-side that uses LifecycleManagerClient for RPC.
///
/// This is separate from the base ReviveManager in revive.rs, as it communicates
/// with the remote LifecycleManager in the Driver process.
pub struct ExecutorReviveManager {
    /// Configuration
    #[allow(dead_code)]
    config: Arc<CelebornConfig>,
    /// LifecycleManager client
    lifecycle_client: Arc<dyn LifecycleManagerClient>,
}

impl ExecutorReviveManager {
    /// Create a new ExecutorReviveManager.
    pub fn new(
        config: Arc<CelebornConfig>,
        lifecycle_client: Arc<dyn LifecycleManagerClient>,
    ) -> Self {
        Self {
            config,
            lifecycle_client,
        }
    }

    /// Revive a single partition.
    pub async fn revive_single(
        &self,
        shuffle_id: i32,
        map_id: i32,
        _attempt_id: i32,
        partition_id: i32,
        epoch: i32,
        old_partition: Option<&PartitionLocation>,
        cause: StatusCode,
    ) -> Result<PartitionLocation> {
        let info = RevivePartitionInfo {
            partition_id,
            epoch,
            old_partition: old_partition.cloned(),
            status: cause,
        };

        let response = self
            .lifecycle_client
            .revive(shuffle_id, vec![map_id], vec![info])
            .await?;

        if !response.status.is_success() {
            return Err(CelebornError::ServerError {
                status: response.status,
                message: format!("Revive failed for partition {}", partition_id),
            });
        }

        response
            .partition_locations
            .into_iter()
            .next()
            .ok_or_else(|| CelebornError::PartitionNotFound {
                shuffle_id,
                partition_id,
            })
    }

    /// Revive multiple partitions.
    pub async fn revive_batch(
        &self,
        shuffle_id: i32,
        map_ids: Vec<i32>,
        partition_infos: Vec<RevivePartitionInfo>,
    ) -> Result<ReviveResponse> {
        self.lifecycle_client
            .revive(shuffle_id, map_ids, partition_infos)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shuffle_state() {
        let state = ShuffleState::new(10, 100);
        assert_eq!(state.num_mappers, 10);
        assert_eq!(state.num_partitions, 100);
        assert!(!state.registered.load(Ordering::Relaxed));
    }

    #[test]
    fn test_shuffle_key() {
        let config = CelebornConfig::builder()
            .app_id("test-app")
            .master_endpoints(vec!["localhost:9097".to_string()])
            .build()
            .unwrap();

        let client = ExecutorShuffleClient::new(config);
        assert_eq!(client.shuffle_key(0), "test-app-0");
        assert_eq!(client.shuffle_key(123), "test-app-123");
    }

    #[test]
    fn test_not_initialized() {
        let config = CelebornConfig::builder()
            .app_id("test-app")
            .master_endpoints(vec!["localhost:9097".to_string()])
            .build()
            .unwrap();

        let client = ExecutorShuffleClient::new(config);
        assert!(client.check_initialized().is_err());
    }
}
