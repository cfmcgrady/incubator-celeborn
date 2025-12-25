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

//! Data pusher for sending shuffle data to workers.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use tokio::sync::Semaphore;
use tracing::{debug, trace, warn};

use crate::client::lifecycle::LifecycleManager;
use crate::client::revive::ReviveManager;
use crate::config::{CelebornConfig, CompressionCodec};
use crate::error::{CelebornError, Result, StatusCode};
use crate::network::codec::Frame;
use crate::network::{ConnectionPool, TransportClient};
use crate::protocol::message::{MessageType, PushData, PushMergedData};
use crate::protocol::{Encodable, PartitionLocation};

/// Request ID counter.
static REQUEST_ID_COUNTER: AtomicI64 = AtomicI64::new(1);

/// Generate a new request ID.
fn next_request_id() -> i64 {
    REQUEST_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Data pusher for sending shuffle data to Celeborn workers.
pub struct DataPusher {
    /// Configuration
    config: Arc<CelebornConfig>,
    /// Transport client
    transport_client: Arc<TransportClient>,
    /// Lifecycle manager
    lifecycle_manager: Arc<LifecycleManager>,
    /// Revive manager for handling push failures
    revive_manager: Option<Arc<ReviveManager>>,
    /// Connection pool for push connections
    push_connection_pool: ConnectionPool,
    /// Pending push buffers (partition_id -> buffer)
    pending_buffers: DashMap<String, PushBuffer>,
    /// In-flight request semaphore
    in_flight_semaphore: Arc<Semaphore>,
    /// Maximum number of revive retries
    max_revive_retries: u32,
}

/// Buffer for accumulating push data.
struct PushBuffer {
    /// Shuffle key
    shuffle_key: String,
    /// Partition unique ID
    partition_unique_id: String,
    /// Partition location
    location: PartitionLocation,
    /// Buffered data
    data: BytesMut,
    /// Maximum buffer size
    max_size: usize,
}

impl PushBuffer {
    fn new(
        shuffle_key: String,
        partition_unique_id: String,
        location: PartitionLocation,
        max_size: usize,
    ) -> Self {
        Self {
            shuffle_key,
            partition_unique_id,
            location,
            data: BytesMut::with_capacity(max_size),
            max_size,
        }
    }

    fn is_full(&self) -> bool {
        self.data.len() >= self.max_size
    }

    fn remaining_capacity(&self) -> usize {
        self.max_size.saturating_sub(self.data.len())
    }

    fn append(&mut self, data: &[u8]) {
        self.data.put_slice(data);
    }

    fn take_data(&mut self) -> Bytes {
        std::mem::replace(&mut self.data, BytesMut::with_capacity(self.max_size)).freeze()
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl DataPusher {
    /// Create a new data pusher.
    pub fn new(
        config: Arc<CelebornConfig>,
        transport_client: Arc<TransportClient>,
        lifecycle_manager: Arc<LifecycleManager>,
    ) -> Self {
        let push_connection_pool = ConnectionPool::new(
            config.connection_pool_size,
            config.max_in_flight_requests,
        );

        let in_flight_semaphore = Arc::new(Semaphore::new(config.max_in_flight_requests * 4));
        let max_revive_retries = config.max_retries;

        Self {
            config,
            transport_client,
            lifecycle_manager,
            revive_manager: None,
            push_connection_pool,
            pending_buffers: DashMap::new(),
            in_flight_semaphore,
            max_revive_retries,
        }
    }

    /// Create a new data pusher with revive manager.
    pub fn with_revive_manager(
        config: Arc<CelebornConfig>,
        transport_client: Arc<TransportClient>,
        lifecycle_manager: Arc<LifecycleManager>,
        revive_manager: Arc<ReviveManager>,
    ) -> Self {
        let push_connection_pool = ConnectionPool::new(
            config.connection_pool_size,
            config.max_in_flight_requests,
        );

        let in_flight_semaphore = Arc::new(Semaphore::new(config.max_in_flight_requests * 4));
        let max_revive_retries = config.max_retries;

        Self {
            config,
            transport_client,
            lifecycle_manager,
            revive_manager: Some(revive_manager),
            push_connection_pool,
            pending_buffers: DashMap::new(),
            in_flight_semaphore,
            max_revive_retries,
        }
    }

    /// Push data to a partition with automatic revive on failure.
    ///
    /// The data body format expected by Celeborn Worker is:
    /// - mapId: 4 bytes (int)
    /// - attemptId: 4 bytes (int)
    /// - batchId: 4 bytes (int)
    /// - compressedTotalSize: 4 bytes (int)
    /// - data: remaining bytes
    pub async fn push_data(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        partition_id: i32,
        data: &[u8],
    ) -> Result<()> {
        self.push_data_with_retry(shuffle_id, map_id, attempt_id, partition_id, data, 0).await
    }

    /// Push data with retry logic.
    fn push_data_with_retry<'a>(
        &'a self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        partition_id: i32,
        data: &'a [u8],
        retry_count: u32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
        // Get partition location
        let locations = self
            .lifecycle_manager
            .get_partition_location(shuffle_id, partition_id);

        let location = match locations {
            Ok(locs) if !locs.is_empty() => locs[0].clone(),
            _ => {
                // No location available, try to revive
                if let Some(ref revive_manager) = self.revive_manager {
                    debug!(
                        "No location for partition {}, attempting revive",
                        partition_id
                    );
                    revive_manager
                        .revive_single(
                            shuffle_id,
                            map_id,
                            attempt_id,
                            partition_id,
                            -1,
                            None,
                            StatusCode::PushDataFailNonCriticalCause,
                        )
                        .await?
                } else {
                    return Err(CelebornError::PartitionNotFound {
                        shuffle_id,
                        partition_id,
                    });
                }
            }
        };

        let shuffle_key = self.lifecycle_manager.shuffle_key(shuffle_id);
        let partition_unique_id = location.unique_id();
        let buffer_key = format!("{}-{}", shuffle_key, partition_unique_id);

        // Compress data if needed
        let compressed_data = self.compress_data(data)?;

        // Build body with batch header: mapId (4) + attemptId (4) + batchId (4) + compressedTotalSize (4) + data
        // BATCH_HEADER_SIZE = 16 bytes
        // Note: Java's Platform.getInt uses native endian (little-endian on x86/x64),
        // so we must use little-endian encoding for the batch header.
        let batch_id = self.lifecycle_manager.next_batch_id(shuffle_id, partition_id);
        let compressed_size = compressed_data.len() as i32;
        
        let mut body_with_header = BytesMut::with_capacity(16 + compressed_data.len());
        body_with_header.put_i32_le(map_id);
        body_with_header.put_i32_le(attempt_id);
        body_with_header.put_i32_le(batch_id);
        body_with_header.put_i32_le(compressed_size);
        body_with_header.put_slice(&compressed_data);
        let body_bytes = body_with_header.freeze();

        // Check if we need to flush existing buffer
        let should_flush = {
            if let Some(buffer) = self.pending_buffers.get(&buffer_key) {
                buffer.remaining_capacity() < body_bytes.len()
            } else {
                false
            }
        };

        if should_flush {
            self.flush_buffer(&buffer_key).await?;
        }

        // Add to buffer or send directly
        if body_bytes.len() >= self.config.push_buffer_size {
            // Send directly for large data
            let result = self
                .send_push_data_with_revive(
                    shuffle_id,
                    map_id,
                    attempt_id,
                    partition_id,
                    &shuffle_key,
                    &partition_unique_id,
                    &location,
                    body_bytes.clone(),
                    retry_count,
                )
                .await;

            if let Err(ref e) = result {
                // Check if we should retry with revive
                if self.should_retry_with_revive(e) && retry_count < self.max_revive_retries {
                    warn!(
                        "Push failed for partition {}, attempting revive (retry {})",
                        partition_id,
                        retry_count + 1
                    );
                    return self
                        .push_data_with_retry(
                            shuffle_id,
                            map_id,
                            attempt_id,
                            partition_id,
                            data,
                            retry_count + 1,
                        )
                        .await;
                }
            }
            result
        } else {
            // Buffer small data
            let mut buffer = self.pending_buffers.entry(buffer_key.clone()).or_insert_with(|| {
                PushBuffer::new(
                    shuffle_key.clone(),
                    partition_unique_id.clone(),
                    location.clone(),
                    self.config.push_buffer_size,
                )
            });
            buffer.append(&body_bytes);

            // Flush if buffer is full
            if buffer.is_full() {
                drop(buffer);
                self.flush_buffer(&buffer_key).await?;
            }
            Ok(())
        }
        })
    }

    /// Check if an error should trigger a revive retry.
    fn should_retry_with_revive(&self, error: &CelebornError) -> bool {
        matches!(
            error,
            CelebornError::Connection(_)
                | CelebornError::Timeout(_)
                | CelebornError::WorkerUnavailable { .. }
                | CelebornError::PushFailed(_)
        )
    }

    /// Send push data with revive support.
    async fn send_push_data_with_revive(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        partition_id: i32,
        shuffle_key: &str,
        partition_unique_id: &str,
        location: &PartitionLocation,
        data: Bytes,
        retry_count: u32,
    ) -> Result<()> {
        let result = self
            .send_push_data(shuffle_key, partition_unique_id, location, data.clone())
            .await;

        match result {
            Ok(()) => Ok(()),
            Err(e) if self.should_retry_with_revive(&e) && retry_count < self.max_revive_retries => {
                // Try to revive the partition
                if let Some(ref revive_manager) = self.revive_manager {
                    warn!(
                        "Push to {}:{} failed, attempting revive: {}",
                        location.host, location.push_port, e
                    );

                    let cause = match &e {
                        CelebornError::Connection(_) => StatusCode::PushDataCreateConnectionFailPrimary,
                        CelebornError::Timeout(_) => StatusCode::PushDataTimeoutPrimary,
                        _ => StatusCode::PushDataFailPrimary,
                    };

                    match revive_manager
                        .revive_single(
                            shuffle_id,
                            map_id,
                            attempt_id,
                            partition_id,
                            location.epoch,
                            Some(location),
                            cause,
                        )
                        .await
                    {
                        Ok(new_location) => {
                            debug!(
                                "Revive successful, new location: {}:{}",
                                new_location.host, new_location.push_port
                            );
                            // Retry with new location
                            let new_unique_id = new_location.unique_id();
                            self.send_push_data(shuffle_key, &new_unique_id, &new_location, data)
                                .await
                        }
                        Err(revive_err) => {
                            warn!("Revive failed: {}", revive_err);
                            Err(e)
                        }
                    }
                } else {
                    Err(e)
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Push merged data to multiple partitions.
    pub async fn push_merged_data(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        partition_data: &[(i32, &[u8])],
    ) -> Result<()> {
        // Group by worker
        let mut worker_data: HashMap<String, Vec<(PartitionLocation, Bytes)>> = HashMap::new();

        for (partition_id, data) in partition_data {
            let locations = self
                .lifecycle_manager
                .get_partition_location(shuffle_id, *partition_id)?;

            if locations.is_empty() {
                continue;
            }

            let location = &locations[0];
            let worker_key = location.push_address();
            let compressed = self.compress_data(data)?;

            worker_data
                .entry(worker_key)
                .or_insert_with(Vec::new)
                .push((location.clone(), Bytes::from(compressed)));
        }

        // Send to each worker
        let shuffle_key = self.lifecycle_manager.shuffle_key(shuffle_id);
        for (worker_addr, partitions) in worker_data {
            self.send_merged_push_data(&shuffle_key, &partitions).await?;
        }

        Ok(())
    }

    /// Flush all pending buffers.
    pub async fn flush(&self) -> Result<()> {
        let keys: Vec<String> = self.pending_buffers.iter().map(|e| e.key().clone()).collect();
        
        for key in keys {
            self.flush_buffer(&key).await?;
        }

        Ok(())
    }

    /// Flush a specific buffer.
    async fn flush_buffer(&self, buffer_key: &str) -> Result<()> {
        if let Some((_, mut buffer)) = self.pending_buffers.remove(buffer_key) {
            if !buffer.is_empty() {
                let data = buffer.take_data();
                self.send_push_data(
                    &buffer.shuffle_key,
                    &buffer.partition_unique_id,
                    &buffer.location,
                    data,
                )
                .await?;
            }
        }
        Ok(())
    }

    /// Send push data to a worker.
    async fn send_push_data(
        &self,
        shuffle_key: &str,
        partition_unique_id: &str,
        location: &PartitionLocation,
        data: Bytes,
    ) -> Result<()> {
        let _permit = self
            .in_flight_semaphore
            .acquire()
            .await
            .map_err(|_| CelebornError::Internal("Semaphore closed".to_string()))?;

        let request_id = next_request_id();
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

        let conn = self.push_connection_pool.get_connection(addr).await?;

        // Encode message (without body) and send with body as separate frame part
        let message_buf = push_data.encode_to_bytes();
        let frame = Frame::with_body(MessageType::PushData, message_buf.freeze(), data.clone());

        conn.send_one_way(frame).await?;

        // Update metrics
        self.lifecycle_manager.add_bytes_written(data.len() as i64);
        
        // Mark partition as written (for commit)
        // Note: In strict mode, we might wait for ack, but for now we assume send success means write success
        // or at least we track it as attempted.
        // Also note: partition_unique_id passed here is just uniqueId, but committed_ids tracks uniqueId
        self.lifecycle_manager.add_partition_data_pushed(
            // We need shuffle_id, but it's not passed directly, parsed from shuffle_key
            shuffle_key.split('-').last().unwrap_or("0").parse().unwrap_or(0),
            partition_unique_id
        );

        trace!(
            "Pushed {} bytes to partition {} on {}",
            data.len(),
            partition_unique_id,
            location.push_address()
        );

        Ok(())
    }

    /// Send merged push data to a worker.
    async fn send_merged_push_data(
        &self,
        shuffle_key: &str,
        partitions: &[(PartitionLocation, Bytes)],
    ) -> Result<()> {
        if partitions.is_empty() {
            return Ok(());
        }

        let _permit = self
            .in_flight_semaphore
            .acquire()
            .await
            .map_err(|_| CelebornError::Internal("Semaphore closed".to_string()))?;

        let request_id = next_request_id();
        let mode = partitions[0].0.mode as u8;

        // Build merged data
        let mut partition_unique_ids = Vec::with_capacity(partitions.len());
        let mut batch_offsets = Vec::with_capacity(partitions.len() + 1);
        let mut total_size = 0usize;

        batch_offsets.push(0);
        for (location, data) in partitions {
            partition_unique_ids.push(location.unique_id());
            total_size += data.len();
            batch_offsets.push(total_size as i32);
        }

        let mut combined_data = BytesMut::with_capacity(total_size);
        for (_, data) in partitions {
            combined_data.put_slice(data);
        }

        // Clone IDs for tracking before moving them into the message
        let ids_to_track = partition_unique_ids.clone();

        let push_merged = PushMergedData {
            request_id,
            mode,
            shuffle_key: shuffle_key.to_string(),
            partition_unique_ids,
            batch_offsets,
            body: combined_data.freeze(),
        };

        // Get connection to worker
        let addr: SocketAddr = partitions[0]
            .0
            .push_address()
            .parse()
            .map_err(|e| CelebornError::Connection(format!("Invalid address: {}", e)))?;

        let conn = self.push_connection_pool.get_connection(addr).await?;

        // Encode message (without body) and send with body as separate frame part
        let body = push_merged.body.clone();
        let message_buf = push_merged.encode_to_bytes();
        let frame = Frame::with_body(MessageType::PushMergedData, message_buf.freeze(), body);

        conn.send_one_way(frame).await?;

        // Update metrics
        self.lifecycle_manager.add_bytes_written(total_size as i64);
        
        // Mark partitions as written
        let shuffle_id_int = shuffle_key.split('-').last().unwrap_or("0").parse().unwrap_or(0);
        for id in &ids_to_track {
             self.lifecycle_manager.add_partition_data_pushed(shuffle_id_int, id);
        }

        debug!(
            "Pushed merged data ({} partitions, {} bytes) to {}",
            partitions.len(),
            total_size,
            partitions[0].0.push_address()
        );

        Ok(())
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
                    // Fallback: no compression
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
                    // Fallback: no compression
                    Ok(data.to_vec())
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_buffer() {
        let location = PartitionLocation::new(
            0, 0, "localhost".to_string(), 9097, 9098, 9099, 9100,
        );
        let mut buffer = PushBuffer::new(
            "app-1".to_string(),
            "0-0".to_string(),
            location,
            100,
        );

        assert!(buffer.is_empty());
        assert!(!buffer.is_full());
        assert_eq!(buffer.remaining_capacity(), 100);

        buffer.append(&[1, 2, 3, 4, 5]);
        assert!(!buffer.is_empty());
        assert_eq!(buffer.remaining_capacity(), 95);

        let data = buffer.take_data();
        assert_eq!(data.as_ref(), &[1, 2, 3, 4, 5]);
        assert!(buffer.is_empty());
    }
}
