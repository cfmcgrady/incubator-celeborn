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
use tracing::{debug, trace};

use crate::client::lifecycle::LifecycleManager;
use crate::config::{CelebornConfig, CompressionCodec};
use crate::error::{CelebornError, Result};
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
    /// Connection pool for push connections
    push_connection_pool: ConnectionPool,
    /// Pending push buffers (partition_id -> buffer)
    pending_buffers: DashMap<String, PushBuffer>,
    /// In-flight request semaphore
    in_flight_semaphore: Arc<Semaphore>,
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

        Self {
            config,
            transport_client,
            lifecycle_manager,
            push_connection_pool,
            pending_buffers: DashMap::new(),
            in_flight_semaphore,
        }
    }

    /// Push data to a partition.
    pub async fn push_data(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        partition_id: i32,
        data: &[u8],
    ) -> Result<()> {
        // Get partition location
        let locations = self
            .lifecycle_manager
            .get_partition_location(shuffle_id, partition_id)?;

        if locations.is_empty() {
            return Err(CelebornError::PartitionNotFound {
                shuffle_id,
                partition_id,
            });
        }

        // Use the first (primary) location
        let location = &locations[0];
        let shuffle_key = self.lifecycle_manager.shuffle_key(shuffle_id);
        let partition_unique_id = location.unique_id();
        let buffer_key = format!("{}-{}", shuffle_key, partition_unique_id);

        // Compress data if needed
        let compressed_data = self.compress_data(data)?;

        // Check if we need to flush existing buffer
        let should_flush = {
            if let Some(buffer) = self.pending_buffers.get(&buffer_key) {
                buffer.remaining_capacity() < compressed_data.len()
            } else {
                false
            }
        };

        if should_flush {
            self.flush_buffer(&buffer_key).await?;
        }

        // Add to buffer or send directly
        if compressed_data.len() >= self.config.push_buffer_size {
            // Send directly for large data
            self.send_push_data(
                &shuffle_key,
                &partition_unique_id,
                location,
                Bytes::copy_from_slice(&compressed_data),
            )
            .await?;
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
            buffer.append(&compressed_data);

            // Flush if buffer is full
            if buffer.is_full() {
                drop(buffer);
                self.flush_buffer(&buffer_key).await?;
            }
        }

        Ok(())
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

        // Encode and send
        let mut buf = push_data.encode_to_bytes();
        let frame = Frame::new(MessageType::PushData, buf.freeze().slice(1..));

        conn.send_one_way(frame).await?;

        // Update metrics
        self.lifecycle_manager.add_bytes_written(data.len() as i64);

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

        // Encode and send
        let mut buf = push_merged.encode_to_bytes();
        let frame = Frame::new(MessageType::PushMergedData, buf.freeze().slice(1..));

        conn.send_one_way(frame).await?;

        // Update metrics
        self.lifecycle_manager.add_bytes_written(total_size as i64);

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
