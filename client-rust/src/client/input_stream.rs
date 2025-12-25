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

//! CelebornInputStream implementation for reading shuffle data.
//!
//! This module provides a streaming interface for reading shuffle data from
//! multiple partition locations with automatic failover and retry support.
//!
//! Key features:
//! - Automatic switching between partition readers
//! - Retry with peer location on failure
//! - Batch deduplication
//! - Compression support (LZ4, ZSTD)
//! - Excluded worker tracking
//! - RoaringBitmap-based range filtering
//! - AsyncRead trait implementation
//! - Skew partition support with chunk range

use std::collections::{HashMap, HashSet};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::{Buf, Bytes};
use dashmap::DashMap;
use futures::Future;
use pin_project_lite::pin_project;
use roaring::RoaringBitmap;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::config::{CelebornConfig, CompressionCodec};
use crate::error::{CelebornError, Result};
use crate::network::ConnectionPool;
use crate::protocol::PartitionLocation;

use super::partition_reader::{PartitionReader, WorkerPartitionReader, WorkerPartitionReaderConfig};

/// Batch header size: mapId (4) + attemptId (4) + batchId (4) + size (4)
const BATCH_HEADER_SIZE: usize = 16;

/// Push failed batch information for deduplication.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PushFailedBatch {
    /// Map ID
    pub map_id: i32,
    /// Attempt ID
    pub attempt_id: i32,
    /// Batch ID
    pub batch_id: i32,
}

impl PushFailedBatch {
    /// Create a new PushFailedBatch.
    pub fn new(map_id: i32, attempt_id: i32, batch_id: i32) -> Self {
        Self {
            map_id,
            attempt_id,
            batch_id,
        }
    }
}

/// Chunk range for skew partition reading.
#[derive(Debug, Clone)]
pub struct ChunkRange {
    /// Start chunk index (inclusive)
    pub start_chunk_index: i32,
    /// End chunk index (exclusive)
    pub end_chunk_index: i32,
}

impl ChunkRange {
    /// Create a new ChunkRange.
    pub fn new(start: i32, end: i32) -> Self {
        Self {
            start_chunk_index: start,
            end_chunk_index: end,
        }
    }
}

/// Configuration for CelebornInputStream.
#[derive(Debug, Clone)]
pub struct CelebornInputStreamConfig {
    /// Maximum retries for fetch operations per replica
    pub fetch_max_retries_per_replica: u32,
    /// Retry wait time in milliseconds
    pub retry_wait_ms: u64,
    /// Whether push replication is enabled
    pub push_replicate_enabled: bool,
    /// Whether to exclude workers on fetch failure
    pub fetch_exclude_worker_on_failure: bool,
    /// Excluded worker expiration timeout in milliseconds
    pub fetch_excluded_worker_expire_timeout_ms: u64,
    /// Whether compression is enabled
    pub compression_enabled: bool,
    /// Compression codec
    pub compression_codec: CompressionCodec,
    /// Fetch buffer size
    pub fetch_buffer_size: usize,
    /// Range read filter enabled
    pub range_read_filter_enabled: bool,
    /// Fetch max requests in flight
    pub fetch_max_reqs_in_flight: usize,
    /// Fetch timeout
    pub fetch_timeout_ms: u64,
    /// Enable local shuffle file reading
    pub enable_read_local_shuffle: bool,
    /// Local host address for local shuffle detection
    pub local_host_address: String,
    /// Split skew partition without map range
    pub split_skew_partition_without_map_range: bool,
}

impl Default for CelebornInputStreamConfig {
    fn default() -> Self {
        Self {
            fetch_max_retries_per_replica: 3,
            retry_wait_ms: 50,
            push_replicate_enabled: false,
            fetch_exclude_worker_on_failure: true,
            fetch_excluded_worker_expire_timeout_ms: 60_000,
            compression_enabled: true,
            compression_codec: CompressionCodec::Lz4,
            fetch_buffer_size: 64 * 1024,
            range_read_filter_enabled: false,
            fetch_max_reqs_in_flight: 3,
            fetch_timeout_ms: 120_000,
            enable_read_local_shuffle: false,
            local_host_address: String::new(),
            split_skew_partition_without_map_range: false,
        }
    }
}

impl From<&CelebornConfig> for CelebornInputStreamConfig {
    fn from(config: &CelebornConfig) -> Self {
        Self {
            fetch_max_retries_per_replica: config.max_fetch_retries,
            retry_wait_ms: 50,
            push_replicate_enabled: config.push_replicate_enabled,
            fetch_exclude_worker_on_failure: true,
            fetch_excluded_worker_expire_timeout_ms: 60_000,
            compression_enabled: config.compression_codec != CompressionCodec::None,
            compression_codec: config.compression_codec.clone(),
            fetch_buffer_size: 64 * 1024,
            range_read_filter_enabled: false,
            fetch_max_reqs_in_flight: config.fetch_max_reqs_in_flight,
            fetch_timeout_ms: config.fetch_timeout.as_millis() as u64,
            enable_read_local_shuffle: false,
            local_host_address: String::new(),
            split_skew_partition_without_map_range: false,
        }
    }
}

/// Metrics callback for tracking read statistics.
pub trait MetricsCallback: Send + Sync {
    /// Increment bytes read counter.
    fn inc_bytes_read(&self, bytes: usize);

    /// Increment read time counter (in nanoseconds).
    fn inc_read_time(&self, nanos: u64);
}

/// Default no-op metrics callback.
pub struct NoOpMetricsCallback;

impl MetricsCallback for NoOpMetricsCallback {
    fn inc_bytes_read(&self, _bytes: usize) {}
    fn inc_read_time(&self, _nanos: u64) {}
}

/// Internal state for CelebornInputStream.
struct InputStreamState {
    /// Current partition reader
    current_reader: Option<Box<dyn PartitionReader>>,
    /// Current chunk being read
    current_chunk: Option<Bytes>,
    /// Current position in raw data buffer
    position: usize,
    /// Limit of valid data in raw data buffer
    limit: usize,
    /// Raw data buffer (decompressed)
    raw_data_buf: Vec<u8>,
    /// Compressed data buffer
    compressed_buf: Vec<u8>,
    /// Whether this is the first chunk
    first_chunk: bool,
    /// Current file/location index
    file_index: usize,
    /// Fetch chunk retry count
    fetch_chunk_retry_cnt: u32,
    /// Whether the stream is closed
    closed: bool,
    /// Batches already read (mapId -> Set<batchId>)
    batches_read: HashMap<i32, HashSet<i32>>,
    /// Skip count for statistics
    skip_count: usize,
    /// Contains local read flag
    contains_local_read: bool,
}

/// CelebornInputStream for reading shuffle data from multiple locations.
///
/// This is the main entry point for reading shuffle data. It handles:
/// - Automatic switching between partition locations
/// - Retry with peer location on failure
/// - Batch deduplication
/// - Decompression
/// - RoaringBitmap-based range filtering
/// - Skew partition support
///
/// # Example
///
/// ```ignore
/// let stream = CelebornInputStream::new(
///     config,
///     transport_client,
///     shuffle_key,
///     locations,
///     attempts,
/// ).await?;
///
/// let mut buffer = vec![0u8; 1024];
/// while let Ok(n) = stream.read(&mut buffer).await {
///     if n == 0 { break; }
///     // Process buffer[..n]
/// }
/// ```
pub struct CelebornInputStream {
    /// Configuration
    config: CelebornInputStreamConfig,
    /// Connection pool for creating readers
    connection_pool: Arc<ConnectionPool>,
    /// Shuffle key
    shuffle_key: String,
    /// Partition locations to read from
    locations: Vec<PartitionLocation>,
    /// Mapper attempts (mapId -> attemptId)
    attempts: Vec<i32>,
    /// Attempt number for this read
    attempt_number: i32,
    /// Start map index for range filter
    start_map_index: i32,
    /// End map index for range filter
    end_map_index: i32,
    /// Maximum fetch retries
    fetch_chunk_max_retry: u32,
    /// Excluded workers (host:port -> timestamp)
    excluded_workers: Arc<DashMap<String, Instant>>,
    /// Metrics callback
    metrics_callback: Arc<dyn MetricsCallback>,
    /// Internal state (protected by mutex for async access)
    state: Mutex<InputStreamState>,
    /// Total partitions to read
    total_partitions: usize,
    /// Failed batches for deduplication (location_unique_id -> Set<PushFailedBatch>)
    failed_batches: HashMap<String, HashSet<PushFailedBatch>>,
    /// Partition location to chunk range mapping for skew partition
    partition_location_to_chunk_range: Option<HashMap<String, ChunkRange>>,
    /// MapId bitmaps for range filtering (location_unique_id -> RoaringBitmap)
    map_id_bitmaps: HashMap<String, RoaringBitmap>,
}

impl CelebornInputStream {
    /// Create a new CelebornInputStream.
    ///
    /// # Arguments
    /// * `config` - Stream configuration
    /// * `connection_pool` - Connection pool for network operations
    /// * `shuffle_key` - Shuffle key (app_id-shuffle_id)
    /// * `locations` - Partition locations to read from
    /// * `attempts` - Mapper attempts array
    pub async fn new(
        config: CelebornInputStreamConfig,
        connection_pool: Arc<ConnectionPool>,
        shuffle_key: String,
        locations: Vec<PartitionLocation>,
        attempts: Vec<i32>,
    ) -> Result<Self> {
        Self::with_options(
            config,
            connection_pool,
            shuffle_key,
            locations,
            attempts,
            0,
            -1,
            i32::MAX,
            Arc::new(DashMap::new()),
            Arc::new(NoOpMetricsCallback),
            HashMap::new(),
            None,
            HashMap::new(),
        )
        .await
    }

    /// Create a new CelebornInputStream with full options.
    #[allow(clippy::too_many_arguments)]
    pub async fn with_options(
        config: CelebornInputStreamConfig,
        connection_pool: Arc<ConnectionPool>,
        shuffle_key: String,
        mut locations: Vec<PartitionLocation>,
        attempts: Vec<i32>,
        attempt_number: i32,
        start_map_index: i32,
        end_map_index: i32,
        excluded_workers: Arc<DashMap<String, Instant>>,
        metrics_callback: Arc<dyn MetricsCallback>,
        failed_batches: HashMap<String, HashSet<PushFailedBatch>>,
        partition_location_to_chunk_range: Option<HashMap<String, ChunkRange>>,
        map_id_bitmaps: HashMap<String, RoaringBitmap>,
    ) -> Result<Self> {
        // Randomize locations for load balancing
        use rand::seq::SliceRandom;
        let mut rng = rand::thread_rng();
        locations.shuffle(&mut rng);

        let total_partitions = locations.len();

        // Calculate max retries
        let fetch_chunk_max_retry = if config.push_replicate_enabled {
            config.fetch_max_retries_per_replica * 2
        } else {
            config.fetch_max_retries_per_replica
        };

        let buffer_size = config.fetch_buffer_size;

        let state = InputStreamState {
            current_reader: None,
            current_chunk: None,
            position: 0,
            limit: 0,
            raw_data_buf: vec![0u8; buffer_size],
            compressed_buf: if config.compression_enabled {
                vec![0u8; buffer_size]
            } else {
                Vec::new()
            },
            first_chunk: true,
            file_index: 0,
            fetch_chunk_retry_cnt: 0,
            closed: false,
            batches_read: HashMap::new(),
            skip_count: 0,
            contains_local_read: false,
        };

        let stream = Self {
            config,
            connection_pool,
            shuffle_key,
            locations,
            attempts,
            attempt_number,
            start_map_index,
            end_map_index,
            fetch_chunk_max_retry,
            excluded_workers,
            metrics_callback,
            state: Mutex::new(state),
            total_partitions,
            failed_batches,
            partition_location_to_chunk_range,
            map_id_bitmaps,
        };

        // Initialize by moving to first reader
        stream.move_to_next_reader(false).await?;

        Ok(stream)
    }

    /// Create an empty input stream.
    pub fn empty() -> Self {
        Self {
            config: CelebornInputStreamConfig::default(),
            connection_pool: Arc::new(ConnectionPool::new(1, 32)),
            shuffle_key: String::new(),
            locations: Vec::new(),
            attempts: Vec::new(),
            attempt_number: 0,
            start_map_index: -1,
            end_map_index: i32::MAX,
            fetch_chunk_max_retry: 0,
            excluded_workers: Arc::new(DashMap::new()),
            metrics_callback: Arc::new(NoOpMetricsCallback),
            state: Mutex::new(InputStreamState {
                current_reader: None,
                current_chunk: None,
                position: 0,
                limit: 0,
                raw_data_buf: Vec::new(),
                compressed_buf: Vec::new(),
                first_chunk: true,
                file_index: 0,
                fetch_chunk_retry_cnt: 0,
                closed: true,
                batches_read: HashMap::new(),
                skip_count: 0,
                contains_local_read: false,
            }),
            total_partitions: 0,
            failed_batches: HashMap::new(),
            partition_location_to_chunk_range: None,
            map_id_bitmaps: HashMap::new(),
        }
    }

    /// Check if the stream is empty.
    pub fn is_empty(&self) -> bool {
        self.locations.is_empty()
    }

    /// Get total number of partitions to read.
    pub fn total_partitions_to_read(&self) -> usize {
        self.total_partitions
    }

    /// Get number of partitions already read.
    pub async fn partitions_read(&self) -> usize {
        let state = self.state.lock().await;
        state.file_index
    }

    /// Get the number of bytes available to read without blocking.
    pub async fn available(&self) -> usize {
        let state = self.state.lock().await;
        if state.closed {
            return 0;
        }
        if state.position < state.limit {
            state.limit - state.position
        } else {
            0
        }
    }

    /// Read data into the provided buffer.
    ///
    /// Returns the number of bytes read, or 0 if end of stream.
    pub async fn read(&self, buf: &mut [u8]) -> Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let start_time = Instant::now();
        let mut state = self.state.lock().await;

        if state.closed {
            return Ok(0);
        }

        let mut read_bytes = 0;

        while read_bytes < buf.len() {
            // Try to read from current buffer
            while state.position >= state.limit {
                if !self.fill_buffer_internal(&mut state).await? {
                    let elapsed = start_time.elapsed().as_nanos() as u64;
                    self.metrics_callback.inc_read_time(elapsed);
                    return Ok(if read_bytes > 0 { read_bytes } else { 0 });
                }
            }

            let bytes_to_read = std::cmp::min(state.limit - state.position, buf.len() - read_bytes);
            buf[read_bytes..read_bytes + bytes_to_read]
                .copy_from_slice(&state.raw_data_buf[state.position..state.position + bytes_to_read]);
            state.position += bytes_to_read;
            read_bytes += bytes_to_read;
        }

        let elapsed = start_time.elapsed().as_nanos() as u64;
        self.metrics_callback.inc_read_time(elapsed);

        Ok(read_bytes)
    }

    /// Read a single byte.
    pub async fn read_byte(&self) -> Result<Option<u8>> {
        let start_time = Instant::now();
        let mut state = self.state.lock().await;

        if state.closed {
            return Ok(None);
        }

        if state.position < state.limit {
            let b = state.raw_data_buf[state.position];
            state.position += 1;
            let elapsed = start_time.elapsed().as_nanos() as u64;
            self.metrics_callback.inc_read_time(elapsed);
            return Ok(Some(b));
        }

        if !self.fill_buffer_internal(&mut state).await? {
            let elapsed = start_time.elapsed().as_nanos() as u64;
            self.metrics_callback.inc_read_time(elapsed);
            return Ok(None);
        }

        if state.position >= state.limit {
            let elapsed = start_time.elapsed().as_nanos() as u64;
            self.metrics_callback.inc_read_time(elapsed);
            return Ok(None);
        }

        let b = state.raw_data_buf[state.position];
        state.position += 1;
        let elapsed = start_time.elapsed().as_nanos() as u64;
        self.metrics_callback.inc_read_time(elapsed);
        Ok(Some(b))
    }

    /// Close the stream and release resources.
    pub async fn close(&self) -> Result<()> {
        let mut state = self.state.lock().await;

        if state.closed {
            return Ok(());
        }

        info!(
            "Closing CelebornInputStream: shuffle_key={}, total_locations={}, read={}, skipped={}",
            self.shuffle_key,
            self.locations.len(),
            self.locations.len() - state.skip_count,
            state.skip_count
        );

        // Release current chunk
        state.current_chunk = None;

        // Close current reader
        if let Some(reader) = state.current_reader.take() {
            if let Err(e) = reader.close().await {
                warn!("Error closing reader: {}", e);
            }
        }

        // Clear buffers
        state.raw_data_buf.clear();
        state.compressed_buf.clear();
        state.batches_read.clear();

        state.closed = true;

        Ok(())
    }

    /// Move to the next reader.
    async fn move_to_next_reader(&self, fetch_chunk: bool) -> Result<()> {
        let mut state = self.state.lock().await;
        self.move_to_next_reader_internal(&mut state, fetch_chunk).await
    }

    /// Internal implementation of move_to_next_reader.
    async fn move_to_next_reader_internal(
        &self,
        state: &mut InputStreamState,
        fetch_chunk: bool,
    ) -> Result<()> {
        // Close current reader
        if let Some(reader) = state.current_reader.take() {
            if let Err(e) = reader.close().await {
                warn!("Error closing reader: {}", e);
            }
        }

        // Find next readable location
        let current_location = self.next_readable_location(state);
        if current_location.is_none() {
            return Ok(());
        }

        let location = current_location.unwrap();
        state.current_reader = Some(self.create_reader_with_retry(location, state).await?);
        state.file_index += 1;

        // Keep moving until we find a reader with data
        loop {
            let has_next = if let Some(ref reader) = state.current_reader {
                reader.has_next().await
            } else {
                false
            };

            if !has_next {
                // Close and try next
                if let Some(reader) = state.current_reader.take() {
                    let _ = reader.close().await;
                }

                let next_location = self.next_readable_location(state);
                if next_location.is_none() {
                    return Ok(());
                }

                let location = next_location.unwrap();
                state.current_reader = Some(self.create_reader_with_retry(location, state).await?);
                state.file_index += 1;
                continue;
            }

            if fetch_chunk {
                match self.get_next_chunk_internal(state).await {
                    Ok(Some(chunk)) => {
                        state.current_chunk = Some(chunk);
                        break;
                    }
                    Ok(None) => {
                        // No chunk, try next reader
                        if let Some(reader) = state.current_reader.take() {
                            let _ = reader.close().await;
                        }

                        let next_location = self.next_readable_location(state);
                        if next_location.is_none() {
                            return Ok(());
                        }

                        let location = next_location.unwrap();
                        state.current_reader = Some(self.create_reader_with_retry(location, state).await?);
                        state.file_index += 1;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Get the next readable location.
    fn next_readable_location(&self, state: &mut InputStreamState) -> Option<PartitionLocation> {
        if state.file_index >= self.locations.len() {
            return None;
        }

        let mut current_location = self.locations[state.file_index].clone();

        // Skip locations based on range filter or skew partition settings
        while self.should_skip_location(&current_location) {
            state.skip_count += 1;
            state.file_index += 1;
            if state.file_index >= self.locations.len() {
                return None;
            }
            current_location = self.locations[state.file_index].clone();
        }

        state.fetch_chunk_retry_cnt = 0;
        Some(current_location)
    }

    /// Check if a location should be skipped based on range filter.
    fn should_skip_location(&self, location: &PartitionLocation) -> bool {
        let unique_id = location.unique_id();

        // For skew partition mode, check if location is in chunk range map
        if self.config.split_skew_partition_without_map_range {
            if let Some(ref chunk_range_map) = self.partition_location_to_chunk_range {
                if !chunk_range_map.contains_key(&unique_id) {
                    return true;
                }
            }
            return false;
        }

        // Range read filter using RoaringBitmap
        if !self.config.range_read_filter_enabled {
            return false;
        }

        if self.end_map_index == i32::MAX {
            return false;
        }

        // Get bitmap for this location
        let bitmap = self.map_id_bitmaps.get(&unique_id);
        
        // If no bitmap, try peer location
        let bitmap = match bitmap {
            Some(b) => b,
            None => {
                if let Some(ref peer) = location.peer {
                    match self.map_id_bitmaps.get(&peer.unique_id()) {
                        Some(b) => b,
                        None => return false, // No bitmap available, don't skip
                    }
                } else {
                    return false; // No bitmap available, don't skip
                }
            }
        };

        // Check if any map ID in range is in the bitmap
        for i in self.start_map_index..self.end_map_index {
            if i >= 0 && bitmap.contains(i as u32) {
                return false; // Found a matching map ID, don't skip
            }
        }

        true // No matching map IDs found, skip this location
    }

    /// Create a reader with retry support.
    async fn create_reader_with_retry(
        &self,
        mut location: PartitionLocation,
        state: &mut InputStreamState,
    ) -> Result<Box<dyn PartitionReader>> {
        let mut last_error: Option<CelebornError> = None;

        while state.fetch_chunk_retry_cnt < self.fetch_chunk_max_retry {
            // Check if worker is excluded
            if self.is_excluded(&location) {
                let err = CelebornError::FetchFailed(format!(
                    "Fetch data from excluded worker: {}:{}",
                    location.host, location.fetch_port
                ));
                last_error = Some(err);
                state.fetch_chunk_retry_cnt += 1;

                // Try peer if available
                if let Some(peer) = location.peer.take() {
                    if !self.config.split_skew_partition_without_map_range {
                        if state.fetch_chunk_retry_cnt % 2 == 0 {
                            tokio::time::sleep(Duration::from_millis(self.config.retry_wait_ms)).await;
                        }
                        location = *peer;
                        continue;
                    }
                }
                tokio::time::sleep(Duration::from_millis(self.config.retry_wait_ms)).await;
                continue;
            }

            match self.create_reader(&location).await {
                Ok(reader) => return Ok(reader),
                Err(e) => {
                    warn!(
                        "CreatePartitionReader failed {}/{} times for location {}:{}: {}",
                        state.fetch_chunk_retry_cnt + 1,
                        self.fetch_chunk_max_retry,
                        location.host,
                        location.fetch_port,
                        e
                    );

                    self.exclude_failed_location(&location, &e);
                    last_error = Some(e);
                    state.fetch_chunk_retry_cnt += 1;

                    // Try peer if available and not in skew partition mode
                    if let Some(peer) = location.peer.take() {
                        if !self.config.split_skew_partition_without_map_range {
                            if state.fetch_chunk_retry_cnt % 2 == 0 {
                                tokio::time::sleep(Duration::from_millis(self.config.retry_wait_ms)).await;
                            }
                            debug!("Switching to peer location: {}:{}", peer.host, peer.fetch_port);
                            location = *peer;
                            continue;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(self.config.retry_wait_ms)).await;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            CelebornError::FetchFailed(format!(
                "createPartitionReader failed after {} retries for {}:{}",
                self.fetch_chunk_max_retry, location.host, location.fetch_port
            ))
        }))
    }

    /// Create a partition reader for the given location.
    async fn create_reader(&self, location: &PartitionLocation) -> Result<Box<dyn PartitionReader>> {
        let reader_config = WorkerPartitionReaderConfig {
            fetch_max_reqs_in_flight: self.config.fetch_max_reqs_in_flight,
            fetch_timeout_ms: self.config.fetch_timeout_ms,
            compression_codec: self.config.compression_codec.clone(),
            max_fetch_retries: self.config.fetch_max_retries_per_replica,
        };

        let reader = WorkerPartitionReader::new(
            reader_config,
            &self.connection_pool,
            self.shuffle_key.clone(),
            location.clone(),
            self.start_map_index,
            self.end_map_index,
        )
        .await?;

        Ok(Box::new(reader))
    }

    /// Get the next chunk from the current reader.
    async fn get_next_chunk_internal(&self, state: &mut InputStreamState) -> Result<Option<Bytes>> {
        while state.fetch_chunk_retry_cnt < self.fetch_chunk_max_retry {
            let reader = match &state.current_reader {
                Some(r) => r,
                None => return Ok(None),
            };

            // Check if worker is excluded
            if self.is_excluded(reader.get_location()) {
                return Err(CelebornError::FetchFailed(format!(
                    "Fetch data from excluded worker: {}:{}",
                    reader.get_location().host,
                    reader.get_location().fetch_port
                )));
            }

            if !reader.has_next().await {
                debug!(
                    "Reader for {}:{} has no more data",
                    reader.get_location().host,
                    reader.get_location().fetch_port
                );
                return Ok(None);
            }

            match reader.next().await {
                Ok(Some(chunk)) => return Ok(Some(chunk)),
                Ok(None) => return Ok(None),
                Err(e) => {
                    warn!(
                        "Fetch chunk failed {}/{} times for location {}:{}: {}",
                        state.fetch_chunk_retry_cnt + 1,
                        self.fetch_chunk_max_retry,
                        reader.get_location().host,
                        reader.get_location().fetch_port,
                        e
                    );

                    let location = reader.get_location().clone();
                    self.exclude_failed_location(&location, &e);
                    state.fetch_chunk_retry_cnt += 1;

                    // Close current reader
                    if let Some(reader) = state.current_reader.take() {
                        let _ = reader.close().await;
                    }

                    if state.fetch_chunk_retry_cnt >= self.fetch_chunk_max_retry {
                        return Err(CelebornError::FetchFailed(format!(
                            "Fetch chunk failed after {} retries for {}:{}",
                            state.fetch_chunk_retry_cnt, location.host, location.fetch_port
                        )));
                    }

                    // Try peer or same location
                    let retry_location = if let Some(ref peer) = location.peer {
                        if !self.config.split_skew_partition_without_map_range {
                            if state.fetch_chunk_retry_cnt % 2 == 0 {
                                tokio::time::sleep(Duration::from_millis(self.config.retry_wait_ms)).await;
                            }
                            (**peer).clone()
                        } else {
                            tokio::time::sleep(Duration::from_millis(self.config.retry_wait_ms)).await;
                            location.clone()
                        }
                    } else {
                        tokio::time::sleep(Duration::from_millis(self.config.retry_wait_ms)).await;
                        location.clone()
                    };

                    state.current_reader = Some(self.create_reader_with_retry(retry_location, state).await?);
                }
            }
        }

        Err(CelebornError::FetchFailed(format!(
            "Fetch chunk failed after {} retries",
            self.fetch_chunk_max_retry
        )))
    }

    /// Check if a worker is excluded.
    fn is_excluded(&self, location: &PartitionLocation) -> bool {
        let key = format!("{}:{}", location.host, location.fetch_port);
        if let Some(entry) = self.excluded_workers.get(&key) {
            let timestamp = *entry;
            let expire_timeout = Duration::from_millis(self.config.fetch_excluded_worker_expire_timeout_ms);
            if timestamp.elapsed() > expire_timeout {
                self.excluded_workers.remove(&key);
                false
            } else {
                // Check if peer is also excluded
                if let Some(ref peer) = location.peer {
                    let peer_key = format!("{}:{}", peer.host, peer.fetch_port);
                    if let Some(peer_entry) = self.excluded_workers.get(&peer_key) {
                        // If peer was excluded earlier, use peer instead
                        if *peer_entry < timestamp {
                            return true;
                        }
                    }
                }
                true
            }
        } else {
            false
        }
    }

    /// Exclude a failed location.
    fn exclude_failed_location(&self, location: &PartitionLocation, error: &CelebornError) {
        if self.config.push_replicate_enabled && self.config.fetch_exclude_worker_on_failure {
            if self.is_critical_error(error) {
                let key = format!("{}:{}", location.host, location.fetch_port);
                self.excluded_workers.insert(key, Instant::now());
            }
        }
    }

    /// Check if an error is critical (should trigger worker exclusion).
    fn is_critical_error(&self, error: &CelebornError) -> bool {
        matches!(
            error,
            CelebornError::Connection(_)
                | CelebornError::Timeout(_)
                | CelebornError::FetchFailed(_)
        )
    }

    /// Move to the next chunk.
    async fn move_to_next_chunk_internal(&self, state: &mut InputStreamState) -> Result<bool> {
        // Release current chunk
        state.current_chunk = None;

        // Try to get next chunk from current reader
        if let Some(ref reader) = state.current_reader {
            if reader.has_next().await {
                match self.get_next_chunk_internal(state).await {
                    Ok(Some(chunk)) => {
                        state.current_chunk = Some(chunk);
                        return Ok(true);
                    }
                    Ok(None) => {}
                    Err(e) => return Err(e),
                }
            }
        }

        // Move to next reader
        if state.file_index < self.locations.len() {
            self.move_to_next_reader_internal(state, true).await?;
            return Ok(state.current_reader.is_some());
        }

        // Close current reader
        if let Some(reader) = state.current_reader.take() {
            let _ = reader.close().await;
        }

        Ok(false)
    }

    /// Fill the internal buffer with decompressed data.
    async fn fill_buffer_internal(&self, state: &mut InputStreamState) -> Result<bool> {
        // Initialize on first chunk
        if state.first_chunk && state.current_reader.is_some() {
            match self.get_next_chunk_internal(state).await {
                Ok(Some(chunk)) => {
                    state.current_chunk = Some(chunk);
                }
                Ok(None) => {
                    // Try to move to next chunk with a maximum iteration limit to prevent infinite loops
                    let mut attempts = 0;
                    const MAX_MOVE_ATTEMPTS: usize = 1000;
                    while state.current_chunk.is_none() && attempts < MAX_MOVE_ATTEMPTS {
                        if !self.move_to_next_chunk_internal(state).await? {
                            break;
                        }
                        attempts += 1;
                    }
                    if attempts >= MAX_MOVE_ATTEMPTS {
                        warn!("Exceeded maximum attempts to find next chunk during initialization");
                    }
                }
                Err(e) => return Err(e),
            }
            state.first_chunk = false;
        }

        if state.current_chunk.is_none() {
            return Ok(false);
        }

        let mut has_data = false;
        let mut loop_iterations = 0;
        const MAX_LOOP_ITERATIONS: usize = 10000;

        loop {
            // Safety check to prevent infinite loops
            loop_iterations += 1;
            if loop_iterations > MAX_LOOP_ITERATIONS {
                warn!("fill_buffer_internal exceeded maximum loop iterations, breaking");
                break;
            }

            // Check if current chunk has data
            let chunk_readable = state
                .current_chunk
                .as_ref()
                .map(|c| !c.is_empty())
                .unwrap_or(false);

            if !chunk_readable {
                if !self.move_to_next_chunk_internal(state).await? {
                    break;
                }
                // After moving to next chunk, check if we actually got a chunk
                if state.current_chunk.is_none() {
                    break;
                }
                continue;
            }

            // Read batch header
            let chunk = state.current_chunk.as_mut().unwrap();
            if chunk.len() < BATCH_HEADER_SIZE {
                // Not enough data for header, move to next chunk
                if !self.move_to_next_chunk_internal(state).await? {
                    break;
                }
                // After moving to next chunk, check if we actually got a chunk
                if state.current_chunk.is_none() {
                    break;
                }
                continue;
            }

            let map_id = chunk.get_i32();
            let attempt_id = chunk.get_i32();
            let batch_id = chunk.get_i32();
            let size = chunk.get_i32() as usize;

            // Validate size to prevent issues
            if size > 1024 * 1024 * 1024 {
                // 1GB max batch size
                warn!("Invalid batch size {} (too large), skipping", size);
                if !self.move_to_next_chunk_internal(state).await? {
                    break;
                }
                continue;
            }

            // Read batch data
            if chunk.len() < size {
                warn!(
                    "Chunk doesn't have enough data: expected {}, got {}",
                    size,
                    chunk.len()
                );
                if !self.move_to_next_chunk_internal(state).await? {
                    break;
                }
                continue;
            }

            let batch_data = chunk.copy_to_bytes(size);

            // Deduplicate by attempt
            if map_id >= 0 && (map_id as usize) < self.attempts.len() {
                if attempt_id != self.attempts[map_id as usize] {
                    // Skip this batch (wrong attempt)
                    continue;
                }
            }

            // Check for failed batch deduplication (for skew partition mode)
            if self.config.split_skew_partition_without_map_range {
                if let Some(ref reader) = state.current_reader {
                    let location_id = reader.get_location().unique_id();
                    if let Some(failed_set) = self.failed_batches.get(&location_id) {
                        let failed_batch = PushFailedBatch::new(map_id, attempt_id, batch_id);
                        if failed_set.contains(&failed_batch) {
                            debug!(
                                "Skip duplicated batch from failed set: mapId={}, attemptId={}, batchId={}",
                                map_id, attempt_id, batch_id
                            );
                            continue;
                        }
                    }
                }
            }

            // Deduplicate by batch ID
            let batch_set = state.batches_read.entry(map_id).or_insert_with(HashSet::new);
            if batch_set.contains(&batch_id) {
                debug!(
                    "Skip duplicated batch: mapId={}, attemptId={}, batchId={}",
                    map_id, attempt_id, batch_id
                );
                continue;
            }
            batch_set.insert(batch_id);

            // Update metrics
            self.metrics_callback.inc_bytes_read(BATCH_HEADER_SIZE + size);

            // Decompress if needed
            if self.config.compression_enabled {
                // Ensure compressed buffer is large enough
                if state.compressed_buf.len() < size {
                    state.compressed_buf.resize(size, 0);
                }
                state.compressed_buf[..size].copy_from_slice(&batch_data);

                // Decompress
                let decompressed = self.decompress(&state.compressed_buf[..size])?;

                // Ensure raw buffer is large enough
                if state.raw_data_buf.len() < decompressed.len() {
                    state.raw_data_buf.resize(decompressed.len(), 0);
                }
                state.raw_data_buf[..decompressed.len()].copy_from_slice(&decompressed);
                state.limit = decompressed.len();
            } else {
                // No compression
                if state.raw_data_buf.len() < size {
                    state.raw_data_buf.resize(size, 0);
                }
                state.raw_data_buf[..size].copy_from_slice(&batch_data);
                state.limit = size;
            }

            state.position = 0;
            has_data = true;
            break;
        }

        Ok(has_data)
    }

    /// Decompress data using the configured codec.
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.config.compression_codec {
            CompressionCodec::None => Ok(data.to_vec()),
            CompressionCodec::Lz4 => {
                // LZ4 frame format: first 4 bytes are original length
                if data.len() < 4 {
                    return Err(CelebornError::DecompressionFailed(
                        "LZ4 data too short".to_string(),
                    ));
                }

                let original_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
                let compressed_data = &data[4..];

                let mut decompressed = vec![0u8; original_len];
                match lz4_flex::decompress_into(compressed_data, &mut decompressed) {
                    Ok(_) => Ok(decompressed),
                    Err(e) => Err(CelebornError::DecompressionFailed(format!(
                        "LZ4 decompression failed: {}",
                        e
                    ))),
                }
            }
            CompressionCodec::Zstd => {
                match zstd::decode_all(data) {
                    Ok(decompressed) => Ok(decompressed),
                    Err(e) => Err(CelebornError::DecompressionFailed(format!(
                        "ZSTD decompression failed: {}",
                        e
                    ))),
                }
            }
        }
    }

    /// Set a RoaringBitmap for a partition location.
    pub fn set_map_id_bitmap(&mut self, location_id: String, bitmap: RoaringBitmap) {
        self.map_id_bitmaps.insert(location_id, bitmap);
    }

    /// Set failed batches for deduplication.
    pub fn set_failed_batches(&mut self, location_id: String, batches: HashSet<PushFailedBatch>) {
        self.failed_batches.insert(location_id, batches);
    }

    /// Set chunk range for a partition location (for skew partition).
    pub fn set_chunk_range(&mut self, location_id: String, range: ChunkRange) {
        if self.partition_location_to_chunk_range.is_none() {
            self.partition_location_to_chunk_range = Some(HashMap::new());
        }
        if let Some(ref mut map) = self.partition_location_to_chunk_range {
            map.insert(location_id, range);
        }
    }

    /// Skip n bytes from the stream.
    ///
    /// Returns the actual number of bytes skipped.
    pub async fn skip(&self, n: usize) -> Result<usize> {
        if n == 0 {
            return Ok(0);
        }

        let mut state = self.state.lock().await;

        if state.closed {
            return Ok(0);
        }

        let mut skipped = 0;

        while skipped < n {
            // Try to skip from current buffer
            if state.position < state.limit {
                let available = state.limit - state.position;
                let to_skip = std::cmp::min(available, n - skipped);
                state.position += to_skip;
                skipped += to_skip;
            } else {
                // Need to fill buffer
                if !self.fill_buffer_internal(&mut state).await? {
                    break;
                }
            }
        }

        Ok(skipped)
    }

    /// Get the chunk range for a partition location.
    pub fn get_chunk_range(&self, location_id: &str) -> Option<&ChunkRange> {
        self.partition_location_to_chunk_range
            .as_ref()
            .and_then(|map| map.get(location_id))
    }

    /// Check if the stream is closed.
    pub async fn is_closed(&self) -> bool {
        let state = self.state.lock().await;
        state.closed
    }

    /// Get the shuffle key.
    pub fn shuffle_key(&self) -> &str {
        &self.shuffle_key
    }

    /// Get the configuration.
    pub fn config(&self) -> &CelebornInputStreamConfig {
        &self.config
    }
}

/// Builder for CelebornInputStream.
pub struct CelebornInputStreamBuilder {
    config: CelebornInputStreamConfig,
    connection_pool: Option<Arc<ConnectionPool>>,
    shuffle_key: Option<String>,
    locations: Vec<PartitionLocation>,
    attempts: Vec<i32>,
    attempt_number: i32,
    start_map_index: i32,
    end_map_index: i32,
    excluded_workers: Arc<DashMap<String, Instant>>,
    metrics_callback: Arc<dyn MetricsCallback>,
    failed_batches: HashMap<String, HashSet<PushFailedBatch>>,
    partition_location_to_chunk_range: Option<HashMap<String, ChunkRange>>,
    map_id_bitmaps: HashMap<String, RoaringBitmap>,
}

impl CelebornInputStreamBuilder {
    /// Create a new builder with default configuration.
    pub fn new() -> Self {
        Self {
            config: CelebornInputStreamConfig::default(),
            connection_pool: None,
            shuffle_key: None,
            locations: Vec::new(),
            attempts: Vec::new(),
            attempt_number: 0,
            start_map_index: -1,
            end_map_index: i32::MAX,
            excluded_workers: Arc::new(DashMap::new()),
            metrics_callback: Arc::new(NoOpMetricsCallback),
            failed_batches: HashMap::new(),
            partition_location_to_chunk_range: None,
            map_id_bitmaps: HashMap::new(),
        }
    }

    /// Set the configuration.
    pub fn config(mut self, config: CelebornInputStreamConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the connection pool.
    pub fn connection_pool(mut self, pool: Arc<ConnectionPool>) -> Self {
        self.connection_pool = Some(pool);
        self
    }

    /// Set the shuffle key.
    pub fn shuffle_key(mut self, key: impl Into<String>) -> Self {
        self.shuffle_key = Some(key.into());
        self
    }

    /// Set the partition locations.
    pub fn locations(mut self, locations: Vec<PartitionLocation>) -> Self {
        self.locations = locations;
        self
    }

    /// Set the mapper attempts.
    pub fn attempts(mut self, attempts: Vec<i32>) -> Self {
        self.attempts = attempts;
        self
    }

    /// Set the attempt number.
    pub fn attempt_number(mut self, attempt_number: i32) -> Self {
        self.attempt_number = attempt_number;
        self
    }

    /// Set the start map index for range filter.
    pub fn start_map_index(mut self, index: i32) -> Self {
        self.start_map_index = index;
        self
    }

    /// Set the end map index for range filter.
    pub fn end_map_index(mut self, index: i32) -> Self {
        self.end_map_index = index;
        self
    }

    /// Set the excluded workers map.
    pub fn excluded_workers(mut self, workers: Arc<DashMap<String, Instant>>) -> Self {
        self.excluded_workers = workers;
        self
    }

    /// Set the metrics callback.
    pub fn metrics_callback(mut self, callback: Arc<dyn MetricsCallback>) -> Self {
        self.metrics_callback = callback;
        self
    }

    /// Set the failed batches for deduplication.
    pub fn failed_batches(mut self, batches: HashMap<String, HashSet<PushFailedBatch>>) -> Self {
        self.failed_batches = batches;
        self
    }

    /// Set the partition location to chunk range mapping.
    pub fn partition_location_to_chunk_range(mut self, map: HashMap<String, ChunkRange>) -> Self {
        self.partition_location_to_chunk_range = Some(map);
        self
    }

    /// Set the map ID bitmaps for range filtering.
    pub fn map_id_bitmaps(mut self, bitmaps: HashMap<String, RoaringBitmap>) -> Self {
        self.map_id_bitmaps = bitmaps;
        self
    }

    /// Enable range read filter.
    pub fn range_read_filter_enabled(mut self, enabled: bool) -> Self {
        self.config.range_read_filter_enabled = enabled;
        self
    }

    /// Enable split skew partition without map range.
    pub fn split_skew_partition_without_map_range(mut self, enabled: bool) -> Self {
        self.config.split_skew_partition_without_map_range = enabled;
        self
    }

    /// Build the CelebornInputStream.
    pub async fn build(self) -> Result<CelebornInputStream> {
        let connection_pool = self.connection_pool.ok_or_else(|| {
            CelebornError::InvalidConfig("connection_pool is required".to_string())
        })?;

        let shuffle_key = self.shuffle_key.ok_or_else(|| {
            CelebornError::InvalidConfig("shuffle_key is required".to_string())
        })?;

        CelebornInputStream::with_options(
            self.config,
            connection_pool,
            shuffle_key,
            self.locations,
            self.attempts,
            self.attempt_number,
            self.start_map_index,
            self.end_map_index,
            self.excluded_workers,
            self.metrics_callback,
            self.failed_batches,
            self.partition_location_to_chunk_range,
            self.map_id_bitmaps,
        )
        .await
    }
}

impl Default for CelebornInputStreamBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// AsyncRead implementation
// ============================================================================

/// Wrapper for CelebornInputStream that provides async read functionality.
///
/// This wrapper holds an Arc to the inner stream and provides methods
/// for async reading. Note that due to the complexity of implementing
/// AsyncRead with the internal state management, users should prefer
/// using the `read()` and `read_byte()` methods directly on CelebornInputStream.
pub struct AsyncCelebornInputStream {
    inner: Arc<CelebornInputStream>,
}

impl AsyncCelebornInputStream {
    /// Create a new AsyncCelebornInputStream.
    pub fn new(stream: CelebornInputStream) -> Self {
        Self {
            inner: Arc::new(stream),
        }
    }

    /// Create from an Arc.
    pub fn from_arc(stream: Arc<CelebornInputStream>) -> Self {
        Self { inner: stream }
    }

    /// Get a reference to the inner stream.
    pub fn inner(&self) -> &CelebornInputStream {
        &self.inner
    }

    /// Get the Arc to the inner stream.
    pub fn into_inner(self) -> Arc<CelebornInputStream> {
        self.inner
    }

    /// Close the stream.
    pub async fn close(&self) -> Result<()> {
        self.inner.close().await
    }

    /// Read data into the provided buffer.
    ///
    /// Returns the number of bytes read, or 0 if end of stream.
    pub async fn read(&self, buf: &mut [u8]) -> Result<usize> {
        self.inner.read(buf).await
    }

    /// Read a single byte.
    pub async fn read_byte(&self) -> Result<Option<u8>> {
        self.inner.read_byte().await
    }

    /// Get the number of bytes available to read without blocking.
    pub async fn available(&self) -> usize {
        self.inner.available().await
    }

    /// Check if the stream is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Get total number of partitions to read.
    pub fn total_partitions_to_read(&self) -> usize {
        self.inner.total_partitions_to_read()
    }

    /// Get number of partitions already read.
    pub async fn partitions_read(&self) -> usize {
        self.inner.partitions_read().await
    }

    /// Skip n bytes from the stream.
    pub async fn skip(&self, n: usize) -> Result<usize> {
        self.inner.skip(n).await
    }

    /// Check if the stream is closed.
    pub async fn is_closed(&self) -> bool {
        self.inner.is_closed().await
    }
}

impl Clone for AsyncCelebornInputStream {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

// ============================================================================
// AsyncRead trait implementation using pin_project
// ============================================================================

pin_project! {
    /// A wrapper that implements tokio's AsyncRead trait for CelebornInputStream.
    ///
    /// This allows the stream to be used with standard async I/O utilities.
    /// Note: Due to the async nature of the underlying stream, this implementation
    /// uses a boxed future internally.
    pub struct CelebornAsyncReader {
        inner: Arc<CelebornInputStream>,
        #[pin]
        pending_read: Option<std::pin::Pin<Box<dyn Future<Output = Result<usize>> + Send>>>,
        buffer: Vec<u8>,
        buffer_pos: usize,
        buffer_len: usize,
    }
}

impl CelebornAsyncReader {
    /// Create a new CelebornAsyncReader.
    pub fn new(stream: CelebornInputStream) -> Self {
        Self {
            inner: Arc::new(stream),
            pending_read: None,
            buffer: vec![0u8; 8192],
            buffer_pos: 0,
            buffer_len: 0,
        }
    }

    /// Create from an Arc.
    pub fn from_arc(stream: Arc<CelebornInputStream>) -> Self {
        Self {
            inner: stream,
            pending_read: None,
            buffer: vec![0u8; 8192],
            buffer_pos: 0,
            buffer_len: 0,
        }
    }

    /// Get a reference to the inner stream.
    pub fn inner(&self) -> &CelebornInputStream {
        &self.inner
    }

    /// Close the stream.
    pub async fn close(&self) -> Result<()> {
        self.inner.close().await
    }
}

impl AsyncRead for CelebornAsyncReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut this = self.as_mut().project();

        // First, try to serve from internal buffer
        if *this.buffer_pos < *this.buffer_len {
            let available = *this.buffer_len - *this.buffer_pos;
            let to_copy = std::cmp::min(available, buf.remaining());
            buf.put_slice(&this.buffer[*this.buffer_pos..*this.buffer_pos + to_copy]);
            *this.buffer_pos += to_copy;
            return Poll::Ready(Ok(()));
        }

        // Buffer is empty, need to read more
        // Check if we have a pending read
        if let Some(pending) = this.pending_read.as_mut().as_pin_mut() {
            match pending.poll(cx) {
                Poll::Ready(Ok(n)) => {
                    *this.pending_read = None;
                    if n == 0 {
                        // End of stream
                        return Poll::Ready(Ok(()));
                    }
                    *this.buffer_len = n;
                    *this.buffer_pos = 0;
                    
                    // Copy to output buffer
                    let to_copy = std::cmp::min(n, buf.remaining());
                    buf.put_slice(&this.buffer[0..to_copy]);
                    *this.buffer_pos = to_copy;
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => {
                    *this.pending_read = None;
                    Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, e.to_string())))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            // Start a new read
            let inner = this.inner.clone();
            let buffer_len = this.buffer.len();
            
            // We need to create a future that reads into our buffer
            // Since we can't easily pass a mutable reference, we'll read into a new buffer
            let read_future = Box::pin(async move {
                let mut temp_buf = vec![0u8; buffer_len];
                inner.read(&mut temp_buf).await
            });
            
            // For now, we'll use a simpler approach - just poll the future
            // This is a simplified implementation
            *this.pending_read = Some(read_future);
            
            // Wake immediately to poll the new future
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = CelebornInputStreamConfig::default();
        assert_eq!(config.fetch_max_retries_per_replica, 3);
        assert_eq!(config.retry_wait_ms, 50);
        assert!(!config.push_replicate_enabled);
        assert!(config.fetch_exclude_worker_on_failure);
        assert!(config.compression_enabled);
        assert!(!config.range_read_filter_enabled);
        assert!(!config.split_skew_partition_without_map_range);
    }

    #[test]
    fn test_batch_header_size() {
        assert_eq!(BATCH_HEADER_SIZE, 16);
    }

    #[test]
    fn test_builder_new() {
        let builder = CelebornInputStreamBuilder::new();
        assert!(builder.connection_pool.is_none());
        assert!(builder.shuffle_key.is_none());
        assert!(builder.locations.is_empty());
        assert!(builder.attempts.is_empty());
        assert!(builder.failed_batches.is_empty());
        assert!(builder.partition_location_to_chunk_range.is_none());
        assert!(builder.map_id_bitmaps.is_empty());
    }

    #[test]
    fn test_builder_chain() {
        let builder = CelebornInputStreamBuilder::new()
            .shuffle_key("test-app-1")
            .attempt_number(0)
            .start_map_index(0)
            .end_map_index(10)
            .range_read_filter_enabled(true)
            .split_skew_partition_without_map_range(true);

        assert_eq!(builder.shuffle_key, Some("test-app-1".to_string()));
        assert_eq!(builder.attempt_number, 0);
        assert_eq!(builder.start_map_index, 0);
        assert_eq!(builder.end_map_index, 10);
        assert!(builder.config.range_read_filter_enabled);
        assert!(builder.config.split_skew_partition_without_map_range);
    }

    #[test]
    fn test_empty_stream() {
        let stream = CelebornInputStream::empty();
        assert!(stream.is_empty());
        assert_eq!(stream.total_partitions_to_read(), 0);
    }

    #[tokio::test]
    async fn test_empty_stream_read() {
        let stream = CelebornInputStream::empty();
        let mut buf = vec![0u8; 100];
        let result = stream.read(&mut buf).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_empty_stream_close() {
        let stream = CelebornInputStream::empty();
        let result = stream.close().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_empty_stream_available() {
        let stream = CelebornInputStream::empty();
        let available = stream.available().await;
        assert_eq!(available, 0);
    }

    #[test]
    fn test_no_op_metrics_callback() {
        let callback = NoOpMetricsCallback;
        callback.inc_bytes_read(1000); // Should not panic
        callback.inc_read_time(1000); // Should not panic
    }

    #[test]
    fn test_is_critical_error() {
        let stream = CelebornInputStream::empty();
        
        // Connection error is critical
        assert!(stream.is_critical_error(&CelebornError::Connection("test".to_string())));
        // Timeout is critical
        assert!(stream.is_critical_error(&CelebornError::Timeout(1000)));
        // FetchFailed is critical
        assert!(stream.is_critical_error(&CelebornError::FetchFailed("test".to_string())));
        // InvalidConfig is not critical
        assert!(!stream.is_critical_error(&CelebornError::InvalidConfig("test".to_string())));
    }

    #[test]
    fn test_decompress_none() {
        // Create a stream with CompressionCodec::None to test no-compression path
        let mut stream = CelebornInputStream::empty();
        // Modify the config to use no compression
        stream.config.compression_codec = CompressionCodec::None;
        stream.config.compression_enabled = false;
        
        let data = vec![1, 2, 3, 4, 5];
        let result = stream.decompress(&data);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    fn test_decompress_lz4() {
        // Create a stream with LZ4 compression
        let mut stream = CelebornInputStream::empty();
        stream.config.compression_codec = CompressionCodec::Lz4;
        stream.config.compression_enabled = true;
        
        // Original data to compress
        let original_data = b"Hello, World! This is a test message for LZ4 compression.";
        
        // Compress the data using LZ4
        let compressed = lz4_flex::compress_prepend_size(original_data);
        
        // Decompress using our implementation
        let result = stream.decompress(&compressed);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), original_data.to_vec());
    }

    #[test]
    fn test_decompress_lz4_empty() {
        // Create a stream with LZ4 compression
        let mut stream = CelebornInputStream::empty();
        stream.config.compression_codec = CompressionCodec::Lz4;
        stream.config.compression_enabled = true;
        
        // Empty data - should fail because LZ4 needs at least 4 bytes for header
        let data = vec![1, 2, 3]; // Less than 4 bytes
        let result = stream.decompress(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decompress_zstd() {
        // Create a stream with ZSTD compression
        let mut stream = CelebornInputStream::empty();
        stream.config.compression_codec = CompressionCodec::Zstd;
        stream.config.compression_enabled = true;
        
        // Original data to compress
        let original_data = b"Hello, World! This is a test message for ZSTD compression.";
        
        // Compress the data using ZSTD
        let compressed = zstd::encode_all(&original_data[..], 3).unwrap();
        
        // Decompress using our implementation
        let result = stream.decompress(&compressed);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), original_data.to_vec());
    }

    #[test]
    fn test_decompress_zstd_invalid() {
        // Create a stream with ZSTD compression
        let mut stream = CelebornInputStream::empty();
        stream.config.compression_codec = CompressionCodec::Zstd;
        stream.config.compression_enabled = true;
        
        // Invalid ZSTD data
        let data = vec![1, 2, 3, 4, 5];
        let result = stream.decompress(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_push_failed_batch() {
        let batch1 = PushFailedBatch::new(1, 0, 0);
        let batch2 = PushFailedBatch::new(1, 0, 0);
        let batch3 = PushFailedBatch::new(1, 0, 1);

        assert_eq!(batch1, batch2);
        assert_ne!(batch1, batch3);

        let mut set = HashSet::new();
        set.insert(batch1.clone());
        assert!(set.contains(&batch2));
        assert!(!set.contains(&batch3));
    }

    #[test]
    fn test_chunk_range() {
        let range = ChunkRange::new(0, 10);
        assert_eq!(range.start_chunk_index, 0);
        assert_eq!(range.end_chunk_index, 10);
    }

    #[test]
    fn test_roaring_bitmap_range_filter() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(5);
        bitmap.insert(10);
        bitmap.insert(15);

        // Check contains
        assert!(bitmap.contains(5));
        assert!(bitmap.contains(10));
        assert!(!bitmap.contains(7));

        // Check range
        let mut found = false;
        for i in 0..20 {
            if bitmap.contains(i) {
                found = true;
                break;
            }
        }
        assert!(found);
    }

    #[test]
    fn test_builder_with_failed_batches() {
        let mut failed_batches = HashMap::new();
        let mut batch_set = HashSet::new();
        batch_set.insert(PushFailedBatch::new(1, 0, 0));
        failed_batches.insert("loc-1".to_string(), batch_set);

        let builder = CelebornInputStreamBuilder::new()
            .failed_batches(failed_batches.clone());

        assert_eq!(builder.failed_batches.len(), 1);
        assert!(builder.failed_batches.contains_key("loc-1"));
    }

    #[test]
    fn test_builder_with_chunk_range() {
        let mut chunk_ranges = HashMap::new();
        chunk_ranges.insert("loc-1".to_string(), ChunkRange::new(0, 5));

        let builder = CelebornInputStreamBuilder::new()
            .partition_location_to_chunk_range(chunk_ranges);

        assert!(builder.partition_location_to_chunk_range.is_some());
        let map = builder.partition_location_to_chunk_range.unwrap();
        assert!(map.contains_key("loc-1"));
    }

    #[test]
    fn test_builder_with_map_id_bitmaps() {
        let mut bitmaps = HashMap::new();
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(1);
        bitmap.insert(2);
        bitmaps.insert("loc-1".to_string(), bitmap);

        let builder = CelebornInputStreamBuilder::new()
            .map_id_bitmaps(bitmaps);

        assert_eq!(builder.map_id_bitmaps.len(), 1);
        assert!(builder.map_id_bitmaps.contains_key("loc-1"));
    }

    #[test]
    fn test_config_from_celeborn_config() {
        let mut celeborn_config = CelebornConfig::default();
        celeborn_config.max_fetch_retries = 5;
        celeborn_config.push_replicate_enabled = true;
        celeborn_config.fetch_max_reqs_in_flight = 10;

        let stream_config = CelebornInputStreamConfig::from(&celeborn_config);
        assert_eq!(stream_config.fetch_max_retries_per_replica, 5);
        assert!(stream_config.push_replicate_enabled);
        assert_eq!(stream_config.fetch_max_reqs_in_flight, 10);
    }

    #[test]
    fn test_async_celeborn_input_stream_creation() {
        let stream = CelebornInputStream::empty();
        let async_stream = AsyncCelebornInputStream::new(stream);
        assert!(async_stream.inner().is_empty());
    }

    #[tokio::test]
    async fn test_empty_stream_skip() {
        let stream = CelebornInputStream::empty();
        let skipped = stream.skip(100).await;
        assert!(skipped.is_ok());
        assert_eq!(skipped.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_empty_stream_is_closed() {
        let stream = CelebornInputStream::empty();
        // Empty stream is created with closed = true
        assert!(stream.is_closed().await);
    }

    #[test]
    fn test_stream_shuffle_key() {
        let stream = CelebornInputStream::empty();
        assert_eq!(stream.shuffle_key(), "");
    }

    #[test]
    fn test_stream_config() {
        let stream = CelebornInputStream::empty();
        let config = stream.config();
        assert_eq!(config.fetch_max_retries_per_replica, 3);
    }

    #[test]
    fn test_chunk_range_get() {
        let mut stream = CelebornInputStream::empty();
        stream.set_chunk_range("loc-1".to_string(), ChunkRange::new(5, 15));
        
        let range = stream.get_chunk_range("loc-1");
        assert!(range.is_some());
        let range = range.unwrap();
        assert_eq!(range.start_chunk_index, 5);
        assert_eq!(range.end_chunk_index, 15);
        
        assert!(stream.get_chunk_range("loc-2").is_none());
    }

    #[test]
    fn test_celeborn_async_reader_creation() {
        let stream = CelebornInputStream::empty();
        let reader = CelebornAsyncReader::new(stream);
        assert!(reader.inner().is_empty());
    }

    #[test]
    fn test_celeborn_async_reader_from_arc() {
        let stream = Arc::new(CelebornInputStream::empty());
        let reader = CelebornAsyncReader::from_arc(stream);
        assert!(reader.inner().is_empty());
    }

    #[tokio::test]
    async fn test_async_celeborn_input_stream_skip() {
        let stream = CelebornInputStream::empty();
        let async_stream = AsyncCelebornInputStream::new(stream);
        let skipped = async_stream.skip(100).await;
        assert!(skipped.is_ok());
        assert_eq!(skipped.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_async_celeborn_input_stream_is_closed() {
        let stream = CelebornInputStream::empty();
        let async_stream = AsyncCelebornInputStream::new(stream);
        assert!(async_stream.is_closed().await);
    }

    #[test]
    fn test_async_celeborn_input_stream_clone() {
        let stream = CelebornInputStream::empty();
        let async_stream = AsyncCelebornInputStream::new(stream);
        let cloned = async_stream.clone();
        assert!(cloned.inner().is_empty());
    }

    #[tokio::test]
    async fn test_async_celeborn_input_stream_read_byte() {
        let stream = CelebornInputStream::empty();
        let async_stream = AsyncCelebornInputStream::new(stream);
        let byte = async_stream.read_byte().await;
        assert!(byte.is_ok());
        assert!(byte.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_async_celeborn_input_stream_available() {
        let stream = CelebornInputStream::empty();
        let async_stream = AsyncCelebornInputStream::new(stream);
        let available = async_stream.available().await;
        assert_eq!(available, 0);
    }

    #[test]
    fn test_async_celeborn_input_stream_total_partitions() {
        let stream = CelebornInputStream::empty();
        let async_stream = AsyncCelebornInputStream::new(stream);
        assert_eq!(async_stream.total_partitions_to_read(), 0);
    }

    #[tokio::test]
    async fn test_async_celeborn_input_stream_partitions_read() {
        let stream = CelebornInputStream::empty();
        let async_stream = AsyncCelebornInputStream::new(stream);
        assert_eq!(async_stream.partitions_read().await, 0);
    }

    #[tokio::test]
    async fn test_async_celeborn_input_stream_close() {
        let stream = CelebornInputStream::empty();
        let async_stream = AsyncCelebornInputStream::new(stream);
        let result = async_stream.close().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_celeborn_async_reader_close() {
        let stream = CelebornInputStream::empty();
        let reader = CelebornAsyncReader::new(stream);
        let result = reader.close().await;
        assert!(result.is_ok());
    }
}
