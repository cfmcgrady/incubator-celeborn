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

//! Partition reader implementations for reading shuffle data from workers.
//!
//! This module provides the `PartitionReader` trait and its implementations:
//! - `WorkerPartitionReader`: Reads data from a single worker partition location
//!
//! The design follows the Java client's PartitionReader interface with async support.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use prost::Message;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, trace, warn};

use crate::config::{CelebornConfig, CompressionCodec};
use crate::error::{CelebornError, Result};
use crate::network::{Connection, ConnectionPool};
use crate::protocol::generated::{
    MessageType as PbMessageType, PbBufferStreamEnd, PbChunkFetchRequest, PbOpenStream,
    PbStreamChunkSlice, PbStreamHandler, StreamType,
};
use crate::protocol::{decode_transport_message, encode_transport_message, PartitionLocation};

/// Trait for reading data from a partition.
///
/// This trait defines the interface for partition readers, similar to Java's PartitionReader.
/// Implementations can read from workers, local files, or other sources.
#[async_trait]
pub trait PartitionReader: Send + Sync {
    /// Check if there are more chunks available to read.
    async fn has_next(&self) -> bool;

    /// Get the next chunk of data.
    ///
    /// Returns `Ok(Some(data))` if data is available,
    /// `Ok(None)` if no more data, or an error.
    async fn next(&self) -> Result<Option<Bytes>>;

    /// Close the reader and release resources.
    async fn close(&self) -> Result<()>;

    /// Get the partition location this reader is reading from.
    fn get_location(&self) -> &PartitionLocation;
}

/// Result of a chunk fetch operation.
#[derive(Debug)]
enum ChunkResult {
    /// Successfully fetched chunk data
    Success(Bytes),
    /// Fetch failed with error
    Failure(String),
    /// End of stream marker
    EndOfStream,
}

/// Configuration for WorkerPartitionReader.
#[derive(Debug, Clone)]
pub struct WorkerPartitionReaderConfig {
    /// Maximum number of in-flight fetch requests
    pub fetch_max_reqs_in_flight: usize,
    /// Fetch timeout in milliseconds
    pub fetch_timeout_ms: u64,
    /// Compression codec
    pub compression_codec: CompressionCodec,
    /// Maximum retries for fetch operations
    pub max_fetch_retries: u32,
}

impl Default for WorkerPartitionReaderConfig {
    fn default() -> Self {
        Self {
            fetch_max_reqs_in_flight: 3,
            fetch_timeout_ms: 120_000,
            compression_codec: CompressionCodec::None,
            max_fetch_retries: 3,
        }
    }
}

impl From<&CelebornConfig> for WorkerPartitionReaderConfig {
    fn from(config: &CelebornConfig) -> Self {
        Self {
            fetch_max_reqs_in_flight: config.fetch_max_reqs_in_flight,
            fetch_timeout_ms: config.fetch_timeout.as_millis() as u64,
            compression_codec: config.compression_codec.clone(),
            max_fetch_retries: config.max_fetch_retries,
        }
    }
}

/// Reader for fetching shuffle data from a worker.
///
/// This reader opens a stream to a worker and fetches chunks with prefetching support.
/// It implements the same protocol as the Java WorkerPartitionReader:
/// 1. Open stream with PbOpenStream -> receive PbStreamHandler with stream_id and num_chunks
/// 2. Fetch chunks with PbChunkFetchRequest -> receive ChunkFetchSuccess with data
/// 3. Close stream with PbBufferStreamEnd
pub struct WorkerPartitionReader {
    /// Configuration
    config: WorkerPartitionReaderConfig,
    /// Partition location
    location: PartitionLocation,
    /// Shuffle key
    shuffle_key: String,
    /// Connection to the worker
    connection: Arc<Connection>,
    /// Stream ID from the worker
    stream_id: i64,
    /// Total number of chunks
    num_chunks: i32,
    /// Index of the next chunk to return to caller
    return_chunk_index: AtomicI32,
    /// Index of the next chunk to fetch
    fetch_chunk_index: AtomicI32,
    /// Number of in-flight fetch requests
    in_flight_requests: AtomicUsize,
    /// Queue of fetched chunks waiting to be returned
    chunk_queue: Mutex<VecDeque<ChunkResult>>,
    /// Whether the reader is closed
    closed: AtomicBool,
    /// Exception that occurred during fetch
    exception: RwLock<Option<String>>,
}

impl WorkerPartitionReader {
    /// Create a new WorkerPartitionReader.
    ///
    /// This opens a stream to the worker and prepares for chunk fetching.
    pub async fn new(
        config: WorkerPartitionReaderConfig,
        connection_pool: &ConnectionPool,
        shuffle_key: String,
        location: PartitionLocation,
        start_map_index: i32,
        end_map_index: i32,
    ) -> Result<Self> {
        let addr: SocketAddr = location
            .fetch_address()
            .parse()
            .map_err(|e| CelebornError::Connection(format!("Invalid address: {}", e)))?;

        let connection = connection_pool.get_connection(addr).await?;

        // Open stream
        let file_name = location.get_file_name();
        let open_stream = PbOpenStream {
            shuffle_key: shuffle_key.clone(),
            file_name,
            start_index: start_map_index,
            end_index: end_map_index,
            initial_credit: 0,
            read_local_shuffle: false,
        };

        let transport_msg =
            encode_transport_message(PbMessageType::OpenStream as i32, &open_stream);

        debug!(
            "Opening stream for partition {} to {}",
            location.unique_id(),
            addr
        );

        let response = connection
            .send_rpc(
                transport_msg.freeze(),
                std::time::Duration::from_millis(config.fetch_timeout_ms),
            )
            .await?;

        let mut payload = Bytes::copy_from_slice(&response.body);
        let (msg_type, pb_payload) = decode_transport_message(&mut payload)?;

        if msg_type != PbMessageType::StreamHandler as i32 {
            return Err(CelebornError::Protocol(format!(
                "Expected StreamHandler ({}), got message type {}",
                PbMessageType::StreamHandler as i32,
                msg_type
            )));
        }

        let stream_handler = PbStreamHandler::decode(pb_payload)
            .map_err(|e| CelebornError::Protocol(format!("Failed to decode PbStreamHandler: {}", e)))?;

        debug!(
            "Opened stream {} with {} chunks for partition {}",
            stream_handler.stream_id, stream_handler.num_chunks, location.unique_id()
        );

        let reader = Self {
            config,
            location,
            shuffle_key,
            connection,
            stream_id: stream_handler.stream_id,
            num_chunks: stream_handler.num_chunks,
            return_chunk_index: AtomicI32::new(0),
            fetch_chunk_index: AtomicI32::new(0),
            in_flight_requests: AtomicUsize::new(0),
            chunk_queue: Mutex::new(VecDeque::new()),
            closed: AtomicBool::new(false),
            exception: RwLock::new(None),
        };

        // Start prefetching
        reader.trigger_fetch().await;

        Ok(reader)
    }

    /// Create a WorkerPartitionReader with default map index range.
    pub async fn new_default(
        config: WorkerPartitionReaderConfig,
        connection_pool: &ConnectionPool,
        shuffle_key: String,
        location: PartitionLocation,
    ) -> Result<Self> {
        Self::new(
            config,
            connection_pool,
            shuffle_key,
            location,
            0,
            i32::MAX,
        )
        .await
    }

    /// Trigger fetching of chunks up to the max in-flight limit.
    async fn trigger_fetch(&self) {
        while !self.closed.load(Ordering::Acquire) {
            let fetch_index = self.fetch_chunk_index.load(Ordering::Acquire);
            let in_flight = self.in_flight_requests.load(Ordering::Acquire);

            // Check if we should fetch more
            if fetch_index >= self.num_chunks {
                break;
            }
            if in_flight >= self.config.fetch_max_reqs_in_flight {
                break;
            }

            // Try to claim this chunk for fetching
            let claimed = self.fetch_chunk_index.compare_exchange(
                fetch_index,
                fetch_index + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            );

            if claimed.is_ok() {
                self.in_flight_requests.fetch_add(1, Ordering::AcqRel);
                
                // Spawn fetch task
                let connection = self.connection.clone();
                let stream_id = self.stream_id;
                let chunk_index = fetch_index;
                let timeout = std::time::Duration::from_millis(self.config.fetch_timeout_ms);
                let compression_codec = self.config.compression_codec.clone();

                // We need to handle the result - for now, fetch synchronously in the trigger
                // In a full implementation, this would use channels for async notification
                match Self::fetch_chunk_internal(&connection, stream_id, chunk_index, timeout).await
                {
                    Ok(data) => {
                        let decompressed = Self::decompress_data(&compression_codec, &data);
                        let mut queue = self.chunk_queue.lock().await;
                        match decompressed {
                            Ok(d) => queue.push_back(ChunkResult::Success(d)),
                            Err(e) => queue.push_back(ChunkResult::Failure(e.to_string())),
                        }
                    }
                    Err(e) => {
                        let mut queue = self.chunk_queue.lock().await;
                        queue.push_back(ChunkResult::Failure(e.to_string()));
                    }
                }
                self.in_flight_requests.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    /// Internal chunk fetch implementation.
    async fn fetch_chunk_internal(
        connection: &Arc<Connection>,
        stream_id: i64,
        chunk_index: i32,
        timeout: std::time::Duration,
    ) -> Result<Bytes> {
        debug!("Fetching chunk {} from stream {}", chunk_index, stream_id);

        let chunk_slice = PbStreamChunkSlice {
            stream_id,
            chunk_index,
            offset: 0,
            len: i32::MAX,
        };
        let request = PbChunkFetchRequest {
            stream_chunk_slice: Some(chunk_slice),
        };

        let transport_msg =
            encode_transport_message(PbMessageType::ChunkFetchRequest as i32, &request);

        let response = connection
            .fetch_chunk(transport_msg.freeze(), stream_id, timeout)
            .await?;

        match response.message_type {
            crate::protocol::message::MessageType::ChunkFetchSuccess => {
                if !response.body.is_empty() {
                    trace!(
                        "Fetched chunk {} ({} bytes) from stream {}",
                        chunk_index,
                        response.body.len(),
                        stream_id
                    );
                    Self::parse_chunk_batches(&response.body)
                } else {
                    Ok(Bytes::new())
                }
            }
            crate::protocol::message::MessageType::ChunkFetchFailure => {
                Err(CelebornError::FetchFailed(format!(
                    "Chunk fetch failed for stream {} chunk {}",
                    stream_id, chunk_index
                )))
            }
            _ => Err(CelebornError::Protocol(format!(
                "Unexpected response type {:?} for chunk fetch",
                response.message_type
            ))),
        }
    }

    /// Parse chunk data containing one or more batches.
    fn parse_chunk_batches(chunk_data: &[u8]) -> Result<Bytes> {
        let mut result = Vec::new();
        let mut offset = 0;

        const BATCH_HEADER_SIZE: usize = 16;

        while offset + BATCH_HEADER_SIZE <= chunk_data.len() {
            let _map_id = i32::from_le_bytes(chunk_data[offset..offset + 4].try_into().unwrap());
            let _attempt_id =
                i32::from_le_bytes(chunk_data[offset + 4..offset + 8].try_into().unwrap());
            let _batch_id =
                i32::from_le_bytes(chunk_data[offset + 8..offset + 12].try_into().unwrap());
            let data_size =
                i32::from_le_bytes(chunk_data[offset + 12..offset + 16].try_into().unwrap())
                    as usize;

            offset += BATCH_HEADER_SIZE;

            if offset + data_size > chunk_data.len() {
                return Err(CelebornError::Protocol(format!(
                    "Batch data size {} exceeds remaining chunk data {} at offset {}",
                    data_size,
                    chunk_data.len() - offset,
                    offset
                )));
            }

            result.extend_from_slice(&chunk_data[offset..offset + data_size]);
            offset += data_size;
        }

        Ok(Bytes::from(result))
    }

    /// Decompress data using the configured codec.
    fn decompress_data(codec: &CompressionCodec, data: &Bytes) -> Result<Bytes> {
        match codec {
            CompressionCodec::None => Ok(data.clone()),
            CompressionCodec::Lz4 => {
                #[cfg(feature = "compression-lz4")]
                {
                    let decompressed = lz4_flex::decompress_size_prepended(data).map_err(|e| {
                        CelebornError::Compression(format!("LZ4 decompression failed: {}", e))
                    })?;
                    Ok(Bytes::from(decompressed))
                }
                #[cfg(not(feature = "compression-lz4"))]
                {
                    Ok(data.clone())
                }
            }
            CompressionCodec::Zstd => {
                #[cfg(feature = "compression-zstd")]
                {
                    let decompressed = zstd::decode_all(data.as_ref()).map_err(|e| {
                        CelebornError::Compression(format!("Zstd decompression failed: {}", e))
                    })?;
                    Ok(Bytes::from(decompressed))
                }
                #[cfg(not(feature = "compression-zstd"))]
                {
                    Ok(data.clone())
                }
            }
        }
    }

    /// Send buffer stream end message to close the stream.
    async fn send_stream_end(&self) -> Result<()> {
        let end_msg = PbBufferStreamEnd {
            stream_type: StreamType::ChunkStream as i32,
            stream_id: self.stream_id,
        };

        let transport_msg =
            encode_transport_message(PbMessageType::BufferStreamEnd as i32, &end_msg);

        debug!("Sending BufferStreamEnd for stream {}", self.stream_id);

        // Send as one-way message (no response expected)
        self.connection
            .send_rpc(
                transport_msg.freeze(),
                std::time::Duration::from_millis(self.config.fetch_timeout_ms),
            )
            .await?;

        Ok(())
    }

    /// Get the stream ID.
    pub fn stream_id(&self) -> i64 {
        self.stream_id
    }

    /// Get the total number of chunks.
    pub fn num_chunks(&self) -> i32 {
        self.num_chunks
    }

    /// Get the number of chunks already returned.
    pub fn chunks_returned(&self) -> i32 {
        self.return_chunk_index.load(Ordering::Acquire)
    }
}

#[async_trait]
impl PartitionReader for WorkerPartitionReader {
    async fn has_next(&self) -> bool {
        if self.closed.load(Ordering::Acquire) {
            return false;
        }

        // Check for exception
        if self.exception.read().await.is_some() {
            return false;
        }

        let return_index = self.return_chunk_index.load(Ordering::Acquire);
        return_index < self.num_chunks
    }

    async fn next(&self) -> Result<Option<Bytes>> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(None);
        }

        // Check for exception
        if let Some(ref err) = *self.exception.read().await {
            return Err(CelebornError::FetchFailed(err.clone()));
        }

        let return_index = self.return_chunk_index.load(Ordering::Acquire);
        if return_index >= self.num_chunks {
            return Ok(None);
        }

        // Trigger more fetches
        self.trigger_fetch().await;

        // Wait for chunk to be available
        loop {
            {
                let mut queue = self.chunk_queue.lock().await;
                if let Some(result) = queue.pop_front() {
                    self.return_chunk_index.fetch_add(1, Ordering::AcqRel);
                    
                    // Trigger more fetches after consuming
                    drop(queue);
                    self.trigger_fetch().await;

                    return match result {
                        ChunkResult::Success(data) => Ok(Some(data)),
                        ChunkResult::Failure(err) => Err(CelebornError::FetchFailed(err)),
                        ChunkResult::EndOfStream => Ok(None),
                    };
                }
            }

            // If no chunk available and we've fetched all, we're done
            let fetch_index = self.fetch_chunk_index.load(Ordering::Acquire);
            if fetch_index >= self.num_chunks
                && self.in_flight_requests.load(Ordering::Acquire) == 0
            {
                return Ok(None);
            }

            // Small yield to allow fetch tasks to complete
            tokio::task::yield_now().await;
        }
    }

    async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            // Already closed
            return Ok(());
        }

        debug!("Closing WorkerPartitionReader for stream {}", self.stream_id);

        // Send stream end message
        if let Err(e) = self.send_stream_end().await {
            warn!("Failed to send stream end message: {}", e);
        }

        // Clear the queue
        let mut queue = self.chunk_queue.lock().await;
        queue.clear();

        Ok(())
    }

    fn get_location(&self) -> &PartitionLocation {
        &self.location
    }
}

impl Drop for WorkerPartitionReader {
    fn drop(&mut self) {
        if !self.closed.load(Ordering::Acquire) {
            // Note: We can't send async stream end in drop
            // The caller should call close() explicitly
            debug!(
                "WorkerPartitionReader dropped without close() for stream {}",
                self.stream_id
            );
        }
    }
}

/// Builder for creating WorkerPartitionReader with custom options.
pub struct WorkerPartitionReaderBuilder {
    config: WorkerPartitionReaderConfig,
    shuffle_key: String,
    location: PartitionLocation,
    start_map_index: i32,
    end_map_index: i32,
}

impl WorkerPartitionReaderBuilder {
    /// Create a new builder.
    pub fn new(shuffle_key: String, location: PartitionLocation) -> Self {
        Self {
            config: WorkerPartitionReaderConfig::default(),
            shuffle_key,
            location,
            start_map_index: 0,
            end_map_index: i32::MAX,
        }
    }

    /// Set the configuration.
    pub fn config(mut self, config: WorkerPartitionReaderConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the start map index.
    pub fn start_map_index(mut self, index: i32) -> Self {
        self.start_map_index = index;
        self
    }

    /// Set the end map index.
    pub fn end_map_index(mut self, index: i32) -> Self {
        self.end_map_index = index;
        self
    }

    /// Set the max in-flight requests.
    pub fn fetch_max_reqs_in_flight(mut self, count: usize) -> Self {
        self.config.fetch_max_reqs_in_flight = count;
        self
    }

    /// Set the compression codec.
    pub fn compression_codec(mut self, codec: CompressionCodec) -> Self {
        self.config.compression_codec = codec;
        self
    }

    /// Build the reader.
    pub async fn build(self, connection_pool: &ConnectionPool) -> Result<WorkerPartitionReader> {
        WorkerPartitionReader::new(
            self.config,
            connection_pool,
            self.shuffle_key,
            self.location,
            self.start_map_index,
            self.end_map_index,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_partition_reader_config_default() {
        let config = WorkerPartitionReaderConfig::default();
        assert_eq!(config.fetch_max_reqs_in_flight, 3);
        assert_eq!(config.fetch_timeout_ms, 120_000);
        assert!(matches!(config.compression_codec, CompressionCodec::None));
        assert_eq!(config.max_fetch_retries, 3);
    }

    #[test]
    fn test_worker_partition_reader_config_from_celeborn_config() {
        let mut celeborn_config = CelebornConfig::default();
        celeborn_config.fetch_max_reqs_in_flight = 5;
        celeborn_config.max_fetch_retries = 10;

        let reader_config = WorkerPartitionReaderConfig::from(&celeborn_config);
        assert_eq!(reader_config.fetch_max_reqs_in_flight, 5);
        assert_eq!(reader_config.max_fetch_retries, 10);
    }

    #[test]
    fn test_builder_creation() {
        let location = PartitionLocation::new(
            0, 0, "localhost".to_string(), 9097, 9098, 9099, 9100,
        );
        let builder = WorkerPartitionReaderBuilder::new(
            "app-123-shuffle-0".to_string(),
            location,
        );
        
        assert_eq!(builder.start_map_index, 0);
        assert_eq!(builder.end_map_index, i32::MAX);
    }

    #[test]
    fn test_builder_with_options() {
        let location = PartitionLocation::new(
            0, 0, "localhost".to_string(), 9097, 9098, 9099, 9100,
        );
        let builder = WorkerPartitionReaderBuilder::new(
            "app-123-shuffle-0".to_string(),
            location,
        )
        .start_map_index(10)
        .end_map_index(100)
        .fetch_max_reqs_in_flight(5)
        .compression_codec(CompressionCodec::Lz4);

        assert_eq!(builder.start_map_index, 10);
        assert_eq!(builder.end_map_index, 100);
        assert_eq!(builder.config.fetch_max_reqs_in_flight, 5);
        assert!(matches!(builder.config.compression_codec, CompressionCodec::Lz4));
    }

    #[test]
    fn test_parse_chunk_batches_single_batch() {
        // Create a single batch: mapId=1, attemptId=0, batchId=0, dataSize=4, data="test"
        let mut chunk_data = Vec::new();
        chunk_data.extend_from_slice(&1i32.to_le_bytes()); // mapId
        chunk_data.extend_from_slice(&0i32.to_le_bytes()); // attemptId
        chunk_data.extend_from_slice(&0i32.to_le_bytes()); // batchId
        chunk_data.extend_from_slice(&4i32.to_le_bytes()); // dataSize
        chunk_data.extend_from_slice(b"test"); // data

        let result = WorkerPartitionReader::parse_chunk_batches(&chunk_data).unwrap();
        assert_eq!(result.as_ref(), b"test");
    }

    #[test]
    fn test_parse_chunk_batches_multiple_batches() {
        let mut chunk_data = Vec::new();
        
        // First batch
        chunk_data.extend_from_slice(&1i32.to_le_bytes());
        chunk_data.extend_from_slice(&0i32.to_le_bytes());
        chunk_data.extend_from_slice(&0i32.to_le_bytes());
        chunk_data.extend_from_slice(&5i32.to_le_bytes());
        chunk_data.extend_from_slice(b"hello");

        // Second batch
        chunk_data.extend_from_slice(&2i32.to_le_bytes());
        chunk_data.extend_from_slice(&0i32.to_le_bytes());
        chunk_data.extend_from_slice(&1i32.to_le_bytes());
        chunk_data.extend_from_slice(&5i32.to_le_bytes());
        chunk_data.extend_from_slice(b"world");

        let result = WorkerPartitionReader::parse_chunk_batches(&chunk_data).unwrap();
        assert_eq!(result.as_ref(), b"helloworld");
    }

    #[test]
    fn test_parse_chunk_batches_empty() {
        let chunk_data: Vec<u8> = Vec::new();
        let result = WorkerPartitionReader::parse_chunk_batches(&chunk_data).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_chunk_batches_invalid_size() {
        let mut chunk_data = Vec::new();
        chunk_data.extend_from_slice(&1i32.to_le_bytes());
        chunk_data.extend_from_slice(&0i32.to_le_bytes());
        chunk_data.extend_from_slice(&0i32.to_le_bytes());
        chunk_data.extend_from_slice(&100i32.to_le_bytes()); // dataSize larger than remaining
        chunk_data.extend_from_slice(b"test");

        let result = WorkerPartitionReader::parse_chunk_batches(&chunk_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_chunk_result_variants() {
        let success = ChunkResult::Success(Bytes::from("data"));
        let failure = ChunkResult::Failure("error".to_string());
        let end = ChunkResult::EndOfStream;

        assert!(matches!(success, ChunkResult::Success(_)));
        assert!(matches!(failure, ChunkResult::Failure(_)));
        assert!(matches!(end, ChunkResult::EndOfStream));
    }

    #[test]
    fn test_decompress_data_none_codec() {
        let data = Bytes::from("test data");
        let result = WorkerPartitionReader::decompress_data(&CompressionCodec::None, &data).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_parse_chunk_batches_large_batch() {
        // Test with a larger batch
        let mut chunk_data = Vec::new();
        let large_data = vec![0u8; 1024]; // 1KB of data
        
        chunk_data.extend_from_slice(&1i32.to_le_bytes()); // mapId
        chunk_data.extend_from_slice(&0i32.to_le_bytes()); // attemptId
        chunk_data.extend_from_slice(&0i32.to_le_bytes()); // batchId
        chunk_data.extend_from_slice(&(large_data.len() as i32).to_le_bytes()); // dataSize
        chunk_data.extend_from_slice(&large_data);

        let result = WorkerPartitionReader::parse_chunk_batches(&chunk_data).unwrap();
        assert_eq!(result.len(), 1024);
    }

    #[test]
    fn test_parse_chunk_batches_many_small_batches() {
        let mut chunk_data = Vec::new();
        
        // Create 10 small batches
        for i in 0..10 {
            chunk_data.extend_from_slice(&(i as i32).to_le_bytes()); // mapId
            chunk_data.extend_from_slice(&0i32.to_le_bytes()); // attemptId
            chunk_data.extend_from_slice(&(i as i32).to_le_bytes()); // batchId
            chunk_data.extend_from_slice(&1i32.to_le_bytes()); // dataSize = 1
            chunk_data.push(b'a' + i as u8); // single byte data
        }

        let result = WorkerPartitionReader::parse_chunk_batches(&chunk_data).unwrap();
        assert_eq!(result.len(), 10);
        assert_eq!(result.as_ref(), b"abcdefghij");
    }

    #[test]
    fn test_parse_chunk_batches_partial_header() {
        // Only partial header (less than 16 bytes)
        let chunk_data = vec![0u8; 10];
        let result = WorkerPartitionReader::parse_chunk_batches(&chunk_data).unwrap();
        // Should return empty since we can't parse a complete header
        assert!(result.is_empty());
    }

    #[test]
    fn test_parse_chunk_batches_zero_size_batch() {
        let mut chunk_data = Vec::new();
        chunk_data.extend_from_slice(&1i32.to_le_bytes()); // mapId
        chunk_data.extend_from_slice(&0i32.to_le_bytes()); // attemptId
        chunk_data.extend_from_slice(&0i32.to_le_bytes()); // batchId
        chunk_data.extend_from_slice(&0i32.to_le_bytes()); // dataSize = 0

        let result = WorkerPartitionReader::parse_chunk_batches(&chunk_data).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_builder_config_override() {
        let location = PartitionLocation::new(
            0, 0, "localhost".to_string(), 9097, 9098, 9099, 9100,
        );
        
        let custom_config = WorkerPartitionReaderConfig {
            fetch_max_reqs_in_flight: 10,
            fetch_timeout_ms: 60_000,
            compression_codec: CompressionCodec::Zstd,
            max_fetch_retries: 5,
        };
        
        let builder = WorkerPartitionReaderBuilder::new(
            "app-123-shuffle-0".to_string(),
            location,
        )
        .config(custom_config);

        assert_eq!(builder.config.fetch_max_reqs_in_flight, 10);
        assert_eq!(builder.config.fetch_timeout_ms, 60_000);
        assert!(matches!(builder.config.compression_codec, CompressionCodec::Zstd));
        assert_eq!(builder.config.max_fetch_retries, 5);
    }

    #[test]
    fn test_partition_location_in_builder() {
        let location = PartitionLocation::new(
            5, 2, "worker-host".to_string(), 9097, 9098, 9099, 9100,
        );
        let builder = WorkerPartitionReaderBuilder::new(
            "app-456-shuffle-1".to_string(),
            location.clone(),
        );
        
        assert_eq!(builder.shuffle_key, "app-456-shuffle-1");
        assert_eq!(builder.location.id, 5);
        assert_eq!(builder.location.epoch, 2);
    }

    #[tokio::test]
    async fn test_chunk_queue_operations() {
        use std::collections::VecDeque;
        
        let mut queue: VecDeque<ChunkResult> = VecDeque::new();
        
        // Test push and pop
        queue.push_back(ChunkResult::Success(Bytes::from("chunk1")));
        queue.push_back(ChunkResult::Success(Bytes::from("chunk2")));
        queue.push_back(ChunkResult::Failure("error".to_string()));
        
        assert_eq!(queue.len(), 3);
        
        if let Some(ChunkResult::Success(data)) = queue.pop_front() {
            assert_eq!(data.as_ref(), b"chunk1");
        } else {
            panic!("Expected Success variant");
        }
        
        if let Some(ChunkResult::Success(data)) = queue.pop_front() {
            assert_eq!(data.as_ref(), b"chunk2");
        } else {
            panic!("Expected Success variant");
        }
        
        if let Some(ChunkResult::Failure(err)) = queue.pop_front() {
            assert_eq!(err, "error");
        } else {
            panic!("Expected Failure variant");
        }
        
        assert!(queue.is_empty());
    }

    #[test]
    fn test_config_timeout_conversion() {
        let mut celeborn_config = CelebornConfig::default();
        celeborn_config.fetch_timeout = std::time::Duration::from_secs(300);
        
        let reader_config = WorkerPartitionReaderConfig::from(&celeborn_config);
        assert_eq!(reader_config.fetch_timeout_ms, 300_000);
    }

    #[test]
    fn test_parse_chunk_batches_exact_header_size() {
        // Exactly 16 bytes (header only, no data)
        let mut chunk_data = Vec::new();
        chunk_data.extend_from_slice(&1i32.to_le_bytes()); // mapId
        chunk_data.extend_from_slice(&0i32.to_le_bytes()); // attemptId
        chunk_data.extend_from_slice(&0i32.to_le_bytes()); // batchId
        chunk_data.extend_from_slice(&0i32.to_le_bytes()); // dataSize = 0

        let result = WorkerPartitionReader::parse_chunk_batches(&chunk_data).unwrap();
        assert!(result.is_empty());
    }
}
