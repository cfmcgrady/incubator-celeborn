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

//! Data fetcher for reading shuffle data from workers.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use tracing::{debug, trace, warn};

use crate::config::{CelebornConfig, CompressionCodec};
use crate::error::{CelebornError, Result};
use crate::network::codec::Frame;
use crate::network::{Connection, ConnectionPool, TransportClient};
use crate::protocol::message::{
    ChunkFetchRequest, ChunkFetchSuccess, MessageType, OpenStream, StreamHandle,
};
use crate::protocol::{Decodable, Encodable, PartitionLocation};

/// Iterator for reading shuffle data chunks.
pub struct ShuffleDataIterator {
    /// Configuration
    config: Arc<CelebornConfig>,
    /// Transport client
    transport_client: Arc<TransportClient>,
    /// Shuffle key
    shuffle_key: String,
    /// Partition locations to read from
    locations: VecDeque<PartitionLocation>,
    /// Current stream state
    current_stream: Option<StreamState>,
    /// Connection pool
    connection_pool: ConnectionPool,
    /// Buffered chunks
    buffered_chunks: VecDeque<Bytes>,
    /// Whether iteration is complete
    finished: bool,
}

/// State for an open stream.
struct StreamState {
    /// Stream ID
    stream_id: i64,
    /// Number of chunks
    num_chunks: i32,
    /// Current chunk index
    current_chunk: i32,
    /// Connection to the worker
    connection: Arc<Connection>,
    /// Partition location
    location: PartitionLocation,
}

impl ShuffleDataIterator {
    /// Create a new shuffle data iterator.
    pub async fn new(
        config: Arc<CelebornConfig>,
        transport_client: Arc<TransportClient>,
        shuffle_key: String,
        locations: Vec<PartitionLocation>,
    ) -> Result<Self> {
        let connection_pool = ConnectionPool::new(
            config.connection_pool_size,
            config.max_in_flight_requests,
        );

        Ok(Self {
            config,
            transport_client,
            shuffle_key,
            locations: VecDeque::from(locations),
            current_stream: None,
            connection_pool,
            buffered_chunks: VecDeque::new(),
            finished: false,
        })
    }

    /// Get the next chunk of data.
    pub async fn next(&mut self) -> Result<Option<Bytes>> {
        loop {
            if self.finished {
                return Ok(None);
            }

            // Return buffered chunk if available
            if let Some(chunk) = self.buffered_chunks.pop_front() {
                return Ok(Some(chunk));
            }

            // Try to fetch from current stream
            if self.current_stream.is_some() {
                let stream = self.current_stream.as_ref().unwrap();
                if stream.current_chunk < stream.num_chunks {
                    let stream_id = stream.stream_id;
                    let chunk_index = stream.current_chunk;
                    let connection = stream.connection.clone();
                    
                    match Self::fetch_chunk_static(&self.config, stream_id, chunk_index, &connection).await {
                        Ok(Some(chunk)) => {
                            self.current_stream.as_mut().unwrap().current_chunk += 1;
                            return Ok(Some(self.decompress_data(&chunk)?));
                        }
                        Ok(None) => {
                            // Stream exhausted, try next location
                            self.current_stream = None;
                        }
                        Err(e) => {
                            warn!("Failed to fetch chunk: {}", e);
                            self.current_stream = None;
                        }
                    }
                } else {
                    // All chunks fetched from this stream
                    self.current_stream = None;
                }
                continue;
            }

            // Open next stream
            if let Some(location) = self.locations.pop_front() {
                match self.open_stream(&location).await {
                    Ok(stream_state) => {
                        if stream_state.num_chunks > 0 {
                            self.current_stream = Some(stream_state);
                            // Continue loop to fetch from new stream
                            continue;
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Failed to open stream for partition {}: {}",
                            location.unique_id(),
                            e
                        );
                        // Try next location
                        continue;
                    }
                }
            } else {
                // No more locations
                self.finished = true;
                return Ok(None);
            }
        }
    }

    /// Check if there are more chunks available.
    pub fn has_next(&self) -> bool {
        if self.finished {
            return false;
        }

        if !self.buffered_chunks.is_empty() {
            return true;
        }

        if let Some(ref stream) = self.current_stream {
            if stream.current_chunk < stream.num_chunks {
                return true;
            }
        }

        !self.locations.is_empty()
    }

    /// Open a stream to read from a partition location.
    async fn open_stream(&self, location: &PartitionLocation) -> Result<StreamState> {
        let addr: SocketAddr = location
            .fetch_address()
            .parse()
            .map_err(|e| CelebornError::Connection(format!("Invalid address: {}", e)))?;

        let connection = self.connection_pool.get_connection(addr).await?;

        // Get file path from storage info
        let file_name = location
            .storage_info
            .as_ref()
            .map(|s| s.file_path.clone())
            .unwrap_or_else(|| format!("{}/{}", self.shuffle_key, location.unique_id()));

        let open_stream = OpenStream {
            shuffle_key: self.shuffle_key.clone(),
            file_name,
            start_index: 0,
            end_index: i32::MAX,
        };

        // Send open stream request
        let mut buf = open_stream.encode_to_bytes();
        let frame = Frame::new(MessageType::OpenStream, buf.freeze().slice(1..));

        let response = connection
            .send_rpc(frame.payload, self.config.fetch_timeout)
            .await?;

        // Parse stream handle response
        if response.message_type != MessageType::StreamHandle {
            return Err(CelebornError::Protocol(format!(
                "Expected StreamHandle, got {:?}",
                response.message_type
            )));
        }

        let mut payload = response.payload;
        let stream_handle = StreamHandle::decode(&mut payload)?;

        debug!(
            "Opened stream {} with {} chunks for partition {}",
            stream_handle.stream_id,
            stream_handle.num_chunks,
            location.unique_id()
        );

        Ok(StreamState {
            stream_id: stream_handle.stream_id,
            num_chunks: stream_handle.num_chunks,
            current_chunk: 0,
            connection,
            location: location.clone(),
        })
    }

    /// Fetch a chunk from the current stream (static version to avoid borrow issues).
    async fn fetch_chunk_static(
        config: &CelebornConfig,
        stream_id: i64,
        chunk_index: i32,
        connection: &Arc<Connection>,
    ) -> Result<Option<Bytes>> {
        let request = ChunkFetchRequest {
            stream_id,
            chunk_index,
            offset: 0,
            len: 0, // 0 means fetch entire chunk
        };

        let buf = request.encode_to_bytes();
        let frame = Frame::new(MessageType::ChunkFetchRequest, buf.freeze().slice(1..));

        let response = connection
            .send_rpc(frame.payload, config.fetch_timeout)
            .await?;

        match response.message_type {
            MessageType::ChunkFetchSuccess => {
                let mut payload = response.payload;
                let success = ChunkFetchSuccess::decode(&mut payload)?;
                
                trace!(
                    "Fetched chunk {} ({} bytes) from stream {}",
                    success.chunk_index,
                    success.body.len(),
                    success.stream_id
                );
                
                Ok(Some(success.body))
            }
            MessageType::ChunkFetchFailure => {
                Err(CelebornError::FetchFailed(format!(
                    "Chunk fetch failed for stream {} chunk {}",
                    stream_id, chunk_index
                )))
            }
            _ => Err(CelebornError::Protocol(format!(
                "Unexpected response type: {:?}",
                response.message_type
            ))),
        }
    }

    /// Decompress data using the configured codec.
    fn decompress_data(&self, data: &Bytes) -> Result<Bytes> {
        match self.config.compression_codec {
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

    /// Collect all remaining data into a vector.
    pub async fn collect(mut self) -> Result<Vec<Bytes>> {
        let mut result = Vec::new();
        while let Some(chunk) = self.next().await? {
            result.push(chunk);
        }
        Ok(result)
    }

    /// Get the total number of locations.
    pub fn num_locations(&self) -> usize {
        self.locations.len()
            + if self.current_stream.is_some() { 1 } else { 0 }
    }
}

/// Builder for creating a shuffle data reader with more options.
pub struct ShuffleDataReaderBuilder {
    config: Arc<CelebornConfig>,
    transport_client: Arc<TransportClient>,
    shuffle_key: String,
    locations: Vec<PartitionLocation>,
    start_chunk: Option<i32>,
    end_chunk: Option<i32>,
    prefetch_chunks: usize,
}

impl ShuffleDataReaderBuilder {
    /// Create a new builder.
    pub fn new(
        config: Arc<CelebornConfig>,
        transport_client: Arc<TransportClient>,
        shuffle_key: String,
        locations: Vec<PartitionLocation>,
    ) -> Self {
        Self {
            config,
            transport_client,
            shuffle_key,
            locations,
            start_chunk: None,
            end_chunk: None,
            prefetch_chunks: 2,
        }
    }

    /// Set the start chunk index.
    pub fn start_chunk(mut self, index: i32) -> Self {
        self.start_chunk = Some(index);
        self
    }

    /// Set the end chunk index.
    pub fn end_chunk(mut self, index: i32) -> Self {
        self.end_chunk = Some(index);
        self
    }

    /// Set the number of chunks to prefetch.
    pub fn prefetch_chunks(mut self, count: usize) -> Self {
        self.prefetch_chunks = count;
        self
    }

    /// Build the iterator.
    pub async fn build(self) -> Result<ShuffleDataIterator> {
        ShuffleDataIterator::new(
            self.config,
            self.transport_client,
            self.shuffle_key,
            self.locations,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_state() {
        // Basic test for stream state structure
        let location = PartitionLocation::new(
            0, 0, "localhost".to_string(), 9097, 9098, 9099, 9100,
        );
        assert_eq!(location.fetch_address(), "localhost:9099");
    }
}
