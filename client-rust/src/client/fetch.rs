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

use bytes::{Buf, Bytes};
use prost::Message;
use tracing::{debug, trace, warn};

use crate::config::{CelebornConfig, CompressionCodec};
use crate::error::{CelebornError, Result};
use crate::network::{Connection, ConnectionPool, TransportClient};
use crate::protocol::generated::{
    MessageType as PbMessageType, PbChunkFetchRequest, PbOpenStream, PbStreamChunkSlice,
    PbStreamHandler,
};
use crate::protocol::{decode_transport_message, encode_transport_message, PartitionLocation};

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

        // Get file name using the partition location's method
        // Format: {id}-{epoch}-{mode} e.g., "0-0-primary"
        let file_name = location.get_file_name();

        // Create PbOpenStream protobuf message
        let open_stream = PbOpenStream {
            shuffle_key: self.shuffle_key.clone(),
            file_name,
            start_index: 0,
            end_index: i32::MAX,
            initial_credit: 0,
            read_local_shuffle: false,
        };

        // Encode as TransportMessage: messageType (4 bytes) + payloadLen (4 bytes) + protobuf payload
        let transport_msg = encode_transport_message(PbMessageType::OpenStream as i32, &open_stream);

        debug!(
            "Sending OpenStream request for partition {} to {}",
            location.unique_id(),
            addr
        );

        let response = connection
            .send_rpc(transport_msg.freeze(), self.config.fetch_timeout)
            .await?;

        // The response body contains the TransportMessage (messageType + payloadLen + payload)
        // The response.message contains RpcResponse header (requestId + bodySize) which we don't need
        let mut payload = Bytes::copy_from_slice(&response.body);

        // Decode TransportMessage response from body
        let (msg_type, pb_payload) = decode_transport_message(&mut payload)?;

        if msg_type != PbMessageType::StreamHandler as i32 {
            return Err(CelebornError::Protocol(format!(
                "Expected StreamHandler ({}), got message type {}",
                PbMessageType::StreamHandler as i32,
                msg_type
            )));
        }

        // Decode PbStreamHandler from protobuf payload
        let stream_handler = PbStreamHandler::decode(pb_payload)
            .map_err(|e| CelebornError::Protocol(format!("Failed to decode PbStreamHandler: {}", e)))?;

        debug!(
            "Opened stream {} with {} chunks for partition {}",
            stream_handler.stream_id,
            stream_handler.num_chunks,
            location.unique_id()
        );

        Ok(StreamState {
            stream_id: stream_handler.stream_id,
            num_chunks: stream_handler.num_chunks,
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
        debug!(
            "Fetching chunk {} from stream {}",
            chunk_index, stream_id
        );

        // Create PbChunkFetchRequest protobuf message
        let chunk_slice = PbStreamChunkSlice {
            stream_id,
            chunk_index,
            offset: 0,
            len: i32::MAX, // Use MAX_VALUE to fetch entire chunk (same as Java client)
        };
        let request = PbChunkFetchRequest {
            stream_chunk_slice: Some(chunk_slice),
        };

        // Encode as TransportMessage
        let transport_msg = encode_transport_message(PbMessageType::ChunkFetchRequest as i32, &request);

        debug!(
            "Sending ChunkFetchRequest for stream {} chunk {}, msg size: {}",
            stream_id, chunk_index, transport_msg.len()
        );

        // Use fetch_chunk which registers the pending request with stream_id
        // (ChunkFetchSuccess response uses stream_id for matching, not request_id)
        let response = connection
            .fetch_chunk(transport_msg.freeze(), stream_id, config.fetch_timeout)
            .await?;

        debug!(
            "Received response for chunk fetch: type={:?}, message_len={}, body_len={}",
            response.message_type, response.message.len(), response.body.len()
        );

        // Check the response message type
        match response.message_type {
            crate::protocol::message::MessageType::ChunkFetchSuccess => {
                // ChunkFetchSuccess format:
                // message: StreamChunkSlice (20 bytes) = streamId (8) + chunkIndex (4) + offset (4) + len (4)
                // body: chunk data containing one or more batches
                // Each batch has a 16-byte header (little-endian):
                //   mapId (4) + attemptId (4) + batchId (4) + dataSize (4)
                // followed by the actual data
                if !response.body.is_empty() {
                    trace!(
                        "Fetched chunk {} ({} bytes) from stream {}",
                        chunk_index,
                        response.body.len(),
                        stream_id
                    );
                    // Parse batches and extract data
                    let data = Self::parse_chunk_batches(&response.body)?;
                    return Ok(Some(data));
                }
                Ok(None)
            }
            crate::protocol::message::MessageType::ChunkFetchFailure => {
                // ChunkFetchFailure format:
                // message: StreamChunkSlice (20 bytes) + error message
                Err(CelebornError::FetchFailed(format!(
                    "Chunk fetch failed for stream {} chunk {}",
                    stream_id, chunk_index
                )))
            }
            _ => {
                warn!(
                    "Unexpected response type {:?} for chunk fetch",
                    response.message_type
                );
                Ok(None)
            }
        }
    }

    /// Parse chunk data containing one or more batches.
    /// Each batch has a 16-byte header (little-endian):
    ///   mapId (4) + attemptId (4) + batchId (4) + dataSize (4)
    /// followed by the actual data of `dataSize` bytes.
    fn parse_chunk_batches(chunk_data: &[u8]) -> Result<Bytes> {
        let mut result = Vec::new();
        let mut offset = 0;
        
        const BATCH_HEADER_SIZE: usize = 16;
        
        while offset + BATCH_HEADER_SIZE <= chunk_data.len() {
            // Read batch header (little-endian)
            let _map_id = i32::from_le_bytes(chunk_data[offset..offset+4].try_into().unwrap());
            let _attempt_id = i32::from_le_bytes(chunk_data[offset+4..offset+8].try_into().unwrap());
            let _batch_id = i32::from_le_bytes(chunk_data[offset+8..offset+12].try_into().unwrap());
            let data_size = i32::from_le_bytes(chunk_data[offset+12..offset+16].try_into().unwrap()) as usize;
            
            offset += BATCH_HEADER_SIZE;
            
            // Extract data
            if offset + data_size > chunk_data.len() {
                return Err(CelebornError::Protocol(format!(
                    "Batch data size {} exceeds remaining chunk data {} at offset {}",
                    data_size, chunk_data.len() - offset, offset
                )));
            }
            
            result.extend_from_slice(&chunk_data[offset..offset + data_size]);
            offset += data_size;
        }
        
        Ok(Bytes::from(result))
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
