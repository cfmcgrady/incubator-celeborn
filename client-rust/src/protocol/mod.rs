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

//! Protocol definitions for Celeborn communication.
//!
//! This module contains the message types and encoding/decoding logic
//! for the Celeborn wire protocol.

pub mod message;
pub mod transport;
pub mod java_serialization;
pub mod generated;

// Include generated protobuf code
pub use generated::*;

pub use message::*;
pub use transport::*;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;

/// Partition location information.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionLocation {
    /// Partition ID
    pub id: i32,
    /// Epoch (version) of this partition location
    pub epoch: i32,
    /// Worker host
    pub host: String,
    /// RPC port
    pub rpc_port: i32,
    /// Push port
    pub push_port: i32,
    /// Fetch port
    pub fetch_port: i32,
    /// Replicate port
    pub replicate_port: i32,
    /// Partition mode (Primary or Replica)
    pub mode: PartitionMode,
    /// Peer location (for replication)
    pub peer: Option<Box<PartitionLocation>>,
    /// Storage information
    pub storage_info: Option<StorageInfo>,
}

impl PartitionLocation {
    /// Create a new partition location.
    pub fn new(
        id: i32,
        epoch: i32,
        host: String,
        rpc_port: i32,
        push_port: i32,
        fetch_port: i32,
        replicate_port: i32,
    ) -> Self {
        Self {
            id,
            epoch,
            host,
            rpc_port,
            push_port,
            fetch_port,
            replicate_port,
            mode: PartitionMode::Primary,
            peer: None,
            storage_info: None,
        }
    }

    /// Get the unique identifier for this partition.
    pub fn unique_id(&self) -> String {
        format!("{}-{}", self.id, self.epoch)
    }

    /// Get the file name for this partition (used for fetch operations).
    /// Format: {id}-{epoch}-{mode} where mode is 0 (PRIMARY) or 1 (REPLICA)
    /// This matches Java's PartitionLocation.getFileName(): id + "-" + epoch + "-" + mode.mode
    pub fn get_file_name(&self) -> String {
        let mode_value = self.mode as i32;
        format!("{}-{}-{}", self.id, self.epoch, mode_value)
    }

    /// Get the worker address for push operations.
    pub fn push_address(&self) -> String {
        format!("{}:{}", self.host, self.push_port)
    }

    /// Get the worker address for fetch operations.
    pub fn fetch_address(&self) -> String {
        format!("{}:{}", self.host, self.fetch_port)
    }
}

/// Partition mode (Primary or Replica).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionMode {
    Primary = 0,
    Replica = 1,
}

impl From<i32> for PartitionMode {
    fn from(value: i32) -> Self {
        match value {
            0 => PartitionMode::Primary,
            1 => PartitionMode::Replica,
            _ => PartitionMode::Primary,
        }
    }
}

/// Storage information for a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageInfo {
    /// Storage type
    pub storage_type: i32,
    /// Mount point
    pub mount_point: String,
    /// Whether this is the final result
    pub final_result: bool,
    /// File path
    pub file_path: String,
    /// Available storage types
    pub available_storage_types: i32,
    /// File size
    pub file_size: i64,
    /// Chunk offsets
    pub chunk_offsets: Vec<i64>,
}

/// Worker information.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkerInfo {
    /// Worker host
    pub host: String,
    /// RPC port
    pub rpc_port: i32,
    /// Push port
    pub push_port: i32,
    /// Fetch port
    pub fetch_port: i32,
    /// Replicate port
    pub replicate_port: i32,
}

impl WorkerInfo {
    /// Create a new worker info.
    pub fn new(
        host: String,
        rpc_port: i32,
        push_port: i32,
        fetch_port: i32,
        replicate_port: i32,
    ) -> Self {
        Self {
            host,
            rpc_port,
            push_port,
            fetch_port,
            replicate_port,
        }
    }

    /// Get the unique identifier for this worker.
    pub fn to_unique_id(&self) -> String {
        format!(
            "{}-{}-{}-{}-{}",
            self.host, self.rpc_port, self.push_port, self.fetch_port, self.replicate_port
        )
    }
}

/// Trait for encoding messages to bytes.
pub trait Encodable {
    /// Get the encoded length of this message.
    fn encoded_length(&self) -> usize;

    /// Encode this message to the buffer.
    fn encode(&self, buf: &mut BytesMut);

    /// Encode this message to a new BytesMut.
    fn encode_to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(self.encoded_length());
        self.encode(&mut buf);
        buf
    }
}

/// Trait for decoding messages from bytes.
pub trait Decodable: Sized {
    /// Decode a message from the buffer.
    fn decode(buf: &mut Bytes) -> io::Result<Self>;
}

/// Encode a string to the buffer (length-prefixed).
pub fn encode_string(buf: &mut BytesMut, s: &str) {
    let bytes = s.as_bytes();
    buf.put_u16(bytes.len() as u16);
    buf.put_slice(bytes);
}

/// Decode a string from the buffer (length-prefixed).
pub fn decode_string(buf: &mut Bytes) -> io::Result<String> {
    if buf.remaining() < 2 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Not enough bytes for string length",
        ));
    }
    let len = buf.get_u16() as usize;
    if buf.remaining() < len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Not enough bytes for string content",
        ));
    }
    let bytes = buf.copy_to_bytes(len);
    String::from_utf8(bytes.to_vec()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Get the encoded length of a string.
pub fn string_encoded_length(s: &str) -> usize {
    2 + s.len()
}

// ============================================================================
// Java-compatible encoding functions (for native protocol messages like PushData)
// Java uses 4-byte length prefix for strings
// ============================================================================

/// Encode a string to the buffer with 4-byte length prefix (Java compatible).
pub fn encode_string_java(buf: &mut BytesMut, s: &str) {
    let bytes = s.as_bytes();
    buf.put_i32(bytes.len() as i32);
    buf.put_slice(bytes);
}

/// Decode a string from the buffer with 4-byte length prefix (Java compatible).
pub fn decode_string_java(buf: &mut Bytes) -> io::Result<String> {
    if buf.remaining() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Not enough bytes for string length",
        ));
    }
    let len = buf.get_i32() as usize;
    if buf.remaining() < len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Not enough bytes for string content",
        ));
    }
    let bytes = buf.copy_to_bytes(len);
    String::from_utf8(bytes.to_vec()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Get the encoded length of a string with 4-byte length prefix (Java compatible).
pub fn string_encoded_length_java(s: &str) -> usize {
    4 + s.len()
}

// ============================================================================
// TransportMessage encoding/decoding (for RPC messages like OpenStream, ChunkFetchRequest)
// TransportMessage format:
//   - messageTypeValue: 4 bytes (int) - Protobuf MessageType enum value
//   - payloadLen: 4 bytes (int) - length of protobuf payload
//   - payload: protobuf encoded message
// ============================================================================

use prost::Message;

/// Encode a TransportMessage to bytes.
///
/// TransportMessage wraps a Protobuf message with a type header.
/// Format: messageTypeValue (4 bytes) + payloadLen (4 bytes) + payload (protobuf)
pub fn encode_transport_message<M: Message>(message_type: i32, msg: &M) -> BytesMut {
    let payload = msg.encode_to_vec();
    let mut buf = BytesMut::with_capacity(8 + payload.len());
    buf.put_i32(message_type);
    buf.put_i32(payload.len() as i32);
    buf.put_slice(&payload);
    buf
}

/// Decode a TransportMessage from bytes.
///
/// Returns (message_type, payload_bytes).
pub fn decode_transport_message(buf: &mut Bytes) -> io::Result<(i32, Bytes)> {
    if buf.remaining() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Not enough bytes for TransportMessage header",
        ));
    }
    let message_type = buf.get_i32();
    let payload_len = buf.get_i32() as usize;
    if buf.remaining() < payload_len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("Not enough bytes for TransportMessage payload: need {}, have {}", payload_len, buf.remaining()),
        ));
    }
    let payload = buf.copy_to_bytes(payload_len);
    Ok((message_type, payload))
}
