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

//! Message types for the Celeborn wire protocol.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;

use super::{decode_string, encode_string, string_encoded_length, Decodable, Encodable};

/// Message type identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    UnknownType = 255,
    ChunkFetchRequest = 0,
    ChunkFetchSuccess = 1,
    ChunkFetchFailure = 2,
    RpcRequest = 3,
    RpcResponse = 4,
    RpcFailure = 5,
    OpenStream = 6,
    StreamHandle = 7,
    OneWayMessage = 9,
    PushData = 11,
    PushMergedData = 12,
    RegionStart = 13,
    RegionFinish = 14,
    PushDataHandShake = 15,
    ReadAddCredit = 16,
    ReadData = 17,
    OpenStreamWithCredit = 18,
    BacklogAnnouncement = 19,
    TransportableError = 20,
    BufferStreamEnd = 21,
    Heartbeat = 22,
}

impl From<u8> for MessageType {
    fn from(value: u8) -> Self {
        match value {
            0 => MessageType::ChunkFetchRequest,
            1 => MessageType::ChunkFetchSuccess,
            2 => MessageType::ChunkFetchFailure,
            3 => MessageType::RpcRequest,
            4 => MessageType::RpcResponse,
            5 => MessageType::RpcFailure,
            6 => MessageType::OpenStream,
            7 => MessageType::StreamHandle,
            9 => MessageType::OneWayMessage,
            11 => MessageType::PushData,
            12 => MessageType::PushMergedData,
            13 => MessageType::RegionStart,
            14 => MessageType::RegionFinish,
            15 => MessageType::PushDataHandShake,
            16 => MessageType::ReadAddCredit,
            17 => MessageType::ReadData,
            18 => MessageType::OpenStreamWithCredit,
            19 => MessageType::BacklogAnnouncement,
            20 => MessageType::TransportableError,
            21 => MessageType::BufferStreamEnd,
            22 => MessageType::Heartbeat,
            _ => MessageType::UnknownType,
        }
    }
}

/// Base trait for all messages.
pub trait Message: Encodable {
    /// Get the message type.
    fn message_type(&self) -> MessageType;

    /// Get the optional body of the message.
    fn body(&self) -> Option<&Bytes> {
        None
    }
}

/// RPC request message.
#[derive(Debug, Clone)]
pub struct RpcRequest {
    /// Unique request ID
    pub request_id: i64,
    /// Request body
    pub body: Bytes,
}

impl RpcRequest {
    pub fn new(request_id: i64, body: Bytes) -> Self {
        Self { request_id, body }
    }
}

impl Encodable for RpcRequest {
    fn encoded_length(&self) -> usize {
        8 + 4 + self.body.len() // request_id + body_length + body
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(MessageType::RpcRequest as u8);
        buf.put_i64(self.request_id);
        buf.put_i32(self.body.len() as i32);
        buf.put_slice(&self.body);
    }
}

impl Decodable for RpcRequest {
    fn decode(buf: &mut Bytes) -> io::Result<Self> {
        if buf.remaining() < 12 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes for RpcRequest",
            ));
        }
        let request_id = buf.get_i64();
        let body_len = buf.get_i32() as usize;
        if buf.remaining() < body_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes for RpcRequest body",
            ));
        }
        let body = buf.copy_to_bytes(body_len);
        Ok(Self { request_id, body })
    }
}

impl Message for RpcRequest {
    fn message_type(&self) -> MessageType {
        MessageType::RpcRequest
    }

    fn body(&self) -> Option<&Bytes> {
        Some(&self.body)
    }
}

/// RPC response message.
#[derive(Debug, Clone)]
pub struct RpcResponse {
    /// Request ID this is responding to
    pub request_id: i64,
    /// Response body
    pub body: Bytes,
}

impl RpcResponse {
    pub fn new(request_id: i64, body: Bytes) -> Self {
        Self { request_id, body }
    }
}

impl Encodable for RpcResponse {
    fn encoded_length(&self) -> usize {
        8 + 4 + self.body.len()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(MessageType::RpcResponse as u8);
        buf.put_i64(self.request_id);
        buf.put_i32(self.body.len() as i32);
        buf.put_slice(&self.body);
    }
}

impl Decodable for RpcResponse {
    fn decode(buf: &mut Bytes) -> io::Result<Self> {
        if buf.remaining() < 12 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes for RpcResponse",
            ));
        }
        let request_id = buf.get_i64();
        let body_len = buf.get_i32() as usize;
        if buf.remaining() < body_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes for RpcResponse body",
            ));
        }
        let body = buf.copy_to_bytes(body_len);
        Ok(Self { request_id, body })
    }
}

impl Message for RpcResponse {
    fn message_type(&self) -> MessageType {
        MessageType::RpcResponse
    }

    fn body(&self) -> Option<&Bytes> {
        Some(&self.body)
    }
}

/// RPC failure message.
#[derive(Debug, Clone)]
pub struct RpcFailure {
    /// Request ID this is responding to
    pub request_id: i64,
    /// Error message
    pub error_message: String,
}

impl Encodable for RpcFailure {
    fn encoded_length(&self) -> usize {
        8 + string_encoded_length(&self.error_message)
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(MessageType::RpcFailure as u8);
        buf.put_i64(self.request_id);
        encode_string(buf, &self.error_message);
    }
}

impl Decodable for RpcFailure {
    fn decode(buf: &mut Bytes) -> io::Result<Self> {
        if buf.remaining() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes for RpcFailure",
            ));
        }
        let request_id = buf.get_i64();
        let error_message = decode_string(buf)?;
        Ok(Self {
            request_id,
            error_message,
        })
    }
}

impl Message for RpcFailure {
    fn message_type(&self) -> MessageType {
        MessageType::RpcFailure
    }
}

/// Push data message.
#[derive(Debug, Clone)]
pub struct PushData {
    /// Request ID
    pub request_id: i64,
    /// Mode (0 for primary, 1 for replica)
    pub mode: u8,
    /// Shuffle key (appId-shuffleId)
    pub shuffle_key: String,
    /// Partition unique ID (partitionId-epoch)
    pub partition_unique_id: String,
    /// Data body
    pub body: Bytes,
}

impl PushData {
    pub fn new(
        request_id: i64,
        mode: u8,
        shuffle_key: String,
        partition_unique_id: String,
        body: Bytes,
    ) -> Self {
        Self {
            request_id,
            mode,
            shuffle_key,
            partition_unique_id,
            body,
        }
    }
}

impl Encodable for PushData {
    fn encoded_length(&self) -> usize {
        1 + // message type
        8 + // request_id
        1 + // mode
        string_encoded_length(&self.shuffle_key) +
        string_encoded_length(&self.partition_unique_id) +
        4 + // body length
        self.body.len()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(MessageType::PushData as u8);
        buf.put_i64(self.request_id);
        buf.put_u8(self.mode);
        encode_string(buf, &self.shuffle_key);
        encode_string(buf, &self.partition_unique_id);
        buf.put_i32(self.body.len() as i32);
        buf.put_slice(&self.body);
    }
}

impl Decodable for PushData {
    fn decode(buf: &mut Bytes) -> io::Result<Self> {
        if buf.remaining() < 9 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes for PushData header",
            ));
        }
        let request_id = buf.get_i64();
        let mode = buf.get_u8();
        let shuffle_key = decode_string(buf)?;
        let partition_unique_id = decode_string(buf)?;
        
        if buf.remaining() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes for PushData body length",
            ));
        }
        let body_len = buf.get_i32() as usize;
        if buf.remaining() < body_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes for PushData body",
            ));
        }
        let body = buf.copy_to_bytes(body_len);
        
        Ok(Self {
            request_id,
            mode,
            shuffle_key,
            partition_unique_id,
            body,
        })
    }
}

impl Message for PushData {
    fn message_type(&self) -> MessageType {
        MessageType::PushData
    }

    fn body(&self) -> Option<&Bytes> {
        Some(&self.body)
    }
}

/// Push merged data message (for multiple partitions).
#[derive(Debug, Clone)]
pub struct PushMergedData {
    /// Request ID
    pub request_id: i64,
    /// Mode (0 for primary, 1 for replica)
    pub mode: u8,
    /// Shuffle key
    pub shuffle_key: String,
    /// Partition unique IDs
    pub partition_unique_ids: Vec<String>,
    /// Batch offsets (cumulative offsets for each partition's data)
    pub batch_offsets: Vec<i32>,
    /// Combined data body
    pub body: Bytes,
}

impl Encodable for PushMergedData {
    fn encoded_length(&self) -> usize {
        let mut len = 1 + 8 + 1; // type + request_id + mode
        len += string_encoded_length(&self.shuffle_key);
        len += 4; // partition count
        for id in &self.partition_unique_ids {
            len += string_encoded_length(id);
        }
        len += 4 + self.batch_offsets.len() * 4; // offsets count + offsets
        len += 4 + self.body.len(); // body length + body
        len
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(MessageType::PushMergedData as u8);
        buf.put_i64(self.request_id);
        buf.put_u8(self.mode);
        encode_string(buf, &self.shuffle_key);
        
        buf.put_i32(self.partition_unique_ids.len() as i32);
        for id in &self.partition_unique_ids {
            encode_string(buf, id);
        }
        
        buf.put_i32(self.batch_offsets.len() as i32);
        for offset in &self.batch_offsets {
            buf.put_i32(*offset);
        }
        
        buf.put_i32(self.body.len() as i32);
        buf.put_slice(&self.body);
    }
}

impl Message for PushMergedData {
    fn message_type(&self) -> MessageType {
        MessageType::PushMergedData
    }

    fn body(&self) -> Option<&Bytes> {
        Some(&self.body)
    }
}

/// Open stream request.
#[derive(Debug, Clone)]
pub struct OpenStream {
    /// Shuffle key
    pub shuffle_key: String,
    /// File name
    pub file_name: String,
    /// Start chunk index
    pub start_index: i32,
    /// End chunk index
    pub end_index: i32,
}

impl Encodable for OpenStream {
    fn encoded_length(&self) -> usize {
        1 + // type
        string_encoded_length(&self.shuffle_key) +
        string_encoded_length(&self.file_name) +
        4 + 4 // start_index + end_index
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(MessageType::OpenStream as u8);
        encode_string(buf, &self.shuffle_key);
        encode_string(buf, &self.file_name);
        buf.put_i32(self.start_index);
        buf.put_i32(self.end_index);
    }
}

impl Decodable for OpenStream {
    fn decode(buf: &mut Bytes) -> io::Result<Self> {
        let shuffle_key = decode_string(buf)?;
        let file_name = decode_string(buf)?;
        if buf.remaining() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes for OpenStream indices",
            ));
        }
        let start_index = buf.get_i32();
        let end_index = buf.get_i32();
        Ok(Self {
            shuffle_key,
            file_name,
            start_index,
            end_index,
        })
    }
}

impl Message for OpenStream {
    fn message_type(&self) -> MessageType {
        MessageType::OpenStream
    }
}

/// Stream handle response.
#[derive(Debug, Clone)]
pub struct StreamHandle {
    /// Stream ID
    pub stream_id: i64,
    /// Number of chunks
    pub num_chunks: i32,
}

impl Encodable for StreamHandle {
    fn encoded_length(&self) -> usize {
        1 + 8 + 4 // type + stream_id + num_chunks
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(MessageType::StreamHandle as u8);
        buf.put_i64(self.stream_id);
        buf.put_i32(self.num_chunks);
    }
}

impl Decodable for StreamHandle {
    fn decode(buf: &mut Bytes) -> io::Result<Self> {
        if buf.remaining() < 12 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes for StreamHandle",
            ));
        }
        let stream_id = buf.get_i64();
        let num_chunks = buf.get_i32();
        Ok(Self {
            stream_id,
            num_chunks,
        })
    }
}

impl Message for StreamHandle {
    fn message_type(&self) -> MessageType {
        MessageType::StreamHandle
    }
}

/// Chunk fetch request.
#[derive(Debug, Clone)]
pub struct ChunkFetchRequest {
    /// Stream ID
    pub stream_id: i64,
    /// Chunk index
    pub chunk_index: i32,
    /// Offset within chunk
    pub offset: i32,
    /// Length to read
    pub len: i32,
}

impl Encodable for ChunkFetchRequest {
    fn encoded_length(&self) -> usize {
        1 + 8 + 4 + 4 + 4 // type + stream_id + chunk_index + offset + len
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(MessageType::ChunkFetchRequest as u8);
        buf.put_i64(self.stream_id);
        buf.put_i32(self.chunk_index);
        buf.put_i32(self.offset);
        buf.put_i32(self.len);
    }
}

impl Decodable for ChunkFetchRequest {
    fn decode(buf: &mut Bytes) -> io::Result<Self> {
        if buf.remaining() < 20 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes for ChunkFetchRequest",
            ));
        }
        let stream_id = buf.get_i64();
        let chunk_index = buf.get_i32();
        let offset = buf.get_i32();
        let len = buf.get_i32();
        Ok(Self {
            stream_id,
            chunk_index,
            offset,
            len,
        })
    }
}

impl Message for ChunkFetchRequest {
    fn message_type(&self) -> MessageType {
        MessageType::ChunkFetchRequest
    }
}

/// Chunk fetch success response.
#[derive(Debug, Clone)]
pub struct ChunkFetchSuccess {
    /// Stream ID
    pub stream_id: i64,
    /// Chunk index
    pub chunk_index: i32,
    /// Chunk data
    pub body: Bytes,
}

impl Encodable for ChunkFetchSuccess {
    fn encoded_length(&self) -> usize {
        1 + 8 + 4 + 4 + self.body.len()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(MessageType::ChunkFetchSuccess as u8);
        buf.put_i64(self.stream_id);
        buf.put_i32(self.chunk_index);
        buf.put_i32(self.body.len() as i32);
        buf.put_slice(&self.body);
    }
}

impl Decodable for ChunkFetchSuccess {
    fn decode(buf: &mut Bytes) -> io::Result<Self> {
        if buf.remaining() < 16 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes for ChunkFetchSuccess header",
            ));
        }
        let stream_id = buf.get_i64();
        let chunk_index = buf.get_i32();
        let body_len = buf.get_i32() as usize;
        if buf.remaining() < body_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes for ChunkFetchSuccess body",
            ));
        }
        let body = buf.copy_to_bytes(body_len);
        Ok(Self {
            stream_id,
            chunk_index,
            body,
        })
    }
}

impl Message for ChunkFetchSuccess {
    fn message_type(&self) -> MessageType {
        MessageType::ChunkFetchSuccess
    }

    fn body(&self) -> Option<&Bytes> {
        Some(&self.body)
    }
}

/// Chunk fetch failure response.
#[derive(Debug, Clone)]
pub struct ChunkFetchFailure {
    /// Stream ID
    pub stream_id: i64,
    /// Chunk index
    pub chunk_index: i32,
    /// Error message
    pub error_message: String,
}

impl Encodable for ChunkFetchFailure {
    fn encoded_length(&self) -> usize {
        1 + 8 + 4 + string_encoded_length(&self.error_message)
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(MessageType::ChunkFetchFailure as u8);
        buf.put_i64(self.stream_id);
        buf.put_i32(self.chunk_index);
        encode_string(buf, &self.error_message);
    }
}

impl Decodable for ChunkFetchFailure {
    fn decode(buf: &mut Bytes) -> io::Result<Self> {
        if buf.remaining() < 12 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes for ChunkFetchFailure header",
            ));
        }
        let stream_id = buf.get_i64();
        let chunk_index = buf.get_i32();
        let error_message = decode_string(buf)?;
        Ok(Self {
            stream_id,
            chunk_index,
            error_message,
        })
    }
}

impl Message for ChunkFetchFailure {
    fn message_type(&self) -> MessageType {
        MessageType::ChunkFetchFailure
    }
}

/// Push data handshake message.
#[derive(Debug, Clone)]
pub struct PushDataHandShake {
    /// Mode (0 for primary, 1 for replica)
    pub mode: u8,
    /// Shuffle key
    pub shuffle_key: String,
    /// Partition unique ID
    pub partition_unique_id: String,
    /// Attempt ID
    pub attempt_id: i32,
    /// Number of partitions
    pub num_partitions: i32,
    /// Buffer size
    pub buffer_size: i32,
}

impl Encodable for PushDataHandShake {
    fn encoded_length(&self) -> usize {
        1 + // type
        1 + // mode
        string_encoded_length(&self.shuffle_key) +
        string_encoded_length(&self.partition_unique_id) +
        4 + 4 + 4 // attempt_id + num_partitions + buffer_size
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(MessageType::PushDataHandShake as u8);
        buf.put_u8(self.mode);
        encode_string(buf, &self.shuffle_key);
        encode_string(buf, &self.partition_unique_id);
        buf.put_i32(self.attempt_id);
        buf.put_i32(self.num_partitions);
        buf.put_i32(self.buffer_size);
    }
}

impl Message for PushDataHandShake {
    fn message_type(&self) -> MessageType {
        MessageType::PushDataHandShake
    }
}

/// Region start message.
#[derive(Debug, Clone)]
pub struct RegionStart {
    /// Mode
    pub mode: u8,
    /// Shuffle key
    pub shuffle_key: String,
    /// Partition unique ID
    pub partition_unique_id: String,
    /// Attempt ID
    pub attempt_id: i32,
    /// Current region index
    pub current_region_index: i32,
    /// Whether this is a broadcast
    pub is_broadcast: bool,
}

impl Encodable for RegionStart {
    fn encoded_length(&self) -> usize {
        1 + 1 + 
        string_encoded_length(&self.shuffle_key) +
        string_encoded_length(&self.partition_unique_id) +
        4 + 4 + 1
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(MessageType::RegionStart as u8);
        buf.put_u8(self.mode);
        encode_string(buf, &self.shuffle_key);
        encode_string(buf, &self.partition_unique_id);
        buf.put_i32(self.attempt_id);
        buf.put_i32(self.current_region_index);
        buf.put_u8(if self.is_broadcast { 1 } else { 0 });
    }
}

impl Message for RegionStart {
    fn message_type(&self) -> MessageType {
        MessageType::RegionStart
    }
}

/// Region finish message.
#[derive(Debug, Clone)]
pub struct RegionFinish {
    /// Mode
    pub mode: u8,
    /// Shuffle key
    pub shuffle_key: String,
    /// Partition unique ID
    pub partition_unique_id: String,
    /// Attempt ID
    pub attempt_id: i32,
}

impl Encodable for RegionFinish {
    fn encoded_length(&self) -> usize {
        1 + 1 +
        string_encoded_length(&self.shuffle_key) +
        string_encoded_length(&self.partition_unique_id) +
        4
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(MessageType::RegionFinish as u8);
        buf.put_u8(self.mode);
        encode_string(buf, &self.shuffle_key);
        encode_string(buf, &self.partition_unique_id);
        buf.put_i32(self.attempt_id);
    }
}

impl Message for RegionFinish {
    fn message_type(&self) -> MessageType {
        MessageType::RegionFinish
    }
}

/// Heartbeat message.
#[derive(Debug, Clone, Default)]
pub struct Heartbeat;

impl Encodable for Heartbeat {
    fn encoded_length(&self) -> usize {
        1
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(MessageType::Heartbeat as u8);
    }
}

impl Decodable for Heartbeat {
    fn decode(_buf: &mut Bytes) -> io::Result<Self> {
        Ok(Self)
    }
}

impl Message for Heartbeat {
    fn message_type(&self) -> MessageType {
        MessageType::Heartbeat
    }
}

/// One-way message (no response expected).
#[derive(Debug, Clone)]
pub struct OneWayMessage {
    /// Message body
    pub body: Bytes,
}

impl Encodable for OneWayMessage {
    fn encoded_length(&self) -> usize {
        1 + 4 + self.body.len()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(MessageType::OneWayMessage as u8);
        buf.put_i32(self.body.len() as i32);
        buf.put_slice(&self.body);
    }
}

impl Decodable for OneWayMessage {
    fn decode(buf: &mut Bytes) -> io::Result<Self> {
        if buf.remaining() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes for OneWayMessage",
            ));
        }
        let body_len = buf.get_i32() as usize;
        if buf.remaining() < body_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes for OneWayMessage body",
            ));
        }
        let body = buf.copy_to_bytes(body_len);
        Ok(Self { body })
    }
}

impl Message for OneWayMessage {
    fn message_type(&self) -> MessageType {
        MessageType::OneWayMessage
    }

    fn body(&self) -> Option<&Bytes> {
        Some(&self.body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_request_encode_decode() {
        let request = RpcRequest::new(12345, Bytes::from("test body"));
        let mut buf = request.encode_to_bytes();
        
        // Skip message type byte
        let mut bytes = buf.freeze();
        let _ = bytes.get_u8();
        
        let decoded = RpcRequest::decode(&mut bytes).unwrap();
        assert_eq!(decoded.request_id, 12345);
        assert_eq!(decoded.body, Bytes::from("test body"));
    }

    #[test]
    fn test_push_data_encode_decode() {
        let push_data = PushData::new(
            1,
            0,
            "app-1".to_string(),
            "0-0".to_string(),
            Bytes::from(vec![1u8, 2, 3, 4, 5]),
        );
        let mut buf = push_data.encode_to_bytes();
        
        // Skip message type byte
        let mut bytes = buf.freeze();
        let _ = bytes.get_u8();
        
        let decoded = PushData::decode(&mut bytes).unwrap();
        assert_eq!(decoded.request_id, 1);
        assert_eq!(decoded.mode, 0);
        assert_eq!(decoded.shuffle_key, "app-1");
        assert_eq!(decoded.partition_unique_id, "0-0");
        assert_eq!(decoded.body, Bytes::from(vec![1u8, 2, 3, 4, 5]));
    }
}
