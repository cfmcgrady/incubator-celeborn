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

//! Codec for encoding and decoding Celeborn messages.
//!
//! Celeborn frame format:
//! ```text
//! +------------+----------+------------+------------------+---------------+
//! | msgSize    | msgType  | bodySize   | message content  | body (opt)    |
//! | (4 bytes)  | (1 byte) | (4 bytes)  | (msgSize bytes)  | (bodySize B)  |
//! +------------+----------+------------+------------------+---------------+
//! ```
//!
//! - msgSize: length of message content (not including header)
//! - msgType: message type ID (1 byte)
//! - bodySize: length of optional body data
//! - message content: encoded message fields
//! - body: optional body data (e.g., protobuf payload for RPC)

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

use crate::protocol::message::MessageType;

/// Header size: msgSize (4) + msgType (1) + bodySize (4) = 9 bytes
const HEADER_SIZE: usize = 9;

/// Maximum frame size (2GB - reasonable limit).
const MAX_FRAME_SIZE: usize = 2 * 1024 * 1024 * 1024;

/// A frame containing a message type, message content, and optional body.
#[derive(Debug, Clone)]
pub struct Frame {
    /// Message type
    pub message_type: MessageType,
    /// Message content (encoded message fields, excluding body)
    pub message: Bytes,
    /// Optional body data
    pub body: Bytes,
}

impl Frame {
    /// Create a new frame with message content only (no body).
    pub fn new(message_type: MessageType, message: Bytes) -> Self {
        Self {
            message_type,
            message,
            body: Bytes::new(),
        }
    }

    /// Create a new frame with message content and body.
    pub fn with_body(message_type: MessageType, message: Bytes, body: Bytes) -> Self {
        Self {
            message_type,
            message,
            body,
        }
    }

    /// Get the total frame size (header + message + body).
    pub fn total_size(&self) -> usize {
        HEADER_SIZE + self.message.len() + self.body.len()
    }
}

/// Decoder state for parsing Celeborn frames.
#[derive(Debug, Default)]
struct DecoderState {
    /// Message size (from header)
    msg_size: Option<usize>,
    /// Message type (from header)
    msg_type: Option<MessageType>,
    /// Body size (from header)
    body_size: Option<usize>,
}

/// Codec for Celeborn message framing.
#[derive(Debug, Default)]
pub struct CelebornCodec {
    /// Current decoder state
    state: DecoderState,
}

impl CelebornCodec {
    /// Create a new codec.
    pub fn new() -> Self {
        Self {
            state: DecoderState::default(),
        }
    }

    /// Reset decoder state.
    fn reset_state(&mut self) {
        self.state = DecoderState::default();
    }
}

impl Decoder for CelebornCodec {
    type Item = Frame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Read header if we haven't yet
        if self.state.msg_size.is_none() {
            if src.len() < HEADER_SIZE {
                return Ok(None);
            }

            // Parse header
            let msg_size = src.get_u32() as usize;
            let msg_type_id = src.get_u8();
            let body_size = src.get_u32() as usize;

            let total_frame_size = msg_size + body_size;
            if total_frame_size > MAX_FRAME_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Frame too large: {} bytes (max: {})",
                        total_frame_size, MAX_FRAME_SIZE
                    ),
                ));
            }

            self.state.msg_size = Some(msg_size);
            self.state.msg_type = Some(MessageType::from(msg_type_id));
            self.state.body_size = Some(body_size);
        }

        let msg_size = self.state.msg_size.unwrap();
        let msg_type = self.state.msg_type.unwrap();
        let body_size = self.state.body_size.unwrap();
        let total_content_size = msg_size + body_size;

        // Wait for complete frame content
        if src.len() < total_content_size {
            src.reserve(total_content_size - src.len());
            return Ok(None);
        }

        // Extract message content
        let message = src.split_to(msg_size).freeze();

        // Extract body
        let body = if body_size > 0 {
            src.split_to(body_size).freeze()
        } else {
            Bytes::new()
        };

        // Reset state for next frame
        self.reset_state();

        Ok(Some(Frame {
            message_type: msg_type,
            message,
            body,
        }))
    }
}

impl Encoder<Frame> for CelebornCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let msg_size = item.message.len();
        let body_size = item.body.len();
        let total_size = HEADER_SIZE + msg_size + body_size;

        if total_size > MAX_FRAME_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Frame too large: {} bytes (max: {})", total_size, MAX_FRAME_SIZE),
            ));
        }

        // Reserve space
        dst.reserve(total_size);

        // Write header
        dst.put_u32(msg_size as u32); // msgSize
        dst.put_u8(item.message_type as u8); // msgType
        dst.put_u32(body_size as u32); // bodySize

        // Write message content
        dst.put_slice(&item.message);

        // Write body
        if body_size > 0 {
            dst.put_slice(&item.body);
        }

        Ok(())
    }
}

/// Encoder for raw bytes (used for push data).
#[derive(Debug, Default)]
pub struct RawBytesCodec;

impl Decoder for RawBytesCodec {
    type Item = BytesMut;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            Ok(None)
        } else {
            Ok(Some(src.split()))
        }
    }
}

impl Encoder<Bytes> for RawBytesCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.reserve(item.len());
        dst.put_slice(&item);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_codec_encode_decode() {
        let mut codec = CelebornCodec::new();
        let mut buf = BytesMut::new();

        // Encode a frame with message content only
        let frame = Frame::new(MessageType::RpcRequest, Bytes::from(vec![1, 2, 3, 4, 5]));
        codec.encode(frame.clone(), &mut buf).unwrap();

        // Verify header format
        assert_eq!(buf.len(), HEADER_SIZE + 5); // header + message

        // Decode the frame
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.message_type, MessageType::RpcRequest);
        assert_eq!(decoded.message, Bytes::from(vec![1, 2, 3, 4, 5]));
        assert!(decoded.body.is_empty());
    }

    #[test]
    fn test_codec_with_body() {
        let mut codec = CelebornCodec::new();
        let mut buf = BytesMut::new();

        // Encode a frame with message and body
        let frame = Frame::with_body(
            MessageType::RpcRequest,
            Bytes::from(vec![1, 2, 3]), // message content
            Bytes::from(vec![4, 5, 6, 7, 8]), // body
        );
        codec.encode(frame, &mut buf).unwrap();

        // Verify total size
        assert_eq!(buf.len(), HEADER_SIZE + 3 + 5);

        // Decode the frame
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.message_type, MessageType::RpcRequest);
        assert_eq!(decoded.message, Bytes::from(vec![1, 2, 3]));
        assert_eq!(decoded.body, Bytes::from(vec![4, 5, 6, 7, 8]));
    }

    #[test]
    fn test_codec_partial_frame() {
        let mut codec = CelebornCodec::new();
        let mut buf = BytesMut::new();

        // Encode a frame
        let frame = Frame::new(MessageType::PushData, Bytes::from(vec![1, 2, 3, 4, 5]));
        codec.encode(frame, &mut buf).unwrap();

        // Split the buffer to simulate partial read (less than header)
        let mut partial = buf.split_to(5);

        // Should return None for partial frame
        assert!(codec.decode(&mut partial).unwrap().is_none());

        // Add the rest
        partial.unsplit(buf);

        // Now should decode successfully
        let decoded = codec.decode(&mut partial).unwrap().unwrap();
        assert_eq!(decoded.message_type, MessageType::PushData);
    }

    #[test]
    fn test_codec_frame_too_large() {
        let mut codec = CelebornCodec::new();
        let mut buf = BytesMut::new();

        // Write a header with frame size that's too large
        buf.put_u32(u32::MAX); // msgSize
        buf.put_u8(3); // msgType (RPC_REQUEST)
        buf.put_u32(0); // bodySize

        let result = codec.decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_header_format() {
        let mut codec = CelebornCodec::new();
        let mut buf = BytesMut::new();

        // Create a frame
        let message = Bytes::from(vec![0x01, 0x02, 0x03, 0x04]); // 4 bytes
        let body = Bytes::from(vec![0x05, 0x06]); // 2 bytes
        let frame = Frame::with_body(MessageType::RpcRequest, message, body);

        codec.encode(frame, &mut buf).unwrap();

        // Verify header bytes
        let header: Vec<u8> = buf[..HEADER_SIZE].to_vec();
        assert_eq!(header[0..4], [0, 0, 0, 4]); // msgSize = 4 (big endian)
        assert_eq!(header[4], 3); // msgType = RPC_REQUEST (3)
        assert_eq!(header[5..9], [0, 0, 0, 2]); // bodySize = 2 (big endian)
    }
}
