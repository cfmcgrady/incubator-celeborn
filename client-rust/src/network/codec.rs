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

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

use crate::protocol::message::MessageType;

/// Frame header size (frame length field).
const FRAME_HEADER_SIZE: usize = 4;

/// Maximum frame size (64MB).
const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;

/// A frame containing a message type and payload.
#[derive(Debug, Clone)]
pub struct Frame {
    /// Message type
    pub message_type: MessageType,
    /// Message payload (excluding type byte)
    pub payload: Bytes,
}

impl Frame {
    /// Create a new frame.
    pub fn new(message_type: MessageType, payload: Bytes) -> Self {
        Self {
            message_type,
            payload,
        }
    }

    /// Get the total size of the frame (type + payload).
    pub fn size(&self) -> usize {
        1 + self.payload.len()
    }
}

/// Codec for Celeborn message framing.
///
/// Frame format:
/// ```text
/// +----------------+------+---------+
/// | Frame Length   | Type | Payload |
/// | (4 bytes, BE)  | (1B) | (var)   |
/// +----------------+------+---------+
/// ```
#[derive(Debug, Default)]
pub struct CelebornCodec {
    /// Current frame length being decoded
    current_frame_len: Option<usize>,
}

impl CelebornCodec {
    /// Create a new codec.
    pub fn new() -> Self {
        Self {
            current_frame_len: None,
        }
    }
}

impl Decoder for CelebornCodec {
    type Item = Frame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Read frame length if we haven't yet
        if self.current_frame_len.is_none() {
            if src.len() < FRAME_HEADER_SIZE {
                return Ok(None);
            }
            let frame_len = (&src[..FRAME_HEADER_SIZE]).get_u32() as usize;
            
            if frame_len > MAX_FRAME_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Frame too large: {} bytes (max: {})", frame_len, MAX_FRAME_SIZE),
                ));
            }
            
            self.current_frame_len = Some(frame_len);
            src.advance(FRAME_HEADER_SIZE);
        }

        // Read frame content
        let frame_len = self.current_frame_len.unwrap();
        if src.len() < frame_len {
            // Reserve space for the rest of the frame
            src.reserve(frame_len - src.len());
            return Ok(None);
        }

        // Extract the frame
        let frame_data = src.split_to(frame_len);
        self.current_frame_len = None;

        if frame_data.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Empty frame",
            ));
        }

        // Parse message type
        let message_type = MessageType::from(frame_data[0]);
        let payload = frame_data.freeze().slice(1..);

        Ok(Some(Frame {
            message_type,
            payload,
        }))
    }
}

impl Encoder<Frame> for CelebornCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let frame_len = item.size();
        
        if frame_len > MAX_FRAME_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Frame too large: {} bytes (max: {})", frame_len, MAX_FRAME_SIZE),
            ));
        }

        // Reserve space
        dst.reserve(FRAME_HEADER_SIZE + frame_len);

        // Write frame length
        dst.put_u32(frame_len as u32);

        // Write message type
        dst.put_u8(item.message_type as u8);

        // Write payload
        dst.put_slice(&item.payload);

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

        // Encode a frame
        let frame = Frame::new(
            MessageType::RpcRequest,
            Bytes::from(vec![1, 2, 3, 4, 5]),
        );
        codec.encode(frame.clone(), &mut buf).unwrap();

        // Decode the frame
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.message_type, MessageType::RpcRequest);
        assert_eq!(decoded.payload, Bytes::from(vec![1, 2, 3, 4, 5]));
    }

    #[test]
    fn test_codec_partial_frame() {
        let mut codec = CelebornCodec::new();
        let mut buf = BytesMut::new();

        // Encode a frame
        let frame = Frame::new(
            MessageType::PushData,
            Bytes::from(vec![1, 2, 3, 4, 5]),
        );
        codec.encode(frame, &mut buf).unwrap();

        // Split the buffer to simulate partial read
        let mut partial = buf.split_to(3);
        
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

        // Write a frame length that's too large
        buf.put_u32((MAX_FRAME_SIZE + 1) as u32);
        buf.put_u8(0);

        let result = codec.decode(&mut buf);
        assert!(result.is_err());
    }
}
