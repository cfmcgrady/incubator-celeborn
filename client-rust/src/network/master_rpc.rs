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

//! Master RPC client using Java serialization format.
//!
//! Celeborn Master uses NettyRpcEnv which requires Java serialization for RPC messages.
//! This module provides a client that can communicate with the Master using the correct
//! wire format.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use prost::Message as ProstMessage;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{debug, warn};

use crate::error::{CelebornError, Result};
use crate::protocol::java_serialization::{encode_request_message, RpcAddress};
use crate::protocol::transport::TransportMessageType;

/// Request ID counter
static REQUEST_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Generate a unique request ID.
fn next_request_id() -> u64 {
    REQUEST_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Master RPC client for communicating with Celeborn Master.
pub struct MasterRpcClient {
    /// Master endpoints
    master_endpoints: Vec<SocketAddr>,
    /// Current master index
    current_master_index: std::sync::atomic::AtomicUsize,
    /// Local address for RPC
    local_address: Option<RpcAddress>,
    /// RPC timeout
    rpc_timeout: Duration,
    /// Max retries
    max_retries: usize,
    /// Retry wait duration
    retry_wait: Duration,
}

impl MasterRpcClient {
    /// Create a new Master RPC client.
    pub fn new(
        master_endpoints: Vec<SocketAddr>,
        local_address: Option<RpcAddress>,
        rpc_timeout: Duration,
        max_retries: usize,
        retry_wait: Duration,
    ) -> Result<Self> {
        if master_endpoints.is_empty() {
            return Err(CelebornError::Config(
                "No master endpoints provided".to_string(),
            ));
        }

        Ok(Self {
            master_endpoints,
            current_master_index: std::sync::atomic::AtomicUsize::new(0),
            local_address,
            rpc_timeout,
            max_retries,
            retry_wait,
        })
    }

    /// Get the current master endpoint.
    fn current_master(&self) -> SocketAddr {
        let index = self
            .current_master_index
            .load(std::sync::atomic::Ordering::Relaxed);
        self.master_endpoints[index % self.master_endpoints.len()]
    }

    /// Switch to the next master endpoint.
    fn switch_master(&self) {
        self.current_master_index
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Send an RPC request to the master.
    pub async fn send_rpc<Req, Resp>(
        &self,
        message_type: TransportMessageType,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: ProstMessage,
        Resp: ProstMessage + Default,
    {
        let mut last_error = None;

        for attempt in 0..self.max_retries {
            let master_addr = self.current_master();

            match self
                .send_rpc_to_addr(master_addr, message_type, request)
                .await
            {
                Ok(response) => return Ok(response),
                Err(e) => {
                    warn!(
                        "Failed to send RPC to master {} (attempt {}): {}",
                        master_addr,
                        attempt + 1,
                        e
                    );
                    last_error = Some(e);
                    self.switch_master();

                    if attempt < self.max_retries - 1 {
                        tokio::time::sleep(self.retry_wait).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            CelebornError::Connection("All master endpoints failed".to_string())
        }))
    }

    /// Send an RPC request to a specific master address.
    async fn send_rpc_to_addr<Req, Resp>(
        &self,
        addr: SocketAddr,
        message_type: TransportMessageType,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: ProstMessage,
        Resp: ProstMessage + Default,
    {
        debug!("Connecting to master at {}", addr);

        // Connect to master
        let mut stream = timeout(self.rpc_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| CelebornError::Timeout(self.rpc_timeout.as_millis() as u64))?
            .map_err(|e| CelebornError::Connection(format!("Failed to connect: {}", e)))?;

        // Encode the protobuf request
        let mut payload = Vec::new();
        request.encode(&mut payload).map_err(|e| {
            CelebornError::Serialization(format!("Failed to encode request: {}", e))
        })?;

        // Create receiver address from master endpoint
        let receiver_address = RpcAddress::new(addr.ip().to_string(), addr.port() as i32);

        // Encode the RequestMessage with Java serialization
        let request_message = encode_request_message(
            self.local_address.as_ref(),
            Some(&receiver_address),
            "MasterEndpoint",
            message_type as i32,
            &payload,
        );

        // Generate request ID
        let request_id = next_request_id();

        // Build the complete frame according to TransportFrameDecoder format:
        //
        // Header (9 bytes):
        // - msgSize (4 bytes): size of message content (requestId + bodySize = 12 bytes)
        // - msgType (1 byte): Message.Type.id (RPC_REQUEST = 3)
        // - bodySize (4 bytes): size of body (the RequestMessage)
        //
        // Message content (12 bytes for RPC_REQUEST):
        // - requestId (8 bytes)
        // - bodySize (4 bytes) - redundant but required
        //
        // Body:
        // - The actual payload (RequestMessage serialized)
        
        let body_size = request_message.len();
        let msg_size = 8 + 4; // requestId (8) + bodySize field in message (4)
        
        let mut frame = BytesMut::with_capacity(9 + msg_size + body_size);
        
        // Header
        frame.put_i32(msg_size as i32);           // msgSize
        frame.put_u8(3);                           // msgType: RPC_REQUEST = 3
        frame.put_i32(body_size as i32);          // bodySize
        
        // Message content (RpcRequest.encode)
        frame.put_i64(request_id as i64);         // requestId
        frame.put_i32(body_size as i32);          // bodySize (redundant but required)
        
        // Body (the RequestMessage)
        frame.extend_from_slice(&request_message);

        debug!(
            "Sending RPC request {} to master, frame size: {}",
            request_id,
            frame.len()
        );

        // Send the frame
        timeout(self.rpc_timeout, stream.write_all(&frame))
            .await
            .map_err(|_| CelebornError::Timeout(self.rpc_timeout.as_millis() as u64))?
            .map_err(|e| CelebornError::Connection(format!("Failed to write: {}", e)))?;

        // Read response
        let response = self.read_response(&mut stream, request_id).await?;

        Ok(response)
    }

    /// Read and decode the RPC response.
    ///
    /// Response frame format (TransportFrameDecoder):
    /// Header (9 bytes):
    /// - msgSize (4 bytes): size of message content
    /// - msgType (1 byte): Message.Type.id (RPC_RESPONSE=4, RPC_FAILURE=5)
    /// - bodySize (4 bytes): size of body
    ///
    /// Message content (for RPC_RESPONSE/RPC_FAILURE):
    /// - requestId (8 bytes)
    /// - bodySize (4 bytes) - redundant
    ///
    /// Body:
    /// - The actual payload (Java serialized response)
    async fn read_response<Resp>(&self, stream: &mut TcpStream, expected_request_id: u64) -> Result<Resp>
    where
        Resp: ProstMessage + Default,
    {
        // Read header (9 bytes)
        let mut header = [0u8; 9];
        timeout(self.rpc_timeout, stream.read_exact(&mut header))
            .await
            .map_err(|_| CelebornError::Timeout(self.rpc_timeout.as_millis() as u64))?
            .map_err(|e| CelebornError::Connection(format!("Failed to read header: {}", e)))?;

        let msg_size = i32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let msg_type = header[4];
        let body_size = i32::from_be_bytes([header[5], header[6], header[7], header[8]]) as usize;

        debug!(
            "Response header: msg_size={}, msg_type={}, body_size={}",
            msg_size, msg_type, body_size
        );

        // Validate sizes
        let total_size = msg_size + body_size;
        if total_size > 100 * 1024 * 1024 {
            return Err(CelebornError::Protocol(format!(
                "Frame too large: {} bytes",
                total_size
            )));
        }

        // Read message content
        let mut msg_content = vec![0u8; msg_size];
        if msg_size > 0 {
            timeout(self.rpc_timeout, stream.read_exact(&mut msg_content))
                .await
                .map_err(|_| CelebornError::Timeout(self.rpc_timeout.as_millis() as u64))?
                .map_err(|e| CelebornError::Connection(format!("Failed to read message: {}", e)))?;
        }

        // Read body
        let mut body = vec![0u8; body_size];
        if body_size > 0 {
            timeout(self.rpc_timeout, stream.read_exact(&mut body))
                .await
                .map_err(|_| CelebornError::Timeout(self.rpc_timeout.as_millis() as u64))?
                .map_err(|e| CelebornError::Connection(format!("Failed to read body: {}", e)))?;
        }

        // Parse request ID from message content
        let response_request_id = if msg_content.len() >= 8 {
            i64::from_be_bytes([
                msg_content[0], msg_content[1], msg_content[2], msg_content[3],
                msg_content[4], msg_content[5], msg_content[6], msg_content[7],
            ]) as u64
        } else {
            0
        };

        debug!(
            "Response: msg_type={}, request_id={}",
            msg_type, response_request_id
        );

        if response_request_id != expected_request_id {
            warn!(
                "Request ID mismatch: expected {}, got {}",
                expected_request_id, response_request_id
            );
        }

        match msg_type {
            4 => {
                // RPC_RESPONSE - body is Java serialized TransportMessage
                debug!("Response body ({} bytes): {:02x?}", body.len(), &body[..std::cmp::min(100, body.len())]);
                self.decode_java_response(&body)
            }
            5 => {
                // RPC_FAILURE
                let error_msg = self.decode_java_error(&body)?;
                Err(CelebornError::ServerError {
                    status: crate::error::StatusCode::RpcFailed,
                    message: error_msg,
                })
            }
            _ => Err(CelebornError::Protocol(format!(
                "Unknown response type: {}",
                msg_type
            ))),
        }
    }

    /// Decode a Java serialized response.
    ///
    /// The response can be either:
    /// 1. TransportMessage - contains protobuf payload
    /// 2. RpcFailure - contains a Throwable (error)
    fn decode_java_response<Resp>(&self, data: &[u8]) -> Result<Resp>
    where
        Resp: ProstMessage + Default,
    {
        if data.len() < 4 {
            return Err(CelebornError::Protocol(
                "Response too short for Java serialization".to_string(),
            ));
        }

        // Verify Java serialization magic
        if data[0] != 0xAC || data[1] != 0xED {
            return Err(CelebornError::Protocol(format!(
                "Invalid Java serialization magic: {:02X}{:02X}",
                data[0], data[1]
            )));
        }

        // Check what type of object this is by looking at the class name
        // Format after header: TC_OBJECT (0x73) + TC_CLASSDESC (0x72) + length (2 bytes) + class name
        if data.len() > 10 && data[4] == 0x73 && data[5] == 0x72 {
            let class_name_len = u16::from_be_bytes([data[6], data[7]]) as usize;
            if data.len() > 8 + class_name_len {
                let class_name = String::from_utf8_lossy(&data[8..8 + class_name_len]);
                debug!("Response object class: {}", class_name);
                
                if class_name.contains("RpcFailure") {
                    // This is an RpcFailure - extract the error message
                    let error_msg = self.extract_rpc_failure_message(data)?;
                    return Err(CelebornError::ServerError {
                        status: crate::error::StatusCode::RpcFailed,
                        message: error_msg,
                    });
                }
            }
        }

        // Try to extract TransportMessage payload
        let payload = self.extract_transport_message_payload(data)?;
        
        // Decode the protobuf response from the payload
        Resp::decode(payload.as_slice()).map_err(|e| {
            CelebornError::Serialization(format!("Failed to decode protobuf response: {}", e))
        })
    }

    /// Extract error message from RpcFailure Java serialization.
    fn extract_rpc_failure_message(&self, data: &[u8]) -> Result<String> {
        // RpcFailure contains a Throwable. We need to find the exception message.
        // Look for strings in the serialization that might be the error message.
        
        let mut messages = Vec::new();
        let mut pos = 0;
        
        while pos < data.len() - 3 {
            // Look for TC_STRING (0x74)
            if data[pos] == 0x74 {
                pos += 1;
                if pos + 2 <= data.len() {
                    let str_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
                    pos += 2;
                    if pos + str_len <= data.len() {
                        if let Ok(s) = String::from_utf8(data[pos..pos + str_len].to_vec()) {
                            // Skip class names and type descriptors
                            if !s.starts_with("java.")
                                && !s.starts_with("org.apache.celeborn.common.rpc")
                                && !s.starts_with("[")
                                && !s.starts_with("L")
                                && !s.is_empty()
                                && s.len() > 5 // Skip short strings like field names
                            {
                                messages.push(s);
                            }
                        }
                        pos += str_len;
                        continue;
                    }
                }
            }
            pos += 1;
        }

        if messages.is_empty() {
            Ok("Unknown RPC failure".to_string())
        } else {
            // Return the longest message (likely the actual error)
            Ok(messages.into_iter().max_by_key(|s| s.len()).unwrap_or_default())
        }
    }

    /// Extract the payload from a Java serialized TransportMessage.
    fn extract_transport_message_payload(&self, data: &[u8]) -> Result<Vec<u8>> {
        // This is a simplified parser for Java serialization.
        // It looks for the byte array payload in the TransportMessage.
        //
        // The structure is:
        // - Stream header (4 bytes)
        // - TC_OBJECT (1 byte)
        // - Class descriptor for TransportMessage
        // - Field values:
        //   - messageTypeValue (int, 4 bytes)
        //   - payload (byte array)
        
        // Skip stream header
        let mut pos = 4;
        
        // We need to parse through the Java serialization to find the byte array
        // This is complex, so we'll use a heuristic approach:
        // Look for TC_ARRAY (0x75) followed by byte array class descriptor
        
        while pos < data.len() - 10 {
            // Look for TC_ARRAY marker
            if data[pos] == 0x75 {
                // Check if this is followed by a byte array class descriptor
                // TC_CLASSDESC (0x72) + "[B" or TC_REFERENCE
                if pos + 1 < data.len() {
                    let next = data[pos + 1];
                    if next == 0x72 || next == 0x71 {
                        // Try to parse as byte array
                        if let Some((array_data, _)) = self.try_parse_byte_array(&data[pos..]) {
                            return Ok(array_data);
                        }
                    }
                }
            }
            pos += 1;
        }

        Err(CelebornError::Protocol(
            "Could not find TransportMessage payload in Java serialization".to_string(),
        ))
    }

    /// Try to parse a byte array from Java serialization data.
    fn try_parse_byte_array(&self, data: &[u8]) -> Option<(Vec<u8>, usize)> {
        if data.len() < 10 {
            return None;
        }

        // TC_ARRAY
        if data[0] != 0x75 {
            return None;
        }

        let mut pos = 1;

        // Skip class descriptor (either TC_CLASSDESC or TC_REFERENCE)
        if data[pos] == 0x72 {
            // TC_CLASSDESC - need to skip the full descriptor
            pos += 1;
            
            // Class name length (2 bytes)
            if pos + 2 > data.len() {
                return None;
            }
            let name_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2 + name_len;
            
            // Serial version UID (8 bytes)
            pos += 8;
            
            // Skip to end of class descriptor
            // This is simplified - in reality we'd need to parse the full descriptor
            while pos < data.len() && data[pos] != 0x78 {
                pos += 1;
            }
            if pos < data.len() {
                pos += 1; // Skip TC_ENDBLOCKDATA
            }
            // Skip super class (TC_NULL)
            if pos < data.len() && data[pos] == 0x70 {
                pos += 1;
            }
        } else if data[pos] == 0x71 {
            // TC_REFERENCE - 4 bytes handle
            pos += 5;
        } else {
            return None;
        }

        // Array length (4 bytes)
        if pos + 4 > data.len() {
            return None;
        }
        let array_len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;

        // Array data
        if pos + array_len > data.len() {
            return None;
        }
        let array_data = data[pos..pos + array_len].to_vec();
        pos += array_len;

        Some((array_data, pos))
    }

    /// Decode a Java serialized error message.
    fn decode_java_error(&self, data: &[u8]) -> Result<String> {
        // The error is typically a Java exception serialized with ObjectOutputStream.
        // For now, we'll try to extract any readable string from it.
        
        // Look for UTF strings in the data
        let mut pos = 0;
        while pos < data.len() - 3 {
            // Look for TC_STRING (0x74)
            if data[pos] == 0x74 {
                pos += 1;
                if pos + 2 <= data.len() {
                    let str_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
                    pos += 2;
                    if pos + str_len <= data.len() {
                        if let Ok(s) = String::from_utf8(data[pos..pos + str_len].to_vec()) {
                            // Skip class names and look for actual error messages
                            if !s.starts_with("java.") && !s.starts_with("org.") && !s.starts_with("[") {
                                return Ok(s);
                            }
                        }
                    }
                }
            }
            pos += 1;
        }

        Ok("Unknown error from master".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_id_generation() {
        let id1 = next_request_id();
        let id2 = next_request_id();
        assert!(id2 > id1);
    }

    #[test]
    fn test_master_rpc_client_creation() {
        let endpoints = vec!["127.0.0.1:9097".parse().unwrap()];
        let client = MasterRpcClient::new(
            endpoints,
            Some(RpcAddress::new("localhost", 12345)),
            Duration::from_secs(30),
            3,
            Duration::from_millis(100),
        );
        assert!(client.is_ok());
    }

    #[test]
    fn test_master_rpc_client_no_endpoints() {
        let client = MasterRpcClient::new(
            vec![],
            None,
            Duration::from_secs(30),
            3,
            Duration::from_millis(100),
        );
        assert!(client.is_err());
    }
}
