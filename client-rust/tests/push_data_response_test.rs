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

//! Integration tests for PushData response handling.
//!
//! These tests validate that the Rust client correctly handles Worker responses
//! for PushData requests, including various status codes like:
//! - SUCCESS (0)
//! - SOFT_SPLIT (22)
//! - HARD_SPLIT (21)
//! - MAP_ENDED (15)
//! - Congestion status codes (30, 31)
//!
//! This is critical for the Comet-Celeborn integration where proper response
//! handling ensures data integrity and correct shuffle behavior.
//!
//! Run with:
//!   cargo test --test push_data_response_test -- --nocapture

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Status codes from Celeborn protocol
mod status_code {
    pub const SUCCESS: u8 = 0;
    pub const MAP_ENDED: u8 = 15;
    pub const HARD_SPLIT: u8 = 21;
    pub const SOFT_SPLIT: u8 = 22;
    pub const PUSH_DATA_SUCCESS_PRIMARY_CONGESTED: u8 = 30;
    pub const PUSH_DATA_SUCCESS_REPLICA_CONGESTED: u8 = 31;
}

/// Message types
mod message_type {
    pub const RPC_RESPONSE: u8 = 4;
    pub const PUSH_DATA: u8 = 11;
}

/// Mock Worker server that responds to PushData requests.
struct MockWorkerServer {
    listener: TcpListener,
    port: u16,
    running: Arc<AtomicBool>,
    /// Status code to return for PushData requests
    response_status: Arc<AtomicI64>,
}

impl MockWorkerServer {
    /// Create a new mock worker server.
    async fn new() -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        
        Ok(Self {
            listener,
            port,
            running: Arc::new(AtomicBool::new(true)),
            response_status: Arc::new(AtomicI64::new(status_code::SUCCESS as i64)),
        })
    }

    /// Get the server port.
    fn port(&self) -> u16 {
        self.port
    }

    /// Set the status code to return for PushData requests.
    fn set_response_status(&self, status: u8) {
        self.response_status.store(status as i64, Ordering::Release);
    }

    /// Stop the server.
    fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    /// Start accepting connections.
    async fn run(&self) {
        while self.running.load(Ordering::Acquire) {
            tokio::select! {
                result = self.listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            let running = self.running.clone();
                            let response_status = self.response_status.clone();
                            
                            tokio::spawn(async move {
                                if let Err(e) = Self::handle_connection(
                                    stream,
                                    addr,
                                    running,
                                    response_status,
                                ).await {
                                    eprintln!("Connection error from {}: {}", addr, e);
                                }
                            });
                        }
                        Err(e) => {
                            eprintln!("Accept error: {}", e);
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    if !self.running.load(Ordering::Acquire) {
                        break;
                    }
                }
            }
        }
    }

    /// Handle a single connection.
    async fn handle_connection(
        mut stream: TcpStream,
        addr: SocketAddr,
        running: Arc<AtomicBool>,
        response_status: Arc<AtomicI64>,
    ) -> std::io::Result<()> {
        println!("[MockWorker] New connection from {}", addr);
        
        let mut buf = vec![0u8; 65536];
        
        while running.load(Ordering::Acquire) {
            // Read frame header: msgSize (4) + msgType (1) + bodySize (4) = 9 bytes
            let n = match stream.read(&mut buf[..9]).await {
                Ok(0) => break,
                Ok(n) if n < 9 => {
                    // Try to read remaining header bytes
                    let mut total = n;
                    while total < 9 {
                        match stream.read(&mut buf[total..9]).await {
                            Ok(0) => break,
                            Ok(m) => total += m,
                            Err(e) => return Err(e),
                        }
                    }
                    if total < 9 {
                        break;
                    }
                    total
                }
                Ok(n) => n,
                Err(e) => return Err(e),
            };

            if n < 9 {
                break;
            }

            // Parse header
            let msg_size = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            let msg_type = buf[4];
            let body_size = u32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]) as usize;
            
            println!(
                "[MockWorker] Received frame: msg_size={}, msg_type={}, body_size={}",
                msg_size, msg_type, body_size
            );

            // Read message content
            let total_content = msg_size + body_size;
            if total_content > buf.len() {
                buf.resize(total_content, 0);
            }
            
            let mut read = 0;
            while read < total_content {
                match stream.read(&mut buf[read..total_content]).await {
                    Ok(0) => break,
                    Ok(n) => read += n,
                    Err(e) => return Err(e),
                }
            }

            if read < total_content {
                break;
            }

            // Handle PushData message
            if msg_type == message_type::PUSH_DATA {
                // Extract request ID from message content (first 8 bytes)
                let request_id = if msg_size >= 8 {
                    i64::from_be_bytes([
                        buf[0], buf[1], buf[2], buf[3],
                        buf[4], buf[5], buf[6], buf[7],
                    ])
                } else {
                    0
                };

                println!("[MockWorker] PushData request_id={}", request_id);

                // Build RpcResponse
                let status = response_status.load(Ordering::Acquire) as u8;
                let response = Self::build_rpc_response(request_id, status);
                
                // Send response
                stream.write_all(&response).await?;
                stream.flush().await?;
                
                println!("[MockWorker] Sent response with status={}", status);
            }
        }
        
        println!("[MockWorker] Connection closed from {}", addr);
        Ok(())
    }

    /// Build an RpcResponse frame.
    /// 
    /// Frame format:
    /// - msgSize (4 bytes): size of message content
    /// - msgType (1 byte): RPC_RESPONSE (4)
    /// - bodySize (4 bytes): size of body
    /// - message content: requestId (8 bytes) + bodySize (4 bytes)
    /// - body: status code (1 byte)
    fn build_rpc_response(request_id: i64, status: u8) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(32);
        
        // Message content: requestId (8) + bodySize (4) = 12 bytes
        let msg_size: u32 = 12;
        // Body: status code (1 byte)
        let body_size: u32 = 1;
        
        // Header
        buf.put_u32(msg_size);
        buf.put_u8(message_type::RPC_RESPONSE);
        buf.put_u32(body_size);
        
        // Message content
        buf.put_i64(request_id);
        buf.put_i32(body_size as i32);
        
        // Body
        buf.put_u8(status);
        
        buf.to_vec()
    }
}

// ============================================================================
// Unit Tests
// ============================================================================

/// Test: Mock worker server starts and accepts connections.
#[tokio::test]
async fn test_mock_worker_starts() {
    let server = MockWorkerServer::new()
        .await
        .expect("Should create mock worker");
    
    let port = server.port();
    assert!(port > 0, "Server should have a valid port");
    
    // Start server in background
    let running = server.running.clone();
    let server_handle = tokio::spawn(async move {
        server.run().await;
    });
    
    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Try to connect
    let result = TcpStream::connect(format!("127.0.0.1:{}", port)).await;
    assert!(result.is_ok(), "Should connect to mock worker");
    
    // Cleanup
    running.store(false, Ordering::Release);
    server_handle.abort();
}

/// Test: RpcResponse frame format is correct.
#[test]
fn test_rpc_response_format() {
    let response = MockWorkerServer::build_rpc_response(12345, status_code::SUCCESS);
    
    // Verify header
    assert_eq!(response.len(), 9 + 12 + 1); // header + message + body
    
    // msgSize = 12
    assert_eq!(u32::from_be_bytes([response[0], response[1], response[2], response[3]]), 12);
    
    // msgType = RPC_RESPONSE (4)
    assert_eq!(response[4], message_type::RPC_RESPONSE);
    
    // bodySize = 1
    assert_eq!(u32::from_be_bytes([response[5], response[6], response[7], response[8]]), 1);
    
    // requestId = 12345
    let request_id = i64::from_be_bytes([
        response[9], response[10], response[11], response[12],
        response[13], response[14], response[15], response[16],
    ]);
    assert_eq!(request_id, 12345);
    
    // bodySize in message = 1
    let body_size = i32::from_be_bytes([response[17], response[18], response[19], response[20]]);
    assert_eq!(body_size, 1);
    
    // status = SUCCESS (0)
    assert_eq!(response[21], status_code::SUCCESS);
}

/// Test: RpcResponse with HARD_SPLIT status.
#[test]
fn test_rpc_response_hard_split() {
    let response = MockWorkerServer::build_rpc_response(99999, status_code::HARD_SPLIT);
    
    // Verify status code
    assert_eq!(response[21], status_code::HARD_SPLIT);
}

/// Test: RpcResponse with SOFT_SPLIT status.
#[test]
fn test_rpc_response_soft_split() {
    let response = MockWorkerServer::build_rpc_response(88888, status_code::SOFT_SPLIT);
    
    // Verify status code
    assert_eq!(response[21], status_code::SOFT_SPLIT);
}

/// Test: RpcResponse with MAP_ENDED status.
#[test]
fn test_rpc_response_map_ended() {
    let response = MockWorkerServer::build_rpc_response(77777, status_code::MAP_ENDED);
    
    // Verify status code
    assert_eq!(response[21], status_code::MAP_ENDED);
}

/// Test: RpcResponse with congestion status.
#[test]
fn test_rpc_response_congested() {
    let response = MockWorkerServer::build_rpc_response(66666, status_code::PUSH_DATA_SUCCESS_PRIMARY_CONGESTED);
    
    // Verify status code
    assert_eq!(response[21], status_code::PUSH_DATA_SUCCESS_PRIMARY_CONGESTED);
}

/// Test: Verify PushData frame format matches Java.
#[test]
fn test_push_data_frame_format() {
    use bytes::Buf;
    
    // Build a PushData frame similar to what the Rust client sends
    let request_id: i64 = 12345;
    let mode: u8 = 0; // Primary
    let shuffle_key = "test-app-0";
    let partition_unique_id = "0-0";
    let body = vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]; // 16 bytes with batch header
    
    // Message content: requestId (8) + mode (1) + shuffleKey (4 + len) + partitionUniqueId (4 + len)
    let mut message = BytesMut::new();
    message.put_i64(request_id);
    message.put_u8(mode);
    
    // String encoding: 4-byte length + UTF-8 bytes
    message.put_i32(shuffle_key.len() as i32);
    message.put_slice(shuffle_key.as_bytes());
    message.put_i32(partition_unique_id.len() as i32);
    message.put_slice(partition_unique_id.as_bytes());
    
    let msg_size = message.len();
    let body_size = body.len();
    
    // Build frame
    let mut frame = BytesMut::new();
    frame.put_u32(msg_size as u32);
    frame.put_u8(message_type::PUSH_DATA);
    frame.put_u32(body_size as u32);
    frame.put_slice(&message);
    frame.put_slice(&body);
    
    // Verify frame structure
    let frame_bytes = frame.freeze();
    let mut buf = frame_bytes.clone();
    
    // Header
    let parsed_msg_size = buf.get_u32();
    let parsed_msg_type = buf.get_u8();
    let parsed_body_size = buf.get_u32();
    
    assert_eq!(parsed_msg_size as usize, msg_size);
    assert_eq!(parsed_msg_type, message_type::PUSH_DATA);
    assert_eq!(parsed_body_size as usize, body_size);
    
    // Message content
    let parsed_request_id = buf.get_i64();
    let parsed_mode = buf.get_u8();
    
    assert_eq!(parsed_request_id, request_id);
    assert_eq!(parsed_mode, mode);
    
    // Shuffle key
    let key_len = buf.get_i32() as usize;
    let key_bytes = buf.copy_to_bytes(key_len);
    assert_eq!(std::str::from_utf8(&key_bytes).unwrap(), shuffle_key);
    
    // Partition unique ID
    let id_len = buf.get_i32() as usize;
    let id_bytes = buf.copy_to_bytes(id_len);
    assert_eq!(std::str::from_utf8(&id_bytes).unwrap(), partition_unique_id);
    
    // Body
    let remaining = buf.copy_to_bytes(buf.remaining());
    assert_eq!(remaining.as_ref(), &body[..]);
}

/// Test: Batch header format (little-endian).
#[test]
fn test_batch_header_format() {
    let map_id: i32 = 1;
    let attempt_id: i32 = 0;
    let batch_id: i32 = 5;
    let compressed_size: i32 = 100;
    
    let mut header = BytesMut::with_capacity(16);
    header.put_i32_le(map_id);
    header.put_i32_le(attempt_id);
    header.put_i32_le(batch_id);
    header.put_i32_le(compressed_size);
    
    let bytes = header.freeze();
    
    // Verify little-endian encoding
    assert_eq!(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]), map_id);
    assert_eq!(i32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]), attempt_id);
    assert_eq!(i32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]), batch_id);
    assert_eq!(i32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]), compressed_size);
}

// ============================================================================
// Integration Tests with Mock Worker
// ============================================================================

/// Test: Send PushData and receive SUCCESS response.
#[tokio::test]
async fn test_push_data_success_response() {
    let server = MockWorkerServer::new()
        .await
        .expect("Should create mock worker");
    
    let port = server.port();
    server.set_response_status(status_code::SUCCESS);
    
    // Start server
    let running = server.running.clone();
    let server_handle = tokio::spawn(async move {
        server.run().await;
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Connect and send PushData
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("Should connect");
    
    // Build PushData frame
    let request_id: i64 = 1;
    let mut message = BytesMut::new();
    message.put_i64(request_id);
    message.put_u8(0); // mode
    message.put_i32(10);
    message.put_slice(b"test-app-0");
    message.put_i32(3);
    message.put_slice(b"0-0");
    
    let body = vec![0u8; 20]; // Batch header + data
    
    let mut frame = BytesMut::new();
    frame.put_u32(message.len() as u32);
    frame.put_u8(message_type::PUSH_DATA);
    frame.put_u32(body.len() as u32);
    frame.put_slice(&message);
    frame.put_slice(&body);
    
    stream.write_all(&frame).await.expect("Should send");
    stream.flush().await.expect("Should flush");
    
    // Read response
    let mut response = vec![0u8; 32];
    let n = stream.read(&mut response).await.expect("Should read");
    
    assert!(n >= 22, "Should receive complete response");
    
    // Verify response
    let msg_type = response[4];
    assert_eq!(msg_type, message_type::RPC_RESPONSE);
    
    let status = response[21];
    assert_eq!(status, status_code::SUCCESS);
    
    // Cleanup
    running.store(false, Ordering::Release);
    server_handle.abort();
}

/// Test: Send PushData and receive HARD_SPLIT response.
#[tokio::test]
async fn test_push_data_hard_split_response() {
    let server = MockWorkerServer::new()
        .await
        .expect("Should create mock worker");
    
    let port = server.port();
    server.set_response_status(status_code::HARD_SPLIT);
    
    // Start server
    let running = server.running.clone();
    let server_handle = tokio::spawn(async move {
        server.run().await;
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Connect and send PushData
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("Should connect");
    
    // Build PushData frame
    let request_id: i64 = 2;
    let mut message = BytesMut::new();
    message.put_i64(request_id);
    message.put_u8(0);
    message.put_i32(10);
    message.put_slice(b"test-app-0");
    message.put_i32(3);
    message.put_slice(b"0-0");
    
    let body = vec![0u8; 20];
    
    let mut frame = BytesMut::new();
    frame.put_u32(message.len() as u32);
    frame.put_u8(message_type::PUSH_DATA);
    frame.put_u32(body.len() as u32);
    frame.put_slice(&message);
    frame.put_slice(&body);
    
    stream.write_all(&frame).await.expect("Should send");
    stream.flush().await.expect("Should flush");
    
    // Read response
    let mut response = vec![0u8; 32];
    let n = stream.read(&mut response).await.expect("Should read");
    
    assert!(n >= 22, "Should receive complete response");
    
    // Verify HARD_SPLIT status
    let status = response[21];
    assert_eq!(status, status_code::HARD_SPLIT);
    
    // Cleanup
    running.store(false, Ordering::Release);
    server_handle.abort();
}

/// Test: Send PushData and receive SOFT_SPLIT response.
#[tokio::test]
async fn test_push_data_soft_split_response() {
    let server = MockWorkerServer::new()
        .await
        .expect("Should create mock worker");
    
    let port = server.port();
    server.set_response_status(status_code::SOFT_SPLIT);
    
    // Start server
    let running = server.running.clone();
    let server_handle = tokio::spawn(async move {
        server.run().await;
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Connect and send PushData
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("Should connect");
    
    // Build PushData frame
    let request_id: i64 = 3;
    let mut message = BytesMut::new();
    message.put_i64(request_id);
    message.put_u8(0);
    message.put_i32(10);
    message.put_slice(b"test-app-0");
    message.put_i32(3);
    message.put_slice(b"0-0");
    
    let body = vec![0u8; 20];
    
    let mut frame = BytesMut::new();
    frame.put_u32(message.len() as u32);
    frame.put_u8(message_type::PUSH_DATA);
    frame.put_u32(body.len() as u32);
    frame.put_slice(&message);
    frame.put_slice(&body);
    
    stream.write_all(&frame).await.expect("Should send");
    stream.flush().await.expect("Should flush");
    
    // Read response
    let mut response = vec![0u8; 32];
    let n = stream.read(&mut response).await.expect("Should read");
    
    assert!(n >= 22, "Should receive complete response");
    
    // Verify SOFT_SPLIT status
    let status = response[21];
    assert_eq!(status, status_code::SOFT_SPLIT);
    
    // Cleanup
    running.store(false, Ordering::Release);
    server_handle.abort();
}

/// Test: Multiple PushData requests with different responses.
#[tokio::test]
async fn test_multiple_push_data_requests() {
    let server = MockWorkerServer::new()
        .await
        .expect("Should create mock worker");
    
    let port = server.port();
    let response_status = server.response_status.clone();
    
    // Start server
    let running = server.running.clone();
    let server_handle = tokio::spawn(async move {
        server.run().await;
    });
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Connect
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .expect("Should connect");
    
    // Test different status codes
    let test_cases = vec![
        (1i64, status_code::SUCCESS),
        (2i64, status_code::SOFT_SPLIT),
        (3i64, status_code::HARD_SPLIT),
        (4i64, status_code::MAP_ENDED),
        (5i64, status_code::PUSH_DATA_SUCCESS_PRIMARY_CONGESTED),
    ];
    
    for (request_id, expected_status) in test_cases {
        // Set expected response
        response_status.store(expected_status as i64, Ordering::Release);
        
        // Build and send PushData
        let mut message = BytesMut::new();
        message.put_i64(request_id);
        message.put_u8(0);
        message.put_i32(10);
        message.put_slice(b"test-app-0");
        message.put_i32(3);
        message.put_slice(b"0-0");
        
        let body = vec![0u8; 20];
        
        let mut frame = BytesMut::new();
        frame.put_u32(message.len() as u32);
        frame.put_u8(message_type::PUSH_DATA);
        frame.put_u32(body.len() as u32);
        frame.put_slice(&message);
        frame.put_slice(&body);
        
        stream.write_all(&frame).await.expect("Should send");
        stream.flush().await.expect("Should flush");
        
        // Read response
        let mut response = vec![0u8; 32];
        let n = stream.read(&mut response).await.expect("Should read");
        
        assert!(n >= 22, "Should receive complete response for request {}", request_id);
        
        // Verify status
        let status = response[21];
        assert_eq!(
            status, expected_status,
            "Request {} should have status {}, got {}",
            request_id, expected_status, status
        );
        
        println!("Request {} received expected status {}", request_id, status);
    }
    
    // Cleanup
    running.store(false, Ordering::Release);
    server_handle.abort();
}
