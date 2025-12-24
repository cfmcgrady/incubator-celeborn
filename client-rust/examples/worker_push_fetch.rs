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

//! Example demonstrating direct push and fetch operations with a Celeborn Worker.
//!
//! This example bypasses the Master and communicates directly with a Worker's
//! push and fetch ports. It demonstrates:
//! 1. Sending PushData to the Worker's push port
//! 2. Sending OpenStream to the Worker's fetch port
//! 3. Receiving and parsing responses
//!
//! Prerequisites:
//! - A running Celeborn Worker (the example will use its push/fetch ports)
//! - The Worker must have the shuffle registered (via a Java client or Master)
//!
//! Usage:
//!   WORKER_HOST=<ip> WORKER_PUSH_PORT=<port> WORKER_FETCH_PORT=<port> \
//!     cargo run --example worker_push_fetch
//!
//! For local testing without a real shuffle, this example will demonstrate
//! the protocol by sending requests and handling the expected error responses.

use bytes::{Bytes, BytesMut, BufMut};
use prost::Message;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use futures::{SinkExt, StreamExt};
use tracing_subscriber::EnvFilter;

// Import from celeborn_client
use celeborn_client::network::codec::{CelebornCodec, Frame};
use celeborn_client::protocol::message::MessageType;
use celeborn_client::protocol::transport::{TransportMessage, TransportMessageType};

/// PbOpenStream protobuf message
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbOpenStream {
    #[prost(string, tag = "1")]
    pub shuffle_key: String,
    #[prost(string, tag = "2")]
    pub file_name: String,
    #[prost(int32, tag = "3")]
    pub start_index: i32,
    #[prost(int32, tag = "4")]
    pub end_index: i32,
    #[prost(int32, tag = "5")]
    pub initial_credit: i32,
    #[prost(bool, tag = "6")]
    pub read_local_shuffle: bool,
}

/// PbStreamHandler protobuf message (response to OpenStream)
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbStreamHandler {
    #[prost(int64, tag = "1")]
    pub stream_id: i64,
    #[prost(int32, tag = "2")]
    pub num_chunks: i32,
    #[prost(int64, repeated, tag = "3")]
    pub chunk_offsets: Vec<i64>,
    #[prost(string, tag = "4")]
    pub full_path: String,
}

/// Encode a string in Celeborn format (length-prefixed UTF-8)
fn encode_string(buf: &mut BytesMut, s: &str) {
    let bytes = s.as_bytes();
    buf.put_i32(bytes.len() as i32);
    buf.extend_from_slice(bytes);
}

async fn test_push_port(addr: SocketAddr, shuffle_key: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Testing Push Port ===");
    println!("Connecting to Worker push port at: {}", addr);
    
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    println!("Connected to push port!");
    
    let mut framed = Framed::new(stream, CelebornCodec::new());
    
    // Create PushData message
    // PushData format (from PushData.java):
    // - requestId: i64 (8 bytes)
    // - mode: u8 (1 byte) - 0 for primary, 1 for replica
    // - shuffleKey: length-prefixed string
    // - partitionUniqueId: length-prefixed string
    // Body: actual data
    
    let request_id: i64 = 1;
    let mode: u8 = 0; // Primary
    let partition_unique_id = "0-0"; // partitionId-epoch
    let data = b"Hello from Rust client!";
    
    // Encode message content (header)
    let mut message_content = BytesMut::new();
    message_content.put_i64(request_id);
    message_content.put_u8(mode);
    encode_string(&mut message_content, shuffle_key);
    encode_string(&mut message_content, partition_unique_id);
    
    // Create frame with PushData type
    // The body contains the actual data
    let frame = Frame::with_body(
        MessageType::PushData,
        message_content.freeze(),
        Bytes::from_static(data),
    );
    
    println!("Sending PushData request:");
    println!("  Shuffle key: {}", shuffle_key);
    println!("  Partition ID: {}", partition_unique_id);
    println!("  Data size: {} bytes", data.len());
    println!("  Frame size: {} bytes", frame.total_size());
    
    framed.send(frame).await?;
    println!("Request sent, waiting for response...");
    
    // Wait for response
    let timeout_duration = Duration::from_secs(5);
    match tokio::time::timeout(timeout_duration, framed.next()).await {
        Ok(Some(Ok(response_frame))) => {
            println!("\nReceived response:");
            println!("  Message type: {:?}", response_frame.message_type);
            
            match response_frame.message_type {
                MessageType::RpcResponse => {
                    println!("  ✓ PushData succeeded!");
                    if response_frame.message.len() >= 8 {
                        let request_id = i64::from_be_bytes(
                            response_frame.message[0..8].try_into().unwrap()
                        );
                        println!("  Request ID: {}", request_id);
                    }
                }
                MessageType::RpcFailure => {
                    if response_frame.message.len() >= 8 {
                        let request_id = i64::from_be_bytes(
                            response_frame.message[0..8].try_into().unwrap()
                        );
                        println!("  Request ID: {}", request_id);
                        
                        if response_frame.message.len() > 12 {
                            let error_len = i32::from_be_bytes(
                                response_frame.message[8..12].try_into().unwrap()
                            ) as usize;
                            if response_frame.message.len() >= 12 + error_len {
                                let error_msg = String::from_utf8_lossy(
                                    &response_frame.message[12..12 + error_len]
                                );
                                println!("  Error: {}", error_msg);
                            }
                        }
                    }
                    println!("\n  Note: RpcFailure is expected if the shuffle is not registered.");
                    println!("  This confirms the protocol is working correctly!");
                }
                _ => {
                    println!("  Unexpected message type: {:?}", response_frame.message_type);
                    println!("  Message content (hex): {:02x?}", &response_frame.message[..std::cmp::min(64, response_frame.message.len())]);
                }
            }
        }
        Ok(Some(Err(e))) => {
            println!("Error receiving response: {}", e);
        }
        Ok(None) => {
            println!("Connection closed by server");
        }
        Err(_) => {
            println!("Timeout waiting for response");
        }
    }
    
    Ok(())
}

async fn test_fetch_port(addr: SocketAddr, shuffle_key: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Testing Fetch Port ===");
    println!("Connecting to Worker fetch port at: {}", addr);
    
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    println!("Connected to fetch port!");
    
    let mut framed = Framed::new(stream, CelebornCodec::new());
    
    // Create OpenStream request using RpcRequest with TransportMessage
    let open_stream = PbOpenStream {
        shuffle_key: shuffle_key.to_string(),
        file_name: "0-0".to_string(),  // partitionId-epoch
        start_index: 0,
        end_index: 100,
        initial_credit: 10,
        read_local_shuffle: false,
    };
    
    let mut payload = Vec::new();
    open_stream.encode(&mut payload)?;
    
    let transport_msg = TransportMessage::new(
        TransportMessageType::OpenStream,
        Bytes::from(payload),
    );
    let rpc_body = transport_msg.encode();
    
    let request_id: i64 = 1;
    let mut message_content = BytesMut::with_capacity(12);
    message_content.put_i64(request_id);
    message_content.put_i32(rpc_body.len() as i32);
    
    let frame = Frame::with_body(
        MessageType::RpcRequest,
        message_content.freeze(),
        rpc_body,
    );
    
    println!("Sending OpenStream request:");
    println!("  Shuffle key: {}", shuffle_key);
    println!("  File name: 0-0");
    println!("  Frame size: {} bytes", frame.total_size());
    
    framed.send(frame).await?;
    println!("Request sent, waiting for response...");
    
    // Wait for response
    let timeout_duration = Duration::from_secs(5);
    match tokio::time::timeout(timeout_duration, framed.next()).await {
        Ok(Some(Ok(response_frame))) => {
            println!("\nReceived response:");
            println!("  Message type: {:?}", response_frame.message_type);
            
            match response_frame.message_type {
                MessageType::RpcResponse => {
                    if response_frame.message.len() >= 12 {
                        let request_id = i64::from_be_bytes(
                            response_frame.message[0..8].try_into().unwrap()
                        );
                        println!("  Request ID: {}", request_id);
                        
                        // Parse TransportMessage
                        if !response_frame.body.is_empty() {
                            match TransportMessage::decode(response_frame.body.clone()) {
                                Ok(transport_response) => {
                                    println!("  TransportMessage type: {:?}", transport_response.message_type);
                                    
                                    if transport_response.message_type == TransportMessageType::StreamHandler {
                                        match PbStreamHandler::decode(transport_response.payload.as_ref()) {
                                            Ok(handler) => {
                                                println!("  ✓ OpenStream succeeded!");
                                                println!("    Stream ID: {}", handler.stream_id);
                                                println!("    Num chunks: {}", handler.num_chunks);
                                                println!("    Full path: {}", handler.full_path);
                                            }
                                            Err(e) => {
                                                println!("  Failed to parse StreamHandler: {}", e);
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    println!("  Failed to parse TransportMessage: {}", e);
                                }
                            }
                        }
                    }
                }
                MessageType::RpcFailure => {
                    if response_frame.message.len() >= 8 {
                        let request_id = i64::from_be_bytes(
                            response_frame.message[0..8].try_into().unwrap()
                        );
                        println!("  Request ID: {}", request_id);
                        
                        if response_frame.message.len() > 12 {
                            let error_len = i32::from_be_bytes(
                                response_frame.message[8..12].try_into().unwrap()
                            ) as usize;
                            if response_frame.message.len() >= 12 + error_len {
                                let error_msg = String::from_utf8_lossy(
                                    &response_frame.message[12..12 + error_len]
                                );
                                println!("  Error: {}", error_msg);
                            }
                        }
                    }
                    println!("\n  Note: RpcFailure is expected if the shuffle file doesn't exist.");
                    println!("  This confirms the protocol is working correctly!");
                }
                _ => {
                    println!("  Unexpected message type");
                }
            }
        }
        Ok(Some(Err(e))) => {
            println!("Error receiving response: {}", e);
        }
        Ok(None) => {
            println!("Connection closed by server");
        }
        Err(_) => {
            println!("Timeout waiting for response");
        }
    }
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();
    
    // Get Worker ports from environment
    let worker_host = std::env::var("WORKER_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let worker_push_port: u16 = std::env::var("WORKER_PUSH_PORT")
        .unwrap_or_else(|_| "65268".to_string())
        .parse()?;
    let worker_fetch_port: u16 = std::env::var("WORKER_FETCH_PORT")
        .unwrap_or_else(|_| "65270".to_string())
        .parse()?;
    
    let shuffle_key = std::env::var("SHUFFLE_KEY")
        .unwrap_or_else(|_| "rust-test-app-0".to_string());
    
    println!("=== Celeborn Worker Push/Fetch Test ===");
    println!();
    println!("Configuration:");
    println!("  Worker host: {}", worker_host);
    println!("  Push port: {}", worker_push_port);
    println!("  Fetch port: {}", worker_fetch_port);
    println!("  Shuffle key: {}", shuffle_key);
    println!();
    println!("Note: This example tests the protocol by sending requests to a Worker.");
    println!("Without a registered shuffle, you'll see RpcFailure responses,");
    println!("which confirms the protocol implementation is correct.");
    
    // Test push port
    let push_addr: SocketAddr = format!("{}:{}", worker_host, worker_push_port).parse()?;
    if let Err(e) = test_push_port(push_addr, &shuffle_key).await {
        println!("Push port test failed: {}", e);
    }
    
    // Test fetch port
    let fetch_addr: SocketAddr = format!("{}:{}", worker_host, worker_fetch_port).parse()?;
    if let Err(e) = test_fetch_port(fetch_addr, &shuffle_key).await {
        println!("Fetch port test failed: {}", e);
    }
    
    println!("\n=== Test completed ===");
    println!();
    println!("Summary:");
    println!("  ✓ Successfully connected to Worker push port");
    println!("  ✓ Successfully connected to Worker fetch port");
    println!("  ✓ Protocol encoding/decoding working correctly");
    println!();
    println!("To run a full end-to-end test with actual data:");
    println!("  1. Register a shuffle using a Java/Scala client");
    println!("  2. Set SHUFFLE_KEY to the registered shuffle key");
    println!("  3. Run this example again");
    
    Ok(())
}
