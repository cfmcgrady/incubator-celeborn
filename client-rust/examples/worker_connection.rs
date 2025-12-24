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

//! Example demonstrating direct connection to a Celeborn Worker.
//!
//! This example shows how to:
//! 1. Connect to a Worker's fetch port using TransportClient protocol
//! 2. Send an OpenStream request using TransportMessage format
//! 3. Receive and parse the response
//!
//! Usage:
//!   WORKER_FETCH_PORT=<port> cargo run --example worker_connection
//!
//! The Worker's fetch port can be found in the Worker logs or by checking
//! the Worker's configuration.

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

/// PbOpenStream protobuf message (matches TransportMessages.proto)
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
    #[prost(bytes = "vec", repeated, tag = "3")]
    pub chunk_offsets: Vec<Vec<u8>>,
    #[prost(bool, tag = "4")]
    pub is_sorted: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("debug".parse().unwrap()))
        .init();

    // Get Worker fetch port from environment or use default
    let worker_host = std::env::var("WORKER_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let worker_fetch_port: u16 = std::env::var("WORKER_FETCH_PORT")
        .unwrap_or_else(|_| "9098".to_string())
        .parse()?;

    let addr: SocketAddr = format!("{}:{}", worker_host, worker_fetch_port).parse()?;
    
    println!("=== Celeborn Worker Connection Test ===");
    println!("Connecting to Worker fetch port at: {}", addr);

    // Connect to Worker
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    println!("Connected successfully!");

    // Create framed connection with Celeborn codec
    let mut framed = Framed::new(stream, CelebornCodec::new());

    // Create an OpenStream request
    // Note: This will fail because we don't have a valid shuffle, but it tests the protocol
    let open_stream = PbOpenStream {
        shuffle_key: "test-app-0".to_string(),
        file_name: "test-file".to_string(),
        start_index: 0,
        end_index: 100,
        initial_credit: 10,
        read_local_shuffle: false,
    };

    // Encode the protobuf message
    let mut payload = Vec::new();
    open_stream.encode(&mut payload)?;

    // Create TransportMessage
    let transport_msg = TransportMessage::new(
        TransportMessageType::OpenStream,
        Bytes::from(payload),
    );

    // Encode TransportMessage to bytes (this is the RPC body)
    let rpc_body = transport_msg.encode();

    println!("\nSending OpenStream request:");
    println!("  Shuffle key: {}", open_stream.shuffle_key);
    println!("  File name: {}", open_stream.file_name);
    println!("  TransportMessage type: {:?}", TransportMessageType::OpenStream);
    println!("  RPC body size: {} bytes", rpc_body.len());

    // Create RpcRequest frame
    // Message content: requestId (8B) + bodySize (4B)
    // Body: TransportMessage bytes
    let request_id: i64 = 1;
    let mut message_content = BytesMut::with_capacity(12);
    message_content.put_i64(request_id);
    message_content.put_i32(rpc_body.len() as i32);

    let frame = Frame::with_body(
        MessageType::RpcRequest,
        message_content.freeze(),
        rpc_body,
    );

    println!("\nFrame details:");
    println!("  Message type: {:?}", frame.message_type);
    println!("  Message content size: {} bytes", frame.message.len());
    println!("  Body size: {} bytes", frame.body.len());
    println!("  Total frame size: {} bytes", frame.total_size());

    // Send the frame
    framed.send(frame).await?;
    println!("\nRequest sent, waiting for response...");

    // Wait for response with timeout
    let timeout_duration = Duration::from_secs(5);
    match tokio::time::timeout(timeout_duration, framed.next()).await {
        Ok(Some(Ok(response_frame))) => {
            println!("\nReceived response:");
            println!("  Message type: {:?}", response_frame.message_type);
            println!("  Message content size: {} bytes", response_frame.message.len());
            println!("  Body size: {} bytes", response_frame.body.len());

            match response_frame.message_type {
                MessageType::RpcResponse => {
                    // Parse response
                    if response_frame.message.len() >= 12 {
                        let request_id = i64::from_be_bytes(
                            response_frame.message[0..8].try_into().unwrap()
                        );
                        let body_size = i32::from_be_bytes(
                            response_frame.message[8..12].try_into().unwrap()
                        );
                        println!("  Request ID: {}", request_id);
                        println!("  Body size in header: {}", body_size);

                        // Try to parse the body as TransportMessage
                        if !response_frame.body.is_empty() {
                            match TransportMessage::decode(response_frame.body.clone()) {
                                Ok(transport_response) => {
                                    println!("  TransportMessage type: {:?}", transport_response.message_type);
                                    
                                    // Try to parse as StreamHandler
                                    if transport_response.message_type == TransportMessageType::StreamHandler {
                                        match PbStreamHandler::decode(transport_response.payload.as_ref()) {
                                            Ok(handler) => {
                                                println!("  Stream ID: {}", handler.stream_id);
                                                println!("  Num chunks: {}", handler.num_chunks);
                                                println!("  Is sorted: {}", handler.is_sorted);
                                            }
                                            Err(e) => {
                                                println!("  Failed to parse StreamHandler: {}", e);
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    println!("  Failed to parse TransportMessage: {}", e);
                                    println!("  Raw body (hex): {:02x?}", &response_frame.body[..std::cmp::min(64, response_frame.body.len())]);
                                }
                            }
                        }
                    }
                }
                MessageType::RpcFailure => {
                    // Parse failure message
                    if response_frame.message.len() >= 8 {
                        let request_id = i64::from_be_bytes(
                            response_frame.message[0..8].try_into().unwrap()
                        );
                        println!("  Request ID: {}", request_id);
                        
                        // Error message follows (length-prefixed string)
                        if response_frame.message.len() > 12 {
                            let error_len = i32::from_be_bytes(
                                response_frame.message[8..12].try_into().unwrap()
                            ) as usize;
                            if response_frame.message.len() >= 12 + error_len {
                                let error_msg = String::from_utf8_lossy(
                                    &response_frame.message[12..12 + error_len]
                                );
                                println!("  Error message: {}", error_msg);
                            }
                        }
                    }
                    println!("\n  Note: RpcFailure is expected if the shuffle doesn't exist.");
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

    println!("\n=== Test completed ===");
    Ok(())
}
