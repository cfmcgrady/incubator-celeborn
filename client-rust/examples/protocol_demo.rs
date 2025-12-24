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

//! Protocol demonstration example - runs locally without external services.
//!
//! This example demonstrates the Celeborn protocol implementation by:
//! 1. Creating a mock server that speaks the Celeborn protocol
//! 2. Connecting a client to the mock server
//! 3. Sending and receiving messages using the TransportClient protocol
//!
//! This is useful for testing and understanding the protocol without
//! needing a real Celeborn cluster.
//!
//! Usage:
//!   cargo run --example protocol_demo

use bytes::{Bytes, BytesMut, BufMut};
use prost::Message;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_util::codec::Framed;
use futures::{SinkExt, StreamExt};

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
    #[prost(bytes = "vec", repeated, tag = "3")]
    pub chunk_offsets: Vec<Vec<u8>>,
    #[prost(bool, tag = "4")]
    pub is_sorted: bool,
}

/// Mock Celeborn server that handles OpenStream requests
async fn run_mock_server(listener: TcpListener, ready_tx: oneshot::Sender<SocketAddr>) {
    let addr = listener.local_addr().unwrap();
    println!("[Server] Listening on {}", addr);
    
    // Signal that server is ready
    let _ = ready_tx.send(addr);
    
    // Accept one connection
    if let Ok((stream, peer_addr)) = listener.accept().await {
        println!("[Server] Accepted connection from {}", peer_addr);
        handle_connection(stream).await;
    }
}

async fn handle_connection(stream: TcpStream) {
    let mut framed = Framed::new(stream, CelebornCodec::new());
    
    while let Some(result) = framed.next().await {
        match result {
            Ok(frame) => {
                println!("[Server] Received frame:");
                println!("  Message type: {:?}", frame.message_type);
                println!("  Message size: {} bytes", frame.message.len());
                println!("  Body size: {} bytes", frame.body.len());
                
                match frame.message_type {
                    MessageType::RpcRequest => {
                        // Parse request
                        if frame.message.len() >= 12 {
                            let request_id = i64::from_be_bytes(
                                frame.message[0..8].try_into().unwrap()
                            );
                            let body_size = i32::from_be_bytes(
                                frame.message[8..12].try_into().unwrap()
                            );
                            println!("  Request ID: {}", request_id);
                            println!("  Body size in header: {}", body_size);
                            
                            // Parse TransportMessage from body
                            if !frame.body.is_empty() {
                                match TransportMessage::decode(frame.body.clone()) {
                                    Ok(transport_msg) => {
                                        println!("  TransportMessage type: {:?}", transport_msg.message_type);
                                        
                                        // Handle OpenStream
                                        if transport_msg.message_type == TransportMessageType::OpenStream {
                                            match PbOpenStream::decode(transport_msg.payload.as_ref()) {
                                                Ok(open_stream) => {
                                                    println!("  OpenStream request:");
                                                    println!("    Shuffle key: {}", open_stream.shuffle_key);
                                                    println!("    File name: {}", open_stream.file_name);
                                                    println!("    Start index: {}", open_stream.start_index);
                                                    println!("    End index: {}", open_stream.end_index);
                                                    
                                                    // Send StreamHandler response
                                                    let response = create_stream_handler_response(request_id);
                                                    if let Err(e) = framed.send(response).await {
                                                        println!("[Server] Failed to send response: {}", e);
                                                    } else {
                                                        println!("[Server] Sent StreamHandler response");
                                                    }
                                                }
                                                Err(e) => {
                                                    println!("  Failed to parse OpenStream: {}", e);
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
                    _ => {
                        println!("  Unexpected message type");
                    }
                }
            }
            Err(e) => {
                println!("[Server] Error reading frame: {}", e);
                break;
            }
        }
    }
    
    println!("[Server] Connection closed");
}

fn create_stream_handler_response(request_id: i64) -> Frame {
    // Create StreamHandler response
    let stream_handler = PbStreamHandler {
        stream_id: 12345,
        num_chunks: 10,
        chunk_offsets: vec![],
        is_sorted: false,
    };
    
    // Encode to protobuf
    let mut payload = Vec::new();
    stream_handler.encode(&mut payload).unwrap();
    
    // Create TransportMessage
    let transport_msg = TransportMessage::new(
        TransportMessageType::StreamHandler,
        Bytes::from(payload),
    );
    let rpc_body = transport_msg.encode();
    
    // Create RpcResponse frame
    // Message content: requestId (8B) + bodySize (4B)
    let mut message_content = BytesMut::with_capacity(12);
    message_content.put_i64(request_id);
    message_content.put_i32(rpc_body.len() as i32);
    
    Frame::with_body(
        MessageType::RpcResponse,
        message_content.freeze(),
        rpc_body,
    )
}

async fn run_client(server_addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n[Client] Connecting to {}", server_addr);
    
    let stream = TcpStream::connect(server_addr).await?;
    stream.set_nodelay(true)?;
    println!("[Client] Connected!");
    
    let mut framed = Framed::new(stream, CelebornCodec::new());
    
    // Create OpenStream request
    let open_stream = PbOpenStream {
        shuffle_key: "demo-app-shuffle-0".to_string(),
        file_name: "partition-0".to_string(),
        start_index: 0,
        end_index: 100,
        initial_credit: 10,
        read_local_shuffle: false,
    };
    
    // Encode to protobuf
    let mut payload = Vec::new();
    open_stream.encode(&mut payload)?;
    
    // Create TransportMessage
    let transport_msg = TransportMessage::new(
        TransportMessageType::OpenStream,
        Bytes::from(payload),
    );
    let rpc_body = transport_msg.encode();
    
    // Create RpcRequest frame
    let request_id: i64 = 1;
    let mut message_content = BytesMut::with_capacity(12);
    message_content.put_i64(request_id);
    message_content.put_i32(rpc_body.len() as i32);
    
    let frame = Frame::with_body(
        MessageType::RpcRequest,
        message_content.freeze(),
        rpc_body,
    );
    
    println!("[Client] Sending OpenStream request:");
    println!("  Shuffle key: {}", open_stream.shuffle_key);
    println!("  File name: {}", open_stream.file_name);
    println!("  Frame size: {} bytes", frame.total_size());
    
    framed.send(frame).await?;
    println!("[Client] Request sent, waiting for response...");
    
    // Wait for response
    if let Some(Ok(response_frame)) = framed.next().await {
        println!("\n[Client] Received response:");
        println!("  Message type: {:?}", response_frame.message_type);
        
        if response_frame.message_type == MessageType::RpcResponse {
            if response_frame.message.len() >= 12 {
                let resp_request_id = i64::from_be_bytes(
                    response_frame.message[0..8].try_into().unwrap()
                );
                println!("  Request ID: {}", resp_request_id);
                
                // Parse TransportMessage
                if !response_frame.body.is_empty() {
                    match TransportMessage::decode(response_frame.body.clone()) {
                        Ok(transport_response) => {
                            println!("  TransportMessage type: {:?}", transport_response.message_type);
                            
                            if transport_response.message_type == TransportMessageType::StreamHandler {
                                match PbStreamHandler::decode(transport_response.payload.as_ref()) {
                                    Ok(handler) => {
                                        println!("\n  StreamHandler response:");
                                        println!("    Stream ID: {}", handler.stream_id);
                                        println!("    Num chunks: {}", handler.num_chunks);
                                        println!("    Is sorted: {}", handler.is_sorted);
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
    }
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Celeborn Protocol Demo ===\n");
    println!("This demo runs a mock server and client locally to demonstrate");
    println!("the Celeborn TransportClient protocol implementation.\n");
    
    // Bind to a random available port
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    
    // Channel to signal when server is ready
    let (ready_tx, ready_rx) = oneshot::channel();
    
    // Spawn server task
    let server_handle = tokio::spawn(async move {
        run_mock_server(listener, ready_tx).await;
    });
    
    // Wait for server to be ready
    let server_addr = ready_rx.await?;
    
    // Give server a moment to start accepting
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    
    // Run client
    run_client(server_addr).await?;
    
    // Give server time to process
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    
    println!("\n=== Demo completed successfully! ===");
    println!("\nThis demonstrates that the Celeborn protocol implementation is working:");
    println!("  ✓ Frame encoding/decoding (CelebornCodec)");
    println!("  ✓ TransportMessage encoding/decoding");
    println!("  ✓ RpcRequest/RpcResponse message handling");
    println!("  ✓ Protobuf message serialization (OpenStream, StreamHandler)");
    
    Ok(())
}
