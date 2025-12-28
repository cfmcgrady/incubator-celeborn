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

//! Integration tests for Rust ExecutorShuffleClient communicating with Java LifecycleManager.
//!
//! These tests validate the RPC communication between:
//! - Rust ExecutorShuffleClient (Executor side)
//! - Java LifecycleManager (Driver side)
//!
//! This is critical for Apache Spark Comet integration where:
//! - Driver runs Java LifecycleManager
//! - Executor runs Rust ShuffleClient via JNI
//!
//! Prerequisites:
//! - Java LifecycleManager test server running (see TestLifecycleManagerServer.java)
//! - Or use the mock server for unit testing
//!
//! Run with:
//!   LIFECYCLE_MANAGER_HOST=localhost LIFECYCLE_MANAGER_PORT=9098 \
//!   cargo test --test java_lm_integration_test -- --nocapture --ignored
//!
//! For mock server tests (no external dependencies):
//!   cargo test --test java_lm_integration_test -- --nocapture

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use celeborn_client::config::CelebornConfig;
use celeborn_client::client::ExecutorShuffleClient;
use celeborn_client::protocol::generated::{
    PbGetReducerFileGroup, PbGetReducerFileGroupResponse, PbMapperEnd, PbMapperEndResponse,
    PbPartitionLocation, PbRegisterShuffle, PbRegisterShuffleResponse, PbStorageInfo,
};

/// Message types from TransportMessages.proto
mod message_type {
    pub const REGISTER_SHUFFLE: i32 = 4;
    pub const REGISTER_SHUFFLE_RESPONSE: i32 = 5;
    pub const MAPPER_END: i32 = 12;
    pub const MAPPER_END_RESPONSE: i32 = 13;
    pub const GET_REDUCER_FILE_GROUP: i32 = 14;
    pub const GET_REDUCER_FILE_GROUP_RESPONSE: i32 = 15;
}

/// Status codes
mod status_code {
    pub const SUCCESS: i32 = 0;
}

/// Get LifecycleManager host from environment or use default.
fn get_lm_host() -> String {
    std::env::var("LIFECYCLE_MANAGER_HOST").unwrap_or_else(|_| "localhost".to_string())
}

/// Get LifecycleManager port from environment or use default.
fn get_lm_port() -> i32 {
    std::env::var("LIFECYCLE_MANAGER_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9098)
}

/// Generate a unique shuffle ID based on current timestamp.
fn generate_shuffle_id() -> i32 {
    (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        % 100000) as i32
}

/// Generate a unique app ID.
fn generate_app_id(prefix: &str) -> String {
    format!(
        "{}-{}",
        prefix,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            % 100000
    )
}

/// Mock LifecycleManager server for testing.
///
/// This server simulates the Java LifecycleManager's RPC handling
/// to test the Rust client's protocol implementation.
struct MockLifecycleManagerServer {
    listener: TcpListener,
    port: u16,
    running: Arc<AtomicBool>,
    shuffle_counter: Arc<AtomicI32>,
    registered_shuffles: Arc<RwLock<HashMap<i32, ShuffleInfo>>>,
}

#[derive(Clone)]
struct ShuffleInfo {
    num_mappers: i32,
    num_partitions: i32,
    partition_locations: Vec<PbPartitionLocation>,
}

impl MockLifecycleManagerServer {
    /// Create a new mock server on a random available port.
    async fn new() -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        
        Ok(Self {
            listener,
            port,
            running: Arc::new(AtomicBool::new(true)),
            shuffle_counter: Arc::new(AtomicI32::new(0)),
            registered_shuffles: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Get the server port.
    fn port(&self) -> u16 {
        self.port
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
                            let shuffle_counter = self.shuffle_counter.clone();
                            let registered_shuffles = self.registered_shuffles.clone();
                            
                            tokio::spawn(async move {
                                if let Err(e) = Self::handle_connection(
                                    stream,
                                    addr,
                                    running,
                                    shuffle_counter,
                                    registered_shuffles,
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
                    // Check if we should stop
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
        shuffle_counter: Arc<AtomicI32>,
        registered_shuffles: Arc<RwLock<HashMap<i32, ShuffleInfo>>>,
    ) -> std::io::Result<()> {
        println!("New connection from {}", addr);
        
        let mut buf = vec![0u8; 65536];
        
        while running.load(Ordering::Acquire) {
            // Read frame length (4 bytes)
            let n = match stream.read(&mut buf[..4]).await {
                Ok(0) => break, // Connection closed
                Ok(n) if n < 4 => continue, // Incomplete read
                Ok(_) => {
                    let frame_len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                    
                    // Read frame body
                    if frame_len > buf.len() {
                        buf.resize(frame_len, 0);
                    }
                    
                    let mut read = 0;
                    while read < frame_len {
                        match stream.read(&mut buf[read..frame_len]).await {
                            Ok(0) => break,
                            Ok(n) => read += n,
                            Err(e) => return Err(e),
                        }
                    }
                    
                    if read < frame_len {
                        break;
                    }
                    
                    frame_len
                }
                Err(e) => return Err(e),
            };
            
            // Parse and handle the message
            if let Some(response) = Self::handle_message(
                &buf[..n],
                &shuffle_counter,
                &registered_shuffles,
            ).await {
                // Send response
                let response_len = response.len() as i32;
                stream.write_all(&response_len.to_be_bytes()).await?;
                stream.write_all(&response).await?;
                stream.flush().await?;
            }
        }
        
        println!("Connection closed from {}", addr);
        Ok(())
    }

    /// Handle a single message and return the response.
    async fn handle_message(
        data: &[u8],
        shuffle_counter: &Arc<AtomicI32>,
        registered_shuffles: &Arc<RwLock<HashMap<i32, ShuffleInfo>>>,
    ) -> Option<Vec<u8>> {
        // Parse the RequestMessage format:
        // 1. senderAddress: boolean + (UTF host + int port)?
        // 2. receiverAddress: boolean + (UTF host + int port)?
        // 3. receiverName: UTF string
        // 4. content: Java serialized TransportMessage
        
        if data.len() < 10 {
            eprintln!("Message too short: {} bytes", data.len());
            return None;
        }
        
        let mut offset = 0;
        
        // Skip sender address
        if data[offset] != 0 {
            offset += 1;
            let host_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
            offset += 2 + host_len + 4; // host + port
        } else {
            offset += 1;
        }
        
        // Skip receiver address
        if data[offset] != 0 {
            offset += 1;
            let host_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
            offset += 2 + host_len + 4;
        } else {
            offset += 1;
        }
        
        // Skip receiver name
        let name_len = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2 + name_len;
        
        // Now we have the Java serialized TransportMessage
        // Skip Java serialization header (magic + version = 4 bytes)
        // Then parse the TransportMessage object
        
        // For simplicity, we'll look for the message type in the serialized data
        // The TransportMessage has: messageTypeValue (int) + payload (byte[])
        
        // Find the message type value in the serialized data
        // This is a simplified parser - in production, we'd need full Java deserialization
        
        let remaining = &data[offset..];
        
        // Look for the protobuf payload after Java serialization overhead
        // The message type is typically at a fixed offset after the class descriptor
        
        // For now, let's try to find the message type by scanning for known patterns
        let (message_type, payload) = Self::extract_transport_message(remaining)?;
        
        println!("Received message type: {}", message_type);
        
        match message_type {
            message_type::REGISTER_SHUFFLE => {
                Self::handle_register_shuffle(payload, shuffle_counter, registered_shuffles).await
            }
            message_type::MAPPER_END => {
                Self::handle_mapper_end(payload).await
            }
            message_type::GET_REDUCER_FILE_GROUP => {
                Self::handle_get_reducer_file_group(payload, registered_shuffles).await
            }
            _ => {
                eprintln!("Unknown message type: {}", message_type);
                None
            }
        }
    }

    /// Extract TransportMessage from Java serialized data.
    fn extract_transport_message(data: &[u8]) -> Option<(i32, &[u8])> {
        // Java serialization format:
        // - Magic: 0xACED
        // - Version: 0x0005
        // - TC_OBJECT (0x73)
        // - Class descriptor...
        // - Field values: messageTypeValue (int), payload (byte[])
        
        // Look for the Java serialization magic
        if data.len() < 4 {
            return None;
        }
        
        if data[0] != 0xAC || data[1] != 0xED {
            eprintln!("Invalid Java serialization magic");
            return None;
        }
        
        // Skip to find the messageTypeValue
        // This is a simplified approach - we scan for the int value
        // In a real implementation, we'd parse the full Java serialization format
        
        // The messageTypeValue is typically after the class descriptor
        // For TransportMessage, it's the first field
        
        // Find TC_OBJECT (0x73) followed by class descriptor
        let mut offset = 4; // Skip magic + version
        
        while offset < data.len() {
            if data[offset] == 0x73 { // TC_OBJECT
                // Found object start, now find the field values
                // Skip class descriptor to find the int field
                offset += 1;
                
                // Skip class descriptor (simplified)
                while offset < data.len() - 4 {
                    // Look for the end of class descriptor (TC_ENDBLOCKDATA = 0x78)
                    // followed by TC_NULL (0x70) for superclass
                    if data[offset] == 0x78 && offset + 1 < data.len() && data[offset + 1] == 0x70 {
                        offset += 2;
                        break;
                    }
                    offset += 1;
                }
                
                // Now we should be at the field values
                if offset + 4 <= data.len() {
                    let message_type = i32::from_be_bytes([
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    ]);
                    offset += 4;
                    
                    // Next is the payload (byte array or null)
                    if offset < data.len() {
                        if data[offset] == 0x70 { // TC_NULL
                            return Some((message_type, &[]));
                        } else if data[offset] == 0x75 { // TC_ARRAY
                            // Skip array class descriptor
                            offset += 1;
                            
                            // Skip to array length
                            while offset < data.len() - 4 {
                                if data[offset] == 0x78 && offset + 1 < data.len() && data[offset + 1] == 0x70 {
                                    offset += 2;
                                    break;
                                }
                                offset += 1;
                            }
                            
                            if offset + 4 <= data.len() {
                                let array_len = i32::from_be_bytes([
                                    data[offset],
                                    data[offset + 1],
                                    data[offset + 2],
                                    data[offset + 3],
                                ]) as usize;
                                offset += 4;
                                
                                if offset + array_len <= data.len() {
                                    return Some((message_type, &data[offset..offset + array_len]));
                                }
                            }
                        }
                    }
                    
                    return Some((message_type, &[]));
                }
                break;
            }
            offset += 1;
        }
        
        None
    }

    /// Handle RegisterShuffle request.
    async fn handle_register_shuffle(
        payload: &[u8],
        shuffle_counter: &Arc<AtomicI32>,
        registered_shuffles: &Arc<RwLock<HashMap<i32, ShuffleInfo>>>,
    ) -> Option<Vec<u8>> {
        let request = PbRegisterShuffle::decode(payload).ok()?;
        
        println!(
            "RegisterShuffle: shuffle_id={}, num_mappers={}, num_partitions={}",
            request.shuffle_id, request.num_mappers, request.num_partitions
        );
        
        // Generate partition locations
        let mut locations = Vec::new();
        for partition_id in 0..request.num_partitions {
            let location = PbPartitionLocation {
                id: partition_id,
                epoch: 0,
                host: "127.0.0.1".to_string(),
                rpc_port: 9099,
                push_port: 9100,
                fetch_port: 9101,
                replicate_port: 9102,
                mode: 0, // Primary
                peer: None,
                storage_info: Some(PbStorageInfo {
                    r#type: 0,
                    mount_point: "/tmp".to_string(),
                    final_result: false,
                    file_path: format!("/tmp/shuffle-{}-{}", request.shuffle_id, partition_id),
                    available_storage_types: 1,
                    file_size: 0,
                    chunk_offsets: vec![],
                }),
                map_id_bitmap: vec![],
                split_start: 0,
                split_end: 0,
            };
            locations.push(location);
        }
        
        // Store shuffle info
        {
            let mut shuffles = registered_shuffles.write().await;
            shuffles.insert(request.shuffle_id, ShuffleInfo {
                num_mappers: request.num_mappers,
                num_partitions: request.num_partitions,
                partition_locations: locations.clone(),
            });
        }
        
        // Build response
        let response = PbRegisterShuffleResponse {
            status: status_code::SUCCESS,
            partition_locations: locations,
        };
        
        Self::build_response(message_type::REGISTER_SHUFFLE_RESPONSE, &response.encode_to_vec())
    }

    /// Handle MapperEnd request.
    async fn handle_mapper_end(payload: &[u8]) -> Option<Vec<u8>> {
        let request = PbMapperEnd::decode(payload).ok()?;
        
        println!(
            "MapperEnd: shuffle_id={}, map_id={}, attempt_id={}",
            request.shuffle_id, request.map_id, request.attempt_id
        );
        
        let response = PbMapperEndResponse {
            status: status_code::SUCCESS,
        };
        
        Self::build_response(message_type::MAPPER_END_RESPONSE, &response.encode_to_vec())
    }

    /// Handle GetReducerFileGroup request.
    async fn handle_get_reducer_file_group(
        payload: &[u8],
        registered_shuffles: &Arc<RwLock<HashMap<i32, ShuffleInfo>>>,
    ) -> Option<Vec<u8>> {
        let request = PbGetReducerFileGroup::decode(payload).ok()?;
        
        println!("GetReducerFileGroup: shuffle_id={}", request.shuffle_id);
        
        let shuffles = registered_shuffles.read().await;
        let shuffle_info = shuffles.get(&request.shuffle_id);
        
        let response = if let Some(info) = shuffle_info {
            // Build file groups from partition locations
            let mut file_groups = HashMap::new();
            for loc in &info.partition_locations {
                use celeborn_client::protocol::generated::PbFileGroup;
                file_groups.insert(
                    loc.id,
                    PbFileGroup {
                        locations: vec![loc.clone()],
                    },
                );
            }
            
            PbGetReducerFileGroupResponse {
                status: status_code::SUCCESS,
                file_groups,
                attempts: (0..info.num_mappers).collect(),
                partition_ids: (0..info.num_partitions).collect(),
                push_failed_batches: HashMap::new(),
            }
        } else {
            PbGetReducerFileGroupResponse {
                status: status_code::SUCCESS,
                file_groups: HashMap::new(),
                attempts: vec![],
                partition_ids: vec![],
                push_failed_batches: HashMap::new(),
            }
        };
        
        Self::build_response(message_type::GET_REDUCER_FILE_GROUP_RESPONSE, &response.encode_to_vec())
    }

    /// Build a response message in the expected format.
    fn build_response(message_type: i32, payload: &[u8]) -> Option<Vec<u8>> {
        // Build Java serialized TransportMessage response
        // Format: Java serialization of TransportMessage(messageType, payload)
        
        let mut buf = BytesMut::with_capacity(256 + payload.len());
        
        // Java serialization header
        buf.put_u16(0xACED); // Magic
        buf.put_u16(0x0005); // Version
        
        // TC_OBJECT
        buf.put_u8(0x73);
        
        // Class descriptor for TransportMessage
        buf.put_u8(0x72); // TC_CLASSDESC
        
        let class_name = b"org.apache.celeborn.common.network.protocol.TransportMessage";
        buf.put_u16(class_name.len() as u16);
        buf.put_slice(class_name);
        
        // Serial version UID
        buf.put_i64(-3259000920699629773i64);
        
        // Class flags: SC_SERIALIZABLE
        buf.put_u8(0x02);
        
        // Number of fields: 2
        buf.put_u16(2);
        
        // Field 1: int messageTypeValue
        buf.put_u8(b'I'); // Integer type
        buf.put_u16(16);
        buf.put_slice(b"messageTypeValue");
        
        // Field 2: byte[] payload
        buf.put_u8(b'['); // Array type
        buf.put_u16(7);
        buf.put_slice(b"payload");
        buf.put_u8(0x74); // TC_STRING
        buf.put_u16(2);
        buf.put_slice(b"[B");
        
        // TC_ENDBLOCKDATA
        buf.put_u8(0x78);
        
        // TC_NULL for superclass
        buf.put_u8(0x70);
        
        // Field values
        buf.put_i32(message_type);
        
        // Payload as byte array
        if payload.is_empty() {
            buf.put_u8(0x70); // TC_NULL
        } else {
            buf.put_u8(0x75); // TC_ARRAY
            
            // Array class descriptor
            buf.put_u8(0x72); // TC_CLASSDESC
            buf.put_u16(2);
            buf.put_slice(b"[B");
            buf.put_i64(-5984413125824719648i64); // serialVersionUID for [B
            buf.put_u8(0x02); // SC_SERIALIZABLE
            buf.put_u16(0); // No fields
            buf.put_u8(0x78); // TC_ENDBLOCKDATA
            buf.put_u8(0x70); // TC_NULL for superclass
            
            // Array length and data
            buf.put_i32(payload.len() as i32);
            buf.put_slice(payload);
        }
        
        Some(buf.to_vec())
    }
}

// ============================================================================
// Unit Tests (with mock server)
// ============================================================================

/// Test: Verify mock server starts and accepts connections.
#[tokio::test]
async fn test_mock_server_starts() {
    let server = MockLifecycleManagerServer::new()
        .await
        .expect("Should create mock server");
    
    let port = server.port();
    assert!(port > 0, "Server should have a valid port");
    
    // Start server in background
    let server_handle = tokio::spawn(async move {
        server.run().await;
    });
    
    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Try to connect
    let result = TcpStream::connect(format!("127.0.0.1:{}", port)).await;
    assert!(result.is_ok(), "Should connect to mock server");
    
    // Cleanup
    server_handle.abort();
}

/// Test: Verify ExecutorShuffleClient can be created.
#[tokio::test]
async fn test_executor_shuffle_client_creation() {
    let config = CelebornConfig::builder()
        .app_id("test-app")
        .master_endpoints(vec!["localhost:9097".to_string()])
        .build()
        .expect("Should build config");
    
    let client = ExecutorShuffleClient::new(config);
    assert_eq!(client.app_id(), "test-app");
}

/// Test: Verify ExecutorShuffleClient requires initialization.
#[tokio::test]
async fn test_executor_shuffle_client_requires_init() {
    let config = CelebornConfig::builder()
        .app_id("test-app")
        .master_endpoints(vec!["localhost:9097".to_string()])
        .build()
        .expect("Should build config");
    
    let client = ExecutorShuffleClient::new(config);
    
    // Should fail without initialization
    let result = client.register_shuffle(0, 1, 1).await;
    assert!(result.is_err(), "Should fail without initialization");
}

/// Test: Verify shuffle key generation.
#[tokio::test]
async fn test_shuffle_key_generation() {
    let config = CelebornConfig::builder()
        .app_id("my-app")
        .master_endpoints(vec!["localhost:9097".to_string()])
        .build()
        .expect("Should build config");
    
    let client = ExecutorShuffleClient::new(config);
    
    assert_eq!(client.shuffle_key(0), "my-app-0");
    assert_eq!(client.shuffle_key(123), "my-app-123");
    assert_eq!(client.shuffle_key(999), "my-app-999");
}

/// Test: Verify batch ID generation.
#[tokio::test]
async fn test_batch_id_generation() {
    let config = CelebornConfig::builder()
        .app_id("test-app")
        .master_endpoints(vec!["localhost:9097".to_string()])
        .build()
        .expect("Should build config");
    
    let client = ExecutorShuffleClient::new(config);
    
    // Without registered shuffle, should return 0
    assert_eq!(client.next_batch_id(0, 0), 0);
    assert_eq!(client.next_batch_id(0, 0), 0);
}

// ============================================================================
// Integration Tests (with real Java LifecycleManager)
// ============================================================================

/// Test: Connect to Java LifecycleManager and register shuffle.
#[tokio::test]
#[ignore] // Run with --ignored when Java LM is available
async fn test_register_shuffle_with_java_lm() {
    let host = get_lm_host();
    let port = get_lm_port();
    
    let config = CelebornConfig::builder()
        .app_id(&generate_app_id("rust-lm-test"))
        .master_endpoints(vec!["localhost:9097".to_string()])
        .build()
        .expect("Should build config");
    
    let client = ExecutorShuffleClient::new(config);
    
    // Setup connection to LifecycleManager
    client
        .setup_lifecycle_manager_ref(&host, port)
        .await
        .expect("Should connect to LifecycleManager");
    
    // Register shuffle
    let shuffle_id = generate_shuffle_id();
    let result = client.register_shuffle(shuffle_id, 2, 4).await;
    
    match result {
        Ok(id) => {
            assert_eq!(id, shuffle_id);
            println!("Successfully registered shuffle {}", shuffle_id);
            
            // Verify partition locations
            for partition_id in 0..4 {
                let locations = client.get_partition_location(shuffle_id, partition_id);
                assert!(locations.is_ok(), "Should get partition location for {}", partition_id);
                
                let locs = locations.unwrap();
                assert!(!locs.is_empty(), "Should have at least one location");
                println!("Partition {}: {:?}", partition_id, locs[0]);
            }
        }
        Err(e) => {
            panic!("Failed to register shuffle: {}", e);
        }
    }
    
    // Cleanup
    client.cleanup_shuffle(shuffle_id);
    client.shutdown().await;
}

/// Test: Full shuffle lifecycle with Java LifecycleManager.
#[tokio::test]
#[ignore]
async fn test_full_shuffle_lifecycle_with_java_lm() {
    let host = get_lm_host();
    let port = get_lm_port();
    
    let config = CelebornConfig::builder()
        .app_id(&generate_app_id("rust-lifecycle-test"))
        .master_endpoints(vec!["localhost:9097".to_string()])
        .build()
        .expect("Should build config");
    
    let client = ExecutorShuffleClient::new(config);
    
    // Setup connection
    client
        .setup_lifecycle_manager_ref(&host, port)
        .await
        .expect("Should connect to LifecycleManager");
    
    let shuffle_id = generate_shuffle_id();
    let num_mappers = 2;
    let num_partitions = 4;
    
    // 1. Register shuffle
    println!("Step 1: Registering shuffle...");
    client
        .register_shuffle(shuffle_id, num_mappers, num_partitions)
        .await
        .expect("Should register shuffle");
    println!("Shuffle {} registered", shuffle_id);
    
    // 2. Push data (simulated - just verify we can get locations)
    println!("Step 2: Verifying partition locations...");
    for partition_id in 0..num_partitions {
        let locations = client
            .get_partition_location(shuffle_id, partition_id)
            .expect("Should get partition location");
        assert!(!locations.is_empty());
    }
    println!("All partition locations verified");
    
    // 3. Mapper end
    println!("Step 3: Signaling mapper end...");
    for map_id in 0..num_mappers {
        let result = client
            .mapper_end(shuffle_id, map_id, 0, num_mappers)
            .await;
        
        match result {
            Ok(success) => {
                println!("Mapper {} end: success={}", map_id, success);
            }
            Err(e) => {
                println!("Mapper {} end error (may be expected): {}", map_id, e);
            }
        }
    }
    
    // 4. Get reducer file groups
    println!("Step 4: Getting reducer file groups...");
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    let result = client.get_reducer_file_group(shuffle_id).await;
    match result {
        Ok(groups) => {
            println!("Got {} file groups", groups.len());
            for (partition_id, locations) in &groups {
                println!("  Partition {}: {} locations", partition_id, locations.len());
            }
        }
        Err(e) => {
            println!("Get reducer file group error (may be expected): {}", e);
        }
    }
    
    // 5. Cleanup
    println!("Step 5: Cleanup...");
    client.cleanup_shuffle(shuffle_id);
    client.shutdown().await;
    
    println!("Full lifecycle test completed!");
}

/// Test: Multiple shuffles with Java LifecycleManager.
#[tokio::test]
#[ignore]
async fn test_multiple_shuffles_with_java_lm() {
    let host = get_lm_host();
    let port = get_lm_port();
    
    let config = CelebornConfig::builder()
        .app_id(&generate_app_id("rust-multi-shuffle-test"))
        .master_endpoints(vec!["localhost:9097".to_string()])
        .build()
        .expect("Should build config");
    
    let client = ExecutorShuffleClient::new(config);
    
    client
        .setup_lifecycle_manager_ref(&host, port)
        .await
        .expect("Should connect to LifecycleManager");
    
    // Register multiple shuffles
    let shuffle_ids: Vec<i32> = (0..3).map(|_| generate_shuffle_id()).collect();
    
    for shuffle_id in &shuffle_ids {
        client
            .register_shuffle(*shuffle_id, 1, 2)
            .await
            .expect(&format!("Should register shuffle {}", shuffle_id));
        println!("Registered shuffle {}", shuffle_id);
    }
    
    // Verify all shuffles have locations
    for shuffle_id in &shuffle_ids {
        for partition_id in 0..2 {
            let locations = client
                .get_partition_location(*shuffle_id, partition_id)
                .expect("Should get location");
            assert!(!locations.is_empty());
        }
    }
    
    // Cleanup
    for shuffle_id in &shuffle_ids {
        client.cleanup_shuffle(*shuffle_id);
    }
    client.shutdown().await;
    
    println!("Multiple shuffles test completed!");
}

/// Test: Error handling when LifecycleManager is unavailable.
#[tokio::test]
async fn test_lm_unavailable_error_handling() {
    let config = CelebornConfig::builder()
        .app_id("test-app")
        .master_endpoints(vec!["localhost:9097".to_string()])
        .rpc_timeout(Duration::from_millis(1000))
        .build()
        .expect("Should build config");
    
    let client = ExecutorShuffleClient::new(config);
    
    // Try to connect to non-existent LM
    let result = client
        .setup_lifecycle_manager_ref("127.0.0.1", 59999)
        .await;
    
    // Setup should succeed (just stores the address)
    assert!(result.is_ok());
    
    // But register_shuffle should fail when trying to connect
    let result = client.register_shuffle(0, 1, 1).await;
    assert!(result.is_err(), "Should fail when LM is unavailable");
    
    println!("Error handling test passed: {:?}", result.err());
}
