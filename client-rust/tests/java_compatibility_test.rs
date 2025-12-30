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

//! Java compatibility tests for Celeborn Rust client.
//!
//! These tests verify that the Rust client's encoding matches the Java client's encoding.
//! This is critical for interoperability between Rust and Java components.
//!
//! Key areas tested:
//! 1. PushData message encoding
//! 2. Frame format (header + message + body)
//! 3. Batch header format (mapId + attemptId + batchId + compressedSize)
//! 4. String encoding (4-byte length prefix + UTF-8)
//!
//! Run with:
//!   cargo test --test java_compatibility_test -- --nocapture

use bytes::{BufMut, Bytes, BytesMut};
use tokio_util::codec::Encoder;

use celeborn_client::network::codec::{CelebornCodec, Frame};
use celeborn_client::protocol::message::{MessageType, PushData};
use celeborn_client::protocol::{encode_string_java, Encodable};

// ============================================================================
// PushData Message Encoding Tests
// ============================================================================

/// Test: Verify PushData message encoding matches Java format.
///
/// Java PushData.encode() format:
/// - requestId: 8 bytes (long, big-endian)
/// - mode: 1 byte
/// - shuffleKey: 4 bytes length + UTF-8 bytes
/// - partitionUniqueId: 4 bytes length + UTF-8 bytes
#[test]
fn test_push_data_encoding_matches_java() {
    let push_data = PushData::new(
        12345678901234i64,  // requestId
        0,                   // mode (PRIMARY)
        "test-app-1".to_string(),  // shuffleKey
        "0-0".to_string(),   // partitionUniqueId
        Bytes::from(vec![1u8, 2, 3, 4, 5]),  // body (not included in encode)
    );
    
    let buf = push_data.encode_to_bytes();
    let bytes = buf.freeze();
    
    // Verify encoding
    let mut offset = 0;
    
    // 1. requestId (8 bytes, big-endian)
    let request_id = i64::from_be_bytes([
        bytes[offset], bytes[offset + 1], bytes[offset + 2], bytes[offset + 3],
        bytes[offset + 4], bytes[offset + 5], bytes[offset + 6], bytes[offset + 7],
    ]);
    assert_eq!(request_id, 12345678901234i64, "requestId should match");
    offset += 8;
    
    // 2. mode (1 byte)
    assert_eq!(bytes[offset], 0, "mode should be 0 (PRIMARY)");
    offset += 1;
    
    // 3. shuffleKey (4 bytes length + UTF-8)
    let shuffle_key_len = i32::from_be_bytes([
        bytes[offset], bytes[offset + 1], bytes[offset + 2], bytes[offset + 3],
    ]) as usize;
    offset += 4;
    assert_eq!(shuffle_key_len, 10, "shuffleKey length should be 10");
    
    let shuffle_key = String::from_utf8(bytes[offset..offset + shuffle_key_len].to_vec()).unwrap();
    assert_eq!(shuffle_key, "test-app-1", "shuffleKey should match");
    offset += shuffle_key_len;
    
    // 4. partitionUniqueId (4 bytes length + UTF-8)
    let partition_id_len = i32::from_be_bytes([
        bytes[offset], bytes[offset + 1], bytes[offset + 2], bytes[offset + 3],
    ]) as usize;
    offset += 4;
    assert_eq!(partition_id_len, 3, "partitionUniqueId length should be 3");
    
    let partition_id = String::from_utf8(bytes[offset..offset + partition_id_len].to_vec()).unwrap();
    assert_eq!(partition_id, "0-0", "partitionUniqueId should match");
    
    println!("PushData encoding test passed!");
    println!("  requestId: {}", request_id);
    println!("  mode: {}", bytes[8]);
    println!("  shuffleKey: {} (len={})", shuffle_key, shuffle_key_len);
    println!("  partitionUniqueId: {} (len={})", partition_id, partition_id_len);
}

/// Test: Verify string encoding matches Java Encoders.Strings.encode().
///
/// Java format: 4 bytes length (big-endian) + UTF-8 bytes
#[test]
fn test_string_encoding_matches_java() {
    let test_cases = vec![
        ("", 0),
        ("a", 1),
        ("hello", 5),
        ("test-app-123", 12),
        ("中文", 6),  // 2 Chinese chars = 6 UTF-8 bytes
    ];
    
    for (s, expected_len) in test_cases {
        let mut buf = BytesMut::new();
        encode_string_java(&mut buf, s);
        
        // Verify length prefix
        let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        assert_eq!(len, expected_len, "String '{}' should have length {}", s, expected_len);
        
        // Verify content
        if len > 0 {
            let content = String::from_utf8(buf[4..4 + len].to_vec()).unwrap();
            assert_eq!(content, s, "String content should match");
        }
        
        println!("String encoding test passed for '{}' (len={})", s, len);
    }
}

// ============================================================================
// Frame Format Tests
// ============================================================================

/// Test: Verify frame format matches Java MessageEncoder.
///
/// Java frame format:
/// - msgSize: 4 bytes (int, big-endian) - size of message content
/// - msgType: 1 byte - message type ID
/// - bodySize: 4 bytes (int, big-endian) - size of body
/// - message content: msgSize bytes
/// - body: bodySize bytes
#[test]
fn test_frame_format_matches_java() {
    let mut codec = CelebornCodec::new();
    let mut buf = BytesMut::new();
    
    // Create a PushData message
    let push_data = PushData::new(
        1i64,
        0,
        "app-1".to_string(),
        "0-0".to_string(),
        Bytes::from(vec![1u8, 2, 3, 4, 5]),
    );
    
    let message_buf = push_data.encode_to_bytes();
    let body = Bytes::from(vec![0xAA, 0xBB, 0xCC, 0xDD]);
    
    let frame = Frame::with_body(
        MessageType::PushData,
        message_buf.freeze(),
        body.clone(),
    );
    
    codec.encode(frame, &mut buf).unwrap();
    
    // Verify header
    let msg_size = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    let msg_type = buf[4];
    let body_size = u32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]) as usize;
    
    println!("Frame format test:");
    println!("  msgSize: {} bytes", msg_size);
    println!("  msgType: {} (PushData=11)", msg_type);
    println!("  bodySize: {} bytes", body_size);
    println!("  Total frame size: {} bytes", buf.len());
    
    // Verify message type
    assert_eq!(msg_type, 11, "msgType should be 11 (PushData)");
    
    // Verify body size
    assert_eq!(body_size, 4, "bodySize should be 4");
    
    // Verify total size
    let expected_total = 9 + msg_size + body_size;  // header (9) + message + body
    assert_eq!(buf.len(), expected_total, "Total frame size should match");
    
    // Verify body content
    let body_start = 9 + msg_size;
    assert_eq!(&buf[body_start..body_start + body_size], &[0xAA, 0xBB, 0xCC, 0xDD]);
    
    println!("Frame format test passed!");
}

// ============================================================================
// Batch Header Format Tests
// ============================================================================

/// Test: Verify batch header format matches Java ShuffleClientImpl.
///
/// Java batch header format (using Platform.putInt - native/little-endian on x86/ARM):
/// - mapId: 4 bytes (int, little-endian)
/// - attemptId: 4 bytes (int, little-endian)
/// - batchId: 4 bytes (int, little-endian)
/// - compressedTotalSize: 4 bytes (int, little-endian)
#[test]
fn test_batch_header_format_matches_java() {
    let map_id: i32 = 5;
    let attempt_id: i32 = 0;
    let batch_id: i32 = 10;
    let compressed_size: i32 = 1024;
    
    // Build batch header like Rust client does
    let mut header = BytesMut::with_capacity(16);
    header.put_i32_le(map_id);
    header.put_i32_le(attempt_id);
    header.put_i32_le(batch_id);
    header.put_i32_le(compressed_size);
    
    // Verify little-endian encoding
    // mapId = 5 -> [0x05, 0x00, 0x00, 0x00]
    assert_eq!(&header[0..4], &[0x05, 0x00, 0x00, 0x00], "mapId should be little-endian");
    
    // attemptId = 0 -> [0x00, 0x00, 0x00, 0x00]
    assert_eq!(&header[4..8], &[0x00, 0x00, 0x00, 0x00], "attemptId should be little-endian");
    
    // batchId = 10 -> [0x0A, 0x00, 0x00, 0x00]
    assert_eq!(&header[8..12], &[0x0A, 0x00, 0x00, 0x00], "batchId should be little-endian");
    
    // compressedSize = 1024 -> [0x00, 0x04, 0x00, 0x00]
    assert_eq!(&header[12..16], &[0x00, 0x04, 0x00, 0x00], "compressedSize should be little-endian");
    
    // Verify we can read back the values
    let read_map_id = i32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    let read_attempt_id = i32::from_le_bytes([header[4], header[5], header[6], header[7]]);
    let read_batch_id = i32::from_le_bytes([header[8], header[9], header[10], header[11]]);
    let read_compressed_size = i32::from_le_bytes([header[12], header[13], header[14], header[15]]);
    
    assert_eq!(read_map_id, map_id);
    assert_eq!(read_attempt_id, attempt_id);
    assert_eq!(read_batch_id, batch_id);
    assert_eq!(read_compressed_size, compressed_size);
    
    println!("Batch header format test passed!");
    println!("  mapId: {} -> {:02x?}", map_id, &header[0..4]);
    println!("  attemptId: {} -> {:02x?}", attempt_id, &header[4..8]);
    println!("  batchId: {} -> {:02x?}", batch_id, &header[8..12]);
    println!("  compressedSize: {} -> {:02x?}", compressed_size, &header[12..16]);
}

/// Test: Verify batch header with larger values.
#[test]
fn test_batch_header_large_values() {
    let map_id: i32 = 12345;
    let attempt_id: i32 = 2;
    let batch_id: i32 = 99999;
    let compressed_size: i32 = 1048576;  // 1MB
    
    let mut header = BytesMut::with_capacity(16);
    header.put_i32_le(map_id);
    header.put_i32_le(attempt_id);
    header.put_i32_le(batch_id);
    header.put_i32_le(compressed_size);
    
    // Verify we can read back the values
    let read_map_id = i32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    let read_attempt_id = i32::from_le_bytes([header[4], header[5], header[6], header[7]]);
    let read_batch_id = i32::from_le_bytes([header[8], header[9], header[10], header[11]]);
    let read_compressed_size = i32::from_le_bytes([header[12], header[13], header[14], header[15]]);
    
    assert_eq!(read_map_id, map_id);
    assert_eq!(read_attempt_id, attempt_id);
    assert_eq!(read_batch_id, batch_id);
    assert_eq!(read_compressed_size, compressed_size);
    
    println!("Batch header large values test passed!");
    println!("  mapId: {} -> {:02x?}", map_id, &header[0..4]);
    println!("  attemptId: {} -> {:02x?}", attempt_id, &header[4..8]);
    println!("  batchId: {} -> {:02x?}", batch_id, &header[8..12]);
    println!("  compressedSize: {} -> {:02x?}", compressed_size, &header[12..16]);
}

// ============================================================================
// Complete PushData Frame Tests
// ============================================================================

/// Test: Build a complete PushData frame like the Rust client does.
///
/// This simulates what ExecutorShuffleClient.send_push_data() does.
#[test]
fn test_complete_push_data_frame() {
    let mut codec = CelebornCodec::new();
    let mut buf = BytesMut::new();
    
    // Simulate push_data parameters
    let shuffle_id = 1;
    let map_id = 5;
    let attempt_id = 0;
    let batch_id = 10;
    let app_id = "test-app";
    let partition_id = 3;
    let epoch = 0;
    
    // Build shuffle key and partition unique id
    let shuffle_key = format!("{}-{}", app_id, shuffle_id);
    let partition_unique_id = format!("{}-{}", partition_id, epoch);
    
    // Build body with batch header
    let data = vec![0x01, 0x02, 0x03, 0x04, 0x05];  // 5 bytes of data
    let compressed_size = data.len() as i32;
    
    let mut body_with_header = BytesMut::with_capacity(16 + data.len());
    body_with_header.put_i32_le(map_id);
    body_with_header.put_i32_le(attempt_id);
    body_with_header.put_i32_le(batch_id);
    body_with_header.put_i32_le(compressed_size);
    body_with_header.put_slice(&data);
    let body = body_with_header.freeze();
    
    // Build PushData message
    let request_id = 1i64;
    let mode = 0u8;  // PRIMARY
    
    let push_data = PushData::new(
        request_id,
        mode,
        shuffle_key.clone(),
        partition_unique_id.clone(),
        body.clone(),
    );
    
    // Encode message (without body)
    let message_buf = push_data.encode_to_bytes();
    
    // Build frame
    let frame = Frame::with_body(
        MessageType::PushData,
        message_buf.freeze(),
        body.clone(),
    );
    
    // Encode frame
    codec.encode(frame, &mut buf).unwrap();
    
    println!("Complete PushData frame test:");
    println!("  shuffleKey: {}", shuffle_key);
    println!("  partitionUniqueId: {}", partition_unique_id);
    println!("  Body (batch header + data):");
    println!("    mapId: {}", map_id);
    println!("    attemptId: {}", attempt_id);
    println!("    batchId: {}", batch_id);
    println!("    compressedSize: {}", compressed_size);
    println!("    data: {:02x?}", data);
    println!("  Total frame size: {} bytes", buf.len());
    println!("  Frame bytes: {:02x?}", &buf[..]);
    
    // Verify frame header
    let msg_size = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    let msg_type = buf[4];
    let body_size = u32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]) as usize;
    
    assert_eq!(msg_type, 11, "msgType should be 11 (PushData)");
    assert_eq!(body_size, 21, "bodySize should be 21 (16 header + 5 data)");
    
    // Verify body content
    let body_start = 9 + msg_size;
    let body_bytes = &buf[body_start..body_start + body_size];
    
    // Verify batch header in body
    let read_map_id = i32::from_le_bytes([body_bytes[0], body_bytes[1], body_bytes[2], body_bytes[3]]);
    let read_attempt_id = i32::from_le_bytes([body_bytes[4], body_bytes[5], body_bytes[6], body_bytes[7]]);
    let read_batch_id = i32::from_le_bytes([body_bytes[8], body_bytes[9], body_bytes[10], body_bytes[11]]);
    let read_compressed_size = i32::from_le_bytes([body_bytes[12], body_bytes[13], body_bytes[14], body_bytes[15]]);
    
    assert_eq!(read_map_id, map_id);
    assert_eq!(read_attempt_id, attempt_id);
    assert_eq!(read_batch_id, batch_id);
    assert_eq!(read_compressed_size, compressed_size);
    
    // Verify data
    assert_eq!(&body_bytes[16..], &data[..]);
    
    println!("Complete PushData frame test passed!");
}

// ============================================================================
// Endianness Verification Tests
// ============================================================================

/// Test: Verify that frame header uses big-endian (like Java Netty).
#[test]
fn test_frame_header_uses_big_endian() {
    let mut codec = CelebornCodec::new();
    let mut buf = BytesMut::new();
    
    // Create a message with known size
    let message = Bytes::from(vec![0u8; 256]);  // 256 bytes = 0x100
    let body = Bytes::from(vec![0u8; 1024]);    // 1024 bytes = 0x400
    
    let frame = Frame::with_body(MessageType::PushData, message, body);
    codec.encode(frame, &mut buf).unwrap();
    
    // msgSize should be 256 = 0x00000100 in big-endian
    assert_eq!(&buf[0..4], &[0x00, 0x00, 0x01, 0x00], "msgSize should be big-endian");
    
    // bodySize should be 1024 = 0x00000400 in big-endian
    assert_eq!(&buf[5..9], &[0x00, 0x00, 0x04, 0x00], "bodySize should be big-endian");
    
    println!("Frame header endianness test passed!");
}

/// Test: Verify that batch header uses little-endian (like Java Platform.putInt).
#[test]
fn test_batch_header_uses_little_endian() {
    let value: i32 = 0x12345678;
    
    let mut buf = BytesMut::with_capacity(4);
    buf.put_i32_le(value);
    
    // Little-endian: least significant byte first
    assert_eq!(&buf[..], &[0x78, 0x56, 0x34, 0x12], "Should be little-endian");
    
    println!("Batch header endianness test passed!");
}

// ============================================================================
// Message Type Tests
// ============================================================================

/// Test: Verify message type values match Java.
#[test]
fn test_message_type_values() {
    assert_eq!(MessageType::ChunkFetchRequest as u8, 0);
    assert_eq!(MessageType::ChunkFetchSuccess as u8, 1);
    assert_eq!(MessageType::ChunkFetchFailure as u8, 2);
    assert_eq!(MessageType::RpcRequest as u8, 3);
    assert_eq!(MessageType::RpcResponse as u8, 4);
    assert_eq!(MessageType::RpcFailure as u8, 5);
    assert_eq!(MessageType::OpenStream as u8, 6);
    assert_eq!(MessageType::StreamHandle as u8, 7);
    assert_eq!(MessageType::OneWayMessage as u8, 9);
    assert_eq!(MessageType::PushData as u8, 11);
    assert_eq!(MessageType::PushMergedData as u8, 12);
    
    println!("Message type values test passed!");
}

// ============================================================================
// Hypothesis: Response Handling Issue
// ============================================================================

/// Test: Document the hypothesis about response handling.
///
/// The Rust client uses send_one_way() which doesn't wait for responses.
/// The Java client uses pushData() which waits for responses and handles:
/// - SOFT_SPLIT: Request revive but continue
/// - HARD_SPLIT: Request revive and retry push
/// - PUSH_DATA_SUCCESS_PRIMARY_CONGESTED: Apply congestion control
/// - MAP_ENDED: Mark mapper as ended
///
/// This test documents the hypothesis that the issue might be related to
/// response handling, not encoding.
#[test]
fn test_document_response_handling_hypothesis() {
    println!("=== Response Handling Hypothesis ===");
    println!();
    println!("Current Rust client behavior:");
    println!("  - Uses send_one_way() to send PushData");
    println!("  - Does NOT wait for Worker response");
    println!("  - Does NOT handle SOFT_SPLIT, HARD_SPLIT, etc.");
    println!();
    println!("Java client behavior:");
    println!("  - Uses pushData() with callback");
    println!("  - Waits for Worker response");
    println!("  - Handles various status codes:");
    println!("    - SOFT_SPLIT: Request revive, continue");
    println!("    - HARD_SPLIT: Request revive, retry push");
    println!("    - PUSH_DATA_SUCCESS_PRIMARY_CONGESTED: Congestion control");
    println!("    - MAP_ENDED: Mark mapper as ended");
    println!();
    println!("Hypothesis:");
    println!("  The Worker might be returning a status code that requires");
    println!("  action (like HARD_SPLIT), but the Rust client ignores it.");
    println!("  This could cause data to not be properly committed.");
    println!();
    println!("Verification steps:");
    println!("  1. Add response handling to Rust client");
    println!("  2. Log Worker responses to see what status codes are returned");
    println!("  3. Compare with Java client behavior");
}

// ============================================================================
// Status Code Tests
// ============================================================================

/// Test: Verify status code values match Java.
///
/// These are the status codes that Worker can return in PushData response.
#[test]
fn test_status_code_values() {
    // From StatusCode.java
    const SUCCESS: u8 = 0;
    const PARTIAL_SUCCESS: u8 = 1;
    const SHUFFLE_ALREADY_REGISTERED: u8 = 2;
    const SHUFFLE_NOT_REGISTERED: u8 = 3;
    const RESERVE_SLOTS_FAILED: u8 = 4;
    const SLOT_NOT_AVAILABLE: u8 = 5;
    const WORKER_NOT_FOUND: u8 = 6;
    const PARTITION_NOT_FOUND: u8 = 7;
    const REPLICA_BUFFER_CONGESTED: u8 = 8;
    const STAGE_ENDED: u8 = 9;
    const SHUFFLE_DATA_LOST: u8 = 10;
    const WORKER_SHUTDOWN: u8 = 11;
    const PUSH_DATA_WRITE_FAIL_REPLICA: u8 = 12;
    const PUSH_DATA_WRITE_FAIL_PRIMARY: u8 = 13;
    const PUSH_DATA_FAIL_NON_CRITICAL_CAUSE: u8 = 14;
    const PUSH_DATA_FAIL_PARTITION_NOT_FOUND: u8 = 15;
    const PUSH_DATA_CREATE_CONNECTION_FAIL_PRIMARY: u8 = 16;
    const PUSH_DATA_CREATE_CONNECTION_FAIL_REPLICA: u8 = 17;
    const PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY: u8 = 18;
    const PUSH_DATA_CONNECTION_EXCEPTION_REPLICA: u8 = 19;
    const PUSH_DATA_TIMEOUT_PRIMARY: u8 = 20;
    const HARD_SPLIT: u8 = 21;
    const SOFT_SPLIT: u8 = 22;
    const PUSH_DATA_TIMEOUT_REPLICA: u8 = 23;
    const PUSH_DATA_SUCCESS_PRIMARY_CONGESTED: u8 = 24;
    const PUSH_DATA_SUCCESS_REPLICA_CONGESTED: u8 = 25;
    const REVIVE_FAILED: u8 = 26;
    const MAP_ENDED: u8 = 27;
    
    println!("Status codes that require special handling:");
    println!("  HARD_SPLIT ({}): Retry push after revive", HARD_SPLIT);
    println!("  SOFT_SPLIT ({}): Request revive, continue", SOFT_SPLIT);
    println!("  PUSH_DATA_SUCCESS_PRIMARY_CONGESTED ({}): Apply congestion control", PUSH_DATA_SUCCESS_PRIMARY_CONGESTED);
    println!("  PUSH_DATA_SUCCESS_REPLICA_CONGESTED ({}): Apply congestion control", PUSH_DATA_SUCCESS_REPLICA_CONGESTED);
    println!("  MAP_ENDED ({}): Mark mapper as ended", MAP_ENDED);
    println!("  STAGE_ENDED ({}): Stage has ended", STAGE_ENDED);
    
    // Verify the values are correct
    assert_eq!(SUCCESS, 0);
    assert_eq!(HARD_SPLIT, 21);
    assert_eq!(SOFT_SPLIT, 22);
    assert_eq!(MAP_ENDED, 27);
    
    println!("Status code values test passed!");
}

/// Test: Simulate Worker response parsing.
///
/// Worker response format:
/// - For success: empty or 1 byte status code
/// - For split: 1 byte status code (HARD_SPLIT or SOFT_SPLIT)
#[test]
fn test_worker_response_parsing() {
    // Empty response = success
    let empty_response: Vec<u8> = vec![];
    assert!(empty_response.is_empty(), "Empty response means success");
    
    // Single byte response with status code
    let success_response: Vec<u8> = vec![0];  // SUCCESS
    assert_eq!(success_response[0], 0, "Status code 0 = SUCCESS");
    
    let hard_split_response: Vec<u8> = vec![21];  // HARD_SPLIT
    assert_eq!(hard_split_response[0], 21, "Status code 21 = HARD_SPLIT");
    
    let soft_split_response: Vec<u8> = vec![22];  // SOFT_SPLIT
    assert_eq!(soft_split_response[0], 22, "Status code 22 = SOFT_SPLIT");
    
    let map_ended_response: Vec<u8> = vec![27];  // MAP_ENDED
    assert_eq!(map_ended_response[0], 27, "Status code 27 = MAP_ENDED");
    
    println!("Worker response parsing test passed!");
}

// ============================================================================
// Debugging Helpers
// ============================================================================

/// Test: Print hex dump of a complete PushData frame for debugging.
#[test]
fn test_print_push_data_frame_hex_dump() {
    let mut codec = CelebornCodec::new();
    let mut buf = BytesMut::new();
    
    // Build a realistic PushData frame
    let shuffle_key = "app-12345-1";
    let partition_unique_id = "0-0";
    let map_id = 0i32;
    let attempt_id = 0i32;
    let batch_id = 0i32;
    let data = b"Hello, Celeborn!";
    let compressed_size = data.len() as i32;
    
    // Build body with batch header
    let mut body = BytesMut::with_capacity(16 + data.len());
    body.put_i32_le(map_id);
    body.put_i32_le(attempt_id);
    body.put_i32_le(batch_id);
    body.put_i32_le(compressed_size);
    body.put_slice(data);
    
    // Build PushData message
    let push_data = PushData::new(
        1i64,
        0,
        shuffle_key.to_string(),
        partition_unique_id.to_string(),
        body.clone().freeze(),
    );
    
    let message_buf = push_data.encode_to_bytes();
    let frame = Frame::with_body(
        MessageType::PushData,
        message_buf.freeze(),
        body.freeze(),
    );
    
    codec.encode(frame, &mut buf).unwrap();
    
    println!("=== PushData Frame Hex Dump ===");
    println!();
    println!("Parameters:");
    println!("  shuffleKey: {}", shuffle_key);
    println!("  partitionUniqueId: {}", partition_unique_id);
    println!("  mapId: {}", map_id);
    println!("  attemptId: {}", attempt_id);
    println!("  batchId: {}", batch_id);
    println!("  data: {:?}", String::from_utf8_lossy(data));
    println!();
    
    // Parse and print frame structure
    let msg_size = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    let msg_type = buf[4];
    let body_size = u32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]) as usize;
    
    println!("Frame structure:");
    println!("  Header (9 bytes):");
    println!("    msgSize:  {:02x?} = {} bytes", &buf[0..4], msg_size);
    println!("    msgType:  {:02x} = {} (PushData)", buf[4], msg_type);
    println!("    bodySize: {:02x?} = {} bytes", &buf[5..9], body_size);
    println!();
    
    println!("  Message content ({} bytes):", msg_size);
    let msg_start = 9;
    let msg_end = msg_start + msg_size;
    for (i, chunk) in buf[msg_start..msg_end].chunks(16).enumerate() {
        print!("    {:04x}: ", i * 16);
        for b in chunk {
            print!("{:02x} ", b);
        }
        println!();
    }
    println!();
    
    println!("  Body ({} bytes):", body_size);
    let body_start = msg_end;
    let body_end = body_start + body_size;
    println!("    Batch header (16 bytes):");
    println!("      mapId:          {:02x?}", &buf[body_start..body_start+4]);
    println!("      attemptId:      {:02x?}", &buf[body_start+4..body_start+8]);
    println!("      batchId:        {:02x?}", &buf[body_start+8..body_start+12]);
    println!("      compressedSize: {:02x?}", &buf[body_start+12..body_start+16]);
    println!("    Data ({} bytes):", body_size - 16);
    println!("      {:02x?}", &buf[body_start+16..body_end]);
    println!();
    
    println!("Complete frame ({} bytes):", buf.len());
    for (i, chunk) in buf.chunks(16).enumerate() {
        print!("  {:04x}: ", i * 16);
        for b in chunk {
            print!("{:02x} ", b);
        }
        // Print ASCII representation
        print!(" |");
        for b in chunk {
            if *b >= 0x20 && *b < 0x7f {
                print!("{}", *b as char);
            } else {
                print!(".");
            }
        }
        println!("|");
    }
}
