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

//! Demo of Java serialization format for Celeborn Master RPC.
//!
//! This example demonstrates the Java ObjectOutputStream serialization format
//! used by Celeborn Master RPC, and shows how the Rust implementation generates
//! compatible binary data.
//!
//! Usage:
//!   cargo run --example java_serialization_demo

use celeborn_client::protocol::java_serialization::{
    encode_request_message, JavaObjectOutputStream, RpcAddress,
};
use celeborn_client::protocol::transport::{PbRegisterShuffle, TransportMessageType};
use prost::Message;

fn main() {
    println!("=== Java Serialization Format Demo ===\n");

    // Demo 1: Java ObjectOutputStream header
    println!("### 1. Java ObjectOutputStream Header\n");
    let stream = JavaObjectOutputStream::new();
    let header = stream.into_bytes();
    println!("Stream header (4 bytes):");
    print_hex(&header);
    println!("  - Magic: 0xACED (Java serialization magic)");
    println!("  - Version: 0x0005 (stream version 5)");

    // Demo 2: TransportMessage serialization
    println!("\n### 2. TransportMessage Serialization\n");
    
    // Create a simple payload
    let payload = b"test";
    let mut stream = JavaObjectOutputStream::new();
    stream.write_transport_message(4, payload); // 4 = REGISTER_SHUFFLE
    let serialized = stream.into_bytes();
    
    println!("TransportMessage with payload 'test' (message_type=4):");
    print_hex(&serialized);
    println!("\nStructure breakdown:");
    println!("  [0-3]   Stream header: AC ED 00 05");
    println!("  [4]     TC_OBJECT: 73");
    println!("  [5]     TC_CLASSDESC: 72");
    println!("  [6-7]   Class name length");
    println!("  [8-..] Class name: org.apache.celeborn.common.network.protocol.TransportMessage");
    println!("  [..]    Serial version UID: -3259000920699629773");
    println!("  [..]    Class flags, fields, etc.");
    println!("  [..]    Field values: messageTypeValue (int), payload (byte[])");

    // Demo 3: RequestMessage encoding
    println!("\n### 3. RequestMessage Encoding\n");
    
    let sender = RpcAddress::new("client-host", 12345);
    let receiver = RpcAddress::new("master-host", 9097);
    
    // Create a RegisterShuffle protobuf message
    let register = PbRegisterShuffle {
        shuffle_id: 1,
        num_mappers: 4,
        num_partitions: 10,
    };
    let mut pb_payload = Vec::new();
    register.encode(&mut pb_payload).unwrap();
    
    println!("Protobuf payload (PbRegisterShuffle):");
    print_hex(&pb_payload);
    println!("  shuffle_id: 1, num_mappers: 4, num_partitions: 10");
    
    let request_message = encode_request_message(
        Some(&sender),
        Some(&receiver),
        "MasterEndpoint",
        TransportMessageType::RegisterShuffle as i32,
        &pb_payload,
    );
    
    println!("\nComplete RequestMessage:");
    print_hex(&request_message);
    
    println!("\nRequestMessage structure:");
    println!("  1. Sender address:");
    println!("     - hasAddress: true (1 byte)");
    println!("     - host: 'client-host' (2-byte length + UTF-8)");
    println!("     - port: 12345 (4 bytes, big-endian)");
    println!("  2. Receiver address:");
    println!("     - hasAddress: true (1 byte)");
    println!("     - host: 'master-host' (2-byte length + UTF-8)");
    println!("     - port: 9097 (4 bytes, big-endian)");
    println!("  3. Receiver name: 'MasterEndpoint' (2-byte length + UTF-8)");
    println!("  4. Content: Java serialized TransportMessage");

    // Demo 4: Complete RPC frame
    println!("\n### 4. Complete RPC Frame\n");
    
    let request_id: u64 = 1;
    let frame_body_len = 1 + 8 + request_message.len();
    
    println!("RPC Frame structure:");
    println!("  - Frame length: {} bytes (4 bytes, big-endian)", frame_body_len);
    println!("  - Request type: 1 (RpcRequest, 1 byte)");
    println!("  - Request ID: {} (8 bytes, big-endian)", request_id);
    println!("  - Message body: {} bytes", request_message.len());
    println!("\nTotal frame size: {} bytes", 4 + frame_body_len);

    // Demo 5: Message type values
    println!("\n### 5. Common Message Types\n");
    println!("  REGISTER_SHUFFLE = 4");
    println!("  REGISTER_SHUFFLE_RESPONSE = 5");
    println!("  REQUEST_SLOTS = 6");
    println!("  REQUEST_SLOTS_RESPONSE = 9");
    println!("  HEARTBEAT_FROM_APPLICATION = 20");
    println!("  RESERVE_SLOTS = 28");
    println!("  COMMIT_FILES = 30");

    println!("\n=== Demo Complete ===");
}

fn print_hex(data: &[u8]) {
    for (i, chunk) in data.chunks(16).enumerate() {
        print!("  {:04x}: ", i * 16);
        for byte in chunk {
            print!("{:02x} ", byte);
        }
        // Pad if less than 16 bytes
        for _ in chunk.len()..16 {
            print!("   ");
        }
        print!(" |");
        for byte in chunk {
            if *byte >= 0x20 && *byte < 0x7f {
                print!("{}", *byte as char);
            } else {
                print!(".");
            }
        }
        println!("|");
    }
}
