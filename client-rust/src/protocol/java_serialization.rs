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

//! Java ObjectOutputStream serialization format implementation.
//!
//! This module implements a subset of Java's ObjectOutputStream format
//! sufficient to serialize TransportMessage objects for Celeborn Master RPC.
//!
//! Java Serialization Stream Format:
//! - Magic: 0xACED
//! - Version: 0x0005
//! - Content: sequence of objects
//!
//! Reference: https://docs.oracle.com/javase/8/docs/platform/serialization/spec/protocol.html

use bytes::{BufMut, Bytes, BytesMut};
use std::collections::HashMap;

/// Java serialization stream magic number
const STREAM_MAGIC: u16 = 0xACED;
/// Java serialization stream version
const STREAM_VERSION: u16 = 0x0005;

/// Java serialization type constants
#[allow(dead_code)]
mod tc {
    pub const NULL: u8 = 0x70;
    pub const REFERENCE: u8 = 0x71;
    pub const CLASSDESC: u8 = 0x72;
    pub const OBJECT: u8 = 0x73;
    pub const STRING: u8 = 0x74;
    pub const ARRAY: u8 = 0x75;
    pub const CLASS: u8 = 0x76;
    pub const BLOCKDATA: u8 = 0x77;
    pub const ENDBLOCKDATA: u8 = 0x78;
    pub const RESET: u8 = 0x79;
    pub const BLOCKDATALONG: u8 = 0x7A;
    pub const EXCEPTION: u8 = 0x7B;
    pub const LONGSTRING: u8 = 0x7C;
    pub const PROXYCLASSDESC: u8 = 0x7D;
    pub const ENUM: u8 = 0x7E;
}

/// Java serialization class descriptor flags
#[allow(dead_code)]
mod sc {
    pub const WRITE_METHOD: u8 = 0x01;
    pub const BLOCK_DATA: u8 = 0x08;
    pub const SERIALIZABLE: u8 = 0x02;
    pub const EXTERNALIZABLE: u8 = 0x04;
    pub const ENUM: u8 = 0x10;
}

/// Java primitive type codes
#[allow(dead_code)]
mod prim {
    pub const BYTE: u8 = b'B';
    pub const CHAR: u8 = b'C';
    pub const DOUBLE: u8 = b'D';
    pub const FLOAT: u8 = b'F';
    pub const INTEGER: u8 = b'I';
    pub const LONG: u8 = b'J';
    pub const SHORT: u8 = b'S';
    pub const BOOLEAN: u8 = b'Z';
    pub const ARRAY: u8 = b'[';
    pub const OBJECT: u8 = b'L';
}

/// Base handle for object references
const BASE_WIRE_HANDLE: u32 = 0x7E0000;

/// Java ObjectOutputStream writer for Celeborn protocol.
pub struct JavaObjectOutputStream {
    buffer: BytesMut,
    /// Handle counter for object references
    next_handle: u32,
    /// Map of class descriptors to their handles
    class_handles: HashMap<String, u32>,
}

impl JavaObjectOutputStream {
    /// Create a new Java object output stream.
    pub fn new() -> Self {
        let mut stream = Self {
            buffer: BytesMut::with_capacity(1024),
            next_handle: BASE_WIRE_HANDLE,
            class_handles: HashMap::new(),
        };
        // Write stream header
        stream.buffer.put_u16(STREAM_MAGIC);
        stream.buffer.put_u16(STREAM_VERSION);
        stream
    }

    /// Get the serialized bytes.
    pub fn into_bytes(self) -> Bytes {
        self.buffer.freeze()
    }

    /// Get the current buffer length.
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Allocate a new handle and return it.
    fn new_handle(&mut self) -> u32 {
        let handle = self.next_handle;
        self.next_handle += 1;
        handle
    }

    /// Write a null reference.
    pub fn write_null(&mut self) {
        self.buffer.put_u8(tc::NULL);
    }

    /// Write a UTF-8 string (short form, max 65535 bytes).
    pub fn write_utf(&mut self, s: &str) {
        let _bytes = s.as_bytes();
        // Use modified UTF-8 encoding (same as standard UTF-8 for ASCII)
        let utf_len = modified_utf8_len(s);
        if utf_len <= 65535 {
            self.buffer.put_u8(tc::STRING);
            self.buffer.put_u16(utf_len as u16);
            write_modified_utf8(&mut self.buffer, s);
            // String gets a handle
            self.new_handle();
        } else {
            self.buffer.put_u8(tc::LONGSTRING);
            self.buffer.put_u64(utf_len as u64);
            write_modified_utf8(&mut self.buffer, s);
            self.new_handle();
        }
    }

    /// Write a byte array.
    pub fn write_byte_array(&mut self, data: &[u8]) {
        // TC_ARRAY
        self.buffer.put_u8(tc::ARRAY);
        
        // Write array class descriptor [B
        self.write_class_desc_for_byte_array();
        
        // Array length
        self.buffer.put_i32(data.len() as i32);
        
        // Array data
        self.buffer.put_slice(data);
    }

    /// Write class descriptor for byte array [B
    fn write_class_desc_for_byte_array(&mut self) {
        let class_name = "[B";
        
        if let Some(&handle) = self.class_handles.get(class_name) {
            // Reference to existing class descriptor
            self.buffer.put_u8(tc::REFERENCE);
            self.buffer.put_u32(handle);
        } else {
            // New class descriptor
            self.buffer.put_u8(tc::CLASSDESC);
            
            // Class name
            self.buffer.put_u16(class_name.len() as u16);
            self.buffer.put_slice(class_name.as_bytes());
            
            // Serial version UID for byte[]
            self.buffer.put_i64(-5984413125824719648i64); // serialVersionUID for [B
            
            // Assign handle
            let handle = self.new_handle();
            self.class_handles.insert(class_name.to_string(), handle);
            
            // Class descriptor flags: SC_SERIALIZABLE
            self.buffer.put_u8(sc::SERIALIZABLE);
            
            // Number of fields: 0 (array elements are not fields)
            self.buffer.put_u16(0);
            
            // Class annotation: TC_ENDBLOCKDATA
            self.buffer.put_u8(tc::ENDBLOCKDATA);
            
            // Super class descriptor: null
            self.buffer.put_u8(tc::NULL);
        }
    }

    /// Write a TransportMessage object.
    ///
    /// TransportMessage has:
    /// - int messageTypeValue
    /// - byte[] payload
    pub fn write_transport_message(&mut self, message_type: i32, payload: &[u8]) {
        // TC_OBJECT
        self.buffer.put_u8(tc::OBJECT);
        
        // Write class descriptor
        self.write_transport_message_class_desc();
        
        // Object gets a handle after class descriptor
        self.new_handle();
        
        // Write field values in declaration order:
        // 1. messageTypeValue (int)
        self.buffer.put_i32(message_type);
        
        // 2. payload (byte[])
        if payload.is_empty() {
            self.write_null();
        } else {
            self.write_byte_array(payload);
        }
    }

    /// Write TransportMessage class descriptor.
    fn write_transport_message_class_desc(&mut self) {
        let class_name = "org.apache.celeborn.common.network.protocol.TransportMessage";
        
        if let Some(&handle) = self.class_handles.get(class_name) {
            // Reference to existing class descriptor
            self.buffer.put_u8(tc::REFERENCE);
            self.buffer.put_u32(handle);
        } else {
            // New class descriptor
            self.buffer.put_u8(tc::CLASSDESC);
            
            // Class name
            self.buffer.put_u16(class_name.len() as u16);
            self.buffer.put_slice(class_name.as_bytes());
            
            // Serial version UID: -3259000920699629773L
            self.buffer.put_i64(-3259000920699629773i64);
            
            // Assign handle
            let handle = self.new_handle();
            self.class_handles.insert(class_name.to_string(), handle);
            
            // Class descriptor flags: SC_SERIALIZABLE
            self.buffer.put_u8(sc::SERIALIZABLE);
            
            // Number of fields: 2 (messageTypeValue, payload)
            self.buffer.put_u16(2);
            
            // Field 1: int messageTypeValue
            self.buffer.put_u8(prim::INTEGER); // type code 'I'
            self.buffer.put_u16(16); // field name length
            self.buffer.put_slice(b"messageTypeValue");
            
            // Field 2: byte[] payload
            self.buffer.put_u8(prim::ARRAY); // type code '['
            self.buffer.put_u16(7); // field name length
            self.buffer.put_slice(b"payload");
            // Array type string: [B
            self.buffer.put_u8(tc::STRING);
            self.buffer.put_u16(2);
            self.buffer.put_slice(b"[B");
            self.new_handle(); // String gets a handle
            
            // Class annotation: TC_ENDBLOCKDATA
            self.buffer.put_u8(tc::ENDBLOCKDATA);
            
            // Super class descriptor: null
            self.buffer.put_u8(tc::NULL);
        }
    }
}

impl Default for JavaObjectOutputStream {
    fn default() -> Self {
        Self::new()
    }
}

/// Calculate the length of a string in modified UTF-8 encoding.
fn modified_utf8_len(s: &str) -> usize {
    let mut len = 0;
    for c in s.chars() {
        let code = c as u32;
        if code == 0 {
            len += 2; // Null is encoded as 0xC0 0x80
        } else if code <= 0x7F {
            len += 1;
        } else if code <= 0x7FF {
            len += 2;
        } else if code <= 0xFFFF {
            len += 3;
        } else {
            // Supplementary characters are encoded as surrogate pairs
            len += 6;
        }
    }
    len
}

/// Write a string in modified UTF-8 encoding.
fn write_modified_utf8(buf: &mut BytesMut, s: &str) {
    for c in s.chars() {
        let code = c as u32;
        if code == 0 {
            // Null is encoded as 0xC0 0x80
            buf.put_u8(0xC0);
            buf.put_u8(0x80);
        } else if code <= 0x7F {
            buf.put_u8(code as u8);
        } else if code <= 0x7FF {
            buf.put_u8((0xC0 | (code >> 6)) as u8);
            buf.put_u8((0x80 | (code & 0x3F)) as u8);
        } else if code <= 0xFFFF {
            buf.put_u8((0xE0 | (code >> 12)) as u8);
            buf.put_u8((0x80 | ((code >> 6) & 0x3F)) as u8);
            buf.put_u8((0x80 | (code & 0x3F)) as u8);
        } else {
            // Supplementary characters: encode as surrogate pair
            let high = ((code - 0x10000) >> 10) + 0xD800;
            let low = ((code - 0x10000) & 0x3FF) + 0xDC00;
            // High surrogate
            buf.put_u8((0xE0 | (high >> 12)) as u8);
            buf.put_u8((0x80 | ((high >> 6) & 0x3F)) as u8);
            buf.put_u8((0x80 | (high & 0x3F)) as u8);
            // Low surrogate
            buf.put_u8((0xE0 | (low >> 12)) as u8);
            buf.put_u8((0x80 | ((low >> 6) & 0x3F)) as u8);
            buf.put_u8((0x80 | (low & 0x3F)) as u8);
        }
    }
}

/// RPC address for Celeborn communication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcAddress {
    pub host: String,
    pub port: i32,
}

impl RpcAddress {
    pub fn new(host: impl Into<String>, port: i32) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }
}

/// Encode a RequestMessage for Celeborn Master RPC.
///
/// Format:
/// 1. senderAddress: boolean + (UTF host + int port)?
/// 2. receiverAddress: boolean + (UTF host + int port)?
/// 3. receiverName: UTF string (endpoint name)
/// 4. content: Java serialized TransportMessage
pub fn encode_request_message(
    sender_address: Option<&RpcAddress>,
    receiver_address: Option<&RpcAddress>,
    receiver_name: &str,
    message_type: i32,
    payload: &[u8],
) -> Bytes {
    let mut buf = BytesMut::with_capacity(256 + payload.len());
    
    // 1. Write sender address
    write_rpc_address(&mut buf, sender_address);
    
    // 2. Write receiver address
    write_rpc_address(&mut buf, receiver_address);
    
    // 3. Write receiver name (DataOutputStream.writeUTF format)
    write_data_output_utf(&mut buf, receiver_name);
    
    // 4. Write Java serialized TransportMessage
    let mut java_stream = JavaObjectOutputStream::new();
    java_stream.write_transport_message(message_type, payload);
    buf.extend_from_slice(&java_stream.into_bytes());
    
    buf.freeze()
}

/// Write an RPC address in DataOutputStream format.
fn write_rpc_address(buf: &mut BytesMut, addr: Option<&RpcAddress>) {
    match addr {
        None => {
            buf.put_u8(0); // false
        }
        Some(addr) => {
            buf.put_u8(1); // true
            write_data_output_utf(buf, &addr.host);
            buf.put_i32(addr.port);
        }
    }
}

/// Write a string in DataOutputStream.writeUTF format.
/// Format: 2-byte length (modified UTF-8 length) + modified UTF-8 bytes
fn write_data_output_utf(buf: &mut BytesMut, s: &str) {
    let utf_len = modified_utf8_len(s);
    buf.put_u16(utf_len as u16);
    write_modified_utf8(buf, s);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_java_stream_header() {
        let stream = JavaObjectOutputStream::new();
        let bytes = stream.into_bytes();
        
        // Check magic and version
        assert_eq!(bytes[0], 0xAC);
        assert_eq!(bytes[1], 0xED);
        assert_eq!(bytes[2], 0x00);
        assert_eq!(bytes[3], 0x05);
    }

    #[test]
    fn test_write_transport_message() {
        let mut stream = JavaObjectOutputStream::new();
        stream.write_transport_message(4, b"test payload");
        let bytes = stream.into_bytes();
        
        // Should start with magic + version
        assert_eq!(&bytes[0..4], &[0xAC, 0xED, 0x00, 0x05]);
        
        // Should have TC_OBJECT
        assert_eq!(bytes[4], tc::OBJECT);
        
        // Should have TC_CLASSDESC
        assert_eq!(bytes[5], tc::CLASSDESC);
    }

    #[test]
    fn test_encode_request_message() {
        let sender = RpcAddress::new("localhost", 12345);
        let receiver = RpcAddress::new("master-host", 9097);
        
        let payload = b"test";
        let bytes = encode_request_message(
            Some(&sender),
            Some(&receiver),
            "MasterEndpoint",
            4, // REGISTER_SHUFFLE
            payload,
        );
        
        // Should start with sender address (true + host + port)
        assert_eq!(bytes[0], 1); // has address
        
        // Verify it's not empty
        assert!(bytes.len() > 50);
    }

    #[test]
    fn test_modified_utf8_len() {
        assert_eq!(modified_utf8_len("hello"), 5);
        assert_eq!(modified_utf8_len(""), 0);
        assert_eq!(modified_utf8_len("中文"), 6); // 2 chars * 3 bytes each
    }

    #[test]
    fn test_write_data_output_utf() {
        let mut buf = BytesMut::new();
        write_data_output_utf(&mut buf, "hello");
        
        // Length prefix (2 bytes) + content
        assert_eq!(buf.len(), 2 + 5);
        assert_eq!(&buf[0..2], &[0, 5]); // length = 5
        assert_eq!(&buf[2..], b"hello");
    }
}
