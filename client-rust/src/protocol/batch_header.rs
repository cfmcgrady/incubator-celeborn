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

//! Batch Header for Celeborn Shuffle Data
//!
//! This module provides utilities for parsing and encoding batch headers
//! used in Celeborn shuffle data format.
//!
//! ## Batch Header Format
//!
//! Each batch of shuffle data is prefixed with a 16-byte header:
//!
//! ```text
//! +----------+----------+----------+----------------+
//! | mapId    | attemptId| batchId  | compressedSize |
//! | (4 bytes)| (4 bytes)| (4 bytes)| (4 bytes)      |
//! | LE       | LE       | LE       | LE             |
//! +----------+----------+----------+----------------+
//! ```
//!
//! Note: LE = Little Endian
//!
//! ## Example
//!
//! ```rust
//! use celeborn_client::protocol::BatchHeader;
//!
//! // Parse a batch header from bytes
//! let data = [0u8; 20]; // 16-byte header + 4 bytes of data
//! if let Some(header) = BatchHeader::parse(&data) {
//!     println!("Map ID: {}", header.map_id);
//!     println!("Data size: {}", header.compressed_size);
//! }
//!
//! // Encode a batch header
//! let header = BatchHeader {
//!     map_id: 0,
//!     attempt_id: 0,
//!     batch_id: 1,
//!     compressed_size: 1024,
//! };
//! let encoded = header.encode();
//! assert_eq!(encoded.len(), 16);
//! ```

/// Batch header for Celeborn shuffle data.
///
/// Each batch pushed to or fetched from Celeborn workers includes this header
/// to identify the source mapper and batch metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchHeader {
    /// Map task ID that produced this batch
    pub map_id: i32,
    /// Attempt ID of the map task
    pub attempt_id: i32,
    /// Batch ID within the map task (incremental)
    pub batch_id: i32,
    /// Size of the compressed data following this header
    pub compressed_size: i32,
}

impl BatchHeader {
    /// Size of the batch header in bytes
    pub const SIZE: usize = 16;

    /// Parse a batch header from a byte slice.
    ///
    /// Returns `None` if the slice is too short (less than 16 bytes).
    ///
    /// # Arguments
    /// * `data` - Byte slice containing at least 16 bytes
    ///
    /// # Returns
    /// Parsed `BatchHeader` or `None` if data is insufficient
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < Self::SIZE {
            return None;
        }

        Some(Self {
            map_id: i32::from_le_bytes([data[0], data[1], data[2], data[3]]),
            attempt_id: i32::from_le_bytes([data[4], data[5], data[6], data[7]]),
            batch_id: i32::from_le_bytes([data[8], data[9], data[10], data[11]]),
            compressed_size: i32::from_le_bytes([data[12], data[13], data[14], data[15]]),
        })
    }

    /// Parse a batch header using big-endian byte order.
    ///
    /// Some Celeborn versions or configurations may use big-endian format.
    ///
    /// # Arguments
    /// * `data` - Byte slice containing at least 16 bytes
    ///
    /// # Returns
    /// Parsed `BatchHeader` or `None` if data is insufficient
    pub fn parse_be(data: &[u8]) -> Option<Self> {
        if data.len() < Self::SIZE {
            return None;
        }

        Some(Self {
            map_id: i32::from_be_bytes([data[0], data[1], data[2], data[3]]),
            attempt_id: i32::from_be_bytes([data[4], data[5], data[6], data[7]]),
            batch_id: i32::from_be_bytes([data[8], data[9], data[10], data[11]]),
            compressed_size: i32::from_be_bytes([data[12], data[13], data[14], data[15]]),
        })
    }

    /// Encode the batch header to bytes (little-endian).
    ///
    /// # Returns
    /// 16-byte array containing the encoded header
    pub fn encode(&self) -> [u8; Self::SIZE] {
        let mut result = [0u8; Self::SIZE];
        result[0..4].copy_from_slice(&self.map_id.to_le_bytes());
        result[4..8].copy_from_slice(&self.attempt_id.to_le_bytes());
        result[8..12].copy_from_slice(&self.batch_id.to_le_bytes());
        result[12..16].copy_from_slice(&self.compressed_size.to_le_bytes());
        result
    }

    /// Encode the batch header to bytes (big-endian).
    ///
    /// # Returns
    /// 16-byte array containing the encoded header
    pub fn encode_be(&self) -> [u8; Self::SIZE] {
        let mut result = [0u8; Self::SIZE];
        result[0..4].copy_from_slice(&self.map_id.to_be_bytes());
        result[4..8].copy_from_slice(&self.attempt_id.to_be_bytes());
        result[8..12].copy_from_slice(&self.batch_id.to_be_bytes());
        result[12..16].copy_from_slice(&self.compressed_size.to_be_bytes());
        result
    }

    /// Check if this batch should be included based on attempt filtering.
    ///
    /// In Celeborn, when multiple attempts of the same map task exist,
    /// only one attempt's data should be used. This method helps filter
    /// batches based on the expected attempt ID.
    ///
    /// # Arguments
    /// * `expected_attempt` - The attempt ID to accept
    ///
    /// # Returns
    /// `true` if this batch's attempt matches the expected attempt
    pub fn matches_attempt(&self, expected_attempt: i32) -> bool {
        self.attempt_id == expected_attempt
    }
}

/// Iterator over batches in a Celeborn data stream.
///
/// This iterator parses batch headers and yields (header, data) pairs.
///
/// # Example
///
/// ```rust
/// use celeborn_client::protocol::{BatchHeader, BatchIterator};
///
/// let data = vec![0u8; 100]; // Some Celeborn data
/// let mut iter = BatchIterator::new(&data);
///
/// while let Some((header, batch_data)) = iter.next() {
///     println!("Batch {} from map {}: {} bytes",
///         header.batch_id, header.map_id, batch_data.len());
/// }
/// ```
pub struct BatchIterator<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> BatchIterator<'a> {
    /// Create a new batch iterator over the given data.
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    /// Get the current offset in the data.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Get the remaining bytes in the data.
    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.offset)
    }
}

impl<'a> Iterator for BatchIterator<'a> {
    type Item = (BatchHeader, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset + BatchHeader::SIZE > self.data.len() {
            return None;
        }

        let header = BatchHeader::parse(&self.data[self.offset..])?;

        if header.compressed_size < 0 {
            return None;
        }

        let data_start = self.offset + BatchHeader::SIZE;
        let data_end = data_start + header.compressed_size as usize;

        if data_end > self.data.len() {
            return None;
        }

        let batch_data = &self.data[data_start..data_end];
        self.offset = data_end;

        Some((header, batch_data))
    }
}

/// Filter batches by attempt ID.
///
/// This is useful when reading shuffle data that may contain multiple
/// attempts of the same map task.
pub struct AttemptFilteredBatchIterator<'a> {
    inner: BatchIterator<'a>,
    /// Map from map_id to expected attempt_id
    attempts: &'a [i32],
}

impl<'a> AttemptFilteredBatchIterator<'a> {
    /// Create a new filtered batch iterator.
    ///
    /// # Arguments
    /// * `data` - The raw Celeborn data
    /// * `attempts` - Array where `attempts[map_id]` is the expected attempt ID for that mapper
    pub fn new(data: &'a [u8], attempts: &'a [i32]) -> Self {
        Self {
            inner: BatchIterator::new(data),
            attempts,
        }
    }
}

impl<'a> Iterator for AttemptFilteredBatchIterator<'a> {
    type Item = (BatchHeader, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (header, data) = self.inner.next()?;

            // Check if this batch's attempt matches the expected attempt
            let map_id = header.map_id as usize;
            if map_id < self.attempts.len() {
                if header.attempt_id == self.attempts[map_id] {
                    return Some((header, data));
                }
                // Skip this batch - wrong attempt
                continue;
            }

            // If map_id is out of range, include the batch (no filtering)
            return Some((header, data));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_header_parse() {
        let mut data = [0u8; 16];
        // map_id = 1 (little-endian)
        data[0..4].copy_from_slice(&1i32.to_le_bytes());
        // attempt_id = 2
        data[4..8].copy_from_slice(&2i32.to_le_bytes());
        // batch_id = 3
        data[8..12].copy_from_slice(&3i32.to_le_bytes());
        // compressed_size = 100
        data[12..16].copy_from_slice(&100i32.to_le_bytes());

        let header = BatchHeader::parse(&data).unwrap();
        assert_eq!(header.map_id, 1);
        assert_eq!(header.attempt_id, 2);
        assert_eq!(header.batch_id, 3);
        assert_eq!(header.compressed_size, 100);
    }

    #[test]
    fn test_batch_header_parse_insufficient_data() {
        let data = [0u8; 10]; // Less than 16 bytes
        assert!(BatchHeader::parse(&data).is_none());
    }

    #[test]
    fn test_batch_header_encode_decode() {
        let header = BatchHeader {
            map_id: 42,
            attempt_id: 1,
            batch_id: 5,
            compressed_size: 1024,
        };

        let encoded = header.encode();
        let decoded = BatchHeader::parse(&encoded).unwrap();

        assert_eq!(header, decoded);
    }

    #[test]
    fn test_batch_header_big_endian() {
        let header = BatchHeader {
            map_id: 42,
            attempt_id: 1,
            batch_id: 5,
            compressed_size: 1024,
        };

        let encoded = header.encode_be();
        let decoded = BatchHeader::parse_be(&encoded).unwrap();

        assert_eq!(header, decoded);
    }

    #[test]
    fn test_batch_iterator() {
        // Create test data with 2 batches
        let mut data = Vec::new();

        // Batch 1: map_id=0, attempt_id=0, batch_id=0, size=4
        data.extend_from_slice(&0i32.to_le_bytes());
        data.extend_from_slice(&0i32.to_le_bytes());
        data.extend_from_slice(&0i32.to_le_bytes());
        data.extend_from_slice(&4i32.to_le_bytes());
        data.extend_from_slice(&[1, 2, 3, 4]); // 4 bytes of data

        // Batch 2: map_id=1, attempt_id=0, batch_id=0, size=2
        data.extend_from_slice(&1i32.to_le_bytes());
        data.extend_from_slice(&0i32.to_le_bytes());
        data.extend_from_slice(&0i32.to_le_bytes());
        data.extend_from_slice(&2i32.to_le_bytes());
        data.extend_from_slice(&[5, 6]); // 2 bytes of data

        let mut iter = BatchIterator::new(&data);

        let (header1, data1) = iter.next().unwrap();
        assert_eq!(header1.map_id, 0);
        assert_eq!(header1.compressed_size, 4);
        assert_eq!(data1, &[1, 2, 3, 4]);

        let (header2, data2) = iter.next().unwrap();
        assert_eq!(header2.map_id, 1);
        assert_eq!(header2.compressed_size, 2);
        assert_eq!(data2, &[5, 6]);

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_attempt_filtered_iterator() {
        // Create test data with batches from different attempts
        let mut data = Vec::new();

        // Batch from map_id=0, attempt_id=0 (should be included)
        data.extend_from_slice(&0i32.to_le_bytes());
        data.extend_from_slice(&0i32.to_le_bytes());
        data.extend_from_slice(&0i32.to_le_bytes());
        data.extend_from_slice(&2i32.to_le_bytes());
        data.extend_from_slice(&[1, 2]);

        // Batch from map_id=0, attempt_id=1 (should be filtered out)
        data.extend_from_slice(&0i32.to_le_bytes());
        data.extend_from_slice(&1i32.to_le_bytes());
        data.extend_from_slice(&1i32.to_le_bytes());
        data.extend_from_slice(&2i32.to_le_bytes());
        data.extend_from_slice(&[3, 4]);

        // Batch from map_id=1, attempt_id=0 (should be included)
        data.extend_from_slice(&1i32.to_le_bytes());
        data.extend_from_slice(&0i32.to_le_bytes());
        data.extend_from_slice(&0i32.to_le_bytes());
        data.extend_from_slice(&2i32.to_le_bytes());
        data.extend_from_slice(&[5, 6]);

        let attempts = [0, 0]; // Both mappers should use attempt 0
        let mut iter = AttemptFilteredBatchIterator::new(&data, &attempts);

        let (header1, data1) = iter.next().unwrap();
        assert_eq!(header1.map_id, 0);
        assert_eq!(header1.attempt_id, 0);
        assert_eq!(data1, &[1, 2]);

        let (header2, data2) = iter.next().unwrap();
        assert_eq!(header2.map_id, 1);
        assert_eq!(header2.attempt_id, 0);
        assert_eq!(data2, &[5, 6]);

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_matches_attempt() {
        let header = BatchHeader {
            map_id: 0,
            attempt_id: 1,
            batch_id: 0,
            compressed_size: 0,
        };

        assert!(header.matches_attempt(1));
        assert!(!header.matches_attempt(0));
        assert!(!header.matches_attempt(2));
    }
}
