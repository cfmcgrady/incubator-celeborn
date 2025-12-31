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

//! Hash-based shuffle repartitioner implementation.
//!
//! This module provides a hash-based repartitioner that partitions data
//! using hash values and pushes to Celeborn workers.

use crate::client::ExecutorShuffleClient;
use crate::error::Result;
use crate::repartitioner::ShuffleRepartitioner;
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::Arc;

/// Configuration for the repartitioner.
#[derive(Debug, Clone)]
pub struct RepartitionerConfig {
    /// Shuffle ID
    pub shuffle_id: i32,
    /// Map ID (task partition)
    pub map_id: i32,
    /// Attempt ID
    pub attempt_id: i32,
    /// Number of mappers
    pub num_mappers: i32,
    /// Number of output partitions
    pub num_partitions: usize,
    /// Batch size for processing
    pub batch_size: usize,
    /// Buffer size threshold for pushing to Celeborn (default: 4MB)
    pub push_buffer_size: usize,
}

impl Default for RepartitionerConfig {
    fn default() -> Self {
        Self {
            shuffle_id: 0,
            map_id: 0,
            attempt_id: 0,
            num_mappers: 1,
            num_partitions: 1,
            batch_size: 4096,
            push_buffer_size: 4 * 1024 * 1024, // 4MB
        }
    }
}

/// Scratch space for computing partition indices.
///
/// This is used to avoid repeated allocations during partitioning.
#[derive(Default)]
pub struct ScratchSpace {
    /// Buffer for hash values
    pub hashes_buf: Vec<u32>,
    /// Partition IDs for each row
    pub partition_ids: Vec<u32>,
    /// Row indices sorted by partition
    pub partition_row_indices: Vec<u32>,
    /// Start index for each partition in partition_row_indices
    pub partition_starts: Vec<u32>,
}

impl ScratchSpace {
    /// Create a new scratch space with the given capacity.
    pub fn new(batch_size: usize, num_partitions: usize) -> Self {
        Self {
            hashes_buf: vec![0; batch_size],
            partition_ids: vec![0; batch_size],
            partition_row_indices: vec![0; batch_size],
            partition_starts: vec![0; num_partitions + 1],
        }
    }

    /// Resize the scratch space for a new batch size.
    pub fn resize(&mut self, batch_size: usize, num_partitions: usize) {
        if self.hashes_buf.len() < batch_size {
            self.hashes_buf.resize(batch_size, 0);
            self.partition_ids.resize(batch_size, 0);
            self.partition_row_indices.resize(batch_size, 0);
        }
        if self.partition_starts.len() < num_partitions + 1 {
            self.partition_starts.resize(num_partitions + 1, 0);
        }
    }
}

/// Hash-based shuffle repartitioner.
///
/// This repartitioner:
/// 1. Accepts pre-computed partition IDs for each row
/// 2. Buffers data for each partition
/// 3. Pushes to Celeborn when buffer exceeds threshold
pub struct HashRepartitioner {
    /// Celeborn client
    client: Arc<ExecutorShuffleClient>,
    /// Configuration
    config: RepartitionerConfig,
    /// Partition buffers for accumulating data before push
    partition_buffers: DashMap<i32, Vec<u8>>,
    /// Total bytes pushed
    bytes_pushed: usize,
}

impl HashRepartitioner {
    /// Create a new hash repartitioner.
    pub fn new(client: Arc<ExecutorShuffleClient>, config: RepartitionerConfig) -> Self {
        Self {
            client,
            config,
            partition_buffers: DashMap::new(),
            bytes_pushed: 0,
        }
    }

    /// Push data for a specific partition to Celeborn.
    async fn push_partition_data(&self, partition_id: i32, data: &[u8]) -> Result<()> {
        self.client
            .push_data(
                self.config.shuffle_id,
                self.config.map_id,
                self.config.attempt_id,
                partition_id,
                data,
            )
            .await
    }

    /// Flush a partition buffer if it exceeds the threshold.
    async fn maybe_flush_partition(&mut self, partition_id: i32) -> Result<()> {
        let should_flush = self
            .partition_buffers
            .get(&partition_id)
            .map(|buf| buf.len() >= self.config.push_buffer_size)
            .unwrap_or(false);

        if should_flush {
            if let Some((_, buffer)) = self.partition_buffers.remove(&partition_id) {
                self.push_partition_data(partition_id, &buffer).await?;
                self.bytes_pushed += buffer.len();
            }
        }
        Ok(())
    }

    /// Flush all partition buffers.
    async fn flush_all(&mut self) -> Result<()> {
        let partitions: Vec<i32> = self.partition_buffers.iter().map(|r| *r.key()).collect();
        for partition_id in partitions {
            if let Some((_, buffer)) = self.partition_buffers.remove(&partition_id) {
                if !buffer.is_empty() {
                    self.push_partition_data(partition_id, &buffer).await?;
                    self.bytes_pushed += buffer.len();
                }
            }
        }
        Ok(())
    }

    /// Get total bytes pushed to Celeborn.
    pub fn bytes_pushed(&self) -> usize {
        self.bytes_pushed
    }
}

#[async_trait]
impl ShuffleRepartitioner for HashRepartitioner {
    async fn insert_batch(&mut self, partition_ids: &[u32], data: &[u8]) -> Result<()> {
        // For simplicity, this implementation assumes data is already partitioned
        // and partition_ids contains a single partition ID for the entire batch.
        // More sophisticated implementations can handle row-level partitioning.
        
        if partition_ids.is_empty() || data.is_empty() {
            return Ok(());
        }

        // Get the partition ID (assuming all rows go to the same partition)
        let partition_id = partition_ids[0] as i32;

        // Append to partition buffer
        self.partition_buffers
            .entry(partition_id)
            .or_insert_with(Vec::new)
            .extend_from_slice(data);

        // Check if we need to flush
        self.maybe_flush_partition(partition_id).await?;

        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        // Flush all remaining data
        self.flush_all().await?;

        // Signal mapper end to Celeborn
        self.client
            .mapper_end(
                self.config.shuffle_id,
                self.config.map_id,
                self.config.attempt_id,
                self.config.num_mappers,
            )
            .await?;

        Ok(())
    }

    fn num_partitions(&self) -> usize {
        self.config.num_partitions
    }

    fn shuffle_id(&self) -> i32 {
        self.config.shuffle_id
    }

    fn map_id(&self) -> i32 {
        self.config.map_id
    }
}

/// Map partition IDs to partition starts and row indices.
///
/// This function takes an array of partition IDs and produces:
/// 1. `partition_starts` - Start index for each partition in the sorted row indices
/// 2. `partition_row_indices` - Row indices sorted by partition
///
/// This is useful for efficiently grouping rows by partition before pushing to Celeborn.
///
/// # Arguments
/// * `scratch` - Scratch space containing partition_ids and output arrays
/// * `num_output_partitions` - Number of output partitions
/// * `num_rows` - Number of rows to process
pub fn map_partition_ids_to_starts_and_indices(
    scratch: &mut ScratchSpace,
    num_output_partitions: usize,
    num_rows: usize,
) {
    let partition_ids = &mut scratch.partition_ids[..num_rows];

    let partition_counters = &mut scratch.partition_starts;
    partition_counters.resize(num_output_partitions + 1, 0);
    partition_counters.fill(0);
    partition_ids
        .iter()
        .for_each(|partition_id| partition_counters[*partition_id as usize] += 1);

    let partition_ends = partition_counters;
    let mut accum = 0;
    partition_ends.iter_mut().for_each(|v| {
        *v += accum;
        accum = *v;
    });

    let partition_row_indices = &mut scratch.partition_row_indices;
    partition_row_indices.resize(num_rows, 0);
    for (index, partition_id) in partition_ids.iter().enumerate().rev() {
        partition_ends[*partition_id as usize] -= 1;
        let end = partition_ends[*partition_id as usize];
        partition_row_indices[end as usize] = index as u32;
    }
}

/// Compute partition ID using positive modulo (same as Spark).
///
/// This ensures the result is always non-negative, matching Spark's behavior.
///
/// # Arguments
/// * `hash` - Hash value
/// * `n` - Number of partitions
///
/// # Returns
/// Partition ID in range [0, n)
#[inline]
pub fn pmod(hash: u32, n: usize) -> usize {
    let h = hash as i32;
    let n = n as i32;
    ((h % n + n) % n) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pmod() {
        assert_eq!(pmod(0, 10), 0);
        assert_eq!(pmod(5, 10), 5);
        assert_eq!(pmod(10, 10), 0);
        assert_eq!(pmod(15, 10), 5);
        // Test negative hash values (when cast to i32)
        // u32::MAX as i32 = -1, (-1 % 10 + 10) % 10 = 9
        assert_eq!(pmod(u32::MAX, 10), 9);
    }

    #[test]
    fn test_scratch_space() {
        let scratch = ScratchSpace::new(100, 10);
        assert_eq!(scratch.hashes_buf.len(), 100);
        assert_eq!(scratch.partition_ids.len(), 100);
        assert_eq!(scratch.partition_row_indices.len(), 100);
        assert_eq!(scratch.partition_starts.len(), 11);
    }

    #[test]
    fn test_map_partition_ids_to_starts_and_indices() {
        let mut scratch = ScratchSpace::new(6, 3);
        // Partition IDs: [0, 1, 2, 0, 1, 2]
        scratch.partition_ids[0] = 0;
        scratch.partition_ids[1] = 1;
        scratch.partition_ids[2] = 2;
        scratch.partition_ids[3] = 0;
        scratch.partition_ids[4] = 1;
        scratch.partition_ids[5] = 2;

        map_partition_ids_to_starts_and_indices(&mut scratch, 3, 6);

        // partition_starts should be [0, 2, 4, 6]
        assert_eq!(scratch.partition_starts[0], 0);
        assert_eq!(scratch.partition_starts[1], 2);
        assert_eq!(scratch.partition_starts[2], 4);
        assert_eq!(scratch.partition_starts[3], 6);

        // partition_row_indices should group rows by partition
        // Partition 0: rows 0, 3
        // Partition 1: rows 1, 4
        // Partition 2: rows 2, 5
        let p0_rows: Vec<u32> = scratch.partition_row_indices[0..2].to_vec();
        let p1_rows: Vec<u32> = scratch.partition_row_indices[2..4].to_vec();
        let p2_rows: Vec<u32> = scratch.partition_row_indices[4..6].to_vec();

        assert!(p0_rows.contains(&0) && p0_rows.contains(&3));
        assert!(p1_rows.contains(&1) && p1_rows.contains(&4));
        assert!(p2_rows.contains(&2) && p2_rows.contains(&5));
    }

    #[test]
    fn test_repartitioner_config_default() {
        let config = RepartitionerConfig::default();
        assert_eq!(config.shuffle_id, 0);
        assert_eq!(config.num_partitions, 1);
        assert_eq!(config.batch_size, 4096);
        assert_eq!(config.push_buffer_size, 4 * 1024 * 1024);
    }
}
