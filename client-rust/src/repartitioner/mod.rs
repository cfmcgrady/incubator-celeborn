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

//! Shuffle Repartitioner Module
//!
//! This module provides high-level APIs for repartitioning data and pushing to Celeborn workers.
//! It is designed to be used by query engines like Apache DataFusion, Apache Comet, etc.
//!
//! ## Components
//!
//! - [`ShuffleRepartitioner`] - Trait defining the repartitioner interface
//! - [`HashRepartitioner`] - Hash-based repartitioner implementation
//! - [`ClientManager`] - Connection pool manager for reusing Celeborn clients
//!
//! ## Example
//!
//! ```rust,ignore
//! use celeborn_client::repartitioner::{HashRepartitioner, RepartitionerConfig};
//! use celeborn_client::{ExecutorShuffleClient, CelebornConfig};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = CelebornConfig::builder()
//!         .master_endpoints(vec!["localhost:9097".to_string()])
//!         .app_id("my-app")
//!         .build()?;
//!
//!     let client = Arc::new(ExecutorShuffleClient::new(config));
//!     client.setup_lifecycle_manager_ref("driver-host", 9098).await?;
//!
//!     let repartitioner_config = RepartitionerConfig {
//!         shuffle_id: 0,
//!         map_id: 0,
//!         attempt_id: 0,
//!         num_mappers: 1,
//!         num_partitions: 10,
//!         batch_size: 4096,
//!         push_buffer_size: 4 * 1024 * 1024,
//!     };
//!
//!     let repartitioner = HashRepartitioner::new(client, repartitioner_config);
//!     // Use repartitioner to partition and push data...
//!     Ok(())
//! }
//! ```

mod hash_repartitioner;
mod client_manager;

pub use hash_repartitioner::{
    HashRepartitioner, RepartitionerConfig, ScratchSpace,
    map_partition_ids_to_starts_and_indices, pmod,
};
pub use client_manager::ClientManager;

use crate::error::Result;
use async_trait::async_trait;

/// Trait defining the shuffle repartitioner interface.
///
/// Implementations of this trait are responsible for:
/// 1. Partitioning input data based on partition keys
/// 2. Buffering data for each partition
/// 3. Pushing data to Celeborn workers when buffer is full or on finish
#[async_trait]
pub trait ShuffleRepartitioner: Send + Sync {
    /// Insert a batch of data to be repartitioned.
    ///
    /// The implementation should:
    /// 1. Compute partition IDs for each row
    /// 2. Buffer data for each partition
    /// 3. Push to Celeborn when buffer exceeds threshold
    ///
    /// # Arguments
    /// * `partition_ids` - Partition ID for each row
    /// * `data` - Serialized data for each row (e.g., IPC format)
    async fn insert_batch(&mut self, partition_ids: &[u32], data: &[u8]) -> Result<()>;

    /// Finish repartitioning and flush all remaining data.
    ///
    /// This should:
    /// 1. Push any remaining buffered data
    /// 2. Signal mapper end to Celeborn
    async fn finish(&mut self) -> Result<()>;

    /// Get the number of output partitions.
    fn num_partitions(&self) -> usize;

    /// Get the shuffle ID.
    fn shuffle_id(&self) -> i32;

    /// Get the map ID.
    fn map_id(&self) -> i32;
}
