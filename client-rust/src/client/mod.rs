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

//! Celeborn client implementations.

pub mod lifecycle;
pub mod shuffle;
pub mod push;
pub mod fetch;
pub mod revive;
pub mod partition_split;
pub mod partition_reader;
pub mod input_stream;

pub use lifecycle::LifecycleManager;
pub use shuffle::ShuffleClient;
pub use revive::{ReviveManager, ReviveRequest, ReviveResult};
pub use partition_split::{
    PartitionLocationManager, PartitionLocationWithSplit, SplitHandler,
    SplitRange, SplitStatus, ChangePartitionRequest
};
pub use partition_reader::{
    PartitionReader, WorkerPartitionReader, WorkerPartitionReaderBuilder,
    WorkerPartitionReaderConfig
};
pub use input_stream::{
    CelebornInputStream, CelebornInputStreamBuilder, CelebornInputStreamConfig,
    MetricsCallback, NoOpMetricsCallback, AsyncCelebornInputStream,
    PushFailedBatch, ChunkRange, CelebornAsyncReader
};

use std::sync::Arc;

use crate::config::CelebornConfig;
use crate::error::Result;
use crate::network::TransportClient;

/// Main Celeborn client that provides high-level APIs.
pub struct CelebornClient {
    /// Configuration
    config: Arc<CelebornConfig>,
    /// Lifecycle manager
    lifecycle_manager: Arc<LifecycleManager>,
    /// Shuffle client
    shuffle_client: Arc<ShuffleClient>,
}

impl CelebornClient {
    /// Create a new Celeborn client.
    pub async fn new(config: CelebornConfig) -> Result<Self> {
        let config = Arc::new(config);
        
        // Create transport client
        let transport_client = Arc::new(TransportClient::new(config.clone())?);
        
        // Create lifecycle manager
        let lifecycle_manager = Arc::new(LifecycleManager::new(
            config.clone(),
            transport_client.clone(),
        ));
        
        // Start heartbeat
        lifecycle_manager.start_heartbeat().await;
        
        // Create shuffle client
        let shuffle_client = Arc::new(ShuffleClient::new(
            config.clone(),
            transport_client,
            lifecycle_manager.clone(),
        ));

        Ok(Self {
            config,
            lifecycle_manager,
            shuffle_client,
        })
    }

    /// Get the application ID.
    pub fn app_id(&self) -> &str {
        &self.config.app_id
    }

    /// Register a new shuffle.
    ///
    /// # Arguments
    /// * `shuffle_id` - The shuffle ID
    /// * `num_mappers` - Number of map tasks
    /// * `num_partitions` - Number of reduce partitions
    ///
    /// # Returns
    /// The registered shuffle ID
    pub async fn register_shuffle(
        &self,
        shuffle_id: i32,
        num_mappers: i32,
        num_partitions: i32,
    ) -> Result<i32> {
        self.lifecycle_manager
            .register_shuffle(shuffle_id, num_mappers, num_partitions)
            .await
    }

    /// Push shuffle data.
    ///
    /// # Arguments
    /// * `shuffle_id` - The shuffle ID
    /// * `map_id` - The map task ID
    /// * `attempt_id` - The attempt ID
    /// * `partition_id` - The target partition ID
    /// * `data` - The data to push
    pub async fn push_data(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        partition_id: i32,
        data: &[u8],
    ) -> Result<()> {
        self.shuffle_client
            .push_data(shuffle_id, map_id, attempt_id, partition_id, data)
            .await
    }

    /// Signal that a mapper has finished.
    ///
    /// # Arguments
    /// * `shuffle_id` - The shuffle ID
    /// * `map_id` - The map task ID
    /// * `attempt_id` - The attempt ID
    /// * `num_mappers` - Total number of mappers
    ///
    /// # Returns
    /// `true` if this is the first successful attempt for this mapper
    pub async fn mapper_end(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
    ) -> Result<bool> {
        self.lifecycle_manager
            .mapper_end(shuffle_id, map_id, attempt_id, num_mappers)
            .await
    }

    /// Get the file groups for a reducer.
    ///
    /// # Arguments
    /// * `shuffle_id` - The shuffle ID
    ///
    /// # Returns
    /// Map of partition ID to partition locations
    pub async fn get_reducer_file_group(
        &self,
        shuffle_id: i32,
    ) -> Result<std::collections::HashMap<i32, Vec<crate::protocol::PartitionLocation>>> {
        self.lifecycle_manager
            .get_reducer_file_group(shuffle_id)
            .await
    }

    /// Fetch shuffle data for a partition.
    ///
    /// # Arguments
    /// * `shuffle_id` - The shuffle ID
    /// * `partition_id` - The partition ID to fetch
    ///
    /// # Returns
    /// Iterator over the fetched data chunks
    pub async fn fetch_data(
        &self,
        shuffle_id: i32,
        partition_id: i32,
    ) -> Result<fetch::ShuffleDataIterator> {
        self.shuffle_client
            .fetch_data(shuffle_id, partition_id)
            .await
    }

    /// Unregister a shuffle.
    ///
    /// # Arguments
    /// * `shuffle_id` - The shuffle ID to unregister
    pub async fn unregister_shuffle(&self, shuffle_id: i32) -> Result<()> {
        self.lifecycle_manager.unregister_shuffle(shuffle_id).await
    }

    /// Stop the client and release resources.
    pub async fn stop(&self) -> Result<()> {
        self.lifecycle_manager.stop().await;
        Ok(())
    }

    /// Get the lifecycle manager.
    pub fn lifecycle_manager(&self) -> &Arc<LifecycleManager> {
        &self.lifecycle_manager
    }

    /// Get the shuffle client.
    pub fn shuffle_client(&self) -> &Arc<ShuffleClient> {
        &self.shuffle_client
    }
}

impl Drop for CelebornClient {
    fn drop(&mut self) {
        // Note: async cleanup should be done via stop() before dropping
    }
}
