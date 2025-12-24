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

//! Shuffle client for push and fetch operations.

use std::sync::Arc;

use crate::client::fetch::ShuffleDataIterator;
use crate::client::lifecycle::LifecycleManager;
use crate::client::push::DataPusher;
use crate::config::CelebornConfig;
use crate::error::{CelebornError, Result};
use crate::network::TransportClient;

/// Shuffle client for data push and fetch operations.
pub struct ShuffleClient {
    /// Configuration
    config: Arc<CelebornConfig>,
    /// Transport client
    transport_client: Arc<TransportClient>,
    /// Lifecycle manager
    lifecycle_manager: Arc<LifecycleManager>,
    /// Data pusher
    data_pusher: DataPusher,
}

impl ShuffleClient {
    /// Create a new shuffle client.
    pub fn new(
        config: Arc<CelebornConfig>,
        transport_client: Arc<TransportClient>,
        lifecycle_manager: Arc<LifecycleManager>,
    ) -> Self {
        let data_pusher = DataPusher::new(
            config.clone(),
            transport_client.clone(),
            lifecycle_manager.clone(),
        );

        Self {
            config,
            transport_client,
            lifecycle_manager,
            data_pusher,
        }
    }

    /// Push shuffle data to a partition.
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
        self.data_pusher
            .push_data(shuffle_id, map_id, attempt_id, partition_id, data)
            .await
    }

    /// Push merged data to multiple partitions.
    ///
    /// # Arguments
    /// * `shuffle_id` - The shuffle ID
    /// * `map_id` - The map task ID
    /// * `attempt_id` - The attempt ID
    /// * `partition_data` - Map of partition ID to data
    pub async fn push_merged_data(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        partition_data: &[(i32, &[u8])],
    ) -> Result<()> {
        self.data_pusher
            .push_merged_data(shuffle_id, map_id, attempt_id, partition_data)
            .await
    }

    /// Fetch shuffle data for a partition.
    ///
    /// # Arguments
    /// * `shuffle_id` - The shuffle ID
    /// * `partition_id` - The partition ID to fetch
    ///
    /// # Returns
    /// An iterator over the fetched data chunks
    pub async fn fetch_data(
        &self,
        shuffle_id: i32,
        partition_id: i32,
    ) -> Result<ShuffleDataIterator> {
        // Get file groups for the shuffle
        let file_groups = self
            .lifecycle_manager
            .get_reducer_file_group(shuffle_id)
            .await?;

        // Get locations for this partition
        let locations = file_groups.get(&partition_id).ok_or_else(|| {
            CelebornError::PartitionNotFound {
                shuffle_id,
                partition_id,
            }
        })?;

        if locations.is_empty() {
            return Err(CelebornError::PartitionNotFound {
                shuffle_id,
                partition_id,
            });
        }

        // Create iterator
        ShuffleDataIterator::new(
            self.config.clone(),
            self.transport_client.clone(),
            self.lifecycle_manager.shuffle_key(shuffle_id),
            locations.clone(),
        )
        .await
    }

    /// Flush any buffered data.
    pub async fn flush(&self) -> Result<()> {
        self.data_pusher.flush().await
    }

    /// Get the lifecycle manager.
    pub fn lifecycle_manager(&self) -> &Arc<LifecycleManager> {
        &self.lifecycle_manager
    }
}
