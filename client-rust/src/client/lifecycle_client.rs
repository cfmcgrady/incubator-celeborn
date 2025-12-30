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

//! LifecycleManager client for Driver-Executor separation.
//!
//! This module provides a client interface for Rust ShuffleClient to communicate
//! with Java LifecycleManager running in the Driver process. This enables
//! integration with compute engines like Apache Spark Comet.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     Driver (JVM)                                 │
//! │  ┌─────────────────────────────────────────────────────────────┐│
//! │  │              LifecycleManager (Scala)                       ││
//! │  │  - RegisterShuffle                                          ││
//! │  │  - Revive/PartitionSplit                                    ││
//! │  │  - MapperEnd                                                ││
//! │  │  - GetReducerFileGroup                                      ││
//! │  └─────────────────────────────────────────────────────────────┘│
//! │                           ▲                                      │
//! │                           │ Netty RPC                            │
//! └───────────────────────────┼──────────────────────────────────────┘
//!                             │
//! ┌───────────────────────────┼──────────────────────────────────────┐
//! │                     Executor (Rust via JNI)                      │
//! │                           │                                      │
//! │  ┌────────────────────────▼────────────────────────────────────┐│
//! │  │           LifecycleManagerClient (Rust)                     ││
//! │  │  - Connects to Driver's LifecycleManager                    ││
//! │  │  - Sends RPC requests via Netty protocol                    ││
//! │  └─────────────────────────────────────────────────────────────┘│
//! │                           │                                      │
//! │  ┌────────────────────────▼────────────────────────────────────┐│
//! │  │              ShuffleClient (Rust)                           ││
//! │  │  - Push data to Workers                                     ││
//! │  │  - Fetch data from Workers                                  ││
//! │  └─────────────────────────────────────────────────────────────┘│
//! └──────────────────────────────────────────────────────────────────┘
//! ```

use std::collections::{HashMap, HashSet};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use prost::Message;
use tracing::{debug, info};

use crate::config::CelebornConfig;
use crate::error::{CelebornError, Result, StatusCode};
use crate::network::NettyRpcClient;
use crate::protocol::generated::{
    PbChangeLocationResponse, PbPartitionLocation, PbPartitionSplit, PbRegisterShuffle,
    PbRegisterShuffleResponse, PbRevive, PbRevivePartitionInfo,
};
use crate::protocol::java_serialization::RpcAddress;
use crate::protocol::transport::TransportMessageType;
use crate::protocol::{PartitionLocation, PartitionMode};

/// Request ID counter for RPC calls.
static REQUEST_ID_COUNTER: AtomicI64 = AtomicI64::new(1);

/// Generate a new request ID.
fn next_request_id() -> i64 {
    REQUEST_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Response from RegisterShuffle RPC.
#[derive(Debug, Clone)]
pub struct RegisterShuffleResponse {
    /// Status code
    pub status: StatusCode,
    /// Partition locations (partition_id -> locations)
    pub partition_locations: HashMap<i32, Vec<PartitionLocation>>,
}

/// Response from MapperEnd RPC.
#[derive(Debug, Clone)]
pub struct MapperEndResponse {
    /// Status code
    pub status: StatusCode,
}

/// Response from GetReducerFileGroup RPC.
#[derive(Debug, Clone)]
pub struct ReducerFileGroupResponse {
    /// Status code
    pub status: StatusCode,
    /// File groups (partition_id -> locations)
    pub file_groups: HashMap<i32, Vec<PartitionLocation>>,
    /// Map attempts (mapId -> attemptId)
    pub attempts: Vec<i32>,
    /// Partition IDs
    pub partition_ids: HashSet<i32>,
}

/// Response from Revive RPC.
#[derive(Debug, Clone)]
pub struct ReviveResponse {
    /// Status code
    pub status: StatusCode,
    /// New partition locations
    pub partition_locations: Vec<PartitionLocation>,
}

/// Push failed batch information.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PushFailedBatch {
    /// Map ID
    pub map_id: i32,
    /// Attempt ID
    pub attempt_id: i32,
    /// Batch ID
    pub batch_id: i32,
}

/// Trait for LifecycleManager client operations.
///
/// This trait defines the RPC interface between Rust ShuffleClient (Executor)
/// and Java LifecycleManager (Driver).
#[async_trait]
pub trait LifecycleManagerClient: Send + Sync {
    /// Register a shuffle with the LifecycleManager.
    ///
    /// # Arguments
    /// * `shuffle_id` - The shuffle ID
    /// * `num_mappers` - Number of map tasks
    /// * `num_partitions` - Number of partitions
    ///
    /// # Returns
    /// RegisterShuffleResponse containing partition locations
    async fn register_shuffle(
        &self,
        shuffle_id: i32,
        num_mappers: i32,
        num_partitions: i32,
    ) -> Result<RegisterShuffleResponse>;

    /// Signal that a mapper has finished.
    ///
    /// # Arguments
    /// * `shuffle_id` - The shuffle ID
    /// * `map_id` - The map task ID
    /// * `attempt_id` - The attempt ID
    /// * `num_mappers` - Total number of mappers
    /// * `partition_id` - Partition ID (for MapPartition type, -1 for ReducePartition)
    /// * `push_failed_batches` - Failed batches to report
    async fn mapper_end(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
        partition_id: i32,
        push_failed_batches: HashMap<String, HashSet<PushFailedBatch>>,
    ) -> Result<MapperEndResponse>;

    /// Get reducer file groups for reading.
    ///
    /// # Arguments
    /// * `shuffle_id` - The shuffle ID
    ///
    /// # Returns
    /// ReducerFileGroupResponse containing file locations
    async fn get_reducer_file_group(&self, shuffle_id: i32) -> Result<ReducerFileGroupResponse>;

    /// Revive partitions after push failure.
    ///
    /// # Arguments
    /// * `shuffle_id` - The shuffle ID
    /// * `map_ids` - Map IDs involved
    /// * `partition_infos` - Partition information for revive
    async fn revive(
        &self,
        shuffle_id: i32,
        map_ids: Vec<i32>,
        partition_infos: Vec<RevivePartitionInfo>,
    ) -> Result<ReviveResponse>;

    /// Request partition split.
    ///
    /// # Arguments
    /// * `shuffle_id` - The shuffle ID
    /// * `partition_id` - The partition ID
    /// * `epoch` - Current epoch
    /// * `old_partition` - Old partition location
    async fn partition_split(
        &self,
        shuffle_id: i32,
        partition_id: i32,
        epoch: i32,
        old_partition: &PartitionLocation,
    ) -> Result<PartitionLocation>;

    /// Get shuffle ID for app shuffle.
    ///
    /// # Arguments
    /// * `app_shuffle_id` - Application shuffle ID
    /// * `app_shuffle_identifier` - Application shuffle identifier
    /// * `is_writer` - Whether this is for writer
    async fn get_shuffle_id(
        &self,
        app_shuffle_id: i32,
        app_shuffle_identifier: &str,
        is_writer: bool,
    ) -> Result<i32>;

    /// Report shuffle fetch failure.
    async fn report_shuffle_fetch_failure(
        &self,
        app_shuffle_id: i32,
        shuffle_id: i32,
        failure_type: i32,
    ) -> Result<bool>;
}

/// Partition info for revive request.
#[derive(Debug, Clone)]
pub struct RevivePartitionInfo {
    /// Partition ID
    pub partition_id: i32,
    /// Current epoch
    pub epoch: i32,
    /// Old partition location (optional)
    pub old_partition: Option<PartitionLocation>,
    /// Status code indicating failure cause
    pub status: StatusCode,
}

/// Netty RPC client for communicating with Java LifecycleManager.
///
/// This client implements the Celeborn Netty RPC protocol to communicate
/// with the Java LifecycleManager running in the Driver process.
pub struct NettyLifecycleManagerClient {
    /// LifecycleManager host
    host: String,
    /// LifecycleManager port
    port: i32,
    /// Netty RPC client
    netty_client: NettyRpcClient,
}

impl NettyLifecycleManagerClient {
    /// Create a new NettyLifecycleManagerClient.
    ///
    /// # Arguments
    /// * `config` - Celeborn configuration
    /// * `host` - LifecycleManager host
    /// * `port` - LifecycleManager port
    pub fn new(config: Arc<CelebornConfig>, host: String, port: i32) -> Self {
        // Create Netty RPC client with local address
        let local_address = Some(RpcAddress::new("localhost", 0));
        let netty_client = NettyRpcClient::new(local_address, config.rpc_timeout);

        Self {
            host,
            port,
            netty_client,
        }
    }

    /// Get the SocketAddr for the LifecycleManager.
    fn get_addr(&self) -> Result<SocketAddr> {
        let addr_str = format!("{}:{}", self.host, self.port);
        addr_str
            .to_socket_addrs()
            .map_err(|e| CelebornError::Connection(format!("Invalid address {}: {}", addr_str, e)))?
            .next()
            .ok_or_else(|| CelebornError::Connection(format!("Cannot resolve {}", addr_str)))
    }

    /// Send RPC request and wait for response.
    ///
    /// Uses the Netty RPC protocol with Java serialization.
    async fn send_rpc<Req: Message, Resp: Message + Default>(
        &self,
        message_type: TransportMessageType,
        request: &Req,
    ) -> Result<Resp> {
        let addr = self.get_addr()?;

        debug!(
            "Sending RPC message type {:?} to LifecycleManager at {}:{}",
            message_type, self.host, self.port
        );

        // Use NettyRpcClient to send the RPC with proper Java serialization
        // LifecycleManager endpoint name is "LifecycleManagerEndpoint"
        self.netty_client
            .send_rpc_to_endpoint(addr, "LifecycleManagerEndpoint", message_type, request)
            .await
    }

    /// Convert PbPartitionLocation to PartitionLocation.
    fn convert_partition_location(&self, pb: &PbPartitionLocation) -> PartitionLocation {
        let mode = if pb.mode == 0 {
            PartitionMode::Primary
        } else {
            PartitionMode::Replica
        };

        let mut location = PartitionLocation {
            id: pb.id,
            epoch: pb.epoch,
            host: pb.host.clone(),
            rpc_port: pb.rpc_port,
            push_port: pb.push_port,
            fetch_port: pb.fetch_port,
            replicate_port: pb.replicate_port,
            mode,
            peer: None,
            storage_info: None,
        };

        // Handle peer location
        if let Some(ref peer_pb) = pb.peer {
            let peer = Box::new(self.convert_partition_location(peer_pb));
            location.peer = Some(peer);
        }

        location
    }

    /// Convert PartitionLocation to PbPartitionLocation.
    fn convert_to_pb_location(&self, loc: &PartitionLocation) -> PbPartitionLocation {
        let mut pb = PbPartitionLocation {
            id: loc.id,
            epoch: loc.epoch,
            host: loc.host.clone(),
            rpc_port: loc.rpc_port,
            push_port: loc.push_port,
            fetch_port: loc.fetch_port,
            replicate_port: loc.replicate_port,
            mode: loc.mode as i32,
            peer: None,
            storage_info: None,
            map_id_bitmap: Vec::new(),
            split_start: 0,
            split_end: 0,
        };

        if let Some(ref peer) = loc.peer {
            pb.peer = Some(Box::new(self.convert_to_pb_location(peer)));
        }

        pb
    }
}

#[async_trait]
impl LifecycleManagerClient for NettyLifecycleManagerClient {
    async fn register_shuffle(
        &self,
        shuffle_id: i32,
        num_mappers: i32,
        num_partitions: i32,
    ) -> Result<RegisterShuffleResponse> {
        info!(
            "Registering shuffle {} with {} mappers and {} partitions",
            shuffle_id, num_mappers, num_partitions
        );

        let request = PbRegisterShuffle {
            shuffle_id,
            num_mappers,
            num_partitions,
        };

        let response: PbRegisterShuffleResponse = self
            .send_rpc(TransportMessageType::RegisterShuffle, &request)
            .await?;

        eprintln!(
            "[CELEBORN-DEBUG] Received PbRegisterShuffleResponse: status={}, partition_locations_count={}",
            response.status, response.partition_locations.len()
        );

        let status = StatusCode::from(response.status);
        eprintln!("[CELEBORN-DEBUG] Converted status: {:?}", status);

        // Convert partition locations
        let mut partition_locations: HashMap<i32, Vec<PartitionLocation>> = HashMap::new();
        for pb_loc in &response.partition_locations {
            let loc = self.convert_partition_location(pb_loc);
            partition_locations
                .entry(loc.id)
                .or_insert_with(Vec::new)
                .push(loc);
        }

        Ok(RegisterShuffleResponse {
            status,
            partition_locations,
        })
    }

    async fn mapper_end(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
        partition_id: i32,
        _push_failed_batches: HashMap<String, HashSet<PushFailedBatch>>,
    ) -> Result<MapperEndResponse> {
        debug!(
            "MapperEnd: shuffle={}, map={}, attempt={}",
            shuffle_id, map_id, attempt_id
        );

        // Create protobuf request
        use crate::protocol::generated::{PbMapperEnd, PbMapperEndResponse};
        let request = PbMapperEnd {
            shuffle_id,
            map_id,
            attempt_id,
            num_mappers,
            partition_id,
            push_failure_batches: std::collections::HashMap::new(),
        };

        let response: PbMapperEndResponse = self
            .send_rpc(TransportMessageType::MapperEnd, &request)
            .await?;

        let status = StatusCode::from(response.status);
        Ok(MapperEndResponse { status })
    }

    async fn get_reducer_file_group(&self, shuffle_id: i32) -> Result<ReducerFileGroupResponse> {
        debug!("GetReducerFileGroup: shuffle={}", shuffle_id);

        // Create protobuf request
        use crate::protocol::generated::PbGetReducerFileGroup;
        let request = PbGetReducerFileGroup { shuffle_id };

        let response: crate::protocol::generated::PbGetReducerFileGroupResponse = self
            .send_rpc(TransportMessageType::GetReducerFileGroup, &request)
            .await?;

        let status = StatusCode::from(response.status);

        // Parse file groups from response
        let mut file_groups: HashMap<i32, Vec<PartitionLocation>> = HashMap::new();
        for (partition_id, file_group) in &response.file_groups {
            let locations: Vec<PartitionLocation> = file_group
                .locations
                .iter()
                .map(|pb_loc| self.convert_partition_location(pb_loc))
                .collect();
            file_groups.insert(*partition_id, locations);
        }

        Ok(ReducerFileGroupResponse {
            status,
            file_groups,
            attempts: response.attempts.clone(),
            partition_ids: response.partition_ids.iter().cloned().collect(),
        })
    }

    async fn revive(
        &self,
        shuffle_id: i32,
        map_ids: Vec<i32>,
        partition_infos: Vec<RevivePartitionInfo>,
    ) -> Result<ReviveResponse> {
        debug!(
            "Revive: shuffle={}, partitions={}",
            shuffle_id,
            partition_infos.len()
        );

        // Build protobuf request
        let pb_partition_infos: Vec<PbRevivePartitionInfo> = partition_infos
            .iter()
            .map(|info| PbRevivePartitionInfo {
                partition_id: info.partition_id,
                epoch: info.epoch,
                partition: info
                    .old_partition
                    .as_ref()
                    .map(|p| self.convert_to_pb_location(p)),
                status: info.status as i32,
            })
            .collect();

        let request = PbRevive {
            shuffle_id,
            map_id: map_ids,
            partition_info: pb_partition_infos,
        };

        let response: PbChangeLocationResponse = self
            .send_rpc(TransportMessageType::ChangeLocation, &request)
            .await?;

        // PbChangeLocationResponse doesn't have a status field
        // We infer success if we got partition_info back
        let status = if response.partition_info.is_empty() {
            StatusCode::ReviveFailed
        } else {
            StatusCode::Success
        };

        // Convert partition locations
        let partition_locations: Vec<PartitionLocation> = response
            .partition_info
            .iter()
            .filter_map(|info| {
                info.partition
                    .as_ref()
                    .map(|p| self.convert_partition_location(p))
            })
            .collect();

        Ok(ReviveResponse {
            status,
            partition_locations,
        })
    }

    async fn partition_split(
        &self,
        shuffle_id: i32,
        partition_id: i32,
        epoch: i32,
        old_partition: &PartitionLocation,
    ) -> Result<PartitionLocation> {
        debug!(
            "PartitionSplit: shuffle={}, partition={}, epoch={}",
            shuffle_id, partition_id, epoch
        );

        let request = PbPartitionSplit {
            shuffle_id,
            partition_id,
            epoch,
            old_partition: Some(self.convert_to_pb_location(old_partition)),
        };

        let response: PbChangeLocationResponse = self
            .send_rpc(TransportMessageType::PartitionSplit, &request)
            .await?;

        // Get new location from response
        // PbChangeLocationResponse doesn't have a status field
        response
            .partition_info
            .first()
            .and_then(|info| info.partition.as_ref())
            .map(|p| self.convert_partition_location(p))
            .ok_or_else(|| CelebornError::PartitionNotFound {
                shuffle_id,
                partition_id,
            })
    }

    async fn get_shuffle_id(
        &self,
        app_shuffle_id: i32,
        app_shuffle_identifier: &str,
        is_writer: bool,
    ) -> Result<i32> {
        debug!(
            "GetShuffleId: app_shuffle_id={}, identifier={}, is_writer={}",
            app_shuffle_id, app_shuffle_identifier, is_writer
        );

        // Create protobuf request
        use crate::protocol::generated::PbGetShuffleId;
        let request = PbGetShuffleId {
            app_shuffle_id,
            app_shuffle_identifier: app_shuffle_identifier.to_string(),
            is_shuffle_writer: is_writer,
        };

        let response: crate::protocol::generated::PbGetShuffleIdResponse = self
            .send_rpc(TransportMessageType::GetShuffleId, &request)
            .await?;

        Ok(response.shuffle_id)
    }

    async fn report_shuffle_fetch_failure(
        &self,
        app_shuffle_id: i32,
        shuffle_id: i32,
        failure_type: i32,
    ) -> Result<bool> {
        debug!(
            "ReportShuffleFetchFailure: app_shuffle_id={}, shuffle_id={}, failure_type={}",
            app_shuffle_id, shuffle_id, failure_type
        );

        // Create protobuf request
        use crate::protocol::generated::PbReportShuffleFetchFailure;
        let request = PbReportShuffleFetchFailure {
            app_shuffle_id,
            shuffle_id,
            failure_type,
        };

        let response: crate::protocol::generated::PbReportShuffleFetchFailureResponse = self
            .send_rpc(TransportMessageType::ReportShuffleFetchFailure, &request)
            .await?;

        Ok(response.success)
    }
}

/// Local LifecycleManager client for single-process usage.
///
/// This client wraps the local LifecycleManager for cases where
/// Driver and Executor are in the same process.
pub struct LocalLifecycleManagerClient {
    /// Reference to local LifecycleManager
    lifecycle_manager: Arc<super::LifecycleManager>,
}

impl LocalLifecycleManagerClient {
    /// Create a new LocalLifecycleManagerClient.
    pub fn new(lifecycle_manager: Arc<super::LifecycleManager>) -> Self {
        Self { lifecycle_manager }
    }
}

#[async_trait]
impl LifecycleManagerClient for LocalLifecycleManagerClient {
    async fn register_shuffle(
        &self,
        shuffle_id: i32,
        num_mappers: i32,
        num_partitions: i32,
    ) -> Result<RegisterShuffleResponse> {
        self.lifecycle_manager
            .register_shuffle(shuffle_id, num_mappers, num_partitions)
            .await?;

        // Get partition locations
        let mut partition_locations = HashMap::new();
        if let Ok(locations) = self
            .lifecycle_manager
            .get_reducer_file_group(shuffle_id)
            .await
        {
            partition_locations = locations;
        }

        Ok(RegisterShuffleResponse {
            status: StatusCode::Success,
            partition_locations,
        })
    }

    async fn mapper_end(
        &self,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
        _partition_id: i32,
        _push_failed_batches: HashMap<String, HashSet<PushFailedBatch>>,
    ) -> Result<MapperEndResponse> {
        self.lifecycle_manager
            .mapper_end(shuffle_id, map_id, attempt_id, num_mappers)
            .await?;

        Ok(MapperEndResponse {
            status: StatusCode::Success,
        })
    }

    async fn get_reducer_file_group(&self, shuffle_id: i32) -> Result<ReducerFileGroupResponse> {
        let file_groups = self
            .lifecycle_manager
            .get_reducer_file_group(shuffle_id)
            .await?;

        Ok(ReducerFileGroupResponse {
            status: StatusCode::Success,
            file_groups,
            attempts: Vec::new(),
            partition_ids: HashSet::new(),
        })
    }

    async fn revive(
        &self,
        shuffle_id: i32,
        _map_ids: Vec<i32>,
        partition_infos: Vec<RevivePartitionInfo>,
    ) -> Result<ReviveResponse> {
        let mut locations = Vec::new();

        for info in partition_infos {
            let loc = self
                .lifecycle_manager
                .revive_partition(
                    shuffle_id,
                    info.partition_id,
                    info.epoch,
                    info.old_partition.as_ref(),
                )
                .await?;
            locations.push(loc);
        }

        Ok(ReviveResponse {
            status: StatusCode::Success,
            partition_locations: locations,
        })
    }

    async fn partition_split(
        &self,
        shuffle_id: i32,
        partition_id: i32,
        epoch: i32,
        old_partition: &PartitionLocation,
    ) -> Result<PartitionLocation> {
        self.lifecycle_manager
            .revive_partition(shuffle_id, partition_id, epoch, Some(old_partition))
            .await
    }

    async fn get_shuffle_id(
        &self,
        app_shuffle_id: i32,
        _app_shuffle_identifier: &str,
        _is_writer: bool,
    ) -> Result<i32> {
        // In local mode, app_shuffle_id is the same as shuffle_id
        Ok(app_shuffle_id)
    }

    async fn report_shuffle_fetch_failure(
        &self,
        _app_shuffle_id: i32,
        _shuffle_id: i32,
        _failure_type: i32,
    ) -> Result<bool> {
        // In local mode, just return success
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_failed_batch() {
        let batch1 = PushFailedBatch {
            map_id: 0,
            attempt_id: 0,
            batch_id: 1,
        };
        let batch2 = PushFailedBatch {
            map_id: 0,
            attempt_id: 0,
            batch_id: 1,
        };
        assert_eq!(batch1, batch2);

        let mut set = HashSet::new();
        set.insert(batch1.clone());
        assert!(set.contains(&batch2));
    }

    #[test]
    fn test_revive_partition_info() {
        let info = RevivePartitionInfo {
            partition_id: 0,
            epoch: 1,
            old_partition: None,
            status: StatusCode::PushDataWriteFailPrimary,
        };
        assert_eq!(info.partition_id, 0);
        assert_eq!(info.epoch, 1);
    }

    #[test]
    fn test_register_shuffle_response() {
        let response = RegisterShuffleResponse {
            status: StatusCode::Success,
            partition_locations: HashMap::new(),
        };
        assert!(response.status.is_success());
    }
}
