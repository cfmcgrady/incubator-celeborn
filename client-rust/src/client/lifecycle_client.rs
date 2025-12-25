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
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use prost::Message;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::config::CelebornConfig;
use crate::error::{CelebornError, Result, StatusCode};
use crate::network::{Connection, ConnectionPool};
use crate::protocol::generated::{
    PbChangeLocationResponse, PbGetReducerFileGroupResponse, PbPartitionLocation,
    PbPartitionSplit, PbRegisterShuffle, PbRegisterShuffleResponse, PbRevive,
    PbRevivePartitionInfo,
};
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
    /// Configuration
    config: Arc<CelebornConfig>,
    /// LifecycleManager host
    host: String,
    /// LifecycleManager port
    port: i32,
    /// Connection pool
    connection_pool: ConnectionPool,
    /// Cached connection
    connection: RwLock<Option<Arc<Connection>>>,
    /// RPC timeout
    rpc_timeout: Duration,
}

impl NettyLifecycleManagerClient {
    /// Create a new NettyLifecycleManagerClient.
    ///
    /// # Arguments
    /// * `config` - Celeborn configuration
    /// * `host` - LifecycleManager host
    /// * `port` - LifecycleManager port
    pub fn new(config: Arc<CelebornConfig>, host: String, port: i32) -> Self {
        let connection_pool = ConnectionPool::new(
            config.connection_pool_size,
            config.max_in_flight_requests,
        );

        Self {
            rpc_timeout: config.rpc_timeout,
            config,
            host,
            port,
            connection_pool,
            connection: RwLock::new(None),
        }
    }

    /// Get or create connection to LifecycleManager.
    async fn get_connection(&self) -> Result<Arc<Connection>> {
        // Check cached connection
        {
            let conn = self.connection.read().await;
            if let Some(ref c) = *conn {
                if c.is_active() {
                    return Ok(c.clone());
                }
            }
        }

        // Create new connection
        let addr: SocketAddr = format!("{}:{}", self.host, self.port)
            .parse()
            .map_err(|e| CelebornError::Connection(format!("Invalid address: {}", e)))?;

        let conn = self.connection_pool.get_connection(addr).await?;

        // Cache connection
        {
            let mut cached = self.connection.write().await;
            *cached = Some(conn.clone());
        }

        info!(
            "Connected to LifecycleManager at {}:{}",
            self.host, self.port
        );

        Ok(conn)
    }

    /// Send RPC request and wait for response.
    ///
    /// The Netty RPC protocol format:
    /// - Request: TransportMessage containing protobuf payload
    /// - Response: TransportMessage containing protobuf response
    async fn send_rpc<Req: Message, Resp: Message + Default>(
        &self,
        message_type: i32,
        request: &Req,
    ) -> Result<Resp> {
        let conn = self.get_connection().await?;

        // Encode request as TransportMessage
        let payload = request.encode_to_vec();
        let mut buf = BytesMut::with_capacity(8 + payload.len());
        buf.put_i32(message_type);
        buf.put_i32(payload.len() as i32);
        buf.put_slice(&payload);

        debug!(
            "Sending RPC message type {} with {} bytes payload",
            message_type,
            payload.len()
        );

        // Send RPC and wait for response
        let response = conn.send_rpc(buf.freeze(), self.rpc_timeout).await?;

        // Decode response
        // Response body contains: messageType (4) + payloadLen (4) + payload
        if response.body.len() < 8 {
            return Err(CelebornError::Protocol(format!(
                "Response too short: {} bytes",
                response.body.len()
            )));
        }

        let resp_type = i32::from_be_bytes(response.body[0..4].try_into().unwrap());
        let payload_len = i32::from_be_bytes(response.body[4..8].try_into().unwrap()) as usize;

        if response.body.len() < 8 + payload_len {
            return Err(CelebornError::Protocol(format!(
                "Response payload incomplete: expected {} bytes, got {}",
                payload_len,
                response.body.len() - 8
            )));
        }

        let resp_payload = &response.body[8..8 + payload_len];
        let resp = Resp::decode(resp_payload).map_err(|e| {
            CelebornError::Protocol(format!("Failed to decode response: {}", e))
        })?;

        debug!("Received RPC response type {}", resp_type);

        Ok(resp)
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

        // Message type for RegisterShuffle (from ControlMessages)
        const REGISTER_SHUFFLE: i32 = 1;

        let response: PbRegisterShuffleResponse = self
            .send_rpc(REGISTER_SHUFFLE, &request)
            .await?;

        let status = StatusCode::from(response.status);

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

        // MapperEnd uses Java serialization in the original protocol
        // For now, we'll use a simplified protobuf-based approach
        // TODO: Implement full Java serialization compatibility

        // Message type for MapperEnd
        const MAPPER_END: i32 = 7;

        // Create a simple request (this needs to match Java's MapperEnd case class)
        // The actual implementation would need Java serialization
        let mut buf = BytesMut::new();
        buf.put_i32(shuffle_id);
        buf.put_i32(map_id);
        buf.put_i32(attempt_id);
        buf.put_i32(num_mappers);
        buf.put_i32(partition_id);
        // Empty push failed batches map
        buf.put_i32(0);

        let conn = self.get_connection().await?;

        // Encode as TransportMessage
        let payload = buf.freeze();
        let mut msg = BytesMut::with_capacity(8 + payload.len());
        msg.put_i32(MAPPER_END);
        msg.put_i32(payload.len() as i32);
        msg.put_slice(&payload);

        let response = conn.send_rpc(msg.freeze(), self.rpc_timeout).await?;

        // Parse response status
        let status = if response.body.len() >= 12 {
            let status_code = i32::from_be_bytes(response.body[8..12].try_into().unwrap());
            StatusCode::from(status_code)
        } else {
            StatusCode::Success
        };

        Ok(MapperEndResponse { status })
    }

    async fn get_reducer_file_group(&self, shuffle_id: i32) -> Result<ReducerFileGroupResponse> {
        debug!("GetReducerFileGroup: shuffle={}", shuffle_id);

        // Message type for GetReducerFileGroup
        const GET_REDUCER_FILE_GROUP: i32 = 8;

        // Create request
        let mut buf = BytesMut::new();
        buf.put_i32(shuffle_id);

        let conn = self.get_connection().await?;

        let payload = buf.freeze();
        let mut msg = BytesMut::with_capacity(8 + payload.len());
        msg.put_i32(GET_REDUCER_FILE_GROUP);
        msg.put_i32(payload.len() as i32);
        msg.put_slice(&payload);

        let response = conn.send_rpc(msg.freeze(), self.rpc_timeout).await?;

        // Parse response
        // This is a simplified parsing - full implementation needs Java deserialization
        let status = if response.body.len() >= 12 {
            let status_code = i32::from_be_bytes(response.body[8..12].try_into().unwrap());
            StatusCode::from(status_code)
        } else {
            StatusCode::Success
        };

        // TODO: Parse file groups from response
        // This requires implementing Java serialization deserialization

        Ok(ReducerFileGroupResponse {
            status,
            file_groups: HashMap::new(),
            attempts: Vec::new(),
            partition_ids: HashSet::new(),
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

        // Message type for Revive
        const REVIVE: i32 = 4;

        let response: PbChangeLocationResponse = self.send_rpc(REVIVE, &request).await?;

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

        // Message type for PartitionSplit
        const PARTITION_SPLIT: i32 = 47;

        let response: PbChangeLocationResponse = self.send_rpc(PARTITION_SPLIT, &request).await?;

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

        // Message type for GetShuffleId
        const GET_SHUFFLE_ID: i32 = 9;

        // Create request
        let mut buf = BytesMut::new();
        buf.put_i32(app_shuffle_id);
        let id_bytes = app_shuffle_identifier.as_bytes();
        buf.put_i32(id_bytes.len() as i32);
        buf.put_slice(id_bytes);
        buf.put_u8(if is_writer { 1 } else { 0 });

        let conn = self.get_connection().await?;

        let payload = buf.freeze();
        let mut msg = BytesMut::with_capacity(8 + payload.len());
        msg.put_i32(GET_SHUFFLE_ID);
        msg.put_i32(payload.len() as i32);
        msg.put_slice(&payload);

        let response = conn.send_rpc(msg.freeze(), self.rpc_timeout).await?;

        // Parse shuffle ID from response
        if response.body.len() >= 16 {
            let shuffle_id = i32::from_be_bytes(response.body[12..16].try_into().unwrap());
            Ok(shuffle_id)
        } else {
            Err(CelebornError::Protocol(
                "Invalid GetShuffleId response".to_string(),
            ))
        }
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

        // Message type for ReportShuffleFetchFailure
        const REPORT_SHUFFLE_FETCH_FAILURE: i32 = 10;

        let mut buf = BytesMut::new();
        buf.put_i32(app_shuffle_id);
        buf.put_i32(shuffle_id);
        buf.put_i32(failure_type);

        let conn = self.get_connection().await?;

        let payload = buf.freeze();
        let mut msg = BytesMut::with_capacity(8 + payload.len());
        msg.put_i32(REPORT_SHUFFLE_FETCH_FAILURE);
        msg.put_i32(payload.len() as i32);
        msg.put_slice(&payload);

        let response = conn.send_rpc(msg.freeze(), self.rpc_timeout).await?;

        // Parse success flag from response
        if response.body.len() >= 13 {
            Ok(response.body[12] != 0)
        } else {
            Ok(false)
        }
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
            status: StatusCode::PushDataFailPrimary,
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
