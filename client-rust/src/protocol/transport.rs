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

//! Transport message types for Celeborn RPC.
//!
//! These are the protobuf-based messages used for control plane communication.

use bytes::{Bytes, BytesMut};

/// Transport message type identifiers (from TransportMessages.proto).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum TransportMessageType {
    UnknownMessage = 0,
    RegisterWorker = 1,
    HeartbeatFromWorker = 2,
    HeartbeatFromWorkerResponse = 3,
    RegisterShuffle = 4,
    RegisterShuffleResponse = 5,
    RequestSlots = 6,
    ReleaseSlots = 7,
    ReleaseSlotsResponse = 8,
    RequestSlotsResponse = 9,
    ChangeLocation = 10,
    ChangeLocationResponse = 11,
    MapperEnd = 12,
    MapperEndResponse = 13,
    GetReducerFileGroup = 14,
    GetReducerFileGroupResponse = 15,
    UnregisterShuffle = 16,
    UnregisterShuffleResponse = 17,
    ApplicationLost = 18,
    ApplicationLostResponse = 19,
    HeartbeatFromApplication = 20,
    CheckQuota = 23,
    CheckQuotaResponse = 24,
    ReportWorkerFailure = 25,
    RegisterWorkerResponse = 26,
    ReserveSlots = 28,
    ReserveSlotsResponse = 29,
    CommitFiles = 30,
    CommitFilesResponse = 31,
    Destroy = 32,
    DestroyResponse = 33,
    RemoveExpiredShuffle = 39,
    OneWayMessageResponse = 40,
    CheckWorkerTimeout = 41,
    CheckApplicationTimeout = 42,
    WorkerLost = 43,
    WorkerLostResponse = 44,
    StageEnd = 45,
    StageEndResponse = 46,
    PartitionSplit = 47,
    RegisterMapPartitionTask = 48,
    HeartbeatFromApplicationResponse = 49,
    CheckForHdfsExpiredDirsTimeout = 50,
    OpenStream = 51,
    StreamHandler = 52,
    CheckWorkersAvailable = 53,
    CheckWorkersAvailableResponse = 54,
    RemoveWorkersUnavailableInfo = 55,
    PushDataHandShake = 56,
    RegionStart = 57,
    RegionFinish = 58,
    BacklogAnnouncement = 59,
    BufferStreamEnd = 60,
    ReadAddCredit = 61,
    StreamChunkSlice = 62,
    ChunkFetchRequest = 63,
    TransportableError = 64,
    WorkerExclude = 65,
    WorkerExcludeResponse = 66,
    ReportShuffleFetchFailure = 67,
    ReportShuffleFetchFailureResponse = 68,
    GetShuffleId = 69,
    GetShuffleIdResponse = 70,
    SaslRequest = 71,
}

impl From<i32> for TransportMessageType {
    fn from(value: i32) -> Self {
        match value {
            1 => TransportMessageType::RegisterWorker,
            2 => TransportMessageType::HeartbeatFromWorker,
            3 => TransportMessageType::HeartbeatFromWorkerResponse,
            4 => TransportMessageType::RegisterShuffle,
            5 => TransportMessageType::RegisterShuffleResponse,
            6 => TransportMessageType::RequestSlots,
            7 => TransportMessageType::ReleaseSlots,
            8 => TransportMessageType::ReleaseSlotsResponse,
            9 => TransportMessageType::RequestSlotsResponse,
            10 => TransportMessageType::ChangeLocation,
            11 => TransportMessageType::ChangeLocationResponse,
            12 => TransportMessageType::MapperEnd,
            13 => TransportMessageType::MapperEndResponse,
            14 => TransportMessageType::GetReducerFileGroup,
            15 => TransportMessageType::GetReducerFileGroupResponse,
            16 => TransportMessageType::UnregisterShuffle,
            17 => TransportMessageType::UnregisterShuffleResponse,
            18 => TransportMessageType::ApplicationLost,
            19 => TransportMessageType::ApplicationLostResponse,
            20 => TransportMessageType::HeartbeatFromApplication,
            23 => TransportMessageType::CheckQuota,
            24 => TransportMessageType::CheckQuotaResponse,
            25 => TransportMessageType::ReportWorkerFailure,
            26 => TransportMessageType::RegisterWorkerResponse,
            28 => TransportMessageType::ReserveSlots,
            29 => TransportMessageType::ReserveSlotsResponse,
            30 => TransportMessageType::CommitFiles,
            31 => TransportMessageType::CommitFilesResponse,
            32 => TransportMessageType::Destroy,
            33 => TransportMessageType::DestroyResponse,
            39 => TransportMessageType::RemoveExpiredShuffle,
            40 => TransportMessageType::OneWayMessageResponse,
            41 => TransportMessageType::CheckWorkerTimeout,
            42 => TransportMessageType::CheckApplicationTimeout,
            43 => TransportMessageType::WorkerLost,
            44 => TransportMessageType::WorkerLostResponse,
            45 => TransportMessageType::StageEnd,
            46 => TransportMessageType::StageEndResponse,
            47 => TransportMessageType::PartitionSplit,
            48 => TransportMessageType::RegisterMapPartitionTask,
            49 => TransportMessageType::HeartbeatFromApplicationResponse,
            50 => TransportMessageType::CheckForHdfsExpiredDirsTimeout,
            51 => TransportMessageType::OpenStream,
            52 => TransportMessageType::StreamHandler,
            53 => TransportMessageType::CheckWorkersAvailable,
            54 => TransportMessageType::CheckWorkersAvailableResponse,
            55 => TransportMessageType::RemoveWorkersUnavailableInfo,
            56 => TransportMessageType::PushDataHandShake,
            57 => TransportMessageType::RegionStart,
            58 => TransportMessageType::RegionFinish,
            59 => TransportMessageType::BacklogAnnouncement,
            60 => TransportMessageType::BufferStreamEnd,
            61 => TransportMessageType::ReadAddCredit,
            62 => TransportMessageType::StreamChunkSlice,
            63 => TransportMessageType::ChunkFetchRequest,
            64 => TransportMessageType::TransportableError,
            65 => TransportMessageType::WorkerExclude,
            66 => TransportMessageType::WorkerExcludeResponse,
            67 => TransportMessageType::ReportShuffleFetchFailure,
            68 => TransportMessageType::ReportShuffleFetchFailureResponse,
            69 => TransportMessageType::GetShuffleId,
            70 => TransportMessageType::GetShuffleIdResponse,
            71 => TransportMessageType::SaslRequest,
            _ => TransportMessageType::UnknownMessage,
        }
    }
}

/// A transport message wrapper that includes the message type.
#[derive(Debug, Clone)]
pub struct TransportMessage {
    /// Message type
    pub message_type: TransportMessageType,
    /// Serialized message payload
    pub payload: Bytes,
}

impl TransportMessage {
    /// Create a new transport message.
    pub fn new(message_type: TransportMessageType, payload: Bytes) -> Self {
        Self {
            message_type,
            payload,
        }
    }

    /// Encode the transport message to bytes.
    /// Format: messageTypeValue (4B) + payloadLen (4B) + payload
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(4 + 4 + self.payload.len());
        buf.extend_from_slice(&(self.message_type as i32).to_be_bytes());
        buf.extend_from_slice(&(self.payload.len() as i32).to_be_bytes());
        buf.extend_from_slice(&self.payload);
        buf.freeze()
    }

    /// Decode a transport message from bytes.
    /// Format: messageTypeValue (4B) + payloadLen (4B) + payload
    pub fn decode(data: Bytes) -> std::io::Result<Self> {
        if data.len() < 8 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Not enough bytes for transport message header",
            ));
        }
        let type_bytes: [u8; 4] = data[..4].try_into().unwrap();
        let message_type = TransportMessageType::from(i32::from_be_bytes(type_bytes));
        
        let len_bytes: [u8; 4] = data[4..8].try_into().unwrap();
        let payload_len = i32::from_be_bytes(len_bytes) as usize;
        
        if data.len() < 8 + payload_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("Not enough bytes for payload: expected {}, got {}", payload_len, data.len() - 8),
            ));
        }
        
        let payload = data.slice(8..8 + payload_len);
        Ok(Self {
            message_type,
            payload,
        })
    }
}

// ============================================================================
// Protobuf message definitions (manually defined to match TransportMessages.proto)
// In production, these would be generated by prost-build
// ============================================================================

/// User identifier for quota management.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbUserIdentifier {
    #[prost(string, tag = "1")]
    pub tenant_id: String,
    #[prost(string, tag = "2")]
    pub name: String,
}

/// Storage information.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbStorageInfo {
    #[prost(int32, tag = "1")]
    pub r#type: i32,
    #[prost(string, tag = "2")]
    pub mount_point: String,
    #[prost(bool, tag = "3")]
    pub final_result: bool,
    #[prost(string, tag = "4")]
    pub file_path: String,
    #[prost(int32, tag = "5")]
    pub available_storage_types: i32,
    #[prost(int64, tag = "6")]
    pub file_size: i64,
    #[prost(int64, repeated, tag = "7")]
    pub chunk_offsets: Vec<i64>,
}

/// Partition location.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbPartitionLocation {
    #[prost(enumeration = "pb_partition_location::Mode", tag = "1")]
    pub mode: i32,
    #[prost(int32, tag = "2")]
    pub id: i32,
    #[prost(int32, tag = "3")]
    pub epoch: i32,
    #[prost(string, tag = "4")]
    pub host: String,
    #[prost(int32, tag = "5")]
    pub rpc_port: i32,
    #[prost(int32, tag = "6")]
    pub push_port: i32,
    #[prost(int32, tag = "7")]
    pub fetch_port: i32,
    #[prost(int32, tag = "8")]
    pub replicate_port: i32,
    #[prost(message, optional, boxed, tag = "9")]
    pub peer: Option<Box<PbPartitionLocation>>,
    #[prost(message, optional, tag = "10")]
    pub storage_info: Option<PbStorageInfo>,
    #[prost(bytes = "vec", tag = "11")]
    pub map_id_bitmap: Vec<u8>,
}

pub mod pb_partition_location {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
    #[repr(i32)]
    pub enum Mode {
        Primary = 0,
        Replica = 1,
    }
}

/// Worker information.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbWorkerInfo {
    #[prost(string, tag = "1")]
    pub host: String,
    #[prost(int32, tag = "2")]
    pub rpc_port: i32,
    #[prost(int32, tag = "3")]
    pub push_port: i32,
    #[prost(int32, tag = "4")]
    pub fetch_port: i32,
    #[prost(int32, tag = "5")]
    pub replicate_port: i32,
}

/// Register shuffle request.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbRegisterShuffle {
    #[prost(int32, tag = "1")]
    pub shuffle_id: i32,
    #[prost(int32, tag = "2")]
    pub num_mappers: i32,
    #[prost(int32, tag = "3")]
    pub num_partitions: i32,
}

/// Register shuffle response.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbRegisterShuffleResponse {
    #[prost(int32, tag = "1")]
    pub status: i32,
    #[prost(message, repeated, tag = "2")]
    pub partition_locations: Vec<PbPartitionLocation>,
}

/// Request slots.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbRequestSlots {
    #[prost(string, tag = "1")]
    pub application_id: String,
    #[prost(int32, tag = "2")]
    pub shuffle_id: i32,
    #[prost(int32, repeated, tag = "3")]
    pub partition_id_list: Vec<i32>,
    #[prost(string, tag = "4")]
    pub hostname: String,
    #[prost(bool, tag = "5")]
    pub should_replicate: bool,
    #[prost(string, tag = "6")]
    pub request_id: String,
    #[prost(int32, tag = "7")]
    pub storage_type: i32,
    #[prost(message, optional, tag = "8")]
    pub user_identifier: Option<PbUserIdentifier>,
    #[prost(bool, tag = "9")]
    pub should_rack_aware: bool,
    #[prost(int32, tag = "10")]
    pub max_workers: i32,
    #[prost(int32, tag = "11")]
    pub available_storage_types: i32,
}

/// Worker resource.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbWorkerResource {
    #[prost(message, repeated, tag = "1")]
    pub primary_partitions: Vec<PbPartitionLocation>,
    #[prost(message, repeated, tag = "2")]
    pub replica_partitions: Vec<PbPartitionLocation>,
    #[prost(string, tag = "3")]
    pub network_location: String,
}

/// Request slots response.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbRequestSlotsResponse {
    #[prost(int32, tag = "1")]
    pub status: i32,
    #[prost(map = "string, message", tag = "2")]
    pub worker_resource: std::collections::HashMap<String, PbWorkerResource>,
}

/// Mapper end request.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbMapperEnd {
    #[prost(int32, tag = "1")]
    pub shuffle_id: i32,
    #[prost(int32, tag = "2")]
    pub map_id: i32,
    #[prost(int32, tag = "3")]
    pub attempt_id: i32,
    #[prost(int32, tag = "4")]
    pub num_mappers: i32,
    #[prost(int32, tag = "5")]
    pub partition_id: i32,
}

/// Mapper end response.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbMapperEndResponse {
    #[prost(int32, tag = "1")]
    pub status: i32,
}

/// Get reducer file group request.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbGetReducerFileGroup {
    #[prost(int32, tag = "1")]
    pub shuffle_id: i32,
}

/// File group.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbFileGroup {
    #[prost(message, repeated, tag = "1")]
    pub locations: Vec<PbPartitionLocation>,
}

/// Get reducer file group response.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbGetReducerFileGroupResponse {
    #[prost(int32, tag = "1")]
    pub status: i32,
    #[prost(map = "int32, message", tag = "2")]
    pub file_groups: std::collections::HashMap<i32, PbFileGroup>,
    #[prost(int32, repeated, tag = "3")]
    pub attempts: Vec<i32>,
    #[prost(int32, repeated, tag = "4")]
    pub partition_ids: Vec<i32>,
}

/// Unregister shuffle request.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbUnregisterShuffle {
    #[prost(string, tag = "1")]
    pub app_id: String,
    #[prost(int32, tag = "2")]
    pub shuffle_id: i32,
    #[prost(string, tag = "3")]
    pub request_id: String,
}

/// Unregister shuffle response.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbUnregisterShuffleResponse {
    #[prost(int32, tag = "1")]
    pub status: i32,
}

/// Heartbeat from application.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbHeartbeatFromApplication {
    #[prost(string, tag = "1")]
    pub app_id: String,
    #[prost(int64, tag = "2")]
    pub total_written: i64,
    #[prost(int64, tag = "3")]
    pub file_count: i64,
    #[prost(string, tag = "4")]
    pub request_id: String,
    #[prost(message, repeated, tag = "5")]
    pub need_checked_worker_list: Vec<PbWorkerInfo>,
    #[prost(bool, tag = "6")]
    pub should_response: bool,
}

/// Heartbeat from application response.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbHeartbeatFromApplicationResponse {
    #[prost(int32, tag = "1")]
    pub status: i32,
    #[prost(message, repeated, tag = "2")]
    pub excluded_workers: Vec<PbWorkerInfo>,
    #[prost(message, repeated, tag = "3")]
    pub unknown_workers: Vec<PbWorkerInfo>,
    #[prost(message, repeated, tag = "4")]
    pub shutting_workers: Vec<PbWorkerInfo>,
}

/// Reserve slots request.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbReserveSlots {
    #[prost(string, tag = "1")]
    pub application_id: String,
    #[prost(int32, tag = "2")]
    pub shuffle_id: i32,
    #[prost(message, repeated, tag = "3")]
    pub primary_locations: Vec<PbPartitionLocation>,
    #[prost(message, repeated, tag = "4")]
    pub replica_locations: Vec<PbPartitionLocation>,
    #[prost(int64, tag = "5")]
    pub split_threshold: i64,
    #[prost(int32, tag = "6")]
    pub split_mode: i32,
    #[prost(int32, tag = "7")]
    pub partition_type: i32,
    #[prost(bool, tag = "8")]
    pub range_read_filter: bool,
    #[prost(message, optional, tag = "9")]
    pub user_identifier: Option<PbUserIdentifier>,
    #[prost(int64, tag = "10")]
    pub push_data_timeout: i64,
    #[prost(bool, tag = "11")]
    pub partition_split_enabled: bool,
    #[prost(int32, tag = "12")]
    pub available_storage_types: i32,
}

/// Reserve slots response.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbReserveSlotsResponse {
    #[prost(int32, tag = "1")]
    pub status: i32,
    #[prost(string, tag = "2")]
    pub reason: String,
}

/// Commit files request.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbCommitFiles {
    #[prost(string, tag = "1")]
    pub application_id: String,
    #[prost(int32, tag = "2")]
    pub shuffle_id: i32,
    #[prost(string, repeated, tag = "3")]
    pub primary_ids: Vec<String>,
    #[prost(string, repeated, tag = "4")]
    pub replica_ids: Vec<String>,
    #[prost(int32, repeated, tag = "5")]
    pub map_attempts: Vec<i32>,
    #[prost(int64, tag = "6")]
    pub epoch: i64,
    #[prost(bool, tag = "7")]
    pub mock_failure: bool,
}

/// Commit files response.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbCommitFilesResponse {
    #[prost(int32, tag = "1")]
    pub status: i32,
    #[prost(string, repeated, tag = "2")]
    pub committed_primary_ids: Vec<String>,
    #[prost(string, repeated, tag = "3")]
    pub committed_replica_ids: Vec<String>,
    #[prost(string, repeated, tag = "4")]
    pub failed_primary_ids: Vec<String>,
    #[prost(string, repeated, tag = "5")]
    pub failed_replica_ids: Vec<String>,
    #[prost(map = "string, message", tag = "6")]
    pub committed_primary_storage_infos: std::collections::HashMap<String, PbStorageInfo>,
    #[prost(map = "string, message", tag = "7")]
    pub committed_replica_storage_infos: std::collections::HashMap<String, PbStorageInfo>,
    #[prost(int64, tag = "8")]
    pub total_written: i64,
    #[prost(int32, tag = "9")]
    pub file_count: i32,
}

/// Check quota request.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbCheckQuota {
    #[prost(message, optional, tag = "1")]
    pub user_identifier: Option<PbUserIdentifier>,
}

/// Check quota response.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbCheckQuotaResponse {
    #[prost(bool, tag = "1")]
    pub available: bool,
    #[prost(string, tag = "2")]
    pub reason: String,
}

/// Stage end request.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbStageEnd {
    #[prost(int32, tag = "1")]
    pub shuffle_id: i32,
}

/// Stage end response.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbStageEndResponse {
    #[prost(int32, tag = "1")]
    pub status: i32,
}

/// Revive partition info.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbRevivePartitionInfo {
    #[prost(int32, tag = "1")]
    pub partition_id: i32,
    #[prost(int32, tag = "2")]
    pub epoch: i32,
    #[prost(message, optional, tag = "3")]
    pub partition: Option<PbPartitionLocation>,
    #[prost(int32, tag = "4")]
    pub status: i32,
}

/// Revive request (change location).
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbRevive {
    #[prost(int32, tag = "1")]
    pub shuffle_id: i32,
    #[prost(int32, repeated, tag = "2")]
    pub map_id: Vec<i32>,
    #[prost(message, repeated, tag = "3")]
    pub partition_info: Vec<PbRevivePartitionInfo>,
}

/// Change location partition info.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbChangeLocationPartitionInfo {
    #[prost(int32, tag = "1")]
    pub partition_id: i32,
    #[prost(int32, tag = "2")]
    pub status: i32,
    #[prost(message, optional, tag = "3")]
    pub partition: Option<PbPartitionLocation>,
    #[prost(bool, tag = "4")]
    pub old_available: bool,
}

/// Change location response.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PbChangeLocationResponse {
    #[prost(int32, repeated, tag = "1")]
    pub ended_map_id: Vec<i32>,
    #[prost(message, repeated, tag = "2")]
    pub partition_info: Vec<PbChangeLocationPartitionInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn test_transport_message_encode_decode() {
        let register = PbRegisterShuffle {
            shuffle_id: 1,
            num_mappers: 10,
            num_partitions: 100,
        };
        
        let mut payload = Vec::new();
        register.encode(&mut payload).unwrap();
        let payload_len = payload.len();
        
        let msg = TransportMessage::new(
            TransportMessageType::RegisterShuffle,
            Bytes::from(payload),
        );
        
        let encoded = msg.encode();
        
        // Verify format: messageTypeValue (4B) + payloadLen (4B) + payload
        assert_eq!(encoded.len(), 4 + 4 + payload_len);
        
        // Verify message type
        let type_bytes: [u8; 4] = encoded[..4].try_into().unwrap();
        assert_eq!(i32::from_be_bytes(type_bytes), TransportMessageType::RegisterShuffle as i32);
        
        // Verify payload length
        let len_bytes: [u8; 4] = encoded[4..8].try_into().unwrap();
        assert_eq!(i32::from_be_bytes(len_bytes) as usize, payload_len);
        
        let decoded = TransportMessage::decode(encoded).unwrap();
        
        assert_eq!(decoded.message_type, TransportMessageType::RegisterShuffle);
        
        let decoded_register = PbRegisterShuffle::decode(decoded.payload).unwrap();
        assert_eq!(decoded_register.shuffle_id, 1);
        assert_eq!(decoded_register.num_mappers, 10);
        assert_eq!(decoded_register.num_partitions, 100);
    }
}
