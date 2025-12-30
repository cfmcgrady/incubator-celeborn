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

//! Error types for the Celeborn client.

use std::io;
use thiserror::Error;

/// Result type alias for Celeborn operations.
pub type Result<T> = std::result::Result<T, CelebornError>;

/// Errors that can occur during Celeborn client operations.
#[derive(Error, Debug)]
pub enum CelebornError {
    /// Network I/O error
    #[error("Network I/O error: {0}")]
    Io(#[from] io::Error),

    /// Connection error
    #[error("Connection error: {0}")]
    Connection(String),

    /// Protocol error (invalid message format, etc.)
    #[error("Protocol error: {0}")]
    Protocol(String),

    /// RPC timeout
    #[error("RPC timeout after {0}ms")]
    Timeout(u64),

    /// Server returned an error status
    #[error("Server error: {status:?} - {message}")]
    ServerError {
        status: StatusCode,
        message: String,
    },

    /// Shuffle not found
    #[error("Shuffle {0} not found")]
    ShuffleNotFound(i32),

    /// Partition not found
    #[error("Partition {partition_id} not found in shuffle {shuffle_id}")]
    PartitionNotFound {
        shuffle_id: i32,
        partition_id: i32,
    },

    /// Worker unavailable
    #[error("Worker {host}:{port} is unavailable")]
    WorkerUnavailable {
        host: String,
        port: i32,
    },

    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Compression error
    #[error("Compression error: {0}")]
    Compression(String),

    /// Push data failed
    #[error("Push data failed: {0}")]
    PushFailed(String),

    /// Fetch data failed
    #[error("Fetch data failed: {0}")]
    FetchFailed(String),

    /// Quota exceeded
    #[error("Quota exceeded: {0}")]
    QuotaExceeded(String),

    /// Revive failed
    #[error("Revive failed for shuffle {shuffle_id} partition {partition_id}: {status:?}")]
    ReviveFailed {
        shuffle_id: i32,
        partition_id: i32,
        status: StatusCode,
    },

    /// Stage ended
    #[error("Stage ended for shuffle {0}")]
    StageEnded(i32),

    /// Max retries exceeded
    #[error("Max retries ({max_retries}) exceeded: {message}")]
    MaxRetriesExceeded {
        max_retries: u32,
        message: String,
    },

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),

    /// Decompression failed
    #[error("Decompression failed: {0}")]
    DecompressionFailed(String),

    /// Invalid configuration
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
}

/// Status codes returned by Celeborn server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum StatusCode {
    // 1/0 Status
    Success = 0,
    PartialSuccess = 1,
    RequestFailed = 2,

    // Specific Status
    ShuffleAlreadyRegistered = 3,
    ShuffleNotRegistered = 4,
    ReserveSlotsFailed = 5,
    SlotNotAvailable = 6,
    WorkerNotFound = 7,
    PartitionNotFound = 8,
    ReplicaPartitionNotFound = 9,
    DeleteFilesFailed = 10,
    PartitionExists = 11,
    ReviveFailed = 12,
    ReplicateDataFailed = 13,
    NumMapperZero = 14,
    MapEnded = 15,
    StageEnded = 16,

    // push data fail causes
    PushDataFailNonCriticalCause = 17,
    PushDataWriteFailReplica = 18,
    PushDataWriteFailPrimary = 19,
    PushDataFailPartitionNotFound = 20,

    HardSplit = 21,
    SoftSplit = 22,

    StageEndTimeOut = 23,
    ShuffleDataLost = 24,
    WorkerShutdown = 25,
    NoAvailableWorkingDir = 26,
    WorkerExcluded = 27,
    WorkerUnknown = 28,

    CommitFileException = 29,

    // Rate limit statuses
    PushDataSuccessPrimaryCongested = 30,
    PushDataSuccessReplicaCongested = 31,

    PushDataHandshakeFailReplica = 32,
    PushDataHandshakeFailPrimary = 33,
    RegionStartFailReplica = 34,
    RegionStartFailPrimary = 35,
    RegionFinishFailReplica = 36,
    RegionFinishFailPrimary = 37,

    PushDataCreateConnectionFailPrimary = 38,
    PushDataCreateConnectionFailReplica = 39,
    PushDataConnectionExceptionPrimary = 40,
    PushDataConnectionExceptionReplica = 41,
    PushDataTimeoutPrimary = 42,
    PushDataTimeoutReplica = 43,
    PushDataPrimaryWorkerExcluded = 44,
    PushDataReplicaWorkerExcluded = 45,

    FetchDataTimeout = 46,
    ReviveInitialized = 47,
    DestroySlotsMockFailure = 48,
    CommitFilesMockFailure = 49,
    NoSplit = 54,

    Unknown = -1,
}

impl From<i32> for StatusCode {
    fn from(value: i32) -> Self {
        match value {
            0 => StatusCode::Success,
            1 => StatusCode::PartialSuccess,
            2 => StatusCode::RequestFailed,
            3 => StatusCode::ShuffleAlreadyRegistered,
            4 => StatusCode::ShuffleNotRegistered,
            5 => StatusCode::ReserveSlotsFailed,
            6 => StatusCode::SlotNotAvailable,
            7 => StatusCode::WorkerNotFound,
            8 => StatusCode::PartitionNotFound,
            9 => StatusCode::ReplicaPartitionNotFound,
            10 => StatusCode::DeleteFilesFailed,
            11 => StatusCode::PartitionExists,
            12 => StatusCode::ReviveFailed,
            13 => StatusCode::ReplicateDataFailed,
            14 => StatusCode::NumMapperZero,
            15 => StatusCode::MapEnded,
            16 => StatusCode::StageEnded,
            17 => StatusCode::PushDataFailNonCriticalCause,
            18 => StatusCode::PushDataWriteFailReplica,
            19 => StatusCode::PushDataWriteFailPrimary,
            20 => StatusCode::PushDataFailPartitionNotFound,
            21 => StatusCode::HardSplit,
            22 => StatusCode::SoftSplit,
            23 => StatusCode::StageEndTimeOut,
            24 => StatusCode::ShuffleDataLost,
            25 => StatusCode::WorkerShutdown,
            26 => StatusCode::NoAvailableWorkingDir,
            27 => StatusCode::WorkerExcluded,
            28 => StatusCode::WorkerUnknown,
            29 => StatusCode::CommitFileException,
            30 => StatusCode::PushDataSuccessPrimaryCongested,
            31 => StatusCode::PushDataSuccessReplicaCongested,
            32 => StatusCode::PushDataHandshakeFailReplica,
            33 => StatusCode::PushDataHandshakeFailPrimary,
            34 => StatusCode::RegionStartFailReplica,
            35 => StatusCode::RegionStartFailPrimary,
            36 => StatusCode::RegionFinishFailReplica,
            37 => StatusCode::RegionFinishFailPrimary,
            38 => StatusCode::PushDataCreateConnectionFailPrimary,
            39 => StatusCode::PushDataCreateConnectionFailReplica,
            40 => StatusCode::PushDataConnectionExceptionPrimary,
            41 => StatusCode::PushDataConnectionExceptionReplica,
            42 => StatusCode::PushDataTimeoutPrimary,
            43 => StatusCode::PushDataTimeoutReplica,
            44 => StatusCode::PushDataPrimaryWorkerExcluded,
            45 => StatusCode::PushDataReplicaWorkerExcluded,
            46 => StatusCode::FetchDataTimeout,
            47 => StatusCode::ReviveInitialized,
            48 => StatusCode::DestroySlotsMockFailure,
            49 => StatusCode::CommitFilesMockFailure,
            54 => StatusCode::NoSplit,
            _ => StatusCode::Unknown,
        }
    }
}

impl StatusCode {
    /// Check if the status indicates success.
    pub fn is_success(&self) -> bool {
        matches!(self, StatusCode::Success | StatusCode::PartialSuccess)
    }

    /// Check if the status indicates a retriable error.
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            StatusCode::PushDataTimeoutPrimary
                | StatusCode::PushDataTimeoutReplica
                | StatusCode::FetchDataTimeout
                | StatusCode::WorkerShutdown
                | StatusCode::WorkerExcluded
                | StatusCode::PushDataCreateConnectionFailPrimary
                | StatusCode::PushDataCreateConnectionFailReplica
        )
    }
}
