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
    Success = 0,
    PartialSuccess = 1,
    ShuffleAlreadyRegistered = 2,
    ShuffleNotRegistered = 3,
    ReserveSlotsFailed = 4,
    SlotNotAvailable = 5,
    WorkerNotFound = 6,
    PartitionNotFound = 7,
    PrimaryPushDataFailed = 8,
    ReplicaPushDataFailed = 9,
    NumMapperZero = 10,
    MapEnded = 11,
    StageEnded = 12,
    PushDataFailNonCriticalCause = 13,
    PushDataFailSlave = 14,
    PushDataFailMain = 15,
    PushDataWriteFail = 16,
    PushDataFailPartitionNotFound = 17,
    HardSplit = 18,
    SoftSplit = 19,
    StageEndTimeOut = 20,
    ShuffleDataLost = 21,
    WorkerShutdown = 22,
    NoAvailableWorkingDir = 23,
    WorkerExcluded = 24,
    WorkerUnknown = 25,
    PushDataTimeout = 26,
    ReviveFailed = 27,
    PushDataHandShakeFailNonCriticalCause = 28,
    RegionStartFailNonCriticalCause = 29,
    RegionFinishFailNonCriticalCause = 30,
    PushDataCreateConnectionFail = 31,
    FetchDataTimeout = 32,
    RequestFailed = 33,
    RpcFailed = 34,
    // Additional status codes for push failures
    PushDataFailPrimary = 35,
    PushDataFailReplica = 36,
    PushDataCreateConnectionFailPrimary = 37,
    PushDataCreateConnectionFailReplica = 38,
    PushDataConnectionExceptionPrimary = 39,
    PushDataConnectionExceptionReplica = 40,
    PushDataTimeoutPrimary = 41,
    PushDataTimeoutReplica = 42,
    PushDataSuccessPrimaryCongested = 43,
    PushDataSuccessReplicaCongested = 44,
    Unknown = -1,
}

impl From<i32> for StatusCode {
    fn from(value: i32) -> Self {
        match value {
            0 => StatusCode::Success,
            1 => StatusCode::PartialSuccess,
            2 => StatusCode::ShuffleAlreadyRegistered,
            3 => StatusCode::ShuffleNotRegistered,
            4 => StatusCode::ReserveSlotsFailed,
            5 => StatusCode::SlotNotAvailable,
            6 => StatusCode::WorkerNotFound,
            7 => StatusCode::PartitionNotFound,
            8 => StatusCode::PrimaryPushDataFailed,
            9 => StatusCode::ReplicaPushDataFailed,
            10 => StatusCode::NumMapperZero,
            11 => StatusCode::MapEnded,
            12 => StatusCode::StageEnded,
            13 => StatusCode::PushDataFailNonCriticalCause,
            14 => StatusCode::PushDataFailSlave,
            15 => StatusCode::PushDataFailMain,
            16 => StatusCode::PushDataWriteFail,
            17 => StatusCode::PushDataFailPartitionNotFound,
            18 => StatusCode::HardSplit,
            19 => StatusCode::SoftSplit,
            20 => StatusCode::StageEndTimeOut,
            21 => StatusCode::ShuffleDataLost,
            22 => StatusCode::WorkerShutdown,
            23 => StatusCode::NoAvailableWorkingDir,
            24 => StatusCode::WorkerExcluded,
            25 => StatusCode::WorkerUnknown,
            26 => StatusCode::PushDataTimeout,
            27 => StatusCode::ReviveFailed,
            28 => StatusCode::PushDataHandShakeFailNonCriticalCause,
            29 => StatusCode::RegionStartFailNonCriticalCause,
            30 => StatusCode::RegionFinishFailNonCriticalCause,
            31 => StatusCode::PushDataCreateConnectionFail,
            32 => StatusCode::FetchDataTimeout,
            33 => StatusCode::RequestFailed,
            34 => StatusCode::RpcFailed,
            35 => StatusCode::PushDataFailPrimary,
            36 => StatusCode::PushDataFailReplica,
            37 => StatusCode::PushDataCreateConnectionFailPrimary,
            38 => StatusCode::PushDataCreateConnectionFailReplica,
            39 => StatusCode::PushDataConnectionExceptionPrimary,
            40 => StatusCode::PushDataConnectionExceptionReplica,
            41 => StatusCode::PushDataTimeoutPrimary,
            42 => StatusCode::PushDataTimeoutReplica,
            43 => StatusCode::PushDataSuccessPrimaryCongested,
            44 => StatusCode::PushDataSuccessReplicaCongested,
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
            StatusCode::PushDataTimeout
                | StatusCode::FetchDataTimeout
                | StatusCode::WorkerShutdown
                | StatusCode::WorkerExcluded
                | StatusCode::PushDataCreateConnectionFail
        )
    }
}
