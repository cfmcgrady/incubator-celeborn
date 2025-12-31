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

//! Configuration for the Celeborn client.

use crate::error::{CelebornError, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Compression codec for shuffle data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionCodec {
    /// No compression
    None,
    /// LZ4 compression
    Lz4,
    /// Zstd compression
    Zstd,
}

impl Default for CompressionCodec {
    fn default() -> Self {
        CompressionCodec::Lz4
    }
}

impl CompressionCodec {
    /// Check if the compression codec is enabled via feature flags.
    pub fn is_enabled(&self) -> bool {
        match self {
            CompressionCodec::None => true,
            CompressionCodec::Lz4 => cfg!(feature = "compression-lz4"),
            CompressionCodec::Zstd => cfg!(feature = "compression-zstd"),
        }
    }
}

/// Partition split mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PartitionSplitMode {
    /// Soft split - allows data to continue writing to old partition
    Soft,
    /// Hard split - immediately switches to new partition
    Hard,
}

impl Default for PartitionSplitMode {
    fn default() -> Self {
        PartitionSplitMode::Soft
    }
}

/// Storage type for shuffle data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum StorageType {
    /// Memory storage
    Memory,
    /// Local disk storage
    Hdd,
    /// SSD storage
    Ssd,
    /// HDFS storage
    Hdfs,
    /// S3 storage
    S3,
}

impl Default for StorageType {
    fn default() -> Self {
        StorageType::Hdd
    }
}

/// Configuration for the Celeborn client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CelebornConfig {
    /// Application unique identifier
    pub app_id: String,

    /// Master endpoints (host:port)
    pub master_endpoints: Vec<String>,

    /// Whether to enable push replication
    #[serde(default = "default_push_replicate_enabled")]
    pub push_replicate_enabled: bool,

    /// Push data timeout
    #[serde(default = "default_push_timeout")]
    pub push_timeout: Duration,

    /// Fetch data timeout
    #[serde(default = "default_fetch_timeout")]
    pub fetch_timeout: Duration,

    /// RPC timeout
    #[serde(default = "default_rpc_timeout")]
    pub rpc_timeout: Duration,

    /// Maximum retries for RPC calls
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Retry wait time
    #[serde(default = "default_retry_wait")]
    pub retry_wait: Duration,

    /// Compression codec
    #[serde(default)]
    pub compression_codec: CompressionCodec,

    /// Partition split threshold in bytes
    #[serde(default = "default_partition_split_threshold")]
    pub partition_split_threshold: u64,

    /// Partition split mode
    #[serde(default)]
    pub partition_split_mode: PartitionSplitMode,

    /// Push buffer size
    #[serde(default = "default_push_buffer_size")]
    pub push_buffer_size: usize,

    /// Maximum in-flight requests per connection
    #[serde(default = "default_max_in_flight")]
    pub max_in_flight_requests: usize,

    /// Maximum in-flight fetch requests for partition reader
    #[serde(default = "default_fetch_max_reqs_in_flight")]
    pub fetch_max_reqs_in_flight: usize,

    /// Maximum retries for fetch operations
    #[serde(default = "default_max_fetch_retries")]
    pub max_fetch_retries: u32,

    /// Connection pool size per worker
    #[serde(default = "default_connection_pool_size")]
    pub connection_pool_size: usize,

    /// Heartbeat interval
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval: Duration,

    /// User identifier for quota management
    pub user_identifier: Option<UserIdentifier>,

    /// Available storage types
    #[serde(default = "default_storage_types")]
    pub available_storage_types: Vec<StorageType>,
}

/// User identifier for quota and authentication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserIdentifier {
    pub tenant_id: String,
    pub name: String,
}

// Default value functions
fn default_push_replicate_enabled() -> bool {
    false
}

fn default_push_timeout() -> Duration {
    Duration::from_secs(120)
}

fn default_fetch_timeout() -> Duration {
    Duration::from_secs(600)
}

fn default_rpc_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_max_retries() -> u32 {
    3
}

fn default_retry_wait() -> Duration {
    Duration::from_secs(1)
}

fn default_partition_split_threshold() -> u64 {
    1024 * 1024 * 1024 // 1GB
}

fn default_push_buffer_size() -> usize {
    64 * 1024 // 64KB
}

fn default_max_in_flight() -> usize {
    32
}

fn default_connection_pool_size() -> usize {
    4
}

fn default_fetch_max_reqs_in_flight() -> usize {
    3
}

fn default_max_fetch_retries() -> u32 {
    3
}

fn default_heartbeat_interval() -> Duration {
    Duration::from_secs(15)
}

fn default_storage_types() -> Vec<StorageType> {
    vec![StorageType::Hdd, StorageType::Ssd]
}

impl Default for CelebornConfig {
    fn default() -> Self {
        Self {
            app_id: "default-app".to_string(),
            master_endpoints: vec!["localhost:9097".to_string()],
            push_replicate_enabled: default_push_replicate_enabled(),
            push_timeout: default_push_timeout(),
            fetch_timeout: default_fetch_timeout(),
            rpc_timeout: default_rpc_timeout(),
            max_retries: default_max_retries(),
            retry_wait: default_retry_wait(),
            compression_codec: CompressionCodec::default(),
            partition_split_threshold: default_partition_split_threshold(),
            partition_split_mode: PartitionSplitMode::default(),
            push_buffer_size: default_push_buffer_size(),
            max_in_flight_requests: default_max_in_flight(),
            fetch_max_reqs_in_flight: default_fetch_max_reqs_in_flight(),
            max_fetch_retries: default_max_fetch_retries(),
            connection_pool_size: default_connection_pool_size(),
            heartbeat_interval: default_heartbeat_interval(),
            user_identifier: None,
            available_storage_types: default_storage_types(),
        }
    }
}

impl CelebornConfig {
    /// Create a new configuration builder.
    pub fn builder() -> CelebornConfigBuilder {
        CelebornConfigBuilder::default()
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        if self.app_id.is_empty() {
            return Err(CelebornError::Config("app_id cannot be empty".to_string()));
        }

        if self.master_endpoints.is_empty() {
            return Err(CelebornError::Config(
                "master_endpoints cannot be empty".to_string(),
            ));
        }

        for endpoint in &self.master_endpoints {
            if !endpoint.contains(':') {
                return Err(CelebornError::Config(format!(
                    "Invalid master endpoint format: {}. Expected host:port",
                    endpoint
                )));
            }
        }

        if !self.compression_codec.is_enabled() {
            return Err(CelebornError::Config(format!(
                "Compression codec {:?} is selected but the corresponding feature is not enabled",
                self.compression_codec
            )));
        }

        Ok(())
    }
}

/// Builder for CelebornConfig.
#[derive(Debug, Default)]
pub struct CelebornConfigBuilder {
    app_id: Option<String>,
    master_endpoints: Vec<String>,
    push_replicate_enabled: Option<bool>,
    push_timeout: Option<Duration>,
    fetch_timeout: Option<Duration>,
    rpc_timeout: Option<Duration>,
    max_retries: Option<u32>,
    retry_wait: Option<Duration>,
    compression_codec: Option<CompressionCodec>,
    partition_split_threshold: Option<u64>,
    partition_split_mode: Option<PartitionSplitMode>,
    push_buffer_size: Option<usize>,
    max_in_flight_requests: Option<usize>,
    fetch_max_reqs_in_flight: Option<usize>,
    max_fetch_retries: Option<u32>,
    connection_pool_size: Option<usize>,
    heartbeat_interval: Option<Duration>,
    user_identifier: Option<UserIdentifier>,
    available_storage_types: Option<Vec<StorageType>>,
}

impl CelebornConfigBuilder {
    /// Set the application ID.
    pub fn app_id(mut self, app_id: impl Into<String>) -> Self {
        self.app_id = Some(app_id.into());
        self
    }

    /// Set the master endpoints.
    pub fn master_endpoints(mut self, endpoints: Vec<String>) -> Self {
        self.master_endpoints = endpoints;
        self
    }

    /// Add a master endpoint.
    pub fn add_master_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.master_endpoints.push(endpoint.into());
        self
    }

    /// Enable or disable push replication.
    pub fn push_replicate_enabled(mut self, enabled: bool) -> Self {
        self.push_replicate_enabled = Some(enabled);
        self
    }

    /// Set the push timeout.
    pub fn push_timeout(mut self, timeout: Duration) -> Self {
        self.push_timeout = Some(timeout);
        self
    }

    /// Set the fetch timeout.
    pub fn fetch_timeout(mut self, timeout: Duration) -> Self {
        self.fetch_timeout = Some(timeout);
        self
    }

    /// Set the RPC timeout.
    pub fn rpc_timeout(mut self, timeout: Duration) -> Self {
        self.rpc_timeout = Some(timeout);
        self
    }

    /// Set the maximum retries.
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.max_retries = Some(retries);
        self
    }

    /// Set the compression codec.
    pub fn compression_codec(mut self, codec: CompressionCodec) -> Self {
        self.compression_codec = Some(codec);
        self
    }

    /// Set the partition split threshold.
    pub fn partition_split_threshold(mut self, threshold: u64) -> Self {
        self.partition_split_threshold = Some(threshold);
        self
    }

    /// Set the push buffer size.
    pub fn push_buffer_size(mut self, size: usize) -> Self {
        self.push_buffer_size = Some(size);
        self
    }

    /// Set the maximum in-flight requests per connection.
    pub fn max_in_flight_requests(mut self, count: usize) -> Self {
        self.max_in_flight_requests = Some(count);
        self
    }

    /// Set the connection pool size.
    pub fn connection_pool_size(mut self, size: usize) -> Self {
        self.connection_pool_size = Some(size);
        self
    }

    /// Set the user identifier.
    pub fn user_identifier(mut self, tenant_id: impl Into<String>, name: impl Into<String>) -> Self {
        self.user_identifier = Some(UserIdentifier {
            tenant_id: tenant_id.into(),
            name: name.into(),
        });
        self
    }

    /// Build the configuration.
    pub fn build(self) -> Result<CelebornConfig> {
        let config = CelebornConfig {
            app_id: self.app_id.ok_or_else(|| {
                CelebornError::Config("app_id is required".to_string())
            })?,
            master_endpoints: self.master_endpoints,
            push_replicate_enabled: self.push_replicate_enabled.unwrap_or_else(default_push_replicate_enabled),
            push_timeout: self.push_timeout.unwrap_or_else(default_push_timeout),
            fetch_timeout: self.fetch_timeout.unwrap_or_else(default_fetch_timeout),
            rpc_timeout: self.rpc_timeout.unwrap_or_else(default_rpc_timeout),
            max_retries: self.max_retries.unwrap_or_else(default_max_retries),
            retry_wait: self.retry_wait.unwrap_or_else(default_retry_wait),
            compression_codec: self.compression_codec.unwrap_or_default(),
            partition_split_threshold: self.partition_split_threshold.unwrap_or_else(default_partition_split_threshold),
            partition_split_mode: self.partition_split_mode.unwrap_or_default(),
            push_buffer_size: self.push_buffer_size.unwrap_or_else(default_push_buffer_size),
            max_in_flight_requests: self.max_in_flight_requests.unwrap_or_else(default_max_in_flight),
            fetch_max_reqs_in_flight: self.fetch_max_reqs_in_flight.unwrap_or_else(default_fetch_max_reqs_in_flight),
            max_fetch_retries: self.max_fetch_retries.unwrap_or_else(default_max_fetch_retries),
            connection_pool_size: self.connection_pool_size.unwrap_or_else(default_connection_pool_size),
            heartbeat_interval: self.heartbeat_interval.unwrap_or_else(default_heartbeat_interval),
            user_identifier: self.user_identifier,
            available_storage_types: self.available_storage_types.unwrap_or_else(default_storage_types),
        };

        config.validate()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = CelebornConfig::builder()
            .app_id("test-app")
            .master_endpoints(vec!["localhost:9097".to_string()])
            .push_replicate_enabled(true)
            .compression_codec(CompressionCodec::Zstd)
            .build()
            .unwrap();

        assert_eq!(config.app_id, "test-app");
        assert_eq!(config.master_endpoints, vec!["localhost:9097"]);
        assert!(config.push_replicate_enabled);
        assert_eq!(config.compression_codec, CompressionCodec::Zstd);
    }

    #[test]
    fn test_config_validation() {
        // Missing app_id
        let result = CelebornConfig::builder()
            .master_endpoints(vec!["localhost:9097".to_string()])
            .build();
        assert!(result.is_err());

        // Empty master_endpoints
        let result = CelebornConfig::builder()
            .app_id("test-app")
            .build();
        assert!(result.is_err());

        // Invalid endpoint format
        let result = CelebornConfig::builder()
            .app_id("test-app")
            .master_endpoints(vec!["localhost".to_string()])
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_config_compression_feature_check() {
        // Zstd is not enabled in default features (I'm running this test without features)
        // Note: When running with --features compression-zstd, this test might need adjustment
        // but for a plain 'cargo test', it should work.
        
        if !cfg!(feature = "compression-zstd") {
            let result = CelebornConfig::builder()
                .app_id("test-app")
                .master_endpoints(vec!["localhost:9097".to_string()])
                .compression_codec(CompressionCodec::Zstd)
                .build();
            
            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(err.to_string().contains("corresponding feature is not enabled"));
        }
    }
}
