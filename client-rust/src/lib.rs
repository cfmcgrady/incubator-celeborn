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

//! # Celeborn Rust Client
//!
//! A Rust implementation of the Apache Celeborn client for distributed shuffle operations.
//!
//! ## Overview
//!
//! Apache Celeborn is a distributed shuffle service that provides high-performance
//! shuffle data management for distributed computing frameworks like Apache Spark
//! and Apache Flink.
//!
//! This crate provides a native Rust client that can:
//! - Register and manage shuffle operations
//! - Push shuffle data to Celeborn workers
//! - Fetch shuffle data from Celeborn workers
//! - Handle partition management and failover
//!
//! ## Example
//!
//! ```rust,no_run
//! use celeborn_client::{CelebornClient, CelebornConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create configuration
//!     let config = CelebornConfig::builder()
//!         .master_endpoints(vec!["localhost:9097".to_string()])
//!         .app_id("my-app-001")
//!         .build()?;
//!
//!     // Create client
//!     let client = CelebornClient::new(config).await?;
//!
//!     // Register a shuffle
//!     let shuffle_id = client.register_shuffle(0, 10, 100).await?;
//!
//!     // Push data (shuffle_id, map_id, attempt_id, partition_id, data)
//!     let data = vec![1u8, 2, 3, 4, 5];
//!     client.push_data(shuffle_id, 0, 0, 0, &data).await?;
//!
//!     // Commit and fetch
//!     client.mapper_end(shuffle_id, 0, 0, 10).await?;
//!
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod config;
pub mod error;
pub mod network;
pub mod protocol;

// Re-exports for convenience
pub use client::{CelebornClient, ShuffleClient};
pub use config::CelebornConfig;
pub use error::{CelebornError, Result};
pub use protocol::PartitionLocation;
