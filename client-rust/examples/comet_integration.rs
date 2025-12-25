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

//! Example: Apache Spark Comet Integration with Celeborn Rust Client
//!
//! This example demonstrates how to use the Celeborn Rust client in a
//! Driver-Executor separation architecture, which is required for
//! Apache Spark Comet (vectorized execution engine).
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     Spark Driver (JVM)                          │
//! │  ┌─────────────────────────────────────────────────────────────┐│
//! │  │              LifecycleManager (Scala)                       ││
//! │  │  - Manages shuffle lifecycle                                ││
//! │  │  - Communicates with Celeborn Master                        ││
//! │  │  - Allocates partition locations                            ││
//! │  │  - Listens on RPC port (e.g., 9098)                         ││
//! │  └─────────────────────────────────────────────────────────────┘│
//! │                           ▲                                      │
//! │                           │ Netty RPC                            │
//! └───────────────────────────┼──────────────────────────────────────┘
//!                             │
//! ┌───────────────────────────┼──────────────────────────────────────┐
//! │                     Spark Executor (Comet/Rust via JNI)          │
//! │                           │                                      │
//! │  ┌────────────────────────▼────────────────────────────────────┐│
//! │  │           ExecutorShuffleClient (Rust)                      ││
//! │  │  - Connects to Driver's LifecycleManager via RPC            ││
//! │  │  - Pushes data directly to Celeborn Workers                 ││
//! │  │  - Fetches data directly from Celeborn Workers              ││
//! │  └─────────────────────────────────────────────────────────────┘│
//! │                           │                                      │
//! │                           ▼                                      │
//! │  ┌─────────────────────────────────────────────────────────────┐│
//! │  │              Celeborn Workers                               ││
//! │  │  - Store shuffle data                                       ││
//! │  │  - Serve fetch requests                                     ││
//! │  └─────────────────────────────────────────────────────────────┘│
//! └──────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Usage in Comet
//!
//! In Comet, the Rust code runs in the Executor via JNI. The typical flow is:
//!
//! 1. **Initialization**: When the Executor starts, create an `ExecutorShuffleClient`
//! 2. **Setup**: Call `setup_lifecycle_manager_ref()` with the Driver's host and port
//! 3. **Write Path**: Use `register_shuffle()`, `push_data()`, and `mapper_end()`
//! 4. **Read Path**: Use `read_partition()` to create an input stream
//! 5. **Cleanup**: Call `shutdown()` when done

use celeborn_client::{CelebornConfig, ExecutorShuffleClient};
use std::sync::Arc;

/// Simulates the Comet Executor-side shuffle write operation.
async fn comet_shuffle_write(
    client: &ExecutorShuffleClient,
    shuffle_id: i32,
    map_id: i32,
    attempt_id: i32,
    num_mappers: i32,
    num_partitions: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Comet Shuffle Write ===");
    println!(
        "shuffle_id={}, map_id={}, attempt_id={}",
        shuffle_id, map_id, attempt_id
    );

    // Step 1: Register shuffle (if not already registered)
    // In practice, this might be called once per shuffle from the Driver
    // and the Executor just uses the existing registration
    println!("Registering shuffle...");
    client
        .register_shuffle(shuffle_id, num_mappers, num_partitions)
        .await?;
    println!("Shuffle registered successfully");

    // Step 2: Push data for each partition
    // In Comet, this would be called from the vectorized execution engine
    // with Arrow record batches serialized to bytes
    for partition_id in 0..num_partitions {
        // Simulate Arrow record batch data
        let data = format!(
            "Arrow batch data for partition {} from map {}",
            partition_id, map_id
        );
        let data_bytes = data.as_bytes();

        println!(
            "Pushing {} bytes to partition {}",
            data_bytes.len(),
            partition_id
        );
        client
            .push_data(shuffle_id, map_id, attempt_id, partition_id, data_bytes)
            .await?;
    }

    // Step 3: Signal mapper end
    println!("Signaling mapper end...");
    let success = client
        .mapper_end(shuffle_id, map_id, attempt_id, num_mappers)
        .await?;
    println!("Mapper end result: {}", success);

    Ok(())
}

/// Simulates the Comet Executor-side shuffle read operation.
async fn comet_shuffle_read(
    client: &ExecutorShuffleClient,
    shuffle_id: i32,
    partition_id: i32,
    attempt_number: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Comet Shuffle Read ===");
    println!(
        "shuffle_id={}, partition_id={}, attempt={}",
        shuffle_id, partition_id, attempt_number
    );

    // Create input stream for reading partition data
    println!("Creating input stream for partition {}...", partition_id);
    let stream = client
        .read_partition(
            shuffle_id,
            partition_id,
            attempt_number,
            0,  // start_map_index
            -1, // end_map_index (-1 means all)
        )
        .await?;

    // In Comet, you would read Arrow record batches from this stream
    // and feed them to the vectorized execution engine
    println!("Input stream created successfully");
    
    // Note: CelebornInputStream doesn't implement Debug, so we just confirm it was created
    let _ = stream; // Use the stream variable to avoid unused warning

    // Read data from stream
    // let mut total_bytes = 0;
    // while let Some(chunk) = stream.next().await {
    //     total_bytes += chunk.len();
    // }
    // println!("Read {} bytes from partition {}", total_bytes, partition_id);

    Ok(())
}

/// JNI entry point simulation.
///
/// In actual Comet integration, this would be called from Java via JNI.
/// The `driver_host` and `driver_port` would be passed from the Spark Driver.
#[allow(dead_code)]
async fn jni_init_shuffle_client(
    app_id: &str,
    driver_host: &str,
    driver_port: i32,
) -> Result<Arc<ExecutorShuffleClient>, Box<dyn std::error::Error>> {
    // Create configuration
    // In practice, these settings would come from Spark configuration
    let config = CelebornConfig::builder()
        .app_id(app_id)
        .master_endpoints(vec!["localhost:9097".to_string()]) // Not used in Executor mode
        .push_buffer_size(64 * 1024)
        .build()?;

    // Create executor shuffle client
    let client = ExecutorShuffleClient::new(config);

    // Connect to Driver's LifecycleManager
    client
        .setup_lifecycle_manager_ref(driver_host, driver_port)
        .await?;

    Ok(Arc::new(client))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("Celeborn Rust Client - Comet Integration Example");
    println!("================================================\n");

    // Configuration
    let app_id = "comet-app-001";
    let driver_host = "localhost";
    let driver_port = 9098; // LifecycleManager RPC port

    println!("Configuration:");
    println!("  App ID: {}", app_id);
    println!("  Driver Host: {}", driver_host);
    println!("  Driver Port: {}", driver_port);
    println!();

    // Create configuration
    let config = CelebornConfig::builder()
        .app_id(app_id)
        .master_endpoints(vec!["localhost:9097".to_string()])
        .build()?;

    // Create executor shuffle client
    let client = ExecutorShuffleClient::new(config);

    // In a real scenario, you would connect to the actual Driver
    // For this example, we'll skip the connection step
    println!("Note: This example requires a running Spark Driver with LifecycleManager");
    println!("      listening on {}:{}", driver_host, driver_port);
    println!();

    // Uncomment the following to actually connect and run:
    /*
    // Connect to Driver's LifecycleManager
    println!("Connecting to Driver's LifecycleManager...");
    client.setup_lifecycle_manager_ref(driver_host, driver_port).await?;
    println!("Connected successfully!\n");

    // Simulate shuffle write (Map task)
    let shuffle_id = 0;
    let map_id = 0;
    let attempt_id = 0;
    let num_mappers = 4;
    let num_partitions = 10;

    comet_shuffle_write(
        &client,
        shuffle_id,
        map_id,
        attempt_id,
        num_mappers,
        num_partitions,
    ).await?;

    // Simulate shuffle read (Reduce task)
    let partition_id = 0;
    let attempt_number = 0;

    comet_shuffle_read(
        &client,
        shuffle_id,
        partition_id,
        attempt_number,
    ).await?;

    // Cleanup
    println!("\n=== Cleanup ===");
    client.cleanup_shuffle(shuffle_id);
    client.shutdown().await;
    println!("Client shutdown complete");
    */

    println!("Example code structure:");
    println!("1. Create ExecutorShuffleClient with CelebornConfig");
    println!("2. Call setup_lifecycle_manager_ref(driver_host, driver_port)");
    println!("3. For write: register_shuffle() -> push_data() -> mapper_end()");
    println!("4. For read: read_partition() -> iterate over stream");
    println!("5. Cleanup: cleanup_shuffle() -> shutdown()");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let config = CelebornConfig::builder()
            .app_id("test-app")
            .master_endpoints(vec!["localhost:9097".to_string()])
            .build()
            .unwrap();

        let client = ExecutorShuffleClient::new(config);
        assert_eq!(client.app_id(), "test-app");
    }

    #[test]
    fn test_shuffle_key_generation() {
        let config = CelebornConfig::builder()
            .app_id("test-app")
            .master_endpoints(vec!["localhost:9097".to_string()])
            .build()
            .unwrap();

        let client = ExecutorShuffleClient::new(config);
        assert_eq!(client.shuffle_key(0), "test-app-0");
        assert_eq!(client.shuffle_key(123), "test-app-123");
    }
}
