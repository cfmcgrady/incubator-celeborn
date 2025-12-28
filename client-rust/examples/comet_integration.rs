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

//! Comet Integration Example
//!
//! This example demonstrates how the Rust ExecutorShuffleClient integrates
//! with Apache Spark Comet for Celeborn shuffle.
//!
//! Architecture:
//! ```
//! +------------------+     +------------------+     +------------------+
//! |   Spark Driver   |     |  Spark Executor  |     | Celeborn Workers |
//! |       (JVM)      |     |   (Comet/Rust)   |     |                  |
//! +------------------+     +------------------+     +------------------+
//! |                  |     |                  |     |                  |
//! | LifecycleManager |<--->| ExecutorShuffle  |---->|   Push Data      |
//! |   (Java/Scala)   | RPC |    Client        |     |                  |
//! |                  |     |    (Rust)        |<----|   Fetch Data     |
//! +------------------+     +------------------+     +------------------+
//! ```
//!
//! Usage:
//!   # Start Java LifecycleManager test server first
//!   java -cp ... org.apache.celeborn.client.TestLifecycleManagerServer 9098
//!
//!   # Then run this example
//!   cargo run --example comet_integration -- localhost 9098
//!
//! Or with environment variables:
//!   LIFECYCLE_MANAGER_HOST=localhost LIFECYCLE_MANAGER_PORT=9098 \
//!   cargo run --example comet_integration

use std::env;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use celeborn_client::client::ExecutorShuffleClient;
use celeborn_client::config::CelebornConfig;

/// Simulates the Comet shuffle write flow.
///
/// In real Comet integration:
/// 1. CometCelebornShuffleWriter (Scala) receives ShuffleHandle with LM address
/// 2. It calls JNI to create Rust ExecutorShuffleClient
/// 3. Rust client connects to Java LifecycleManager
/// 4. Rust client pushes data directly to Celeborn Workers
async fn simulate_comet_shuffle_write(
    lm_host: &str,
    lm_port: i32,
    shuffle_id: i32,
    map_id: i32,
    num_partitions: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Comet Shuffle Write Simulation ===\n");

    // Step 1: Create ExecutorShuffleClient (done in JNI createCelebornClient)
    println!("Step 1: Creating ExecutorShuffleClient...");
    let config = CelebornConfig::builder()
        .app_id(&format!(
            "comet-app-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
                % 100000
        ))
        .master_endpoints(vec!["localhost:9097".to_string()])
        .push_buffer_size(64 * 1024)
        .build()?;

    let client = ExecutorShuffleClient::new(config);
    println!("  ✓ Client created with app_id: {}", client.app_id());

    // Step 2: Connect to LifecycleManager (done in JNI with host/port from ShuffleHandle)
    println!("\nStep 2: Connecting to LifecycleManager at {}:{}...", lm_host, lm_port);
    client.setup_lifecycle_manager_ref(lm_host, lm_port).await?;
    println!("  ✓ Connected to LifecycleManager");

    // Step 3: Register shuffle (if not already registered)
    println!("\nStep 3: Registering shuffle {}...", shuffle_id);
    let num_mappers = 2;
    client.register_shuffle(shuffle_id, num_mappers, num_partitions).await?;
    println!("  ✓ Shuffle registered");

    // Step 4: Get partition locations
    println!("\nStep 4: Getting partition locations...");
    for partition_id in 0..num_partitions {
        match client.get_partition_location(shuffle_id, partition_id) {
            Ok(locations) => {
                if !locations.is_empty() {
                    let loc = &locations[0];
                    println!(
                        "  Partition {}: {}:{} (epoch={})",
                        partition_id, loc.host, loc.push_port, loc.epoch
                    );
                }
            }
            Err(e) => {
                println!("  Partition {}: Error - {}", partition_id, e);
            }
        }
    }

    // Step 5: Push data (simulated - in real Comet, this would be Arrow data)
    println!("\nStep 5: Pushing data to partitions...");
    for partition_id in 0..num_partitions {
        let data = format!(
            "Comet shuffle data for partition {} from map {} at {}",
            partition_id,
            map_id,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );

        match client
            .push_data(shuffle_id, map_id, 0, partition_id, data.as_bytes())
            .await
        {
            Ok(_) => {
                println!("  ✓ Pushed {} bytes to partition {}", data.len(), partition_id);
            }
            Err(e) => {
                println!("  ✗ Failed to push to partition {}: {}", partition_id, e);
            }
        }
    }

    // Step 6: Signal mapper end
    println!("\nStep 6: Signaling mapper end...");
    match client.mapper_end(shuffle_id, map_id, 0, num_mappers).await {
        Ok(success) => {
            println!("  ✓ Mapper end signaled (success={})", success);
        }
        Err(e) => {
            println!("  ✗ Mapper end failed: {}", e);
        }
    }

    // Step 7: Cleanup
    println!("\nStep 7: Cleanup...");
    client.cleanup_shuffle(shuffle_id);
    client.shutdown().await;
    println!("  ✓ Client shutdown complete");

    println!("\n=== Shuffle Write Complete ===\n");
    Ok(())
}

/// Simulates the Comet shuffle read flow.
async fn simulate_comet_shuffle_read(
    lm_host: &str,
    lm_port: i32,
    shuffle_id: i32,
    partition_id: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Comet Shuffle Read Simulation ===\n");

    // Create client
    let config = CelebornConfig::builder()
        .app_id(&format!(
            "comet-reader-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
                % 100000
        ))
        .master_endpoints(vec!["localhost:9097".to_string()])
        .build()?;

    let client = ExecutorShuffleClient::new(config);

    // Connect to LifecycleManager
    println!("Connecting to LifecycleManager...");
    client.setup_lifecycle_manager_ref(lm_host, lm_port).await?;

    // Get reducer file groups
    println!("Getting reducer file groups for shuffle {}...", shuffle_id);
    match client.get_reducer_file_group(shuffle_id).await {
        Ok(file_groups) => {
            println!("  Got {} file groups", file_groups.len());
            for (pid, locations) in &file_groups {
                println!("    Partition {}: {} locations", pid, locations.len());
            }
        }
        Err(e) => {
            println!("  Error getting file groups: {}", e);
        }
    }

    // Read partition (would fetch from Celeborn Workers)
    println!("\nReading partition {}...", partition_id);
    match client
        .read_partition(shuffle_id, partition_id, 0, 0, i32::MAX)
        .await
    {
        Ok(stream) => {
            println!("  ✓ Created input stream for partition {}", partition_id);
            // In real usage, we would iterate over the stream
            drop(stream);
        }
        Err(e) => {
            println!("  ✗ Failed to read partition: {}", e);
        }
    }

    client.shutdown().await;
    println!("\n=== Shuffle Read Complete ===\n");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    // Parse arguments
    let args: Vec<String> = env::args().collect();
    let lm_host = if args.len() > 1 {
        args[1].clone()
    } else {
        env::var("LIFECYCLE_MANAGER_HOST").unwrap_or_else(|_| "localhost".to_string())
    };
    let lm_port: i32 = if args.len() > 2 {
        args[2].parse().unwrap_or(9098)
    } else {
        env::var("LIFECYCLE_MANAGER_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(9098)
    };

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║     Comet + Celeborn Integration Example                   ║");
    println!("╠════════════════════════════════════════════════════════════╣");
    println!("║ LifecycleManager: {}:{:<30} ║", lm_host, lm_port);
    println!("╚════════════════════════════════════════════════════════════╝");

    // Generate unique shuffle ID
    let shuffle_id = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        % 100000) as i32;

    // Simulate shuffle write from multiple mappers
    println!("\n--- Simulating Mapper 0 ---");
    simulate_comet_shuffle_write(&lm_host, lm_port, shuffle_id, 0, 4).await?;

    // Wait a bit for data to be committed
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Simulate shuffle read
    println!("\n--- Simulating Reducer ---");
    simulate_comet_shuffle_read(&lm_host, lm_port, shuffle_id, 0).await?;

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║     Integration Example Complete!                          ║");
    println!("╚════════════════════════════════════════════════════════════╝");

    Ok(())
}
