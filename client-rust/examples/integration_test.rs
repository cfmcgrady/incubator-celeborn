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

//! Integration test for Celeborn Rust Client.
//!
//! This example performs a complete end-to-end test:
//! 1. Register a shuffle
//! 2. Push data to a worker
//! 3. Commit files
//! 4. Fetch data back from the worker
//!
//! Usage:
//!   cargo run --example integration_test -- [master_endpoint]
//!
//! Default master endpoint: localhost:9097

use std::env;
use std::sync::Arc;

use celeborn_client::config::CelebornConfig;
use celeborn_client::network::TransportClient;
use celeborn_client::client::lifecycle::LifecycleManager;
use celeborn_client::client::shuffle::ShuffleClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging - use DEBUG level to see detailed info
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(false)
        .init();

    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    let master_endpoint = if args.len() > 1 {
        &args[1]
    } else {
        "localhost:9097"
    };

    println!("=== Celeborn Rust Client Integration Test ===\n");
    println!("Master endpoint: {}", master_endpoint);

    // 1. Create configuration and clients
    let config = Arc::new(
        CelebornConfig::builder()
            .app_id("rust-integration-test")
            .master_endpoints(vec![master_endpoint.to_string()])
            .push_buffer_size(1024)
            .build()
            .unwrap(),
    );

    let transport_client = Arc::new(TransportClient::new(config.clone())?);
    let lifecycle_manager = Arc::new(LifecycleManager::new(config.clone(), transport_client.clone()));
    let shuffle_client = ShuffleClient::new(
        config.clone(),
        transport_client.clone(),
        lifecycle_manager.clone(),
    );

    // Start heartbeat
    lifecycle_manager.start_heartbeat().await;

    // 2. Register Shuffle
    let shuffle_id = 100;
    let num_mappers = 1;
    let num_partitions = 1;

    println!("\n--- Registering Shuffle {} ---", shuffle_id);
    lifecycle_manager
        .register_shuffle(shuffle_id, num_mappers, num_partitions)
        .await?;
    println!("✓ Shuffle registered");

    // 3. Push Data
    let map_id = 0;
    let attempt_id = 0;
    let partition_id = 0;
    let data = b"Hello, Celeborn! This is a test message from Rust client.";

    println!("\n--- Pushing Data ---");
    println!("Data size: {} bytes", data.len());
    
    shuffle_client
        .push_data(shuffle_id, map_id, attempt_id, partition_id, data)
        .await?;
    
    // Flush to ensure data is sent
    shuffle_client.flush().await?;
    println!("✓ Data pushed and flushed");

    // 4. Mapper End & Commit Files
    println!("\n--- Committing Files ---");
    
    // Simulate mapper completion
    lifecycle_manager
        .mapper_end(shuffle_id, map_id, attempt_id, num_mappers)
        .await?;

    // Request commit
    let response = lifecycle_manager.request_commit_files(shuffle_id).await?;
    
    if response.committed_primary_ids.is_empty() && response.committed_replica_ids.is_empty() {
        println!("⚠ Warning: No files committed! (This might happen if Worker didn't ack in time or logic issue)");
    } else {
        println!(
            "✓ Commit successful: {} primary, {} replica",
            response.committed_primary_ids.len(),
            response.committed_replica_ids.len()
        );
    }

    // 5. Fetch Data (Reducer)
    println!("\n--- Fetching Data ---");
    
    // In a real scenario, Reducer runs on a different node, so we test `get_reducer_file_group`
    // which might fetch from Master if not found locally (though here it is local)
    // To simulate "remote", we could clear local cache, but `get_reducer_file_group` falls back to Master anyway.
    
    let mut iterator = shuffle_client
        .fetch_data(shuffle_id, partition_id)
        .await?;

    let mut fetched_data = Vec::new();
    while let Some(chunk) = iterator.next().await? {
        fetched_data.extend_from_slice(&chunk);
    }

    println!("Fetched {} bytes", fetched_data.len());
    
    if fetched_data == data {
        println!("✓ Data verification PASSED: Content matches");
    } else {
        println!("✗ Data verification FAILED");
        println!("  Expected: {:?}", String::from_utf8_lossy(data));
        println!("  Actual:   {:?}", String::from_utf8_lossy(&fetched_data));
        return Err("Data mismatch".into());
    }

    // 6. Cleanup
    println!("\n--- Cleanup ---");
    lifecycle_manager.unregister_shuffle(shuffle_id).await?;
    lifecycle_manager.stop().await;
    
    println!("\n=== Integration Test Complete ===");
    Ok(())
}
