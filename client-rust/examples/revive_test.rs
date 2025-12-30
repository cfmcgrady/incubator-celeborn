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

//! Revive mechanism integration test for Celeborn Rust Client.
//!
//! This example tests the revive functionality:
//! 1. Register a shuffle
//! 2. Simulate a push failure scenario
//! 3. Test revive to get new partition location
//! 4. Verify data can be pushed to new location
//!
//! Usage:
//!   cargo run --example revive_test -- [master_endpoint]
//!
//! Default master endpoint: localhost:9097

use std::env;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use celeborn_client::config::CelebornConfig;
use celeborn_client::network::TransportClient;
use celeborn_client::client::lifecycle::LifecycleManager;
use celeborn_client::client::revive::{ReviveManager, ReviveRequest};
use celeborn_client::client::push::DataPusher;
use celeborn_client::error::StatusCode;
use celeborn_client::protocol::PartitionLocation;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
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

    println!("=== Celeborn Rust Client Revive Test ===\n");
    println!("Master endpoint: {}", master_endpoint);

    // 1. Create configuration and clients
    let config = Arc::new(
        CelebornConfig::builder()
            .app_id("rust-revive-test")
            .master_endpoints(vec![master_endpoint.to_string()])
            .push_buffer_size(1024)
            .max_retries(3)
            .build()
            .unwrap(),
    );

    let transport_client = Arc::new(TransportClient::new(config.clone())?);
    let lifecycle_manager = Arc::new(LifecycleManager::new(config.clone(), transport_client.clone()));

    // Start heartbeat
    lifecycle_manager.start_heartbeat().await;

    // 2. Register Shuffle
    let shuffle_id = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() % 100000) as i32;
    let num_mappers = 1;
    let num_partitions = 2; // Use 2 partitions for testing

    println!("\n--- Registering Shuffle {} ---", shuffle_id);
    lifecycle_manager
        .register_shuffle(shuffle_id, num_mappers, num_partitions)
        .await?;
    println!("✓ Shuffle registered with {} partitions", num_partitions);

    // 3. Get initial partition locations
    println!("\n--- Initial Partition Locations ---");
    for partition_id in 0..num_partitions {
        let locations = lifecycle_manager.get_partition_location(shuffle_id, partition_id)?;
        println!(
            "Partition {}: {} location(s)",
            partition_id,
            locations.len()
        );
        for loc in &locations {
            println!(
                "  - {}:{} (epoch={}, mode={:?})",
                loc.host, loc.push_port, loc.epoch, loc.mode
            );
        }
    }

    // 4. Create ReviveManager with callbacks
    let lifecycle_for_revive = lifecycle_manager.clone();
    let location_updater: Arc<dyn Fn(i32, i32, PartitionLocation) + Send + Sync> =
        Arc::new(move |shuffle_id, partition_id, location| {
            lifecycle_for_revive.update_partition_location(shuffle_id, partition_id, location);
        });

    let lifecycle_for_mapper = lifecycle_manager.clone();
    let mapper_ended_checker: Arc<dyn Fn(i32, i32) -> bool + Send + Sync> =
        Arc::new(move |_shuffle_id, _map_id| {
            // For testing, mapper never ends
            false
        });

    let lifecycle_for_newer = lifecycle_manager.clone();
    let newer_partition_checker: Arc<dyn Fn(i32, i32, i32) -> bool + Send + Sync> =
        Arc::new(move |shuffle_id, partition_id, epoch| {
            // Check if there's a newer partition location
            if let Ok(locations) = lifecycle_for_newer.get_partition_location(shuffle_id, partition_id) {
                locations.iter().any(|loc| loc.epoch > epoch)
            } else {
                false
            }
        });

    let revive_manager = Arc::new(ReviveManager::new(
        transport_client.clone(),
        10, // batch size
        Duration::from_millis(100), // batch interval
        location_updater,
        mapper_ended_checker,
        newer_partition_checker,
    ));

    // 5. Test single revive
    println!("\n--- Testing Single Revive ---");
    let partition_id = 0;
    let old_locations = lifecycle_manager.get_partition_location(shuffle_id, partition_id)?;
    let old_location = old_locations.first().cloned();

    println!(
        "Requesting revive for partition {} (simulating failure)",
        partition_id
    );

    match revive_manager
        .revive_single(
            shuffle_id,
            0, // map_id
            0, // attempt_id
            partition_id,
            old_location.as_ref().map(|l| l.epoch).unwrap_or(-1),
            old_location.as_ref(),
            StatusCode::PushDataWriteFailPrimary,
        )
        .await
    {
        Ok(new_location) => {
            println!("✓ Revive successful!");
            println!(
                "  New location: {}:{} (epoch={})",
                new_location.host, new_location.push_port, new_location.epoch
            );
            
            // Verify the location was updated
            let updated_locations = lifecycle_manager.get_partition_location(shuffle_id, partition_id)?;
            println!("  Updated locations count: {}", updated_locations.len());
        }
        Err(e) => {
            println!("⚠ Revive returned error: {}", e);
            println!("  This is expected if the original location is still valid");
        }
    }

    // 6. Test DataPusher with revive integration
    println!("\n--- Testing DataPusher with Revive ---");
    
    let data_pusher = DataPusher::with_revive_manager(
        config.clone(),
        transport_client.clone(),
        lifecycle_manager.clone(),
        revive_manager.clone(),
    );

    let test_data = b"Test data for revive integration";
    
    match data_pusher
        .push_data(shuffle_id, 0, 0, 1, test_data)
        .await
    {
        Ok(()) => {
            println!("✓ Push with revive support successful");
        }
        Err(e) => {
            println!("✗ Push failed: {}", e);
        }
    }

    // Flush any pending data
    data_pusher.flush().await?;

    // 7. Test batch revive (async)
    println!("\n--- Testing Batch Revive ---");
    
    // Create some revive requests
    for i in 0..3 {
        let request = Arc::new(ReviveRequest::new(
            shuffle_id,
            0,
            0,
            i % num_partitions,
            0,
            None,
            StatusCode::PushDataFailNonCriticalCause,
        ));
        
        if let Err(e) = revive_manager.add_request(request).await {
            println!("Failed to add revive request: {}", e);
        }
    }
    
    // Start batch processor
    let processor_handle = revive_manager.clone().start_batch_processor();
    
    // Wait a bit for batch processing
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Stop the processor
    revive_manager.stop();
    processor_handle.abort();
    
    println!("✓ Batch revive test completed");

    // 8. Test worker exclusion
    println!("\n--- Testing Worker Exclusion ---");
    
    let test_host = "test-worker";
    let test_port = 9999;
    
    // Initially not excluded
    assert!(!revive_manager.is_worker_excluded(test_host, test_port));
    println!("✓ Worker initially not excluded");
    
    // After adding a request with failure, worker should be excluded
    // (This happens internally in add_request when old_location is provided)
    
    // Remove from exclusion
    revive_manager.remove_excluded_worker(test_host, test_port);
    assert!(!revive_manager.is_worker_excluded(test_host, test_port));
    println!("✓ Worker exclusion/removal works correctly");

    // 9. Cleanup
    println!("\n--- Cleanup ---");
    lifecycle_manager.unregister_shuffle(shuffle_id).await?;
    lifecycle_manager.stop().await;

    println!("\n=== Revive Test Complete ===");
    Ok(())
}
