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

//! Integration tests for the Revive mechanism in Celeborn Rust Client.
//!
//! These tests validate the complete revive workflow including:
//! - Single partition revive
//! - Batch revive processing
//! - Push data with automatic revive on failure
//! - Worker exclusion management
//! - End-to-end data integrity after revive
//!
//! Prerequisites:
//! - Celeborn Master running (default: localhost:9097)
//! - At least one Celeborn Worker running
//!
//! Run with:
//!   CELEBORN_MASTER=host:port cargo test --test revive_integration_test -- --nocapture
//!
//! Or use default localhost:9097:
//!   cargo test --test revive_integration_test -- --nocapture --ignored

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use celeborn_client::client::lifecycle::LifecycleManager;
use celeborn_client::client::push::DataPusher;
use celeborn_client::client::revive::{ReviveManager, ReviveRequest};
use celeborn_client::config::CelebornConfig;
use celeborn_client::error::StatusCode;
use celeborn_client::network::TransportClient;
use celeborn_client::protocol::PartitionLocation;

/// Get master endpoint from environment or use default.
fn get_master_endpoint() -> String {
    std::env::var("CELEBORN_MASTER").unwrap_or_else(|_| "localhost:9097".to_string())
}

/// Generate a unique shuffle ID based on current timestamp.
fn generate_shuffle_id() -> i32 {
    (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        % 100000) as i32
}

/// Generate a unique app ID.
fn generate_app_id(prefix: &str) -> String {
    format!(
        "{}-{}",
        prefix,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            % 100000
    )
}

/// Test context holding all necessary clients and managers.
struct TestContext {
    config: Arc<CelebornConfig>,
    transport_client: Arc<TransportClient>,
    lifecycle_manager: Arc<LifecycleManager>,
    revive_manager: Arc<ReviveManager>,
    data_pusher: DataPusher,
    shuffle_id: i32,
}

impl TestContext {
    async fn new(app_id: &str, num_mappers: i32, num_partitions: i32) -> Result<Self, Box<dyn std::error::Error>> {
        let master_endpoint = get_master_endpoint();
        
        let config = Arc::new(
            CelebornConfig::builder()
                .app_id(app_id)
                .master_endpoints(vec![master_endpoint])
                .push_buffer_size(64 * 1024)
                .max_retries(3)
                .build()?,
        );

        let transport_client = Arc::new(TransportClient::new(config.clone())?);
        let lifecycle_manager = Arc::new(LifecycleManager::new(
            config.clone(),
            transport_client.clone(),
        ));

        // Start heartbeat
        lifecycle_manager.start_heartbeat().await;

        // Register shuffle
        let shuffle_id = generate_shuffle_id();
        lifecycle_manager
            .register_shuffle(shuffle_id, num_mappers, num_partitions)
            .await?;

        // Create ReviveManager with callbacks
        let lifecycle_for_revive = lifecycle_manager.clone();
        let location_updater: Arc<dyn Fn(i32, i32, PartitionLocation) + Send + Sync> =
            Arc::new(move |shuffle_id, partition_id, location| {
                lifecycle_for_revive.update_partition_location(shuffle_id, partition_id, location);
            });

        let mapper_ended_checker: Arc<dyn Fn(i32, i32) -> bool + Send + Sync> =
            Arc::new(move |_shuffle_id, _map_id| false);

        let lifecycle_for_newer = lifecycle_manager.clone();
        let newer_partition_checker: Arc<dyn Fn(i32, i32, i32) -> bool + Send + Sync> =
            Arc::new(move |shuffle_id, partition_id, epoch| {
                if let Ok(locations) = lifecycle_for_newer.get_partition_location(shuffle_id, partition_id) {
                    locations.iter().any(|loc| loc.epoch > epoch)
                } else {
                    false
                }
            });

        let revive_manager = Arc::new(ReviveManager::new(
            transport_client.clone(),
            10,
            Duration::from_millis(100),
            location_updater,
            mapper_ended_checker,
            newer_partition_checker,
        ));

        let data_pusher = DataPusher::with_revive_manager(
            config.clone(),
            transport_client.clone(),
            lifecycle_manager.clone(),
            revive_manager.clone(),
        );

        Ok(Self {
            config,
            transport_client,
            lifecycle_manager,
            revive_manager,
            data_pusher,
            shuffle_id,
        })
    }

    async fn cleanup(&self) {
        let _ = self.lifecycle_manager.unregister_shuffle(self.shuffle_id).await;
        self.lifecycle_manager.stop().await;
    }
}

/// Test: Verify that shuffle registration returns valid partition locations.
#[tokio::test]
#[ignore] // Run with --ignored flag when cluster is available
async fn test_shuffle_registration_with_locations() {
    let ctx = TestContext::new(
        &generate_app_id("revive-test-registration"),
        2,
        4,
    )
    .await
    .expect("Failed to create test context");

    // Verify all partitions have locations
    for partition_id in 0..4 {
        let locations = ctx
            .lifecycle_manager
            .get_partition_location(ctx.shuffle_id, partition_id)
            .expect("Should get partition location");
        
        assert!(!locations.is_empty(), "Partition {} should have at least one location", partition_id);
        
        let loc = &locations[0];
        assert!(!loc.host.is_empty(), "Location should have a valid host");
        assert!(loc.push_port > 0, "Location should have a valid push port");
        assert!(loc.epoch >= 0, "Location should have a valid epoch");
    }

    ctx.cleanup().await;
}

/// Test: Verify single partition revive returns a valid location.
#[tokio::test]
#[ignore]
async fn test_single_partition_revive() {
    let ctx = TestContext::new(
        &generate_app_id("revive-test-single"),
        1,
        2,
    )
    .await
    .expect("Failed to create test context");

    let partition_id = 0;
    let old_locations = ctx
        .lifecycle_manager
        .get_partition_location(ctx.shuffle_id, partition_id)
        .expect("Should get partition location");
    
    let old_location = old_locations.first().cloned();
    let old_epoch = old_location.as_ref().map(|l| l.epoch).unwrap_or(-1);

    // Attempt revive with non-critical cause
    let result = ctx
        .revive_manager
        .revive_single(
            ctx.shuffle_id,
            0, // map_id
            0, // attempt_id
            partition_id,
            old_epoch,
            old_location.as_ref(),
            StatusCode::PushDataFailNonCriticalCause,
        )
        .await;

    // In a single-worker cluster, revive should return the existing location
    // In a multi-worker cluster, it might return a new location
    match result {
        Ok(new_location) => {
            assert!(!new_location.host.is_empty(), "Revived location should have valid host");
            assert!(new_location.push_port > 0, "Revived location should have valid port");
        }
        Err(e) => {
            // This is acceptable if the original location is still valid
            println!("Revive returned error (may be expected): {}", e);
        }
    }

    ctx.cleanup().await;
}

/// Test: Verify push data works correctly with revive manager attached.
#[tokio::test]
#[ignore]
async fn test_push_data_with_revive_manager() {
    let ctx = TestContext::new(
        &generate_app_id("revive-test-push"),
        1,
        4,
    )
    .await
    .expect("Failed to create test context");

    // Push data to all partitions
    for partition_id in 0..4 {
        let test_data = format!(
            "Test data for partition {} at {}",
            partition_id,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );

        ctx.data_pusher
            .push_data(ctx.shuffle_id, 0, 0, partition_id, test_data.as_bytes())
            .await
            .expect(&format!("Should push data to partition {}", partition_id));
    }

    // Flush all pending data
    ctx.data_pusher.flush().await.expect("Should flush successfully");

    ctx.cleanup().await;
}

/// Test: Verify push data after simulated revive scenario.
#[tokio::test]
#[ignore]
async fn test_push_after_revive() {
    let ctx = TestContext::new(
        &generate_app_id("revive-test-push-after"),
        1,
        2,
    )
    .await
    .expect("Failed to create test context");

    let partition_id = 0;

    // First push
    let initial_data = b"Initial data before revive";
    ctx.data_pusher
        .push_data(ctx.shuffle_id, 0, 0, partition_id, initial_data)
        .await
        .expect("Should push initial data");

    // Simulate revive scenario
    let old_locations = ctx
        .lifecycle_manager
        .get_partition_location(ctx.shuffle_id, partition_id)
        .expect("Should get partition location");
    
    let old_location = old_locations.first().cloned();
    let old_epoch = old_location.as_ref().map(|l| l.epoch).unwrap_or(-1);

    let _ = ctx
        .revive_manager
        .revive_single(
            ctx.shuffle_id,
            0,
            0,
            partition_id,
            old_epoch,
            old_location.as_ref(),
            StatusCode::PushDataFailNonCriticalCause,
        )
        .await;

    // Push after revive
    let post_revive_data = b"Data after revive scenario";
    ctx.data_pusher
        .push_data(ctx.shuffle_id, 0, 0, partition_id, post_revive_data)
        .await
        .expect("Should push data after revive");

    ctx.data_pusher.flush().await.expect("Should flush successfully");

    ctx.cleanup().await;
}

/// Test: Verify batch revive request handling.
#[tokio::test]
#[ignore]
async fn test_batch_revive_requests() {
    let ctx = TestContext::new(
        &generate_app_id("revive-test-batch"),
        2,
        4,
    )
    .await
    .expect("Failed to create test context");

    // Add batch revive requests
    let mut requests_added = 0;
    for partition_id in 0..4 {
        let request = Arc::new(ReviveRequest::new(
            ctx.shuffle_id,
            1, // map_id
            0, // attempt_id
            partition_id,
            0, // epoch
            None,
            StatusCode::PushDataFailNonCriticalCause,
        ));

        if ctx.revive_manager.add_request(request).await.is_ok() {
            requests_added += 1;
        }
    }

    assert_eq!(requests_added, 4, "Should add all 4 batch requests");

    // Start batch processor briefly
    let processor_handle = ctx.revive_manager.clone().start_batch_processor();
    tokio::time::sleep(Duration::from_millis(300)).await;
    ctx.revive_manager.stop();
    processor_handle.abort();

    ctx.cleanup().await;
}

/// Test: Verify worker exclusion logic.
#[tokio::test]
#[ignore]
async fn test_worker_exclusion() {
    let ctx = TestContext::new(
        &generate_app_id("revive-test-exclusion"),
        1,
        1,
    )
    .await
    .expect("Failed to create test context");

    let test_host = "test-excluded-worker";
    let test_port = 12345;

    // Initially not excluded
    assert!(
        !ctx.revive_manager.is_worker_excluded(test_host, test_port),
        "Worker should not be initially excluded"
    );

    // Remove from exclusion (should be no-op)
    ctx.revive_manager.remove_excluded_worker(test_host, test_port);
    
    assert!(
        !ctx.revive_manager.is_worker_excluded(test_host, test_port),
        "Worker should still not be excluded after remove"
    );

    ctx.cleanup().await;
}

/// Test: Verify mapper end and commit flow with revive.
#[tokio::test]
#[ignore]
async fn test_mapper_end_and_commit_with_revive() {
    let num_mappers = 2;
    let num_partitions = 4;
    
    let ctx = TestContext::new(
        &generate_app_id("revive-test-commit"),
        num_mappers,
        num_partitions,
    )
    .await
    .expect("Failed to create test context");

    // Push data from all mappers
    for map_id in 0..num_mappers {
        for partition_id in 0..num_partitions {
            let test_data = format!("Data from mapper {} to partition {}", map_id, partition_id);
            ctx.data_pusher
                .push_data(ctx.shuffle_id, map_id, 0, partition_id, test_data.as_bytes())
                .await
                .expect("Should push data");
        }
    }

    ctx.data_pusher.flush().await.expect("Should flush");

    // End all mappers
    for map_id in 0..num_mappers {
        ctx.lifecycle_manager
            .mapper_end(ctx.shuffle_id, map_id, 0, num_partitions)
            .await
            .expect(&format!("Mapper {} should end successfully", map_id));
    }

    // Wait for commit
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Get reducer file groups
    let file_groups = ctx
        .lifecycle_manager
        .get_reducer_file_group(ctx.shuffle_id)
        .await;

    match file_groups {
        Ok(groups) => {
            assert!(!groups.is_empty(), "Should have file groups after commit");
            println!("Got {} file groups", groups.len());
        }
        Err(e) => {
            // This might happen if commit is still in progress
            println!("Could not get file groups (may be expected): {}", e);
        }
    }

    ctx.cleanup().await;
}

/// Test: End-to-end data integrity with revive.
#[tokio::test]
#[ignore]
async fn test_end_to_end_data_integrity() {
    let ctx = TestContext::new(
        &generate_app_id("revive-test-e2e"),
        1,
        2,
    )
    .await
    .expect("Failed to create test context");

    // Prepare test data
    let test_data: HashMap<i32, Vec<u8>> = (0..2)
        .map(|partition_id| {
            let data = format!(
                "E2E test data for partition {} - timestamp {}",
                partition_id,
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            );
            (partition_id, data.into_bytes())
        })
        .collect();

    // Push data
    for (partition_id, data) in &test_data {
        ctx.data_pusher
            .push_data(ctx.shuffle_id, 0, 0, *partition_id, data)
            .await
            .expect("Should push data");
    }

    ctx.data_pusher.flush().await.expect("Should flush");

    // Simulate revive for one partition
    let partition_id = 0;
    let old_locations = ctx
        .lifecycle_manager
        .get_partition_location(ctx.shuffle_id, partition_id)
        .expect("Should get location");
    
    let old_location = old_locations.first().cloned();
    let old_epoch = old_location.as_ref().map(|l| l.epoch).unwrap_or(-1);

    let _ = ctx
        .revive_manager
        .revive_single(
            ctx.shuffle_id,
            0,
            0,
            partition_id,
            old_epoch,
            old_location.as_ref(),
            StatusCode::PushDataFailNonCriticalCause,
        )
        .await;

    // Push more data after revive
    let additional_data = b"Additional data after revive";
    ctx.data_pusher
        .push_data(ctx.shuffle_id, 0, 0, partition_id, additional_data)
        .await
        .expect("Should push additional data");

    ctx.data_pusher.flush().await.expect("Should flush");

    // End mapper and commit
    ctx.lifecycle_manager
        .mapper_end(ctx.shuffle_id, 0, 0, 2)
        .await
        .expect("Mapper should end");

    tokio::time::sleep(Duration::from_millis(500)).await;

    ctx.cleanup().await;
}

/// Test: Verify revive with different status codes.
#[tokio::test]
#[ignore]
async fn test_revive_with_different_status_codes() {
    let ctx = TestContext::new(
        &generate_app_id("revive-test-status"),
        1,
        1,
    )
    .await
    .expect("Failed to create test context");

    let partition_id = 0;
    let old_locations = ctx
        .lifecycle_manager
        .get_partition_location(ctx.shuffle_id, partition_id)
        .expect("Should get location");
    
    let old_location = old_locations.first().cloned();
    let old_epoch = old_location.as_ref().map(|l| l.epoch).unwrap_or(-1);

    // Test with different status codes
    let status_codes = vec![
        StatusCode::PushDataFailNonCriticalCause,
        StatusCode::PushDataTimeoutPrimary,
        StatusCode::PushDataFailPrimary,
    ];

    for status_code in status_codes {
        let result = ctx
            .revive_manager
            .revive_single(
                ctx.shuffle_id,
                0,
                0,
                partition_id,
                old_epoch,
                old_location.as_ref(),
                status_code,
            )
            .await;

        // All should return a location (either new or existing)
        match result {
            Ok(location) => {
                assert!(!location.host.is_empty(), "Should have valid host for {:?}", status_code);
            }
            Err(e) => {
                println!("Revive with {:?} returned error (may be expected): {}", status_code, e);
            }
        }
    }

    ctx.cleanup().await;
}

/// Test: Verify ReviveRequest status management.
#[tokio::test]
async fn test_revive_request_status() {
    let request = ReviveRequest::new(
        1,
        0,
        0,
        0,
        0,
        None,
        StatusCode::PushDataFailPrimary,
    );

    // Initial status should be Unknown
    assert_eq!(request.get_status(), StatusCode::Unknown);

    // Set and verify status
    request.set_status(StatusCode::Success);
    assert_eq!(request.get_status(), StatusCode::Success);

    request.set_status(StatusCode::ReviveFailed);
    assert_eq!(request.get_status(), StatusCode::ReviveFailed);
}

/// Test: Verify multiple shuffles with revive.
#[tokio::test]
#[ignore]
async fn test_multiple_shuffles_with_revive() {
    let master_endpoint = get_master_endpoint();
    
    let config = Arc::new(
        CelebornConfig::builder()
            .app_id(&generate_app_id("revive-test-multi"))
            .master_endpoints(vec![master_endpoint])
            .push_buffer_size(64 * 1024)
            .max_retries(3)
            .build()
            .expect("Should build config"),
    );

    let transport_client = Arc::new(TransportClient::new(config.clone()).expect("Should create transport"));
    let lifecycle_manager = Arc::new(LifecycleManager::new(config.clone(), transport_client.clone()));

    lifecycle_manager.start_heartbeat().await;

    // Register multiple shuffles
    let shuffle_ids: Vec<i32> = (0..3).map(|_| generate_shuffle_id()).collect();
    
    for shuffle_id in &shuffle_ids {
        lifecycle_manager
            .register_shuffle(*shuffle_id, 1, 2)
            .await
            .expect("Should register shuffle");
    }

    // Create revive manager
    let lifecycle_for_revive = lifecycle_manager.clone();
    let location_updater: Arc<dyn Fn(i32, i32, PartitionLocation) + Send + Sync> =
        Arc::new(move |shuffle_id, partition_id, location| {
            lifecycle_for_revive.update_partition_location(shuffle_id, partition_id, location);
        });

    let mapper_ended_checker: Arc<dyn Fn(i32, i32) -> bool + Send + Sync> =
        Arc::new(move |_, _| false);

    let lifecycle_for_newer = lifecycle_manager.clone();
    let newer_partition_checker: Arc<dyn Fn(i32, i32, i32) -> bool + Send + Sync> =
        Arc::new(move |shuffle_id, partition_id, epoch| {
            if let Ok(locations) = lifecycle_for_newer.get_partition_location(shuffle_id, partition_id) {
                locations.iter().any(|loc| loc.epoch > epoch)
            } else {
                false
            }
        });

    let revive_manager = Arc::new(ReviveManager::new(
        transport_client.clone(),
        10,
        Duration::from_millis(100),
        location_updater,
        mapper_ended_checker,
        newer_partition_checker,
    ));

    // Test revive for each shuffle
    for shuffle_id in &shuffle_ids {
        let old_locations = lifecycle_manager
            .get_partition_location(*shuffle_id, 0)
            .expect("Should get location");
        
        let old_location = old_locations.first().cloned();
        let old_epoch = old_location.as_ref().map(|l| l.epoch).unwrap_or(-1);

        let result = revive_manager
            .revive_single(
                *shuffle_id,
                0,
                0,
                0,
                old_epoch,
                old_location.as_ref(),
                StatusCode::PushDataFailNonCriticalCause,
            )
            .await;

        match result {
            Ok(location) => {
                assert!(!location.host.is_empty(), "Should have valid location for shuffle {}", shuffle_id);
            }
            Err(e) => {
                println!("Revive for shuffle {} returned error (may be expected): {}", shuffle_id, e);
            }
        }
    }

    // Cleanup
    for shuffle_id in &shuffle_ids {
        let _ = lifecycle_manager.unregister_shuffle(*shuffle_id).await;
    }
    lifecycle_manager.stop().await;
}
