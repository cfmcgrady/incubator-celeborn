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

//! Async push example demonstrating concurrent data pushing.
//!
//! This example shows how to push data from multiple map tasks concurrently
//! using Tokio's async runtime.

use std::sync::Arc;
use std::time::Instant;

use celeborn_client::{CelebornClient, CelebornConfig, Result};
use tokio::task::JoinSet;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    // Create configuration
    let config = CelebornConfig::builder()
        .app_id("rust-async-example")
        .master_endpoints(vec!["localhost:9097".to_string()])
        .push_replicate_enabled(false)
        .max_in_flight_requests(64)
        .build()?;

    println!("Creating Celeborn client...");
    let client = Arc::new(CelebornClient::new(config).await?);

    // Shuffle parameters
    let shuffle_id = 0;
    let num_mappers = 8;
    let num_partitions = 16;
    let records_per_mapper = 1000;

    // Register shuffle
    println!("Registering shuffle with {} mappers and {} partitions...", 
             num_mappers, num_partitions);
    client
        .register_shuffle(shuffle_id, num_mappers, num_partitions)
        .await?;

    // Start timing
    let start = Instant::now();

    // Spawn concurrent map tasks
    let mut join_set = JoinSet::new();

    for map_id in 0..num_mappers {
        let client = client.clone();
        
        join_set.spawn(async move {
            println!("Map task {} starting...", map_id);
            
            for record_id in 0..records_per_mapper {
                // Determine target partition (simple hash)
                let partition_id = (record_id % num_partitions as i32) as i32;
                
                // Create record data
                let data = format!(
                    "{{\"mapper\":{},\"record\":{},\"partition\":{}}}",
                    map_id, record_id, partition_id
                );
                
                // Push data
                if let Err(e) = client
                    .push_data(shuffle_id, map_id, 0, partition_id, data.as_bytes())
                    .await
                {
                    eprintln!("Map task {} failed to push record {}: {}", map_id, record_id, e);
                    return Err(e);
                }
            }
            
            // Signal mapper completion
            client.mapper_end(shuffle_id, map_id, 0, num_mappers).await?;
            println!("Map task {} completed!", map_id);
            
            Ok::<_, celeborn_client::CelebornError>(map_id)
        });
    }

    // Wait for all map tasks to complete
    let mut completed = 0;
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(map_id)) => {
                completed += 1;
                println!("Map task {} finished ({}/{})", map_id, completed, num_mappers);
            }
            Ok(Err(e)) => {
                eprintln!("Map task failed: {}", e);
            }
            Err(e) => {
                eprintln!("Task panicked: {}", e);
            }
        }
    }

    let push_duration = start.elapsed();
    let total_records = num_mappers * records_per_mapper;
    let records_per_sec = total_records as f64 / push_duration.as_secs_f64();

    println!("\n=== Push Statistics ===");
    println!("Total records: {}", total_records);
    println!("Push duration: {:?}", push_duration);
    println!("Throughput: {:.2} records/sec", records_per_sec);

    // Fetch and verify data
    println!("\n=== Fetching Data ===");
    let fetch_start = Instant::now();
    let mut total_fetched = 0;

    for partition_id in 0..num_partitions {
        let mut iterator = client.fetch_data(shuffle_id, partition_id).await?;
        let mut partition_count = 0;
        
        while let Some(_chunk) = iterator.next().await? {
            partition_count += 1;
        }
        
        total_fetched += partition_count;
        println!("Partition {}: {} chunks", partition_id, partition_count);
    }

    let fetch_duration = fetch_start.elapsed();
    println!("\nTotal chunks fetched: {}", total_fetched);
    println!("Fetch duration: {:?}", fetch_duration);

    // Cleanup
    println!("\nCleaning up...");
    client.unregister_shuffle(shuffle_id).await?;
    client.stop().await?;

    println!("Example completed successfully!");
    Ok(())
}
