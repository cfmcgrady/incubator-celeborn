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

//! Basic usage example for the Celeborn Rust client.
//!
//! This example demonstrates how to:
//! 1. Create a Celeborn client
//! 2. Register a shuffle
//! 3. Push shuffle data
//! 4. Signal mapper completion
//! 5. Fetch shuffle data
//! 6. Clean up resources

use celeborn_client::{CelebornClient, CelebornConfig, Result};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    // Create configuration
    // Use the actual IP address where Celeborn master is listening
    // You can find this by running: lsof -i :9097
    let master_endpoint = std::env::var("CELEBORN_MASTER")
        .unwrap_or_else(|_| "127.0.0.1:9097".to_string());
    
    println!("Connecting to Celeborn master at: {}", master_endpoint);
    
    let config = CelebornConfig::builder()
        .app_id("rust-example-app")
        .master_endpoints(vec![master_endpoint])
        .push_replicate_enabled(false)
        .build()?;

    println!("Creating Celeborn client...");
    let client = CelebornClient::new(config).await?;

    // Shuffle parameters
    let shuffle_id = 0;
    let num_mappers = 2;
    let num_partitions = 4;

    // Register shuffle
    println!("Registering shuffle {}...", shuffle_id);
    client
        .register_shuffle(shuffle_id, num_mappers, num_partitions)
        .await?;
    println!("Shuffle registered successfully!");

    // Simulate map task 0
    println!("\nMap task 0 pushing data...");
    for partition_id in 0..num_partitions {
        let data = format!("Data from mapper 0 for partition {}", partition_id);
        client
            .push_data(shuffle_id, 0, 0, partition_id, data.as_bytes())
            .await?;
        println!("  Pushed data to partition {}", partition_id);
    }

    // Signal mapper 0 completion
    client.mapper_end(shuffle_id, 0, 0, num_mappers).await?;
    println!("Map task 0 completed!");

    // Simulate map task 1
    println!("\nMap task 1 pushing data...");
    for partition_id in 0..num_partitions {
        let data = format!("Data from mapper 1 for partition {}", partition_id);
        client
            .push_data(shuffle_id, 1, 0, partition_id, data.as_bytes())
            .await?;
        println!("  Pushed data to partition {}", partition_id);
    }

    // Signal mapper 1 completion
    client.mapper_end(shuffle_id, 1, 0, num_mappers).await?;
    println!("Map task 1 completed!");

    // Fetch data for each partition
    println!("\nFetching shuffle data...");
    for partition_id in 0..num_partitions {
        println!("\nPartition {}:", partition_id);
        let mut iterator = client.fetch_data(shuffle_id, partition_id).await?;
        
        while let Some(chunk) = iterator.next().await? {
            let data = String::from_utf8_lossy(&chunk);
            println!("  Received: {}", data);
        }
    }

    // Unregister shuffle
    println!("\nUnregistering shuffle...");
    client.unregister_shuffle(shuffle_id).await?;

    // Stop client
    println!("Stopping client...");
    client.stop().await?;

    println!("\nExample completed successfully!");
    Ok(())
}
