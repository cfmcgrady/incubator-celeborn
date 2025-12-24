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

//! Example demonstrating Master RPC communication using Java serialization.
//!
//! This example connects to a Celeborn Master and sends a RegisterShuffle request.
//!
//! Usage:
//!   cargo run --example master_rpc_test -- [master_host:port]
//!
//! Default master endpoint: localhost:9097

use std::env;
use std::net::SocketAddr;
use std::time::Duration;

use celeborn_client::network::MasterRpcClient;
use celeborn_client::protocol::java_serialization::RpcAddress;
use celeborn_client::protocol::transport::{
    PbRegisterShuffle, PbRegisterShuffleResponse, TransportMessageType,
};

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

    println!("=== Celeborn Master RPC Test ===\n");
    println!("Master endpoint: {}", master_endpoint);

    // Parse master address
    let master_addr: SocketAddr = master_endpoint.parse().map_err(|e| {
        format!(
            "Invalid master endpoint '{}': {}. Expected format: host:port",
            master_endpoint, e
        )
    })?;

    // Create Master RPC client
    let client = MasterRpcClient::new(
        vec![master_addr],
        Some(RpcAddress::new("localhost", 0)), // Local address (port 0 = ephemeral)
        Duration::from_secs(30),
        3,
        Duration::from_millis(500),
    )?;

    println!("\n--- Sending RegisterShuffle Request ---\n");

    // Create a RegisterShuffle request
    let request = PbRegisterShuffle {
        shuffle_id: 1,
        num_mappers: 4,
        num_partitions: 10,
    };

    println!("Request:");
    println!("  shuffle_id: {}", request.shuffle_id);
    println!("  num_mappers: {}", request.num_mappers);
    println!("  num_partitions: {}", request.num_partitions);

    // Send the request
    match client
        .send_rpc::<PbRegisterShuffle, PbRegisterShuffleResponse>(
            TransportMessageType::RegisterShuffle,
            &request,
        )
        .await
    {
        Ok(response) => {
            println!("\n✓ Received response from Master!");
            println!("Response:");
            println!("  status: {}", response.status);
            println!(
                "  partition_locations: {} locations",
                response.partition_locations.len()
            );

            for (i, loc) in response.partition_locations.iter().enumerate() {
                println!(
                    "    [{}] id={}, epoch={}, host={}, push_port={}, fetch_port={}",
                    i, loc.id, loc.epoch, loc.host, loc.push_port, loc.fetch_port
                );
            }
        }
        Err(e) => {
            println!("\n✗ Failed to communicate with Master: {}", e);
            println!("\nThis is expected if:");
            println!("  - Master is not running at {}", master_endpoint);
            println!("  - The Java serialization format needs adjustment");
            println!("  - Network connectivity issues");
        }
    }

    println!("\n=== Test Complete ===");

    Ok(())
}
