# Celeborn Rust Client

A native Rust client for [Apache Celeborn](https://celeborn.apache.org/) - a distributed shuffle service for big data processing frameworks.

## Overview

This crate provides a high-performance Rust implementation of the Celeborn client, enabling Rust applications to leverage Celeborn's distributed shuffle capabilities. It supports:

- **Shuffle Registration**: Register and manage shuffle operations
- **Data Push**: Push shuffle data to Celeborn workers with buffering and compression
- **Data Fetch**: Fetch shuffle data with streaming support
- **Lifecycle Management**: Automatic heartbeats and resource cleanup
- **Fault Tolerance**: Automatic retry and partition revive on failures

## Features

- 🚀 **Async/Await**: Built on Tokio for high-performance async I/O
- 🔄 **Connection Pooling**: Efficient connection management with configurable pool sizes
- 📦 **Compression**: Optional LZ4 and Zstd compression support
- 🔧 **Configurable**: Extensive configuration options for tuning performance
- 📊 **Metrics**: Built-in tracking of bytes written and file counts

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
celeborn-client = "0.1"
```

For compression support:

```toml
[dependencies]
celeborn-client = { version = "0.1", features = ["compression-lz4", "compression-zstd"] }
```

## Quick Start

```rust
use celeborn_client::{CelebornClient, CelebornConfig, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Create configuration
    let config = CelebornConfig::builder()
        .app_id("my-rust-app")
        .master_endpoints(vec!["localhost:9097".to_string()])
        .build()?;

    // Create client
    let client = CelebornClient::new(config).await?;

    // Register a shuffle
    let shuffle_id = 0;
    client.register_shuffle(shuffle_id, 10, 100).await?;

    // Push data
    let data = b"Hello, Celeborn!";
    client.push_data(shuffle_id, 0, 0, 0, data).await?;

    // Signal mapper completion
    client.mapper_end(shuffle_id, 0, 0, 10).await?;

    // Fetch data
    let mut iterator = client.fetch_data(shuffle_id, 0).await?;
    while let Some(chunk) = iterator.next().await? {
        println!("Received {} bytes", chunk.len());
    }

    // Cleanup
    client.unregister_shuffle(shuffle_id).await?;
    client.stop().await?;

    Ok(())
}
```

## Configuration

The client can be configured using the builder pattern:

```rust
use celeborn_client::{CelebornConfig, CompressionCodec};
use std::time::Duration;

let config = CelebornConfig::builder()
    .app_id("my-app")
    .master_endpoints(vec![
        "master1:9097".to_string(),
        "master2:9097".to_string(),
    ])
    .push_replicate_enabled(true)
    .push_timeout(Duration::from_secs(120))
    .fetch_timeout(Duration::from_secs(600))
    .rpc_timeout(Duration::from_secs(30))
    .max_retries(3)
    .compression_codec(CompressionCodec::Lz4)
    .push_buffer_size(64 * 1024)
    .max_in_flight_requests(32)
    .connection_pool_size(4)
    .heartbeat_interval(Duration::from_secs(15))
    .user_identifier("tenant-1", "user-1")
    .build()?;
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `app_id` | Required | Unique application identifier |
| `master_endpoints` | Required | List of Celeborn master endpoints |
| `push_replicate_enabled` | `false` | Enable data replication |
| `push_timeout` | `120s` | Timeout for push operations |
| `fetch_timeout` | `600s` | Timeout for fetch operations |
| `rpc_timeout` | `30s` | Timeout for RPC calls |
| `max_retries` | `3` | Maximum retry attempts |
| `compression_codec` | `Lz4` | Compression codec (None, Lz4, Zstd) |
| `push_buffer_size` | `64KB` | Buffer size for push operations |
| `max_in_flight_requests` | `32` | Maximum concurrent requests |
| `connection_pool_size` | `4` | Connections per worker |
| `heartbeat_interval` | `15s` | Heartbeat interval |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     CelebornClient                          │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────────────────────┐  │
│  │LifecycleManager │  │         ShuffleClient           │  │
│  │                 │  │  ┌───────────┐ ┌─────────────┐  │  │
│  │ - Registration  │  │  │DataPusher │ │ DataFetcher │  │  │
│  │ - Heartbeat     │  │  └───────────┘ └─────────────┘  │  │
│  │ - Partition Mgmt│  │                                 │  │
│  └────────┬────────┘  └────────────────┬────────────────┘  │
│           │                            │                    │
│           └────────────┬───────────────┘                    │
│                        │                                    │
│              ┌─────────▼─────────┐                         │
│              │  TransportClient  │                         │
│              │                   │                         │
│              │ ┌───────────────┐ │                         │
│              │ │ConnectionPool │ │                         │
│              │ └───────────────┘ │                         │
│              └─────────┬─────────┘                         │
└────────────────────────┼────────────────────────────────────┘
                         │
                         ▼
              ┌─────────────────────┐
              │   Celeborn Cluster  │
              │  ┌───────┐ ┌──────┐ │
              │  │Master │ │Worker│ │
              │  └───────┘ └──────┘ │
              └─────────────────────┘
```

## Examples

See the `examples/` directory for more detailed examples:

### Local Examples (No External Services Required)

- **`protocol_demo.rs`** - Demonstrates the Celeborn protocol implementation with a mock server/client. **Recommended for testing the protocol locally.**

```bash
cargo run --example protocol_demo
```

### Examples Requiring Celeborn Services

These examples require a running Celeborn cluster:

- **`basic_usage.rs`** - Complete shuffle workflow (register, push, fetch, cleanup). Requires a Celeborn Master.
- **`worker_connection.rs`** - Direct connection to a Worker's fetch port. Requires a Celeborn Worker.
- **`revive_integration_test.rs`** - Comprehensive revive mechanism test with 11 test scenarios.

```bash
# Connect to a Celeborn Master (default: 127.0.0.1:9097)
CELEBORN_MASTER=<master-host>:9097 cargo run --example basic_usage

# Connect to a Celeborn Worker's fetch port
WORKER_HOST=<worker-host> WORKER_FETCH_PORT=<port> cargo run --example worker_connection

# Run revive integration test example
cargo run --example revive_integration_test -- <master-host>:9097
```

> **Note**: The `basic_usage` example requires Master RPC communication, which uses Java serialization format (NettyRpcEnv). This is currently a work-in-progress. The `worker_connection` example works with the TransportClient protocol and can successfully communicate with Worker fetch/push/replicate ports.

## Integration Tests

The `tests/` directory contains integration tests that validate the revive mechanism:

### Running Integration Tests

Integration tests require a running Celeborn cluster. They are marked with `#[ignore]` by default to avoid failures in CI environments without a cluster.

```bash
# Run all integration tests (requires running Celeborn cluster)
CELEBORN_MASTER=<master-host>:9097 cargo test --test revive_integration_test -- --ignored --nocapture

# Run a specific integration test
CELEBORN_MASTER=<master-host>:9097 cargo test --test revive_integration_test test_single_partition_revive -- --ignored --nocapture

# Run unit tests only (no cluster required)
cargo test --test revive_integration_test
```

### Available Integration Tests

| Test | Description |
|------|-------------|
| `test_shuffle_registration_with_locations` | Verifies shuffle registration returns valid partition locations |
| `test_single_partition_revive` | Tests single partition revive mechanism |
| `test_push_data_with_revive_manager` | Validates push data with revive manager attached |
| `test_push_after_revive` | Tests push data after simulated revive scenario |
| `test_batch_revive_requests` | Verifies batch revive request handling |
| `test_worker_exclusion` | Tests worker exclusion logic |
| `test_mapper_end_and_commit_with_revive` | Validates mapper end and commit flow with revive |
| `test_end_to_end_data_integrity` | End-to-end data integrity test with revive |
| `test_revive_with_different_status_codes` | Tests revive with various status codes |
| `test_multiple_shuffles_with_revive` | Validates multiple shuffles with revive |
| `test_revive_request_status` | Unit test for ReviveRequest status management |

## Protocol Compatibility

This client implements the Celeborn wire protocol and is compatible with:

- Celeborn 0.3.x
- Celeborn 0.4.x
- Celeborn 0.5.x (main branch)

## Performance Considerations

1. **Connection Pooling**: Increase `connection_pool_size` for high-throughput workloads
2. **Buffering**: Adjust `push_buffer_size` based on your data patterns
3. **Compression**: Use LZ4 for speed, Zstd for better compression ratio
4. **Concurrency**: Tune `max_in_flight_requests` based on network latency

## Error Handling

The client uses a custom error type `CelebornError` that covers:

- Network I/O errors
- Connection failures
- Protocol errors
- Server errors with status codes
- Timeout errors
- Configuration errors

```rust
use celeborn_client::{CelebornError, StatusCode};

match client.push_data(shuffle_id, 0, 0, 0, data).await {
    Ok(()) => println!("Push successful"),
    Err(CelebornError::Timeout(ms)) => println!("Timed out after {}ms", ms),
    Err(CelebornError::ServerError { status, message }) => {
        if status.is_retriable() {
            // Retry the operation
        }
    }
    Err(e) => eprintln!("Error: {}", e),
}
```

## Contributing

Contributions are welcome! Please see the [Apache Celeborn contribution guidelines](https://github.com/apache/incubator-celeborn/blob/main/CONTRIBUTING.md).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](../LICENSE) for details.
