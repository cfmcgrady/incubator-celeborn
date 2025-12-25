---
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

# Rust Client Feature Gap Analysis

This document analyzes the feature gaps between the Rust client and the Java/Scala client implementation, based on the integration requirements described in [integrate.md](developers/integrate.md).

## Overview

The Rust client (`client-rust/`) provides a native Rust implementation of the Celeborn client. This analysis identifies what capabilities are missing for compute engine integration.

## Integration Requirements (from integrate.md)

According to the [integration documentation](developers/integrate.md), integrating Celeborn requires:

1. **Step 1**: Setup Celeborn Cluster
2. **Step 2**: Create LifecycleManager
3. **Step 3**: Create ShuffleClient
4. **Step 4**: Push Data (including `mapperEnd`)
5. **Step 5**: Read Data (via `readPartition`)
6. **Step 6**: Clean Up (via `unregisterShuffle`)

## Current Rust Client Capabilities

### ✅ Implemented Features

| Feature | Status | Implementation |
|---------|--------|----------------|
| **LifecycleManager** | ✅ Implemented | `client-rust/src/client/lifecycle.rs` |
| **ShuffleClient** | ✅ Implemented | `client-rust/src/client/shuffle.rs` |
| **Shuffle Registration** | ✅ Implemented | `LifecycleManager::register_shuffle()` |
| **Push Data** | ✅ Implemented | `ShuffleClient::push_data()` |
| **Push Merged Data** | ✅ Implemented | `ShuffleClient::push_merged_data()` |
| **Mapper End** | ✅ Implemented | `LifecycleManager::mapper_end()` |
| **Stage End** | ✅ Implemented | `LifecycleManager::stage_end()` |
| **Fetch Data** | ✅ Implemented | `ShuffleClient::fetch_data()` |
| **Unregister Shuffle** | ✅ Implemented | `LifecycleManager::unregister_shuffle()` |
| **Heartbeat** | ✅ Implemented | `LifecycleManager::start_heartbeat()` |
| **Partition Revive** | ✅ Implemented | `ReviveManager` in `client-rust/src/client/revive.rs` |
| **Partition Split** | ✅ Implemented | `SplitHandler` in `client-rust/src/client/partition_split.rs` |
| **Compression (LZ4/Zstd)** | ✅ Implemented | Feature flags in `Cargo.toml` |
| **Connection Pooling** | ✅ Implemented | `ConnectionPool` in `client-rust/src/network/connection.rs` |
| **CelebornInputStream** | ✅ Implemented | `client-rust/src/client/input_stream.rs` |
| **WorkerPartitionReader** | ✅ Implemented | `client-rust/src/client/partition_reader.rs` |
| **Map Range Read** | ✅ Implemented | `start_map_index`/`end_map_index` support |
| **Batch Deduplication** | ✅ Implemented | In `CelebornInputStream` |
| **Excluded Worker Tracking** | ✅ Implemented | In `CelebornInputStream` |

### ✅ Newly Implemented Features (Driver-Executor Separation)

| Feature | Status | Implementation |
|---------|--------|----------------|
| **ExecutorShuffleClient** | ✅ Implemented | `client-rust/src/client/executor_shuffle_client.rs` |
| **LifecycleManagerClient Trait** | ✅ Implemented | `client-rust/src/client/lifecycle_client.rs` |
| **NettyLifecycleManagerClient** | ✅ Implemented | Connects to Java LifecycleManager via Netty RPC |
| **LocalLifecycleManagerClient** | ✅ Implemented | For single-process mode |
| **Comet Integration Support** | ✅ Implemented | See `examples/comet_integration.rs` |

### ❌ Missing or Incomplete Features

#### 1. ~~LifecycleManager RPC Service~~ (P0 - ✅ RESOLVED)

**Status**: ✅ **RESOLVED** via Driver-Executor separation architecture.

Instead of implementing an RPC server in Rust, we implemented a **client-side solution**:

- **`ExecutorShuffleClient`**: Executor-side client that connects to Java LifecycleManager
- **`LifecycleManagerClient` trait**: Abstraction for RPC communication
- **`NettyLifecycleManagerClient`**: Implementation that connects to Java LifecycleManager via Netty RPC

This approach is ideal for **Apache Spark Comet** integration where:
- Driver runs JVM with Java LifecycleManager
- Executor runs Rust code via JNI with `ExecutorShuffleClient`

**Usage**:
```rust
use celeborn_client::{ExecutorShuffleClient, CelebornConfig};

let config = CelebornConfig::builder()
    .app_id("comet-app")
    .master_endpoints(vec!["localhost:9097".to_string()])
    .build()?;

let client = ExecutorShuffleClient::new(config);

// Connect to Java LifecycleManager in Driver
client.setup_lifecycle_manager_ref("driver-host", 9098).await?;

// Now use for shuffle operations
client.register_shuffle(0, 10, 100).await?;
client.push_data(0, 0, 0, 0, &data).await?;
client.mapper_end(0, 0, 0, 10).await?;
```

---

#### 2. MapPartition Shuffle Type Support (P1 - High)

**Problem**: Java ShuffleClient supports two shuffle types:
- **ReducePartition** (supported) - Used by Spark
- **MapPartition** (not supported) - Used by Flink

Missing APIs:
```java
// Java ShuffleClient.java
public abstract void mapPartitionMapperEnd(
    int shuffleId, int mapId, int attemptId, int numMappers, int partitionId);

public abstract PartitionLocation registerMapPartitionTask(
    int shuffleId, int numMappers, int mapId, int attemptId, int partitionId);
```

**Rust Status**: Protocol definitions exist (`PbRegisterMapPartitionTask`) but business logic is not implemented.

**Impact**: Cannot integrate with Flink or other engines using MapPartition mode.

---

#### 3. DFS (HDFS/S3) Storage Read Support (P1 - High)

**Problem**: Celeborn supports multi-layered storage (Memory → Local Disk → HDFS/S3). The Java client has `DfsPartitionReader` for reading data directly from distributed file systems.

**Rust Status**: Only `WorkerPartitionReader` is implemented. Cannot read data stored on HDFS/S3.

**Impact**: Cannot read shuffle data that has been flushed to distributed storage.

**Required Changes**:
- Implement `DfsPartitionReader` for HDFS
- Implement `S3PartitionReader` for S3
- Add Hadoop/S3 client dependencies

---

#### 4. Complete mergeData API (P2 - Medium)

**Problem**: Java ShuffleClient provides a complete merge workflow for batching small data locally:

```java
// Java ShuffleClient.java
public abstract void prepareForMergeData(int shuffleId, int mapId, int attemptId);
public abstract int mergeData(int shuffleId, int mapId, int attemptId, int partitionId, 
                               byte[] data, int offset, int length, int numMappers, int numPartitions);
public abstract void pushMergedData(int shuffleId, int mapId, int attemptId);
```

**Rust Status**: Only `push_merged_data()` exists. Missing `prepareForMergeData()` and `mergeData()`.

**Impact**: Less efficient for workloads with many small records.

---

#### 5. Unified readPartition API (P1 - High)

**Problem**: Java's `readPartition` returns `CelebornInputStream` with:
- **Exactly-Once semantics**: Guaranteed no data loss and no duplicate reads
- **ExceptionMaker**: Custom exception handling
- **MetricsCallback**: Metrics reporting

```java
// Java ShuffleClient.java
public abstract CelebornInputStream readPartition(
    int shuffleId, int appShuffleId, int partitionId, int attemptNumber,
    int startMapIndex, int endMapIndex, ExceptionMaker exceptionMaker,
    MetricsCallback metricsCallback);
```

**Rust Status**: 
- `CelebornInputStream` is implemented with full functionality
- But `fetch_data()` returns `ShuffleDataIterator`, not `CelebornInputStream`
- Missing unified `readPartition` API that matches Java interface

**Required Changes**:
- Add `read_partition()` method to `ShuffleClient` returning `CelebornInputStream`
- Ensure API compatibility with Java client

---

#### 6. cleanup API (P2 - Medium)

**Problem**: Java ShuffleClient provides cleanup for map task state:

```java
// Java ShuffleClient.java
public abstract void cleanup(int shuffleId, int mapId, int attemptId);
```

**Rust Status**: Not implemented.

**Impact**: Potential memory leaks in long-running applications.

---

#### 7. Shuffle ID Mapping (P3 - Low)

**Problem**: Java ShuffleClient supports appShuffleId to shuffleId mapping:

```java
// Java ShuffleClient.java
public abstract int getShuffleId(int appShuffleId, String appShuffleIdentifier, boolean isWriter);
```

**Rust Status**: Not implemented. Uses shuffleId directly.

**Impact**: May cause issues with shuffle ID management in complex applications.

---

#### 8. Failure Reporting (P3 - Low)

**Problem**: Java ShuffleClient provides failure reporting APIs:

```java
// Java ShuffleClient.java
public abstract boolean reportShuffleFetchFailure(int appShuffleId, int shuffleId, FailureType failureType);
public abstract void reportFailure(FailureType failureType);
```

**Rust Status**: Not implemented.

**Impact**: LifecycleManager cannot perform special handling for shuffle failures.

---

#### 9. PushState Management (P2 - Medium)

**Problem**: Java ShuffleClient tracks push state per map task:

```java
// Java ShuffleClient.java
public abstract PushState getPushState(String mapKey);
```

**Rust Status**: Not implemented as a public API.

**Impact**: Cannot query push state for debugging or monitoring.

---

#### 10. Extension Support (P3 - Low)

**Problem**: Java ShuffleClient supports extensions:

```java
// Java ShuffleClient.java
public abstract void setExtension(byte[] extension);
```

**Rust Status**: Not implemented.

**Impact**: Cannot use custom extensions for derived implementations.

---

## Priority Summary

| Priority | Feature | Effort | Impact |
|----------|---------|--------|--------|
| **P0** | LifecycleManager RPC Service | High | Critical for distributed deployment |
| **P1** | Unified readPartition API | Low | API compatibility |
| **P1** | DFS Storage Read | High | Multi-layer storage support |
| **P1** | MapPartition Shuffle Type | Medium | Flink integration |
| **P2** | Complete mergeData API | Low | Performance optimization |
| **P2** | cleanup API | Low | Resource management |
| **P2** | PushState Management | Low | Monitoring |
| **P3** | Shuffle ID Mapping | Low | Complex app support |
| **P3** | Failure Reporting | Low | Error handling |
| **P3** | Extension Support | Low | Extensibility |

## Current Usable Scenarios

The Rust client is currently suitable for:

1. **Single-process applications**: No Driver-Executor separation needed
2. **ReducePartition mode**: Spark-like shuffle patterns
3. **Local disk storage**: Data stored on Worker local disks
4. **Basic read/write**: Simple push/fetch operations
5. **Rust-native applications**: DataFusion, Ballista, or custom Rust compute engines

## Recommended Roadmap

### Phase 1: API Completeness
- [ ] Add unified `read_partition()` API
- [ ] Implement `cleanup()` API
- [ ] Complete `mergeData` workflow

### Phase 2: Distributed Deployment
- [ ] Implement LifecycleManager RPC server
- [ ] Support Netty RPC protocol (for Java interop)
- [ ] Or design Rust-native RPC protocol

### Phase 3: Storage Support
- [ ] Implement DfsPartitionReader for HDFS
- [ ] Implement S3PartitionReader for S3
- [ ] Add storage type detection and routing

### Phase 4: Engine Integration
- [ ] Implement MapPartition shuffle type
- [ ] Add Shuffle ID mapping
- [ ] Implement failure reporting

## References

- [Integration Guide](developers/integrate.md)
- [ShuffleClient Documentation](developers/shuffleclient.md)
- [LifecycleManager Documentation](developers/lifecyclemanager.md)
- [Rust Client README](../client-rust/README.md)
