# Rust Client vs Java Spark Client 功能差距分析

本文档分析了 Rust client 要完全替代 Java Spark client 需要实现的功能差距。

## 一、已实现的核心功能 ✅

| 功能 | Rust 实现位置 | 状态 |
|------|---------------|------|
| **Shuffle 注册** | `LifecycleManager::register_shuffle` (lifecycle.rs:158-346) | ✅ 完成 |
| **Push Data** | `DataPusher::push_data` (push.rs:143-230) | ✅ 基础完成 |
| **Push Merged Data** | `DataPusher::push_merged_data` (push.rs:233-269) | ✅ 基础完成 |
| **Mapper End** | `LifecycleManager::mapper_end` (lifecycle.rs:453-486) | ✅ 完成 |
| **Stage End** | `LifecycleManager::stage_end` (lifecycle.rs:492-589) | ✅ 完成 |
| **Wait Stage End** | `LifecycleManager::wait_stage_end` (lifecycle.rs:594-622) | ✅ 完成 |
| **Commit Files** | `LifecycleManager::request_commit_files` (lifecycle.rs:681-795) | ✅ 完成 |
| **Fetch Data** | `ShuffleDataIterator` (fetch.rs:36-53) | ✅ 基础完成 |
| **心跳** | `LifecycleManager::start_heartbeat` (lifecycle.rs:168-215) | ✅ 完成 |
| **LZ4/ZSTD 压缩** | `DataPusher::compress_data` (push.rs:445-473) | ✅ 完成 |
| **Unregister Shuffle** | `LifecycleManager::unregister_shuffle` (lifecycle.rs:853-901) | ✅ 完成 |
| **Revive 机制** | `ReviveManager` (revive.rs) | ✅ 完成 |
| **Partition Split** | `PartitionLocationManager`, `SplitHandler` (partition_split.rs) | ✅ 完成 |
| **WorkerPartitionReader** | `WorkerPartitionReader`, `PartitionReader` trait (partition_reader.rs) | ✅ 完成 |
| **CelebornInputStream** | `CelebornInputStream` (input_stream.rs) | ✅ 完成 |
| **Map Range Read** | `start_map_index`/`end_map_index` 支持 | ✅ 完成 |
| **Batch Deduplication** | 在 `CelebornInputStream` 中实现 | ✅ 完成 |
| **Excluded Worker Tracking** | 在 `CelebornInputStream` 中实现 | ✅ 完成 |

---

## 二、新增功能：Driver-Executor 分离架构 ✅

### 2.1 概述

为支持 **Apache Spark Comet**（向量化引擎）接入，实现了 Driver-Executor 分离架构：

| 功能 | Rust 实现位置 | 状态 |
|------|---------------|------|
| **ExecutorShuffleClient** | `executor_shuffle_client.rs` | ✅ 完成 |
| **LifecycleManagerClient Trait** | `lifecycle_client.rs` | ✅ 完成 |
| **NettyLifecycleManagerClient** | 连接 Java LifecycleManager via Netty RPC | ✅ 完成 |
| **LocalLifecycleManagerClient** | 单进程模式 | ✅ 完成 |
| **Comet 集成示例** | `examples/comet_integration.rs` | ✅ 完成 |

### 2.2 架构设计

```
┌─────────────────────────────────────────────────────────────┐
│                     Spark Driver (JVM)                       │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │              Java LifecycleManager                       │ │
│  │  - RegisterShuffle, MapperEnd, GetReducerFileGroup       │ │
│  │  - Revive, PartitionSplit                                │ │
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │ Netty RPC
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Spark Executor (Comet)                     │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │           Rust ExecutorShuffleClient                     │ │
│  │  - setup_lifecycle_manager_ref(host, port)               │ │
│  │  - register_shuffle(), push_data(), mapper_end()         │ │
│  │  - read_partition()                                      │ │
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### 2.3 使用示例

```rust
use celeborn_client::{ExecutorShuffleClient, CelebornConfig};

let config = CelebornConfig::builder()
    .app_id("comet-app")
    .master_endpoints(vec!["localhost:9097".to_string()])
    .build()?;

let client = ExecutorShuffleClient::new(config);

// 连接到 Driver 的 Java LifecycleManager
client.setup_lifecycle_manager_ref("driver-host", 9098).await?;

// 执行 shuffle 操作
client.register_shuffle(0, 10, 100).await?;
client.push_data(0, 0, 0, 0, &data).await?;
client.mapper_end(0, 0, 0, 10).await?;

// 读取数据
let stream = client.read_partition(0, 0, 0, 0, -1).await?;
```

---

## 三、缺失的核心功能 ❌

### 1. Push 重试与回调机制 - 🔴 高优先级

**Java 实现参考**：
- `ShuffleClientImpl::submitRetryPushData` (ShuffleClientImpl.java:235-327)
- `PushDataRpcResponseCallback` (ShuffleClientImpl.java:980-1162)

**功能描述**：
当前 Rust 实现是同步等待响应，缺少：
- 异步回调机制
- 失败重试队列
- 限流控制（limitMaxInFlight）

**Rust 需要实现**：
- [ ] `PushDataRpcResponseCallback` 异步回调 trait
- [ ] 重试队列管理
- [ ] 并发请求限流 (`limitMaxInFlight`, `limitZeroInFlight`)
- [ ] `PushState` 状态管理

---

### 2. 多种 PartitionReader - 🟡 中优先级 (部分完成)

**Java 实现参考**：
- `WorkerPartitionReader` (WorkerPartitionReader.java) - 从 Worker 读取
- `LocalPartitionReader` (LocalPartitionReader.java) - 本地读取
- `DfsPartitionReader` (DfsPartitionReader.java) - 从 HDFS/S3 读取

**功能描述**：
支持从不同存储位置读取 shuffle 数据

**Rust 实现状态**：
- [x] `PartitionReader` trait 定义 (partition_reader.rs:46-62)
- [x] `WorkerPartitionReader` - 从 Worker 读取 (partition_reader.rs:118-500)
- [x] Chunk 预取机制 (`fetch_max_reqs_in_flight`) (partition_reader.rs:240-290)
- [x] `WorkerPartitionReaderBuilder` 构建器模式 (partition_reader.rs:510-570)
- [ ] `LocalPartitionReader` - 本地磁盘读取
- [ ] `DfsPartitionReader` - 分布式文件系统读取（HDFS/S3）

---

### 3. Worker 状态追踪 - 🟡 中优先级

**Java 实现参考**：
- `WorkerStatusTracker` (WorkerStatusTracker.scala)
- `WorkerStatusListener` (WorkerStatusListener.java)

**功能描述**：
追踪 Worker 健康状态：
- 排除失败的 Worker
- 监听 Worker 状态变化
- 黑名单管理

**Rust 需要实现**：
- [ ] `WorkerStatusTracker` 结构体
- [ ] `WorkerStatusListener` trait
- [ ] Worker 黑名单/排除列表
- [ ] `excludeWorkerByCause` 方法

---

### 4. MapPartition Shuffle 类型支持 - 🟡 中优先级

**问题**：Java ShuffleClient 支持两种 shuffle 类型：
- **ReducePartition** (已支持) - Spark 使用
- **MapPartition** (未支持) - Flink 使用

缺失的 API：
```java
// Java ShuffleClient.java
public abstract void mapPartitionMapperEnd(
    int shuffleId, int mapId, int attemptId, int numMappers, int partitionId);

public abstract PartitionLocation registerMapPartitionTask(
    int shuffleId, int numMappers, int mapId, int attemptId, int partitionId);
```

**Rust 状态**：协议定义存在 (`PbRegisterMapPartitionTask`)，但业务逻辑未实现。

**影响**：无法与 Flink 或其他使用 MapPartition 模式的引擎集成。

---

### 5. Shuffle 过期清理 - 🟢 低优先级

**Java 实现参考**：
- `LifecycleManager` 中的定期清理逻辑
- `shuffleExpiredCheckIntervalMs` 配置

**功能描述**：
定期检查并清理过期的 shuffle 数据

**Rust 需要实现**：
- [ ] 过期 shuffle 检测
- [ ] 资源清理定时任务
- [ ] `removeExpiredShuffle` 方法

---

### 6. cleanup API - 🟢 低优先级

**问题**：Java ShuffleClient 提供 map task 状态清理：

```java
// Java ShuffleClient.java
public abstract void cleanup(int shuffleId, int mapId, int attemptId);
```

**Rust 状态**：未实现。

**影响**：长时间运行的应用可能存在内存泄漏。

---

### 7. Shuffle ID 映射 - 🟢 低优先级

**问题**：Java ShuffleClient 支持 appShuffleId 到 shuffleId 的映射：

```java
// Java ShuffleClient.java
public abstract int getShuffleId(int appShuffleId, String appShuffleIdentifier, boolean isWriter);
```

**Rust 状态**：未实现，直接使用 shuffleId。

**影响**：复杂应用中可能存在 shuffle ID 管理问题。

---

### 8. 失败报告 - 🟢 低优先级

**问题**：Java ShuffleClient 提供失败报告 API：

```java
// Java ShuffleClient.java
public abstract boolean reportShuffleFetchFailure(int appShuffleId, int shuffleId, FailureType failureType);
public abstract void reportFailure(FailureType failureType);
```

**Rust 状态**：未实现。

**影响**：LifecycleManager 无法对 shuffle 失败进行特殊处理。

---

## 四、协议层缺失 ❌

### 1. PushDataHandShake - 已有定义但未使用

**用途**：建立 Push 连接的握手协议

**状态**：已定义 `PushDataHandShake` 结构体，但未在实际流程中使用

---

### 2. RegionStart/RegionFinish - 未实现

**用途**：用于 MapPartition 模式的区域标记

**Rust 需要实现**：
- [ ] `RegionStart` 消息
- [ ] `RegionFinish` 消息
- [ ] MapPartition 模式支持

---

### 3. Backlog 通知 - 未实现

**用途**：用于流控的积压通知机制

**Rust 需要实现**：
- [ ] `BacklogAnnouncement` 消息
- [ ] 流控机制

---

### 4. ReadAddCredit - 未实现

**用途**：读取端信用控制

**Rust 需要实现**：
- [ ] `ReadAddCredit` 消息
- [ ] 信用流控机制

---

## 五、Spark 集成层缺失 ❌

要真正替代 Java Spark client，还需要通过 JNI 或 FFI 与 Spark 集成：

| 组件 | 说明 | 优先级 |
|------|------|--------|
| **ShuffleManager** | Spark ShuffleManager 接口实现 | 高 |
| **ShuffleWriter** | HashBasedShuffleWriter / SortBasedShuffleWriter | 高 |
| **ShuffleReader** | Spark ShuffleReader 接口 | 高 |
| **SortBasedPusher** | 排序后批量推送 | 中 |
| **ShuffleInMemorySorter** | 内存排序器 | 中 |
| **ExecutorShuffleIdTracker** | Executor shuffle ID 追踪 | 低 |

> ⚠️ 这些需要通过 JNI 或 FFI 与 Spark 集成，或者作为独立的 Rust 应用使用

---

## 六、优先级总结

| 优先级 | 功能 | 工作量 | 影响 |
|--------|------|--------|------|
| **P0** | ~~LifecycleManager RPC 服务~~ | ~~高~~ | ~~分布式部署关键~~ ✅ 已通过 Driver-Executor 分离解决 |
| **P1** | Push 重试与回调机制 | 中 | 生产稳定性 |
| **P1** | DFS 存储读取 | 高 | 多层存储支持 |
| **P1** | MapPartition Shuffle 类型 | 中 | Flink 集成 |
| **P2** | Worker 状态追踪 | 中 | 故障处理 |
| **P2** | cleanup API | 低 | 资源管理 |
| **P3** | Shuffle ID 映射 | 低 | 复杂应用支持 |
| **P3** | 失败报告 | 低 | 错误处理 |
| **P3** | Shuffle 过期清理 | 低 | 长期运行 |

---

## 七、当前可用场景

Rust client 目前适用于：

1. **Driver-Executor 分离架构**：Comet 等向量化引擎
2. **单进程应用**：无需 Driver-Executor 分离
3. **ReducePartition 模式**：Spark 风格的 shuffle 模式
4. **本地磁盘存储**：数据存储在 Worker 本地磁盘
5. **基础读写**：简单的 push/fetch 操作
6. **Rust 原生应用**：DataFusion、Ballista 或自定义 Rust 计算引擎

---

## 八、建议实现路线图

```
Phase 1 (核心功能) - 生产可用的最小集:
├── ✅ Revive 机制 (已完成)
├── ✅ Partition Split (已完成)
├── ✅ WorkerPartitionReader (已完成)
├── ✅ Stage End 处理 (已完成)
├── ✅ CelebornInputStream (已完成)
├── ✅ Driver-Executor 分离架构 (已完成)
├── Push 重试与回调
└── Worker 状态追踪

Phase 2 (完整性) - 功能完备:
├── LocalPartitionReader
└── DfsPartitionReader

Phase 3 (生产就绪) - 稳定性增强:
├── Shuffle 过期清理
├── 完整的错误处理和日志
└── Metrics 支持

Phase 4 (Spark 集成) - 完全替代 Java client:
└── JNI/FFI 桥接层
```

---

## 九、技术债务

### 当前代码中的 TODO/FIXME

1. **push.rs**: `map_id` 和 `attempt_id` 未使用（警告）
2. **fetch.rs**: `transport_client` 字段未使用
3. **protocol/mod.rs**: 存在 ambiguous glob re-exports 警告
4. **connection.rs**: `writer_handle` 和 `reader_handle` 未使用

### 需要改进的设计

1. **错误处理**：需要更细粒度的错误类型和重试策略
2. **连接管理**：连接池需要支持健康检查和自动重连
3. **配置管理**：需要支持从配置文件加载
4. **指标收集**：需要添加 metrics 支持

---

## 十、参考资料

- Java Client 源码: `client/src/main/java/org/apache/celeborn/client/`
- Scala LifecycleManager: `client/src/main/scala/org/apache/celeborn/client/LifecycleManager.scala`
- Spark Client: `client-spark/spark-3/src/main/java/org/apache/spark/shuffle/celeborn/`
- 协议定义: `common/src/main/proto/TransportMessages.proto`
- [Integration Guide](../../docs/developers/integrate.md)
- [ShuffleClient Documentation](../../docs/developers/shuffleclient.md)
- [LifecycleManager Documentation](../../docs/developers/lifecyclemanager.md)

---

*文档更新时间: 2025-12-25*
