# Apache Spark Comet 集成指南

本文档介绍如何将 Apache Celeborn Rust Client 与 Apache Spark Comet（向量化执行引擎）集成。

## 一、概述

### 1.1 什么是 Comet？

[Apache Spark Comet](https://github.com/apache/datafusion-comet) 是一个基于 Apache DataFusion 的 Spark 向量化执行引擎，使用 Rust 实现，通过 JNI 与 Spark 集成，可以显著提升 Spark SQL 查询性能。

### 1.2 为什么需要 Rust Celeborn Client？

Comet 的核心执行逻辑在 Rust 中运行，如果 Shuffle 数据需要经过 JNI 传递给 Java Celeborn Client，会产生：
- **序列化/反序列化开销**：数据需要在 Rust 和 Java 之间转换
- **内存拷贝开销**：数据需要跨越 JNI 边界拷贝
- **上下文切换开销**：频繁的 JNI 调用

使用 Rust Celeborn Client 可以：
- **零拷贝**：直接在 Rust 中处理 Arrow 格式数据
- **原生性能**：避免 JNI 调用开销
- **内存安全**：利用 Rust 的内存安全保证

### 1.3 架构设计

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Spark Driver (JVM)                            │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                   Java LifecycleManager                        │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐    │  │
│  │  │ RegisterShuffle │  │ MapperEnd   │  │ GetReducerFileGroup │    │  │
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘    │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐    │  │
│  │  │ Revive      │  │ PartitionSplit │  │ UnregisterShuffle   │    │  │
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘    │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              ▲                                       │
│                              │ RPC Endpoint (host:port)              │
└──────────────────────────────┼───────────────────────────────────────┘
                               │
                               │ Netty RPC Protocol
                               │
┌──────────────────────────────┼───────────────────────────────────────┐
│                              ▼                                       │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │              Rust ExecutorShuffleClient                        │  │
│  │  ┌─────────────────────────────────────────────────────────┐  │  │
│  │  │ NettyLifecycleManagerClient                              │  │  │
│  │  │  - 连接到 Driver 的 LifecycleManager                     │  │  │
│  │  │  - 发送 RPC 请求 (RegisterShuffle, MapperEnd, etc.)      │  │  │
│  │  └─────────────────────────────────────────────────────────┘  │  │
│  │  ┌─────────────────────────────────────────────────────────┐  │  │
│  │  │ DataPusher                                               │  │  │
│  │  │  - 直接推送数据到 Celeborn Worker                        │  │  │
│  │  │  - 支持 LZ4/ZSTD 压缩                                    │  │  │
│  │  └─────────────────────────────────────────────────────────┘  │  │
│  │  ┌─────────────────────────────────────────────────────────┐  │  │
│  │  │ CelebornInputStream                                      │  │  │
│  │  │  - 从 Celeborn Worker 读取数据                           │  │  │
│  │  │  - 支持故障转移和重试                                    │  │  │
│  │  └─────────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                        Spark Executor (Comet/Rust)                   │
└─────────────────────────────────────────────────────────────────────┘
                               │
                               │ Push/Fetch Data
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       Celeborn Workers                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                  │
│  │  Worker 1   │  │  Worker 2   │  │  Worker 3   │  ...             │
│  └─────────────┘  └─────────────┘  └─────────────┘                  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 二、快速开始

### 2.1 添加依赖

在 Comet 项目的 `Cargo.toml` 中添加：

```toml
[dependencies]
celeborn-client = { path = "path/to/incubator-celeborn/client-rust" }
tokio = { version = "1", features = ["full"] }
```

### 2.2 基本使用

```rust
use celeborn_client::{ExecutorShuffleClient, CelebornConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 创建配置
    let config = CelebornConfig::builder()
        .app_id("comet-app-001")
        .master_endpoints(vec!["celeborn-master:9097".to_string()])
        .build()?;

    // 2. 创建 ExecutorShuffleClient
    let client = ExecutorShuffleClient::new(config);

    // 3. 连接到 Driver 的 LifecycleManager
    // 这个地址由 Spark Driver 通过 JNI 传递给 Executor
    client.setup_lifecycle_manager_ref("spark-driver-host", 9098).await?;

    // 4. 现在可以进行 shuffle 操作了
    // ...

    Ok(())
}
```

---

## 三、Shuffle Write 流程

### 3.1 注册 Shuffle

在 Shuffle Write 开始前，需要先注册 Shuffle：

```rust
// 参数说明：
// - shuffle_id: Shuffle 的唯一标识
// - num_mappers: Map 任务数量
// - num_partitions: 分区数量
let num_partitions = client.register_shuffle(
    shuffle_id,    // e.g., 0
    num_mappers,   // e.g., 10
    num_partitions // e.g., 200
).await?;

println!("Registered shuffle {} with {} partitions", shuffle_id, num_partitions);
```

### 3.2 Push 数据

将 Shuffle 数据推送到 Celeborn Worker：

```rust
// 假设我们有 Arrow RecordBatch 数据
let arrow_data: Vec<u8> = serialize_record_batch(&record_batch)?;

// 推送数据到指定分区
// 参数说明：
// - shuffle_id: Shuffle ID
// - map_id: 当前 Map 任务 ID
// - attempt_id: 任务尝试次数
// - partition_id: 目标分区 ID
// - data: 要推送的数据
client.push_data(
    shuffle_id,
    map_id,
    attempt_id,
    partition_id,
    &arrow_data
).await?;
```

### 3.3 批量 Push（推荐）

对于大量小数据，建议使用批量 Push 以提高效率：

```rust
// 收集多个分区的数据
let mut partition_data: HashMap<i32, Vec<u8>> = HashMap::new();

for (partition_id, record_batch) in partitioned_batches {
    let data = serialize_record_batch(&record_batch)?;
    partition_data.entry(partition_id)
        .or_insert_with(Vec::new)
        .extend(data);
}

// 批量推送
for (partition_id, data) in partition_data {
    client.push_data(shuffle_id, map_id, attempt_id, partition_id, &data).await?;
}
```

### 3.4 Mapper End

当一个 Map 任务完成所有数据推送后，调用 `mapper_end`：

```rust
// 参数说明：
// - shuffle_id: Shuffle ID
// - map_id: 当前 Map 任务 ID
// - attempt_id: 任务尝试次数
// - num_mappers: 总 Map 任务数量
let is_stage_ended = client.mapper_end(
    shuffle_id,
    map_id,
    attempt_id,
    num_mappers
).await?;

if is_stage_ended {
    println!("All mappers finished, stage ended");
}
```

---

## 四、Shuffle Read 流程

### 4.1 读取分区数据

```rust
// 创建输入流读取分区数据
// 参数说明：
// - shuffle_id: Shuffle ID
// - partition_id: 要读取的分区 ID
// - attempt_number: 读取尝试次数
// - start_map_index: 起始 Map 索引（0 表示从头开始）
// - end_map_index: 结束 Map 索引（-1 表示读取所有）
let stream = client.read_partition(
    shuffle_id,
    partition_id,
    attempt_number,
    0,   // start_map_index
    -1   // end_map_index (-1 = all)
).await?;

// 从流中读取数据
// CelebornInputStream 实现了异步读取接口
```

### 4.2 处理 Arrow 数据

```rust
use arrow::ipc::reader::StreamReader;

// 假设 shuffle 数据是 Arrow IPC 格式
let stream = client.read_partition(shuffle_id, partition_id, 0, 0, -1).await?;

// 将数据转换为 Arrow RecordBatch
// 注意：这里需要根据实际的数据格式进行处理
let mut buffer = Vec::new();
// ... 从 stream 读取数据到 buffer ...

let cursor = std::io::Cursor::new(buffer);
let reader = StreamReader::try_new(cursor, None)?;

for batch in reader {
    let record_batch = batch?;
    // 处理 Arrow RecordBatch
    process_batch(&record_batch)?;
}
```

---

## 五、与 Comet 集成

### 5.1 JNI 桥接层设计

Comet 通过 JNI 与 Spark 交互，需要设计桥接层：

```rust
// comet_celeborn_bridge.rs

use jni::JNIEnv;
use jni::objects::{JClass, JString};
use jni::sys::{jint, jlong, jbyteArray};
use std::sync::Arc;
use once_cell::sync::OnceCell;
use tokio::runtime::Runtime;

use celeborn_client::{ExecutorShuffleClient, CelebornConfig};

// 全局 Tokio Runtime
static RUNTIME: OnceCell<Runtime> = OnceCell::new();

// 全局 ShuffleClient 存储
static SHUFFLE_CLIENT: OnceCell<Arc<ExecutorShuffleClient>> = OnceCell::new();

fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime")
    })
}

/// 初始化 Celeborn Client
/// 由 Spark Executor 在启动时调用
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_CelebornBridge_initClient(
    env: JNIEnv,
    _class: JClass,
    app_id: JString,
    master_endpoints: JString,
    lifecycle_manager_host: JString,
    lifecycle_manager_port: jint,
) -> jlong {
    let app_id: String = env.get_string(app_id).unwrap().into();
    let master_endpoints: String = env.get_string(master_endpoints).unwrap().into();
    let lm_host: String = env.get_string(lifecycle_manager_host).unwrap().into();
    let lm_port = lifecycle_manager_port as i32;

    let endpoints: Vec<String> = master_endpoints.split(',').map(|s| s.to_string()).collect();

    let config = CelebornConfig::builder()
        .app_id(&app_id)
        .master_endpoints(endpoints)
        .build()
        .expect("Failed to build config");

    let client = ExecutorShuffleClient::new(config);

    // 连接到 LifecycleManager
    get_runtime().block_on(async {
        client.setup_lifecycle_manager_ref(&lm_host, lm_port).await
    }).expect("Failed to connect to LifecycleManager");

    let client = Arc::new(client);
    SHUFFLE_CLIENT.set(client.clone()).ok();

    Arc::into_raw(client) as jlong
}

/// Push 数据到 Celeborn
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_CelebornBridge_pushData(
    env: JNIEnv,
    _class: JClass,
    shuffle_id: jint,
    map_id: jint,
    attempt_id: jint,
    partition_id: jint,
    data: jbyteArray,
) -> jint {
    let client = SHUFFLE_CLIENT.get().expect("Client not initialized");
    
    let data_vec = env.convert_byte_array(data).expect("Failed to convert byte array");

    let result = get_runtime().block_on(async {
        client.push_data(
            shuffle_id as i32,
            map_id as i32,
            attempt_id as i32,
            partition_id as i32,
            &data_vec
        ).await
    });

    match result {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

/// Mapper End
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_CelebornBridge_mapperEnd(
    _env: JNIEnv,
    _class: JClass,
    shuffle_id: jint,
    map_id: jint,
    attempt_id: jint,
    num_mappers: jint,
) -> jint {
    let client = SHUFFLE_CLIENT.get().expect("Client not initialized");

    let result = get_runtime().block_on(async {
        client.mapper_end(
            shuffle_id as i32,
            map_id as i32,
            attempt_id as i32,
            num_mappers as i32
        ).await
    });

    match result {
        Ok(stage_ended) => if stage_ended { 1 } else { 0 },
        Err(_) => -1,
    }
}

/// 清理资源
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_CelebornBridge_cleanup(
    _env: JNIEnv,
    _class: JClass,
) {
    // 清理全局资源
    // 注意：实际实现需要更细致的资源管理
}
```

### 5.2 Java 端接口

```java
// CelebornBridge.java
package org.apache.comet;

public class CelebornBridge {
    static {
        System.loadLibrary("comet_celeborn_bridge");
    }

    /**
     * 初始化 Celeborn Client
     * @param appId 应用 ID
     * @param masterEndpoints Celeborn Master 地址，逗号分隔
     * @param lifecycleManagerHost Driver 上 LifecycleManager 的主机地址
     * @param lifecycleManagerPort Driver 上 LifecycleManager 的端口
     * @return 客户端句柄
     */
    public static native long initClient(
        String appId,
        String masterEndpoints,
        String lifecycleManagerHost,
        int lifecycleManagerPort
    );

    /**
     * Push 数据到 Celeborn
     * @return 0 成功，-1 失败
     */
    public static native int pushData(
        int shuffleId,
        int mapId,
        int attemptId,
        int partitionId,
        byte[] data
    );

    /**
     * Mapper 结束
     * @return 1 stage 结束，0 stage 未结束，-1 失败
     */
    public static native int mapperEnd(
        int shuffleId,
        int mapId,
        int attemptId,
        int numMappers
    );

    /**
     * 清理资源
     */
    public static native void cleanup();
}
```

### 5.3 Comet ShuffleWriter 集成

```rust
// comet_shuffle_writer.rs

use arrow::record_batch::RecordBatch;
use celeborn_client::ExecutorShuffleClient;

pub struct CometCelebornShuffleWriter {
    client: Arc<ExecutorShuffleClient>,
    shuffle_id: i32,
    map_id: i32,
    attempt_id: i32,
    num_mappers: i32,
    num_partitions: i32,
}

impl CometCelebornShuffleWriter {
    pub fn new(
        client: Arc<ExecutorShuffleClient>,
        shuffle_id: i32,
        map_id: i32,
        attempt_id: i32,
        num_mappers: i32,
        num_partitions: i32,
    ) -> Self {
        Self {
            client,
            shuffle_id,
            map_id,
            attempt_id,
            num_mappers,
            num_partitions,
        }
    }

    /// 写入分区数据
    pub async fn write_partition(
        &self,
        partition_id: i32,
        batch: &RecordBatch,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // 序列化 Arrow RecordBatch 为 IPC 格式
        let data = self.serialize_batch(batch)?;
        
        // 推送到 Celeborn
        self.client.push_data(
            self.shuffle_id,
            self.map_id,
            self.attempt_id,
            partition_id,
            &data
        ).await?;

        Ok(())
    }

    /// 完成写入
    pub async fn finish(&self) -> Result<bool, Box<dyn std::error::Error>> {
        let stage_ended = self.client.mapper_end(
            self.shuffle_id,
            self.map_id,
            self.attempt_id,
            self.num_mappers
        ).await?;

        Ok(stage_ended)
    }

    fn serialize_batch(&self, batch: &RecordBatch) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        use arrow::ipc::writer::StreamWriter;
        
        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())?;
            writer.write(batch)?;
            writer.finish()?;
        }
        Ok(buffer)
    }
}
```

---

## 六、配置参数

### 6.1 CelebornConfig 参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `app_id` | String | 必填 | 应用唯一标识 |
| `master_endpoints` | Vec<String> | 必填 | Celeborn Master 地址列表 |
| `push_buffer_size` | usize | 64KB | Push 缓冲区大小 |
| `push_max_reqs_in_flight` | i32 | 32 | 最大并发 Push 请求数 |
| `fetch_max_reqs_in_flight` | i32 | 3 | 最大并发 Fetch 请求数 |
| `fetch_chunk_size` | usize | 8MB | Fetch 块大小 |
| `compression_codec` | String | "lz4" | 压缩算法 (lz4/zstd/none) |
| `rpc_timeout_ms` | u64 | 30000 | RPC 超时时间（毫秒） |

### 6.2 配置示例

```rust
let config = CelebornConfig::builder()
    .app_id("comet-app-001")
    .master_endpoints(vec![
        "celeborn-master-1:9097".to_string(),
        "celeborn-master-2:9097".to_string(),
    ])
    .push_buffer_size(128 * 1024)  // 128KB
    .push_max_reqs_in_flight(64)
    .fetch_max_reqs_in_flight(4)
    .compression_codec("zstd")
    .rpc_timeout_ms(60000)
    .build()?;
```

---

## 七、错误处理

### 7.1 常见错误类型

```rust
use celeborn_client::CelebornError;

match client.push_data(shuffle_id, map_id, attempt_id, partition_id, &data).await {
    Ok(_) => println!("Push successful"),
    Err(CelebornError::ConnectionError(e)) => {
        // 连接错误，可能需要重试
        eprintln!("Connection error: {}", e);
    }
    Err(CelebornError::RpcError(e)) => {
        // RPC 错误
        eprintln!("RPC error: {}", e);
    }
    Err(CelebornError::PartitionNotFound(e)) => {
        // 分区未找到，可能需要 Revive
        eprintln!("Partition not found: {}", e);
    }
    Err(e) => {
        eprintln!("Other error: {}", e);
    }
}
```

### 7.2 重试策略

```rust
use std::time::Duration;
use tokio::time::sleep;

async fn push_with_retry(
    client: &ExecutorShuffleClient,
    shuffle_id: i32,
    map_id: i32,
    attempt_id: i32,
    partition_id: i32,
    data: &[u8],
    max_retries: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut retries = 0;
    
    loop {
        match client.push_data(shuffle_id, map_id, attempt_id, partition_id, data).await {
            Ok(_) => return Ok(()),
            Err(e) if retries < max_retries => {
                retries += 1;
                let delay = Duration::from_millis(100 * 2u64.pow(retries));
                eprintln!("Push failed, retry {} after {:?}: {}", retries, delay, e);
                sleep(delay).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}
```

---

## 八、性能优化建议

### 8.1 批量操作

- 尽量批量收集数据后再 Push，减少 RPC 调用次数
- 使用合适的 `push_buffer_size` 平衡内存使用和网络效率

### 8.2 并发控制

- 根据网络带宽调整 `push_max_reqs_in_flight`
- 对于高延迟网络，可以增加并发请求数

### 8.3 压缩选择

- **LZ4**：压缩/解压速度快，适合 CPU 敏感场景
- **ZSTD**：压缩率高，适合网络带宽受限场景

### 8.4 内存管理

- 使用 Arrow 的零拷贝特性，避免不必要的数据拷贝
- 及时释放不再使用的 RecordBatch

---

## 九、调试与监控

### 9.1 启用日志

```rust
// 在应用启动时初始化日志
tracing_subscriber::fmt()
    .with_env_filter("celeborn_client=debug")
    .init();
```

### 9.2 关键日志点

- `[celeborn] Connecting to LifecycleManager` - 连接 LifecycleManager
- `[celeborn] Registered shuffle` - Shuffle 注册成功
- `[celeborn] Push data to worker` - 数据推送
- `[celeborn] Mapper end` - Mapper 结束
- `[celeborn] Fetch data from worker` - 数据读取

---

## 十、完整示例

参见 [`examples/comet_integration.rs`](../examples/comet_integration.rs)

```rust
//! Comet 集成完整示例
//!
//! 运行方式：
//! ```bash
//! cargo run --example comet_integration
//! ```

use celeborn_client::{CelebornConfig, ExecutorShuffleClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();

    // 配置
    let config = CelebornConfig::builder()
        .app_id("comet-example")
        .master_endpoints(vec!["localhost:9097".to_string()])
        .build()?;

    // 创建客户端
    let client = ExecutorShuffleClient::new(config);

    // 连接到 LifecycleManager（由 Driver 提供地址）
    client.setup_lifecycle_manager_ref("localhost", 9098).await?;

    // Shuffle Write
    let shuffle_id = 0;
    let map_id = 0;
    let attempt_id = 0;
    let num_mappers = 1;
    let num_partitions = 10;

    // 注册 Shuffle
    client.register_shuffle(shuffle_id, num_mappers, num_partitions).await?;

    // 模拟写入数据
    for partition_id in 0..num_partitions {
        let data = format!("data for partition {}", partition_id).into_bytes();
        client.push_data(shuffle_id, map_id, attempt_id, partition_id, &data).await?;
    }

    // Mapper 结束
    client.mapper_end(shuffle_id, map_id, attempt_id, num_mappers).await?;

    // Shuffle Read
    for partition_id in 0..num_partitions {
        let _stream = client.read_partition(shuffle_id, partition_id, 0, 0, -1).await?;
        // 处理数据...
    }

    println!("Comet integration example completed successfully!");
    Ok(())
}
```

---

## 十一、常见问题

### Q1: 如何获取 LifecycleManager 的地址？

LifecycleManager 运行在 Spark Driver 中，其地址通过 Spark 的 TaskContext 传递给 Executor。在 Comet 中，需要通过 JNI 从 Java 端获取这个地址。

### Q2: 是否支持 Spark 动态分区？

目前 Rust Client 支持固定分区数的 Shuffle。动态分区需要额外的 Revive 机制支持，已在 `ExecutorShuffleClient` 中实现。

### Q3: 如何处理 Worker 故障？

`ExecutorShuffleClient` 内置了 Revive 机制，当 Worker 故障时会自动请求新的分区位置并重试。

### Q4: 是否支持推测执行？

支持。通过 `attempt_id` 参数区分不同的任务尝试，Celeborn 会自动处理重复数据。

---

## 十二、参考资料

- [Apache Celeborn 官方文档](https://celeborn.apache.org/)
- [Apache Spark Comet](https://github.com/apache/datafusion-comet)
- [Rust Client 功能差距分析](./FEATURE_GAP_ANALYSIS.md)
- [Celeborn 集成指南](../../docs/developers/integrate.md)

---

*文档更新时间: 2025-12-25*
