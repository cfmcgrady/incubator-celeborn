# 实现计划：Zstd 支持完整化重构

## 概述

当前 Rust 客户端中的 Zstd 支持处于"半成品"状态。虽然代码中存在 Zstd 相关的实现，但由于 Cargo.toml 中的 feature 配置不完整，导致 Zstd 在运行时被 fallback 到 None。本计划旨在：

1. 重构 `Cargo.toml` 使依赖可选且与 feature 绑定
2. 在所有相关文件中实现一致的 feature guard
3. 更新默认配置以支持 LZ4（保持现状）并允许轻松切换到 Zstd
4. 增加完整的测试覆盖确保 Zstd 推送和拉取都正常工作

## 架构分析

### 当前问题

1. **Cargo.toml 依赖配置问题**
   - `lz4_flex` 和 `zstd` 都是强制依赖（无条件引入）
   - `compression-lz4` 和 `compression-zstd` feature 定义为空，未与依赖绑定
   - 默认 feature 为空，导致编译时两个压缩库都被引入但代码中的 feature guard 无效

2. **代码中的 Feature Guard 不一致**
   - `push.rs` 第 669-677 行：`compress_data()` 中 Zstd 有 feature guard
   - `fetch.rs` 第 387-398 行：`decompress_data()` 中 Zstd 有 feature guard
   - `input_stream.rs` 第 1188-1196 行：`decompress()` 中 Zstd **没有** feature guard，直接使用 `zstd::decode_all()`

3. **默认配置问题**
   - `config.rs` 第 36 行：`CompressionCodec::default()` 返回 `Lz4`
   - 但如果用户配置为 Zstd 而未启用 feature，会 fallback 到无压缩

### 现有模式

- **压缩编码枚举**：[`CompressionCodec`](client-rust/src/config.rs:22-32) 定义了三种压缩方式
- **推送压缩**：[`DataPusher::compress_data()`](client-rust/src/client/push.rs:665-693) 在推送前压缩数据
- **拉取解压**：[`ShuffleDataIterator::decompress_data()`](client-rust/src/client/fetch.rs:370-400) 在拉取后解压数据
- **流式解压**：[`CelebornInputStream::decompress()`](client-rust/src/client/input_stream.rs:1165-1198) 在读取时解压数据

## 实现步骤

### 第一步：重构 Cargo.toml 依赖配置

**文件**：[`client-rust/Cargo.toml`](client-rust/Cargo.toml)

**目标**：使压缩库依赖可选，与 feature 绑定

**具体改动**：

```toml
[dependencies]
# ... 其他依赖 ...

# Compression (optional)
lz4_flex = { version = "0.11", optional = true }
zstd = { version = "0.13", optional = true }

# ... 其他依赖 ...

[features]
default = ["compression-lz4"]
compression-lz4 = ["lz4_flex"]
compression-zstd = ["zstd"]
# 允许同时启用两个 feature（用于测试）
compression-all = ["compression-lz4", "compression-zstd"]
```

**关键点**：
- 将 `lz4_flex` 和 `zstd` 标记为 `optional = true`
- 创建 `compression-lz4` feature 依赖 `lz4_flex`
- 创建 `compression-zstd` feature 依赖 `zstd`
- 设置默认 feature 为 `["compression-lz4"]`，保持向后兼容
- 创建 `compression-all` feature 用于测试

### 第二步：修复 push.rs 中的 feature guard

**文件**：[`client-rust/src/client/push.rs`](client-rust/src/client/push.rs:665-693)

**目标**：确保 Zstd 压缩代码被正确保护

**当前代码**（第 665-693 行）：
```rust
fn compress_data(&self, data: &[u8]) -> Result<Vec<u8>> {
    match self.config.compression_codec {
        CompressionCodec::None => Ok(data.to_vec()),
        CompressionCodec::Lz4 => {
            #[cfg(feature = "compression-lz4")]
            {
                Ok(lz4_flex::compress_prepend_size(data))
            }
            #[cfg(not(feature = "compression-lz4"))]
            {
                Ok(data.to_vec())
            }
        }
        CompressionCodec::Zstd => {
            #[cfg(feature = "compression-zstd")]
            {
                zstd::encode_all(data, 3).map_err(|e| {
                    CelebornError::Compression(format!("Zstd compression failed: {}", e))
                })
            }
            #[cfg(not(feature = "compression-zstd"))]
            {
                Ok(data.to_vec())
            }
        }
    }
}
```

**改动**：代码已经正确，无需修改。但需要添加文档注释说明 feature 要求。

### 第三步：修复 fetch.rs 中的 feature guard

**文件**：[`client-rust/src/client/fetch.rs`](client-rust/src/client/fetch.rs:370-400)

**目标**：确保 Zstd 解压代码被正确保护

**当前代码**（第 370-400 行）：已正确实现 feature guard，无需修改。

### 第四步：修复 input_stream.rs 中的 feature guard（关键）

**文件**：[`client-rust/src/client/input_stream.rs`](client-rust/src/client/input_stream.rs:1165-1198)

**目标**：为 Zstd 解压代码添加 feature guard

**当前代码**（第 1188-1196 行）：
```rust
CompressionCodec::Zstd => {
    match zstd::decode_all(data) {
        Ok(decompressed) => Ok(decompressed),
        Err(e) => Err(CelebornError::DecompressionFailed(format!(
            "ZSTD decompression failed: {}",
            e
        ))),
    }
}
```

**改动**：添加 feature guard
```rust
CompressionCodec::Zstd => {
    #[cfg(feature = "compression-zstd")]
    {
        match zstd::decode_all(data) {
            Ok(decompressed) => Ok(decompressed),
            Err(e) => Err(CelebornError::DecompressionFailed(format!(
                "ZSTD decompression failed: {}",
                e
            ))),
        }
    }
    #[cfg(not(feature = "compression-zstd"))]
    {
        // Fallback: no decompression
        Ok(data.to_vec())
    }
}
```

### 第五步：添加编译时验证

**文件**：[`client-rust/src/config.rs`](client-rust/src/config.rs)

**目标**：在编译时验证配置的有效性

**改动**：在 `impl CompressionCodec` 中添加验证方法

```rust
impl CompressionCodec {
    /// Check if this codec is available in the current build.
    pub fn is_available(&self) -> bool {
        match self {
            CompressionCodec::None => true,
            CompressionCodec::Lz4 => cfg!(feature = "compression-lz4"),
            CompressionCodec::Zstd => cfg!(feature = "compression-zstd"),
        }
    }

    /// Get a human-readable name for this codec.
    pub fn name(&self) -> &'static str {
        match self {
            CompressionCodec::None => "none",
            CompressionCodec::Lz4 => "lz4",
            CompressionCodec::Zstd => "zstd",
        }
    }
}
```

### 第六步：增加测试覆盖

**新文件**：[`client-rust/tests/compression_test.rs`](client-rust/tests/compression_test.rs)

**目标**：测试 Zstd 推送和拉取功能

**测试场景**：

1. **单元测试**（在 `src/client/push.rs` 和 `src/client/fetch.rs` 中）
   - 测试 Zstd 压缩/解压的正确性
   - 测试压缩数据的可恢复性
   - 测试边界情况（空数据、大数据）

2. **集成测试**（新文件 `tests/compression_test.rs`）
   - 测试 Zstd 推送：数据压缩后推送，验证压缩率
   - 测试 Zstd 拉取：压缩数据拉取后解压，验证数据完整性
   - 测试混合场景：LZ4 推送 + Zstd 拉取（如果支持）
   - 测试 feature 禁用时的 fallback 行为

**具体测试代码框架**：

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zstd_compression_available() {
        #[cfg(feature = "compression-zstd")]
        {
            assert!(CompressionCodec::Zstd.is_available());
        }
        #[cfg(not(feature = "compression-zstd"))]
        {
            assert!(!CompressionCodec::Zstd.is_available());
        }
    }

    #[test]
    fn test_zstd_compress_decompress() {
        #[cfg(feature = "compression-zstd")]
        {
            let data = b"Hello, World! This is test data for Zstd compression.";
            let compressed = zstd::encode_all(data, 3).unwrap();
            let decompressed = zstd::decode_all(compressed.as_slice()).unwrap();
            assert_eq!(data.to_vec(), decompressed);
        }
    }

    #[test]
    fn test_zstd_empty_data() {
        #[cfg(feature = "compression-zstd")]
        {
            let data = b"";
            let compressed = zstd::encode_all(data, 3).unwrap();
            let decompressed = zstd::decode_all(compressed.as_slice()).unwrap();
            assert_eq!(data.to_vec(), decompressed);
        }
    }

    #[test]
    fn test_zstd_large_data() {
        #[cfg(feature = "compression-zstd")]
        {
            let data = vec![42u8; 1024 * 1024]; // 1MB
            let compressed = zstd::encode_all(data.as_slice(), 3).unwrap();
            let decompressed = zstd::decode_all(compressed.as_slice()).unwrap();
            assert_eq!(data, decompressed);
            // Verify compression ratio
            assert!(compressed.len() < data.len());
        }
    }
}
```

### 第七步：更新文档和示例

**文件**：[`client-rust/README.md`](client-rust/README.md)

**改动**：
- 添加 feature 使用说明
- 添加压缩配置示例
- 添加性能对比说明

**示例内容**：

```markdown
## 压缩支持

Celeborn Rust 客户端支持多种压缩算法：

### 启用 LZ4 压缩（默认）
```bash
cargo build --features compression-lz4
```

### 启用 Zstd 压缩
```bash
cargo build --features compression-zstd
```

### 同时启用两种压缩（用于测试）
```bash
cargo build --features compression-all
```

### 配置压缩算法

在配置文件中设置：
```json
{
  "compression_codec": "zstd"
}
```

或在代码中：
```rust
let config = CelebornConfig {
    compression_codec: CompressionCodec::Zstd,
    // ... 其他配置
};
```
```

## 关键文件清单

实现此计划需要修改的关键文件：

1. **[`client-rust/Cargo.toml`](client-rust/Cargo.toml)** - 依赖和 feature 配置
   - 原因：使压缩库依赖可选，与 feature 绑定

2. **[`client-rust/src/client/input_stream.rs`](client-rust/src/client/input_stream.rs:1188-1196)** - 流式解压
   - 原因：添加缺失的 Zstd feature guard

3. **[`client-rust/src/config.rs`](client-rust/src/config.rs)** - 配置验证
   - 原因：添加编译时验证和辅助方法

4. **[`client-rust/src/client/push.rs`](client-rust/src/client/push.rs:665-693)** - 推送压缩
   - 原因：添加文档注释说明 feature 要求

5. **[`client-rust/src/client/fetch.rs`](client-rust/src/client/fetch.rs:370-400)** - 拉取解压
   - 原因：添加文档注释说明 feature 要求

## 依赖和顺序

实现顺序（必须按此顺序）：

1. **第一步**：修改 `Cargo.toml`（基础）
   - 依赖：无
   - 影响：所有后续步骤

2. **第二、三、四步**：修改源代码文件（并行可行）
   - 依赖：第一步完成
   - 影响：编译和运行时行为

3. **第五步**：添加配置验证（可选但推荐）
   - 依赖：第一步完成
   - 影响：编译时检查

4. **第六步**：增加测试（最后）
   - 依赖：第一到五步完成
   - 影响：验证功能正确性

5. **第七步**：更新文档（最后）
   - 依赖：所有步骤完成
   - 影响：用户指导

## 潜在挑战和缓解策略

### 挑战 1：向后兼容性

**问题**：修改 Cargo.toml 的 feature 配置可能影响现有用户

**缓解**：
- 设置默认 feature 为 `["compression-lz4"]`，保持现有行为
- 在 CHANGELOG 中明确说明变更
- 提供迁移指南

### 挑战 2：Feature 组合测试

**问题**：需要测试多种 feature 组合（无压缩、LZ4、Zstd、全部）

**缓解**：
- 在 CI 中配置多个 feature 组合的测试
- 使用条件编译确保代码在所有组合下都能编译
- 添加 `#[cfg(feature = "...")]` 注释说明依赖

### 挑战 3：性能对比

**问题**：用户需要了解 LZ4 vs Zstd 的性能差异

**缓解**：
- 在 `benches/` 目录中添加压缩性能基准测试
- 在文档中提供性能对比表
- 提供选择指南（LZ4：快速，Zstd：高压缩率）

### 挑战 4：错误处理

**问题**：当 feature 禁用但配置要求使用该压缩时，需要清晰的错误信息

**缓解**：
- 在 `CelebornConfig` 初始化时验证 feature 可用性
- 提供清晰的错误消息指导用户启用相应 feature
- 在日志中记录使用的压缩算法

## 代码片段参考

### 现有 LZ4 实现模式（参考）

**推送压缩**（`push.rs` 第 668-677 行）：
```rust
CompressionCodec::Lz4 => {
    #[cfg(feature = "compression-lz4")]
    {
        Ok(lz4_flex::compress_prepend_size(data))
    }
    #[cfg(not(feature = "compression-lz4"))]
    {
        Ok(data.to_vec())
    }
}
```

**拉取解压**（`fetch.rs` 第 373-384 行）：
```rust
CompressionCodec::Lz4 => {
    #[cfg(feature = "compression-lz4")]
    {
        let decompressed = lz4_flex::decompress_size_prepended(data).map_err(|e| {
            CelebornError::Compression(format!("LZ4 decompression failed: {}", e))
        })?;
        Ok(Bytes::from(decompressed))
    }
    #[cfg(not(feature = "compression-lz4"))]
    {
        Ok(data.clone())
    }
}
```

### 需要修复的 Zstd 实现（input_stream.rs）

**当前代码**（第 1188-1196 行）：
```rust
CompressionCodec::Zstd => {
    match zstd::decode_all(data) {  // ❌ 无 feature guard
        Ok(decompressed) => Ok(decompressed),
        Err(e) => Err(CelebornError::DecompressionFailed(format!(
            "ZSTD decompression failed: {}",
            e
        ))),
    }
}
```

**修复后**：
```rust
CompressionCodec::Zstd => {
    #[cfg(feature = "compression-zstd")]
    {
        match zstd::decode_all(data) {
            Ok(decompressed) => Ok(decompressed),
            Err(e) => Err(CelebornError::DecompressionFailed(format!(
                "ZSTD decompression failed: {}",
                e
            ))),
        }
    }
    #[cfg(not(feature = "compression-zstd"))]
    {
        Ok(data.to_vec())
    }
}
```

## 验证清单

完成后需要验证：

- [ ] `cargo build` 默认编译成功（LZ4 启用）
- [ ] `cargo build --features compression-zstd` 编译成功
- [ ] `cargo build --features compression-all` 编译成功
- [ ] `cargo build --no-default-features` 编译成功（无压缩）
- [ ] `cargo test` 所有测试通过
- [ ] `cargo test --features compression-zstd` 所有测试通过
- [ ] `cargo test --features compression-all` 所有测试通过
- [ ] 文档更新完整
- [ ] 示例代码可运行

## 预期成果

完成此计划后：

1. ✅ Zstd 支持完全可用，可通过 feature 启用/禁用
2. ✅ 所有压缩代码路径都有一致的 feature guard
3. ✅ 默认配置保持 LZ4（向后兼容）
4. ✅ 用户可轻松切换到 Zstd 或禁用压缩
5. ✅ 完整的测试覆盖确保功能正确性
6. ✅ 清晰的文档指导用户使用

---

**计划创建时间**：2025-12-31
**预计工作量**：2-3 天（包括测试和文档）
**优先级**：中等（功能完整化）
