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

//! Benchmarks for push data operations.

use bytes::{Bytes, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

// Benchmark message encoding
fn bench_push_data_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("push_data_encode");
    
    for size in [1024, 4096, 16384, 65536].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(
            format!("encode_{}b", size),
            size,
            |b, &size| {
                let data = vec![0u8; size];
                b.iter(|| {
                    let mut buf = BytesMut::with_capacity(size + 100);
                    buf.extend_from_slice(b"test-app-1");
                    buf.extend_from_slice(b"0-0");
                    buf.extend_from_slice(&data);
                    black_box(buf.freeze())
                });
            },
        );
    }
    
    group.finish();
}

// Benchmark compression
fn bench_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression");
    
    // Generate compressible data
    let data: Vec<u8> = (0..65536).map(|i| (i % 256) as u8).collect();
    
    group.throughput(Throughput::Bytes(data.len() as u64));
    
    group.bench_function("lz4_compress", |b| {
        b.iter(|| {
            black_box(lz4_flex::compress_prepend_size(&data))
        });
    });
    
    let compressed = lz4_flex::compress_prepend_size(&data);
    group.bench_function("lz4_decompress", |b| {
        b.iter(|| {
            black_box(lz4_flex::decompress_size_prepended(&compressed).unwrap())
        });
    });
    
    group.finish();
}

// Benchmark buffer operations
fn bench_buffer_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_ops");
    
    group.bench_function("bytesmut_extend", |b| {
        let data = vec![0u8; 1024];
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(65536);
            for _ in 0..64 {
                buf.extend_from_slice(&data);
            }
            black_box(buf.freeze())
        });
    });
    
    group.bench_function("bytesmut_reserve_extend", |b| {
        let data = vec![0u8; 1024];
        b.iter(|| {
            let mut buf = BytesMut::new();
            for _ in 0..64 {
                buf.reserve(1024);
                buf.extend_from_slice(&data);
            }
            black_box(buf.freeze())
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_push_data_encode,
    bench_compression,
    bench_buffer_operations,
);
criterion_main!(benches);
