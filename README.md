# parquet-lite

[![Crates.io](https://img.shields.io/crates/v/parquet-lite.svg)](https://crates.io/crates/parquet-lite)
[![Documentation](https://docs.rs/parquet-lite/badge.svg)](https://docs.rs/parquet-lite)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A lightweight, pure-Rust alternative to the official Apache Parquet crate.

Designed for projects where the full `parquet` crate is overkill. `parquet-lite` provides read-path essentials in a fraction of the size, with zero `unsafe` code and full WASM compatibility out of the box.

## Why use `parquet-lite`?

If you've ever tried to compile the official `parquet` crate for WebAssembly or embedded environments, you likely noticed it pulls in dozens of dependencies, takes a while to compile, and adds significant bloat to your binary size.

`parquet-lite` takes a different approach:
* **No Thrift dependency**: It uses a hand-rolled, zero-allocation decoder for just the parts of the Thrift Compact Protocol needed to parse Parquet footers.
* **Granular Arrow Integration**: Uses lightweight `arrow-array` and `arrow-schema` sub-crates rather than the monolithic `arrow` crate to avoid pulling in CSV/JSON/IPC modules you don't need.
* **Minimal Footprint**: By omitting write support and complex nested schemas, it strips away thousands of lines of code.

### When to use `parquet-lite`

* **WebAssembly (WASM)**: You are building a frontend application that needs to read Parquet files in the browser and want to keep your `.wasm` payload small.
* **Edge / Serverless**: You have strict deployment size constraints (e.g., Cloudflare Workers, AWS Lambda).
* **Embedded Systems**: Memory and storage are tightly constrained.
* **Simple Workloads**: You just need to read flat tabular data into Arrow arrays and don't need the kitchen sink.

## What is NOT supported

To keep the library small and focused, the following features from the official crate are deliberately **omitted**:

* ❌ **Writing Parquet files**: This is a read-only library.
* ❌ **Nested Types**: Structs, Lists, and Maps are not supported. Only flat tabular schemas are supported.
* ❌ **Complex Encodings**: Only Plain encoding is currently supported (Dictionary, RLE, and Delta encodings are planned but not yet implemented).
* ❌ **All Compressions**: Snappy is supported (and default). GZIP, Brotli, ZSTD, etc. are currently unsupported.
* ❌ **Full Thrift Parsing**: Only the subset of Parquet Thrift metadata required for reading data is parsed.

## Quick Start

Add `parquet-lite` to your `Cargo.toml`:

```toml
[dependencies]
parquet-lite = "0.2"
```

Read metadata and data:

```rust,no_run
use parquet_lite::*;
use std::fs;

// 1. Read raw bytes
let data = fs::read("data.parquet").unwrap();

// 2. Read metadata quickly
let metadata = read_metadata(&data).unwrap();
println!("Rows: {}, Columns: {}", metadata.num_rows, metadata.num_columns);

// 3. Read into Arrow RecordBatches
let batches = read_to_arrow_batches(&data, 1024).unwrap();
for batch in batches {
    let batch = batch.unwrap();
    println!("Batch: {} rows", batch.num_rows());
}
```

## Feature Flags

| Feature  | Default | Description                              |
|----------|---------|------------------------------------------|
| `snappy` | ✅      | Snappy compression/decompression via `snap` |
| `serde`  | ❌      | Serde serialization for metadata types   |
| `wasm`   | ❌      | First-class WASM bindings via `wasm-bindgen` |
| `full`   | ❌      | All optional features enabled            |

## WASM Example

Enable the `wasm` feature:
```toml
parquet-lite = { version = "0.2", features = ["wasm"] }
```

In your JavaScript/TypeScript:
```typescript
import init, { WasmParquetReader } from './parquet_lite.js';

async function readParquet() {
    await init();
    
    const response = await fetch('data.parquet');
    const data = new Uint8Array(await response.arrayBuffer());
    
    const reader = new WasmParquetReader(data);
    
    console.log(`Rows: ${reader.num_rows()}`);
    
    // Read column data as JSON
    const colData = JSON.parse(reader.read_column_json(0));
    console.log('Values:', colData);
}
```

## License

Licensed under the Apache License, Version 2.0 ([LICENSE](LICENSE) or http://www.apache.org/licenses/LICENSE-2.0).
