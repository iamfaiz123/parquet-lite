//! # parquet-lite
//!
//! A lightweight, pure-Rust alternative to the official Apache Parquet crate.
//!
//! Designed for projects where the full `parquet` crate is overkill.
//! `parquet-lite` provides read-path essentials in a fraction of the size,
//! with zero unsafe code and full WASM compatibility.
//!
//! ## Key Differences from Official Crate
//!
//! | Feature                | `parquet` (official) | `parquet-lite`     |
//! |------------------------|----------------------|--------------------|
//! | Binary size            | Large                | Small              |
//! | Dependencies           | ~80                  | ~15                |
//! | Thrift dependency      | Yes                  | No (hand-rolled)   |
//! | Read support           | Full                 | Flat schemas        |
//! | Write support          | Yes                  | Not yet             |
//! | Arrow integration      | Yes                  | Yes                 |
//! | WASM compatible        | Partial              | Full                |
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use parquet_lite::*;
//! use std::fs;
//!
//! let data = fs::read("data.parquet").unwrap();
//!
//! // Read metadata
//! let metadata = read_metadata(&data).unwrap();
//! println!("Rows: {}, Columns: {}", metadata.num_rows, metadata.num_columns);
//!
//! // Read as Arrow batches
//! let batches = read_to_arrow_batches(&data, 1024).unwrap();
//! for batch in batches {
//!     let batch = batch.unwrap();
//!     println!("Batch: {} rows", batch.num_rows());
//! }
//! ```
//!
//! ## Feature Flags
//!
//! | Feature  | Default | Description                              |
//! |----------|---------|------------------------------------------|
//! | `snappy` | ✅      | Snappy compression/decompression         |
//! | `serde`  | ❌      | Serde serialization for metadata         |
//! | `wasm`   | ❌      | WASM bindings via wasm-bindgen           |
//! | `full`   | ❌      | All features enabled                     |

pub mod types;
pub mod schema;
pub mod metadata;
pub mod codecs;
pub mod reader;
pub mod arrow_convert;
pub mod batch_iter_advanced;
pub mod streaming_reader;
pub mod statistics;

#[cfg(target_arch = "wasm32")]
pub mod wasm;

// Re-export key types for convenience
pub use types::{
    Compression, Encoding, ParquetError, ParquetMetadata, ParquetType, Result,
    ColumnMetadata, RowGroupMetadata,
};
pub use schema::{ColumnSchema, LogicalType, SchemaBuilder, TimestampUnit};
pub use reader::{ColumnData, ParquetReader};
pub use arrow_convert::ArrowConverter;
pub use batch_iter_advanced::SelectiveBatchIterator;
pub use streaming_reader::StreamingParquetReader;
pub use statistics::{ColumnStatistics, StatisticsCollector};

// ---------------------------------------------------------------------------
// Top-level convenience API
// ---------------------------------------------------------------------------

/// Parse metadata from raw Parquet file bytes.
///
/// This is a lightweight operation — only the footer is parsed,
/// no column data is read.
pub fn read_metadata(data: &[u8]) -> Result<ParquetMetadata> {
    metadata::MetadataReader::read_metadata(data)
}

/// Create a batch iterator over Arrow RecordBatches.
///
/// Reads all columns and yields batches of `batch_size` rows.
pub fn read_to_arrow_batches(
    data: &[u8],
    batch_size: usize,
) -> Result<SelectiveBatchIterator> {
    let reader = ParquetReader::new(data)?;
    Ok(SelectiveBatchIterator::new(reader, batch_size))
}

/// Create a batch iterator reading only the specified columns.
pub fn read_columns_to_arrow_batches(
    data: &[u8],
    batch_size: usize,
    columns: Vec<usize>,
) -> Result<SelectiveBatchIterator> {
    let reader = ParquetReader::new(data)?;
    Ok(SelectiveBatchIterator::new(reader, batch_size).with_columns(columns))
}

/// Print a formatted statistics summary to stdout.
pub fn print_stats(data: &[u8]) -> Result<()> {
    let metadata = read_metadata(data)?;
    StatisticsCollector::print_summary(&metadata);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_magic() {
        let data = b"NOT_PARQUET_DATA_AT_ALL!!";
        let result = read_metadata(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_too_small() {
        let data = b"PAR1";
        let result = read_metadata(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_builder_integration() {
        let schema = SchemaBuilder::new()
            .add_column("id", ParquetType::Int64, LogicalType::Integer)
            .add_column("name", ParquetType::ByteArray, LogicalType::String)
            .add_optional_column("value", ParquetType::Double, LogicalType::Float)
            .with_compression(Compression::Snappy)
            .build();

        assert_eq!(schema.len(), 3);
        assert_eq!(schema[0].name, "id");
        assert_eq!(schema[1].name, "name");
        assert_eq!(schema[2].name, "value");
        assert!(!schema[2].required);
    }
}
