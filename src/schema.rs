use crate::types::*;

/// Schema definition for a single column
#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub physical_type: ParquetType,
    pub logical_type: LogicalType,
    pub encoding: Encoding,
    pub compression: Compression,
    pub required: bool,
}

/// Logical type annotations that give semantic meaning to physical types
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LogicalType {
    String,
    Integer,
    Float,
    Boolean,
    Timestamp(TimestampUnit),
    Date,
    Decimal { precision: u8, scale: u8 },
}

/// Timestamp precision units
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TimestampUnit {
    Millis,
    Micros,
    Nanos,
}

/// Fluent builder for constructing column schemas
pub struct SchemaBuilder {
    columns: Vec<ColumnSchema>,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        SchemaBuilder {
            columns: Vec::new(),
        }
    }

    /// Add a required column with specified types
    pub fn add_column(
        mut self,
        name: impl Into<String>,
        physical_type: ParquetType,
        logical_type: LogicalType,
    ) -> Self {
        self.columns.push(ColumnSchema {
            name: name.into(),
            physical_type,
            logical_type,
            encoding: Encoding::Plain,
            compression: Compression::Uncompressed,
            required: true,
        });
        self
    }

    /// Add an optional (nullable) column
    pub fn add_optional_column(
        mut self,
        name: impl Into<String>,
        physical_type: ParquetType,
        logical_type: LogicalType,
    ) -> Self {
        self.columns.push(ColumnSchema {
            name: name.into(),
            physical_type,
            logical_type,
            encoding: Encoding::Plain,
            compression: Compression::Uncompressed,
            required: false,
        });
        self
    }

    /// Set compression on the most recently added column
    pub fn with_compression(mut self, compression: Compression) -> Self {
        if let Some(col) = self.columns.last_mut() {
            col.compression = compression;
        }
        self
    }

    /// Set encoding on the most recently added column
    pub fn with_encoding(mut self, encoding: Encoding) -> Self {
        if let Some(col) = self.columns.last_mut() {
            col.encoding = encoding;
        }
        self
    }

    /// Consume the builder and return the column schemas
    pub fn build(self) -> Vec<ColumnSchema> {
        self.columns
    }
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ColumnSchema {
    /// Create a column schema with sensible defaults inferred from the physical type
    pub fn default_for_type(name: impl Into<String>, parquet_type: ParquetType) -> Self {
        let logical_type = match parquet_type {
            ParquetType::Boolean => LogicalType::Boolean,
            ParquetType::Int32 => LogicalType::Integer,
            ParquetType::Int64 => LogicalType::Integer,
            ParquetType::Float => LogicalType::Float,
            ParquetType::Double => LogicalType::Float,
            ParquetType::ByteArray => LogicalType::String,
            ParquetType::FixedLenByteArray(_) => LogicalType::String,
            ParquetType::Int96 => LogicalType::Timestamp(TimestampUnit::Nanos),
        };

        ColumnSchema {
            name: name.into(),
            physical_type: parquet_type,
            logical_type,
            encoding: Encoding::Plain,
            compression: Compression::Uncompressed,
            required: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_builder() {
        let schema = SchemaBuilder::new()
            .add_column("id", ParquetType::Int64, LogicalType::Integer)
            .add_column("name", ParquetType::ByteArray, LogicalType::String)
            .add_optional_column("score", ParquetType::Double, LogicalType::Float)
            .with_compression(Compression::Snappy)
            .build();

        assert_eq!(schema.len(), 3);
        assert_eq!(schema[0].name, "id");
        assert!(schema[0].required);
        assert_eq!(schema[2].name, "score");
        assert!(!schema[2].required);
        assert_eq!(schema[2].compression, Compression::Snappy);
    }

    #[test]
    fn test_default_for_type() {
        let col = ColumnSchema::default_for_type("age", ParquetType::Int32);
        assert_eq!(col.logical_type, LogicalType::Integer);
        assert!(col.required);

        let col = ColumnSchema::default_for_type("name", ParquetType::ByteArray);
        assert_eq!(col.logical_type, LogicalType::String);
    }
}
