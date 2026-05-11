use thiserror::Error;

/// Core Parquet physical types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParquetType {
    Boolean,
    Int32,
    Int64,
    Int96,
    Float,
    Double,
    ByteArray,
    FixedLenByteArray(i32),
}

impl ParquetType {
    /// Convert from the Thrift type code to our enum
    pub fn from_thrift(code: i32) -> Result<Self> {
        match code {
            0 => Ok(ParquetType::Boolean),
            1 => Ok(ParquetType::Int32),
            2 => Ok(ParquetType::Int64),
            3 => Ok(ParquetType::Int96),
            4 => Ok(ParquetType::Float),
            5 => Ok(ParquetType::Double),
            6 => Ok(ParquetType::ByteArray),
            7 => Ok(ParquetType::FixedLenByteArray(0)), // length set later
            _ => Err(ParquetError::UnsupportedType(format!(
                "Unknown physical type code: {code}"
            ))),
        }
    }
}

/// Encoding types supported by Parquet
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    Plain,
    RleBitPacked,
    DeltaBinaryPacked,
    DeltaLengthByteArray,
    DeltaByteArray,
}

impl Encoding {
    /// Convert from the Thrift encoding code
    pub fn from_thrift(code: i32) -> Result<Self> {
        match code {
            0 => Ok(Encoding::Plain),
            4 => Ok(Encoding::RleBitPacked),
            5 => Ok(Encoding::DeltaBinaryPacked),
            6 => Ok(Encoding::DeltaLengthByteArray),
            7 => Ok(Encoding::DeltaByteArray),
            // Treat unknown encodings as Plain for forward compat
            _ => Ok(Encoding::Plain),
        }
    }
}

/// Compression codecs supported by Parquet
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Lz4,
    Zstd,
}

impl Compression {
    /// Convert from the Thrift compression code
    pub fn from_thrift(code: i32) -> Result<Self> {
        match code {
            0 => Ok(Compression::Uncompressed),
            1 => Ok(Compression::Snappy),
            2 => Ok(Compression::Gzip),
            3 => Ok(Compression::Lzo),
            4 => Ok(Compression::Brotli),
            5 => Ok(Compression::Lz4),
            6 => Ok(Compression::Zstd),
            _ => Err(ParquetError::UnsupportedCompression(format!(
                "Unknown compression code: {code}"
            ))),
        }
    }
}

/// Page types within a column chunk
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    DataPage,
    IndexPage,
    DictionaryPage,
    DataPageV2,
}

impl PageType {
    pub fn from_thrift(code: i32) -> Result<Self> {
        match code {
            0 => Ok(PageType::DataPage),
            1 => Ok(PageType::IndexPage),
            2 => Ok(PageType::DictionaryPage),
            3 => Ok(PageType::DataPageV2),
            _ => Err(ParquetError::DataError(format!(
                "Unknown page type: {code}"
            ))),
        }
    }
}

/// Parquet error types
#[derive(Debug, Error)]
pub enum ParquetError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Invalid Parquet file: {0}")]
    InvalidFile(String),

    #[error("Unsupported physical type: {0}")]
    UnsupportedType(String),

    #[error("Unsupported compression: {0}")]
    UnsupportedCompression(String),

    #[error("Unsupported encoding: {0}")]
    UnsupportedEncoding(String),

    #[error("Data error: {0}")]
    DataError(String),

    #[error("Arrow conversion error: {0}")]
    ArrowError(String),

    #[error("Column index {0} out of range")]
    ColumnOutOfRange(usize),
}

/// Convenience Result type
pub type Result<T> = std::result::Result<T, ParquetError>;

/// Metadata for a single column chunk
#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    /// Column name extracted from schema
    pub name: String,
    /// Physical type of the column
    pub physical_type: ParquetType,
    /// Encoding used
    pub encoding: Encoding,
    /// Compression codec used
    pub compression: Compression,
    /// Number of values in this column chunk
    pub num_values: i64,
    /// Byte offset of the column chunk data in the file
    pub data_offset: i64,
    /// Total compressed size in bytes
    pub total_compressed_size: i64,
    /// Total uncompressed size in bytes
    pub total_uncompressed_size: i64,
}

/// Metadata for a row group
#[derive(Debug, Clone)]
pub struct RowGroupMetadata {
    /// Column chunks in this row group
    pub columns: Vec<ColumnMetadata>,
    /// Total number of rows
    pub num_rows: i64,
    /// Total byte size of the row group
    pub total_byte_size: i64,
}

/// Top-level file metadata
#[derive(Debug, Clone)]
pub struct ParquetMetadata {
    /// Parquet format version
    pub version: i32,
    /// Total number of rows across all row groups
    pub num_rows: i64,
    /// Number of columns
    pub num_columns: usize,
    /// Schema element names
    pub schema_names: Vec<String>,
    /// Row groups
    pub row_groups: Vec<RowGroupMetadata>,
    /// Flattened column metadata (first row group, for convenience)
    pub columns: Vec<ColumnMetadata>,
    /// Created by string
    pub created_by: Option<String>,
}
