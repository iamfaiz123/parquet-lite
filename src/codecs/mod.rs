pub mod plain;
pub mod snappy;

use crate::types::{Compression, ParquetError, Result};

/// Trait for compression/decompression codecs
pub trait CompressionCodec {
    fn decompress(&self, data: &[u8], uncompressed_size: usize) -> Result<Vec<u8>>;
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>>;
}

/// Factory: get the appropriate codec for a compression type
pub fn get_codec(compression: Compression) -> Result<Box<dyn CompressionCodec>> {
    match compression {
        Compression::Uncompressed => Ok(Box::new(plain::PlainCodec)),
        Compression::Snappy => {
            #[cfg(feature = "snappy")]
            {
                Ok(Box::new(snappy::SnappyCodec))
            }
            #[cfg(not(feature = "snappy"))]
            {
                Err(ParquetError::UnsupportedCompression(
                    "Snappy support not enabled — build with `--features snappy`".into(),
                ))
            }
        }
        other => Err(ParquetError::UnsupportedCompression(format!(
            "{other:?} codec not implemented"
        ))),
    }
}
