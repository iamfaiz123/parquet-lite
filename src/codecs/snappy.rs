use super::CompressionCodec;
use crate::types::{ParquetError, Result};

/// Snappy compression codec using the `snap` crate.
pub struct SnappyCodec;

#[allow(unused_variables)]
impl CompressionCodec for SnappyCodec {
    fn decompress(&self, data: &[u8], _uncompressed_size: usize) -> Result<Vec<u8>> {
        #[cfg(feature = "snappy")]
        {
            snap::raw::Decoder::new()
                .decompress_vec(data)
                .map_err(|e| ParquetError::DataError(format!("Snappy decompress error: {e}")))
        }
        #[cfg(not(feature = "snappy"))]
        {
            Err(ParquetError::UnsupportedCompression(
                "Snappy support not enabled".into(),
            ))
        }
    }

    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        #[cfg(feature = "snappy")]
        {
            snap::raw::Encoder::new()
                .compress_vec(data)
                .map_err(|e| ParquetError::DataError(format!("Snappy compress error: {e}")))
        }
        #[cfg(not(feature = "snappy"))]
        {
            Err(ParquetError::UnsupportedCompression(
                "Snappy support not enabled".into(),
            ))
        }
    }
}
