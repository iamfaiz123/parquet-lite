use super::CompressionCodec;
use crate::types::Result;

/// Identity codec — data passes through unchanged.
pub struct PlainCodec;

impl CompressionCodec for PlainCodec {
    fn decompress(&self, data: &[u8], _uncompressed_size: usize) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        Ok(data.to_vec())
    }
}
