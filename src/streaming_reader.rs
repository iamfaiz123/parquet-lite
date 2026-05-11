use crate::types::*;
use std::io::Read;

/// Memory-efficient streaming reader for Parquet files.
///
/// Reads data in chunks without loading the entire file into memory.
/// Useful for memory-constrained environments (WASM, embedded).
pub struct StreamingParquetReader<R: Read> {
    reader: R,
    buffer_size: usize,
    current_buffer: Vec<u8>,
}

impl<R: Read> StreamingParquetReader<R> {
    /// Create a new streaming reader with the given buffer size.
    pub fn new(reader: R, buffer_size: usize) -> Self {
        StreamingParquetReader {
            reader,
            buffer_size,
            current_buffer: Vec::with_capacity(buffer_size),
        }
    }

    /// Read the next chunk of data from the underlying reader.
    ///
    /// Returns `None` when the reader is exhausted.
    pub fn read_next_chunk(&mut self) -> Result<Option<Vec<u8>>> {
        self.current_buffer.clear();

        let mut temp_buf = vec![0u8; self.buffer_size];
        match self.reader.read(&mut temp_buf) {
            Ok(0) => Ok(None),
            Ok(n) => {
                self.current_buffer.extend_from_slice(&temp_buf[..n]);
                Ok(Some(self.current_buffer.clone()))
            }
            Err(e) => Err(ParquetError::IoError(e)),
        }
    }

    /// Read the entire stream into memory and parse metadata.
    ///
    /// This is a convenience method for cases where you can afford to
    /// buffer the whole file but want to use the streaming interface.
    pub fn read_all_metadata(&mut self) -> Result<ParquetMetadata> {
        let mut all_data = Vec::new();
        loop {
            match self.read_next_chunk()? {
                Some(chunk) => all_data.extend(chunk),
                None => break,
            }
        }
        crate::metadata::MetadataReader::read_metadata(&all_data)
    }

    /// Get the current buffer contents.
    pub fn current_buffer(&self) -> &[u8] {
        &self.current_buffer
    }

    /// Get the configured buffer size.
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_streaming_reader_chunks() {
        let data = vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let cursor = Cursor::new(data.clone());
        let mut reader = StreamingParquetReader::new(cursor, 4);

        let chunk1 = reader.read_next_chunk().unwrap().unwrap();
        assert_eq!(chunk1.len(), 4);
        assert_eq!(chunk1, vec![1, 2, 3, 4]);

        let chunk2 = reader.read_next_chunk().unwrap().unwrap();
        assert_eq!(chunk2.len(), 4);
        assert_eq!(chunk2, vec![5, 6, 7, 8]);

        let chunk3 = reader.read_next_chunk().unwrap().unwrap();
        assert_eq!(chunk3.len(), 2);
        assert_eq!(chunk3, vec![9, 10]);

        let chunk4 = reader.read_next_chunk().unwrap();
        assert!(chunk4.is_none());
    }
}
