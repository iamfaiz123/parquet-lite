use crate::codecs;
use crate::metadata::MetadataReader;
use crate::types::*;

/// Decoded column data variants
#[derive(Debug, Clone)]
pub enum ColumnData {
    Boolean(Vec<u8>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float(Vec<f32>),
    Double(Vec<f64>),
    ByteArray(Vec<Vec<u8>>),
}

/// Main Parquet file reader.
///
/// Holds a reference to the raw file bytes and parsed metadata.
/// Columns are read on demand via `read_column()`.
pub struct ParquetReader {
    data: Vec<u8>,
    metadata: ParquetMetadata,
}

impl ParquetReader {
    /// Create a new reader from raw Parquet file bytes.
    pub fn new(data: &[u8]) -> Result<Self> {
        let metadata = MetadataReader::read_metadata(data)?;
        Ok(ParquetReader {
            data: data.to_vec(),
            metadata,
        })
    }

    /// Get parsed metadata
    pub fn metadata(&self) -> &ParquetMetadata {
        &self.metadata
    }

    /// Total number of rows
    pub fn num_rows(&self) -> i64 {
        self.metadata.num_rows
    }

    /// Number of columns
    pub fn num_columns(&self) -> usize {
        self.metadata.num_columns
    }

    /// Column names
    pub fn column_names(&self) -> Vec<&str> {
        self.metadata
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect()
    }

    /// Read and decode a single column by index.
    pub fn read_column(&self, column_index: usize) -> Result<ColumnData> {
        if column_index >= self.metadata.columns.len() {
            return Err(ParquetError::ColumnOutOfRange(column_index));
        }

        let col_meta = &self.metadata.columns[column_index];
        let offset = col_meta.data_offset as usize;

        if offset >= self.data.len() {
            return Err(ParquetError::DataError(format!(
                "Column {} data offset {} exceeds file size {}",
                column_index,
                offset,
                self.data.len()
            )));
        }

        // Read column chunk data — may span multiple pages
        let compressed_size = col_meta.total_compressed_size as usize;
        let end = std::cmp::min(offset + compressed_size, self.data.len());
        let raw_data = &self.data[offset..end];

        // Parse pages within this column chunk
        self.decode_column_pages(raw_data, col_meta)
    }

    /// Decode pages within a column chunk.
    ///
    /// Each page has a header (Thrift-encoded) followed by page data.
    /// We read data pages and concatenate the decoded values.
    fn decode_column_pages(
        &self,
        chunk_data: &[u8],
        col_meta: &ColumnMetadata,
    ) -> Result<ColumnData> {
        let mut pos = 0;
        let mut all_values = ColumnDataAccumulator::new(col_meta.physical_type);
        let mut values_read: i64 = 0;

        while pos < chunk_data.len() && values_read < col_meta.num_values {
            // Parse page header (Thrift Compact Protocol)
            let page_result = self.parse_page_header(&chunk_data[pos..])?;

            pos += page_result.header_size;

            let page_data_end = std::cmp::min(pos + page_result.compressed_size, chunk_data.len());
            let page_data = &chunk_data[pos..page_data_end];

            match page_result.page_type {
                PageType::DictionaryPage => {
                    // Skip dictionary pages for now (plain encoding doesn't use them)
                    pos = page_data_end;
                    continue;
                }
                PageType::DataPage | PageType::DataPageV2 => {
                    // Decompress if needed
                    let decompressed = if col_meta.compression == Compression::Uncompressed {
                        page_data.to_vec()
                    } else {
                        let codec = codecs::get_codec(col_meta.compression)?;
                        codec.decompress(page_data, page_result.uncompressed_size)?
                    };

                    // For DataPageV1, skip repetition & definition level data
                    // In flat schemas with required columns, these are absent or minimal
                    let value_data = self.skip_levels(&decompressed, col_meta)?;

                    // Decode the values
                    let num_values = page_result.num_values as usize;
                    all_values.decode_and_append(value_data, col_meta.physical_type, num_values)?;
                    values_read += page_result.num_values as i64;
                }
                _ => {
                    // Skip index pages, etc.
                }
            }

            pos = page_data_end;
        }

        all_values.into_column_data()
    }

    /// Skip repetition and definition level data in a data page.
    ///
    /// For flat schemas with all-required columns, levels are either absent
    /// or encoded with bit-width 0 (which takes 0 bytes). This is a
    /// simplified implementation that works for the common case.
    fn skip_levels<'a>(
        &self,
        data: &'a [u8],
        _col_meta: &ColumnMetadata,
    ) -> Result<&'a [u8]> {
        // In plain flat schemas, there are no repetition/definition levels
        // The entire page data is values
        Ok(data)
    }

    /// Parse a page header from the Thrift stream.
    fn parse_page_header(&self, data: &[u8]) -> Result<PageHeaderInfo> {
        let mut pos = 0;

        // Read page_type (field 1, i32 zigzag)
        let (page_type_code, _bytes_read) = read_thrift_field_and_varint(data, &mut pos)?;

        // Read uncompressed_page_size (field 2, i32 zigzag)
        let (uncompressed_size, _) = read_thrift_field_and_varint(data, &mut pos)?;

        // Read compressed_page_size (field 3, i32 zigzag)
        let (compressed_size, _) = read_thrift_field_and_varint(data, &mut pos)?;

        let page_type = PageType::from_thrift(page_type_code)?;

        // For data pages, read the DataPageHeader struct to get num_values
        let mut num_values: i32 = 0;

        if page_type == PageType::DataPage || page_type == PageType::DataPageV2 {
            // Field 5 (DataPageHeader) or field 8 (DataPageHeaderV2) is a struct
            // We need to skip to it and parse num_values from it
            // For simplicity, scan for the next struct field and parse num_values
            num_values = self.parse_data_page_header_num_values(data, &mut pos)?;
        }

        Ok(PageHeaderInfo {
            page_type,
            uncompressed_size: uncompressed_size as usize,
            compressed_size: compressed_size as usize,
            num_values,
            header_size: pos,
        })
    }

    /// Extract num_values from a DataPageHeader or DataPageHeaderV2.
    fn parse_data_page_header_num_values(&self, data: &[u8], pos: &mut usize) -> Result<i32> {
        // Skip to the struct field (field 5 for DataPageHeader)
        // Read the field header byte
        while *pos < data.len() {
            let byte = data[*pos];
            if byte == 0 {
                *pos += 1;
                break; // stop byte — shouldn't happen here but be safe
            }

            let field_type = byte & 0x0F;
            let field_delta = (byte >> 4) & 0x0F;
            *pos += 1;

            if field_delta == 0 {
                // Long form field ID
                let _ = read_zigzag_varint(data, pos)?;
            }

            if field_type == 12 {
                // Struct — this is our DataPageHeader
                // First field inside should be num_values (field 1, i32)
                if *pos < data.len() {
                    let inner_byte = data[*pos];
                    let inner_type = inner_byte & 0x0F;
                    *pos += 1;

                    if inner_type == 5 {
                        // i32 zigzag
                        let val = read_zigzag_varint(data, pos)?;
                        // Skip the rest of the struct
                        self.skip_remaining_struct(data, pos);
                        // Skip any remaining fields in the page header
                        self.skip_remaining_struct(data, pos);
                        return Ok(val);
                    }
                }
                self.skip_remaining_struct(data, pos);
            } else {
                skip_thrift_value(data, pos, field_type)?;
            }
        }

        // Fallback: couldn't parse num_values, estimate from metadata
        Ok(0)
    }

    /// Skip remaining fields in a Thrift struct until the stop byte.
    fn skip_remaining_struct(&self, data: &[u8], pos: &mut usize) {
        while *pos < data.len() {
            let byte = data[*pos];
            if byte == 0 {
                *pos += 1;
                return;
            }
            *pos += 1;

            let field_type = byte & 0x0F;
            let field_delta = (byte >> 4) & 0x0F;

            if field_delta == 0 && *pos < data.len() {
                let _ = read_zigzag_varint(data, pos);
            }

            let _ = skip_thrift_value(data, pos, field_type);
        }
    }
}

// ---------------------------------------------------------------------------
// Page header parsing helpers
// ---------------------------------------------------------------------------

struct PageHeaderInfo {
    page_type: PageType,
    uncompressed_size: usize,
    compressed_size: usize,
    num_values: i32,
    header_size: usize,
}

/// Read a Thrift field header byte + zigzag varint value.
fn read_thrift_field_and_varint(data: &[u8], pos: &mut usize) -> Result<(i32, usize)> {
    if *pos >= data.len() {
        return Err(ParquetError::InvalidFile("Unexpected end of page header".into()));
    }

    let byte = data[*pos];
    *pos += 1;

    let _field_type = byte & 0x0F;
    let field_delta = (byte >> 4) & 0x0F;

    if field_delta == 0 {
        // Long form — field ID follows
        let _ = read_zigzag_varint(data, pos)?;
    }

    let value = read_zigzag_varint(data, pos)?;
    Ok((value, 0))
}

/// Read a zigzag-encoded varint from data at pos.
fn read_zigzag_varint(data: &[u8], pos: &mut usize) -> Result<i32> {
    let mut result: u32 = 0;
    let mut shift: u32 = 0;
    loop {
        if *pos >= data.len() {
            return Err(ParquetError::InvalidFile("Varint extends past data".into()));
        }
        let b = data[*pos] as u32;
        *pos += 1;
        result |= (b & 0x7F) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 32 {
            return Err(ParquetError::InvalidFile("Varint too long".into()));
        }
    }
    // Zigzag decode
    Ok(((result >> 1) as i32) ^ -((result & 1) as i32))
}

/// Skip a Thrift value of the given type.
fn skip_thrift_value(data: &[u8], pos: &mut usize, field_type: u8) -> Result<()> {
    match field_type {
        1 | 2 => {} // bool
        3..=6 => {
            // i8/i16/i32/i64 varint
            let _ = read_zigzag_varint(data, pos)?;
        }
        7 => {
            // double — 8 bytes
            *pos += 8;
        }
        8 => {
            // binary/string
            let len = {
                let mut result: u32 = 0;
                let mut shift: u32 = 0;
                loop {
                    if *pos >= data.len() {
                        return Ok(());
                    }
                    let b = data[*pos] as u32;
                    *pos += 1;
                    result |= (b & 0x7F) << shift;
                    if b & 0x80 == 0 {
                        break;
                    }
                    shift += 7;
                }
                result as usize
            };
            *pos += len;
        }
        9 | 10 => {
            // list/set
            if *pos >= data.len() {
                return Ok(());
            }
            let header = data[*pos];
            *pos += 1;
            let count = ((header >> 4) & 0x0F) as usize;
            let elem_type = header & 0x0F;
            let actual_count = if count == 0x0F {
                
                read_zigzag_varint(data, pos)? as usize
            } else {
                count
            };
            for _ in 0..actual_count {
                skip_thrift_value(data, pos, elem_type)?;
            }
        }
        12 => {
            // struct
            loop {
                if *pos >= data.len() {
                    return Ok(());
                }
                let byte = data[*pos];
                if byte == 0 {
                    *pos += 1;
                    return Ok(());
                }
                *pos += 1;
                let ft = byte & 0x0F;
                let fd = (byte >> 4) & 0x0F;
                if fd == 0 {
                    let _ = read_zigzag_varint(data, pos)?;
                }
                skip_thrift_value(data, pos, ft)?;
            }
        }
        _ => {
            let _ = read_zigzag_varint(data, pos);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Column data accumulator for building up values across pages
// ---------------------------------------------------------------------------

struct ColumnDataAccumulator {
    booleans: Vec<u8>,
    int32s: Vec<i32>,
    int64s: Vec<i64>,
    floats: Vec<f32>,
    doubles: Vec<f64>,
    byte_arrays: Vec<Vec<u8>>,
    physical_type: ParquetType,
}

impl ColumnDataAccumulator {
    fn new(physical_type: ParquetType) -> Self {
        ColumnDataAccumulator {
            booleans: Vec::new(),
            int32s: Vec::new(),
            int64s: Vec::new(),
            floats: Vec::new(),
            doubles: Vec::new(),
            byte_arrays: Vec::new(),
            physical_type,
        }
    }

    fn decode_and_append(
        &mut self,
        data: &[u8],
        physical_type: ParquetType,
        num_values: usize,
    ) -> Result<()> {
        match physical_type {
            ParquetType::Boolean => {
                // Booleans are bit-packed
                for i in 0..num_values {
                    let byte_idx = i / 8;
                    let bit_idx = i % 8;
                    if byte_idx < data.len() {
                        let val = (data[byte_idx] >> bit_idx) & 1;
                        self.booleans.push(val);
                    }
                }
            }
            ParquetType::Int32 => {
                let values = decode_plain_i32(data, num_values);
                self.int32s.extend(values);
            }
            ParquetType::Int64 => {
                let values = decode_plain_i64(data, num_values);
                self.int64s.extend(values);
            }
            ParquetType::Int96 => {
                // Int96 = 12 bytes each, typically timestamps
                // Convert to i64 nanoseconds
                for i in 0..num_values {
                    let offset = i * 12;
                    if offset + 12 <= data.len() {
                        let nanos = i64::from_le_bytes([
                            data[offset],
                            data[offset + 1],
                            data[offset + 2],
                            data[offset + 3],
                            data[offset + 4],
                            data[offset + 5],
                            data[offset + 6],
                            data[offset + 7],
                        ]);
                        self.int64s.push(nanos);
                    }
                }
            }
            ParquetType::Float => {
                let values = decode_plain_f32(data, num_values);
                self.floats.extend(values);
            }
            ParquetType::Double => {
                let values = decode_plain_f64(data, num_values);
                self.doubles.extend(values);
            }
            ParquetType::ByteArray => {
                let values = decode_plain_byte_array(data, num_values);
                self.byte_arrays.extend(values);
            }
            ParquetType::FixedLenByteArray(len) => {
                let fixed_len = len as usize;
                for i in 0..num_values {
                    let start = i * fixed_len;
                    let end = start + fixed_len;
                    if end <= data.len() {
                        self.byte_arrays.push(data[start..end].to_vec());
                    }
                }
            }
        }
        Ok(())
    }

    fn into_column_data(self) -> Result<ColumnData> {
        match self.physical_type {
            ParquetType::Boolean => Ok(ColumnData::Boolean(self.booleans)),
            ParquetType::Int32 => Ok(ColumnData::Int32(self.int32s)),
            ParquetType::Int64 | ParquetType::Int96 => Ok(ColumnData::Int64(self.int64s)),
            ParquetType::Float => Ok(ColumnData::Float(self.floats)),
            ParquetType::Double => Ok(ColumnData::Double(self.doubles)),
            ParquetType::ByteArray | ParquetType::FixedLenByteArray(_) => {
                Ok(ColumnData::ByteArray(self.byte_arrays))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Plain encoding decoders
// ---------------------------------------------------------------------------

fn decode_plain_i32(data: &[u8], num_values: usize) -> Vec<i32> {
    let mut values = Vec::with_capacity(num_values);
    for i in 0..num_values {
        let offset = i * 4;
        if offset + 4 <= data.len() {
            values.push(i32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]));
        }
    }
    values
}

fn decode_plain_i64(data: &[u8], num_values: usize) -> Vec<i64> {
    let mut values = Vec::with_capacity(num_values);
    for i in 0..num_values {
        let offset = i * 8;
        if offset + 8 <= data.len() {
            values.push(i64::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]));
        }
    }
    values
}

fn decode_plain_f32(data: &[u8], num_values: usize) -> Vec<f32> {
    let mut values = Vec::with_capacity(num_values);
    for i in 0..num_values {
        let offset = i * 4;
        if offset + 4 <= data.len() {
            values.push(f32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]));
        }
    }
    values
}

fn decode_plain_f64(data: &[u8], num_values: usize) -> Vec<f64> {
    let mut values = Vec::with_capacity(num_values);
    for i in 0..num_values {
        let offset = i * 8;
        if offset + 8 <= data.len() {
            values.push(f64::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]));
        }
    }
    values
}

fn decode_plain_byte_array(data: &[u8], num_values: usize) -> Vec<Vec<u8>> {
    let mut values = Vec::with_capacity(num_values);
    let mut pos = 0;
    for _ in 0..num_values {
        if pos + 4 > data.len() {
            break;
        }
        let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
            as usize;
        pos += 4;
        if pos + len > data.len() {
            break;
        }
        values.push(data[pos..pos + len].to_vec());
        pos += len;
    }
    values
}
