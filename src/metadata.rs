use crate::types::*;

/// Parquet magic bytes: "PAR1"
const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

/// Minimum valid Parquet file size (header magic + footer length + footer magic)
const MIN_FILE_SIZE: usize = 12;

/// Lightweight Thrift decoder for Parquet footer metadata.
///
/// Parquet uses Thrift Compact Protocol for its footer. We implement just
/// enough of the protocol to parse FileMetaData, SchemaElement, RowGroup,
/// and ColumnChunk — without pulling in a full Thrift dependency.
pub struct MetadataReader;

impl MetadataReader {
    /// Parse Parquet metadata from the raw file bytes.
    pub fn read_metadata(data: &[u8]) -> Result<ParquetMetadata> {
        if data.len() < MIN_FILE_SIZE {
            return Err(ParquetError::InvalidFile(
                "File too small to be valid Parquet".into(),
            ));
        }

        // Validate header magic
        if &data[..4] != PARQUET_MAGIC {
            return Err(ParquetError::InvalidFile(
                "Missing PAR1 header magic".into(),
            ));
        }

        // Validate footer magic
        if &data[data.len() - 4..] != PARQUET_MAGIC {
            return Err(ParquetError::InvalidFile(
                "Missing PAR1 footer magic".into(),
            ));
        }

        // Read footer length (4 bytes, little-endian, just before the footer magic)
        let footer_len = u32::from_le_bytes([
            data[data.len() - 8],
            data[data.len() - 7],
            data[data.len() - 6],
            data[data.len() - 5],
        ]) as usize;

        if footer_len + 8 > data.len() {
            return Err(ParquetError::InvalidFile(
                "Footer length exceeds file size".into(),
            ));
        }

        let footer_start = data.len() - 8 - footer_len;
        let footer = &data[footer_start..footer_start + footer_len];

        Self::parse_file_metadata(footer, data)
    }

    /// Parse the Thrift-encoded FileMetaData.
    ///
    /// FileMetaData fields (Thrift Compact Protocol):
    ///   1: i32  version
    ///   2: list<SchemaElement> schema
    ///   3: i64  num_rows
    ///   4: list<RowGroup> row_groups
    ///   5: list<KeyValue> key_value_metadata (optional)
    ///   6: string created_by (optional)
    fn parse_file_metadata(footer: &[u8], _file_data: &[u8]) -> Result<ParquetMetadata> {
        let mut reader = ThriftReader::new(footer);

        let mut version: i32 = 0;
        let mut num_rows: i64 = 0;
        let mut schema_names: Vec<String> = Vec::new();
        let mut schema_types: Vec<Option<ParquetType>> = Vec::new();
        let mut row_groups: Vec<RowGroupMetadata> = Vec::new();
        let mut created_by: Option<String> = None;

        loop {
            let (field_delta, field_type) = match reader.read_field_header() {
                Some(header) => header,
                None => break, // stop byte
            };

            match field_delta {
                1 => {
                    // version: i32
                    version = reader.read_i32()?;
                }
                2 => {
                    // schema: list<SchemaElement>
                    let (_elem_type, count) = reader.read_list_header()?;
                    for _ in 0..count {
                        let (name, ptype) = Self::parse_schema_element(&mut reader)?;
                        schema_names.push(name);
                        schema_types.push(ptype);
                    }
                }
                3 => {
                    // num_rows: i64
                    num_rows = reader.read_i64()?;
                }
                4 => {
                    // row_groups: list<RowGroup>
                    let (_elem_type, count) = reader.read_list_header()?;
                    for _ in 0..count {
                        let rg = Self::parse_row_group(&mut reader, &schema_names, &schema_types)?;
                        row_groups.push(rg);
                    }
                }
                5 => {
                    // key_value_metadata: list<KeyValue> — skip
                    reader.skip_field(field_type)?;
                }
                6 => {
                    // created_by: string
                    created_by = Some(reader.read_string()?);
                }
                _ => {
                    reader.skip_field(field_type)?;
                }
            }
        }

        // Build the flattened column metadata from the first row group
        let columns = if let Some(rg) = row_groups.first() {
            rg.columns.clone()
        } else {
            Vec::new()
        };

        let num_columns = columns.len();

        Ok(ParquetMetadata {
            version,
            num_rows,
            num_columns,
            schema_names,
            row_groups,
            columns,
            created_by,
        })
    }

    /// Parse a SchemaElement from the Thrift stream.
    ///
    /// SchemaElement fields:
    ///   1: i32  type (physical type, optional — root element has no type)
    ///   2: i32  type_length (for FixedLenByteArray)
    ///   3: i32  repetition_type
    ///   4: string name
    ///   5: i32  num_children
    ///   6: i32  converted_type
    ///   7: i32  scale
    ///   8: i32  precision
    ///   9: i32  field_id
    ///  10: struct logical_type
    fn parse_schema_element(
        reader: &mut ThriftReader,
    ) -> Result<(String, Option<ParquetType>)> {
        let mut name = String::new();
        let mut physical_type: Option<ParquetType> = None;
        let mut type_length: i32 = 0;

        loop {
            let (field_delta, field_type) = match reader.read_field_header() {
                Some(header) => header,
                None => break,
            };

            match field_delta {
                1 => {
                    // type (physical)
                    let code = reader.read_i32()?;
                    physical_type = Some(ParquetType::from_thrift(code)?);
                }
                2 => {
                    // type_length
                    type_length = reader.read_i32()?;
                }
                3 => {
                    // repetition_type — read but don't use yet
                    let _ = reader.read_i32()?;
                }
                4 => {
                    // name
                    name = reader.read_string()?;
                }
                5 => {
                    // num_children
                    let _ = reader.read_i32()?;
                }
                _ => {
                    reader.skip_field(field_type)?;
                }
            }
        }

        // Fix up FixedLenByteArray with the actual length
        if let Some(ParquetType::FixedLenByteArray(_)) = physical_type {
            physical_type = Some(ParquetType::FixedLenByteArray(type_length));
        }

        Ok((name, physical_type))
    }

    /// Parse a RowGroup from the Thrift stream.
    ///
    /// RowGroup fields:
    ///   1: list<ColumnChunk> columns
    ///   2: i64  total_byte_size
    ///   3: i64  num_rows
    fn parse_row_group(
        reader: &mut ThriftReader,
        schema_names: &[String],
        schema_types: &[Option<ParquetType>],
    ) -> Result<RowGroupMetadata> {
        let mut columns: Vec<ColumnMetadata> = Vec::new();
        let mut num_rows: i64 = 0;
        let mut total_byte_size: i64 = 0;

        loop {
            let (field_delta, field_type) = match reader.read_field_header() {
                Some(header) => header,
                None => break,
            };

            match field_delta {
                1 => {
                    // columns: list<ColumnChunk>
                    let (_elem_type, count) = reader.read_list_header()?;
                    for i in 0..count {
                        let col = Self::parse_column_chunk(reader, i, schema_names, schema_types)?;
                        columns.push(col);
                    }
                }
                2 => {
                    // total_byte_size
                    total_byte_size = reader.read_i64()?;
                }
                3 => {
                    // num_rows
                    num_rows = reader.read_i64()?;
                }
                _ => {
                    reader.skip_field(field_type)?;
                }
            }
        }

        Ok(RowGroupMetadata {
            columns,
            num_rows,
            total_byte_size,
        })
    }

    /// Parse a ColumnChunk from the Thrift stream.
    ///
    /// ColumnChunk fields:
    ///   1: string file_path (optional, for external files)
    ///   2: i64   file_offset
    ///   3: struct ColumnMetaData (the important part)
    fn parse_column_chunk(
        reader: &mut ThriftReader,
        col_index: usize,
        schema_names: &[String],
        schema_types: &[Option<ParquetType>],
    ) -> Result<ColumnMetadata> {
        let mut file_offset: i64 = 0;
        let mut col_meta: Option<ColumnMetadata> = None;

        loop {
            let (field_delta, field_type) = match reader.read_field_header() {
                Some(header) => header,
                None => break,
            };

            match field_delta {
                1 => {
                    // file_path — skip
                    reader.skip_field(field_type)?;
                }
                2 => {
                    // file_offset
                    file_offset = reader.read_i64()?;
                }
                3 => {
                    // ColumnMetaData struct
                    col_meta = Some(Self::parse_column_metadata(
                        reader,
                        col_index,
                        schema_names,
                        schema_types,
                    )?);
                }
                _ => {
                    reader.skip_field(field_type)?;
                }
            }
        }

        match col_meta {
            Some(mut meta) => {
                if meta.data_offset == 0 {
                    meta.data_offset = file_offset;
                }
                Ok(meta)
            }
            None => {
                // Construct a minimal metadata from what we have
                // Column index + 1 because schema_names[0] is root
                let name_idx = col_index + 1;
                let name = schema_names
                    .get(name_idx)
                    .cloned()
                    .unwrap_or_else(|| format!("col_{col_index}"));
                let physical_type = schema_types
                    .get(name_idx)
                    .copied()
                    .flatten()
                    .unwrap_or(ParquetType::ByteArray);

                Ok(ColumnMetadata {
                    name,
                    physical_type,
                    encoding: Encoding::Plain,
                    compression: Compression::Uncompressed,
                    num_values: 0,
                    data_offset: file_offset,
                    total_compressed_size: 0,
                    total_uncompressed_size: 0,
                })
            }
        }
    }

    /// Parse the inner ColumnMetaData struct.
    ///
    /// ColumnMetaData fields:
    ///   1: i32  type (physical type)
    ///   2: list<Encoding> encodings
    ///   3: list<string> path_in_schema
    ///   4: i32  codec (compression)
    ///   5: i64  num_values
    ///   6: i64  total_uncompressed_size
    ///   7: i64  total_compressed_size
    ///   8: list<KeyValue> key_value_metadata
    ///   9: i64  data_page_offset
    ///  10: i64  index_page_offset
    ///  11: i64  dictionary_page_offset
    ///  12: struct statistics
    fn parse_column_metadata(
        reader: &mut ThriftReader,
        col_index: usize,
        schema_names: &[String],
        _schema_types: &[Option<ParquetType>],
    ) -> Result<ColumnMetadata> {
        let mut physical_type = ParquetType::ByteArray;
        let mut encoding = Encoding::Plain;
        let mut compression = Compression::Uncompressed;
        let mut num_values: i64 = 0;
        let mut total_compressed_size: i64 = 0;
        let mut total_uncompressed_size: i64 = 0;
        let mut data_offset: i64 = 0;
        let mut path_in_schema: Vec<String> = Vec::new();

        loop {
            let (field_delta, field_type) = match reader.read_field_header() {
                Some(header) => header,
                None => break,
            };

            match field_delta {
                1 => {
                    // type
                    let code = reader.read_i32()?;
                    physical_type = ParquetType::from_thrift(code)?;
                }
                2 => {
                    // encodings: list<i32>
                    let (_et, count) = reader.read_list_header()?;
                    for _ in 0..count {
                        let enc_code = reader.read_i32()?;
                        encoding = Encoding::from_thrift(enc_code)?;
                    }
                }
                3 => {
                    // path_in_schema: list<string>
                    let (_et, count) = reader.read_list_header()?;
                    for _ in 0..count {
                        path_in_schema.push(reader.read_string()?);
                    }
                }
                4 => {
                    // codec
                    let code = reader.read_i32()?;
                    compression = Compression::from_thrift(code)?;
                }
                5 => {
                    // num_values
                    num_values = reader.read_i64()?;
                }
                6 => {
                    // total_uncompressed_size
                    total_uncompressed_size = reader.read_i64()?;
                }
                7 => {
                    // total_compressed_size
                    total_compressed_size = reader.read_i64()?;
                }
                8 => {
                    // key_value_metadata — skip
                    reader.skip_field(field_type)?;
                }
                9 => {
                    // data_page_offset
                    data_offset = reader.read_i64()?;
                }
                10 | 11 => {
                    // index_page_offset / dictionary_page_offset
                    let _ = reader.read_i64()?;
                }
                12 => {
                    // statistics struct — skip
                    reader.skip_field(field_type)?;
                }
                _ => {
                    reader.skip_field(field_type)?;
                }
            }
        }

        // Determine column name: prefer path_in_schema, fall back to schema_names
        let name = if let Some(last) = path_in_schema.last() {
            last.clone()
        } else {
            let name_idx = col_index + 1; // +1 for root schema element
            schema_names
                .get(name_idx)
                .cloned()
                .unwrap_or_else(|| format!("col_{col_index}"))
        };

        Ok(ColumnMetadata {
            name,
            physical_type,
            encoding,
            compression,
            num_values,
            data_offset,
            total_compressed_size,
            total_uncompressed_size,
        })
    }
}

// ---------------------------------------------------------------------------
// Lightweight Thrift Compact Protocol reader
// ---------------------------------------------------------------------------

/// Minimal Thrift Compact Protocol reader.
///
/// Only implements enough to parse Parquet FileMetaData. Supports:
/// - Varint (zigzag) encoded integers
/// - Strings (length-prefixed)
/// - Lists
/// - Structs (nested field headers with stop byte)
/// - Field skipping for unknown/unneeded fields
struct ThriftReader<'a> {
    data: &'a [u8],
    pos: usize,
    /// Stack of previous field IDs for nested structs
    #[allow(dead_code)]
    field_id_stack: Vec<i16>,
    /// Current field ID
    current_field_id: i16,
}

impl<'a> ThriftReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        ThriftReader {
            data,
            pos: 0,
            field_id_stack: Vec::new(),
            current_field_id: 0,
        }
    }

    #[allow(dead_code)]
    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    /// Read a single byte
    fn read_byte(&mut self) -> Result<u8> {
        if self.pos >= self.data.len() {
            return Err(ParquetError::InvalidFile("Unexpected end of data".into()));
        }
        let b = self.data[self.pos];
        self.pos += 1;
        Ok(b)
    }

    /// Read a varint (unsigned, variable-length encoding)
    fn read_varint(&mut self) -> Result<u64> {
        let mut result: u64 = 0;
        let mut shift: u32 = 0;
        loop {
            let b = self.read_byte()? as u64;
            result |= (b & 0x7F) << shift;
            if b & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
            if shift >= 64 {
                return Err(ParquetError::InvalidFile("Varint too long".into()));
            }
        }
    }

    /// Read a zigzag-encoded i32
    fn read_i32(&mut self) -> Result<i32> {
        let v = self.read_varint()? as u32;
        Ok(((v >> 1) as i32) ^ -((v & 1) as i32))
    }

    /// Read a zigzag-encoded i64
    fn read_i64(&mut self) -> Result<i64> {
        let v = self.read_varint()?;
        Ok(((v >> 1) as i64) ^ -((v & 1) as i64))
    }

    /// Read a length-prefixed string
    fn read_string(&mut self) -> Result<String> {
        let len = self.read_varint()? as usize;
        if self.pos + len > self.data.len() {
            return Err(ParquetError::InvalidFile(
                "String length exceeds data".into(),
            ));
        }
        let s = String::from_utf8_lossy(&self.data[self.pos..self.pos + len]).to_string();
        self.pos += len;
        Ok(s)
    }

    /// Read N raw bytes
    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.data.len() {
            return Err(ParquetError::InvalidFile("Not enough bytes".into()));
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    /// Read a field header in Thrift Compact Protocol.
    /// Returns (field_id, type_id) or None for stop byte.
    fn read_field_header(&mut self) -> Option<(i16, u8)> {
        if self.pos >= self.data.len() {
            return None;
        }

        let byte = self.data[self.pos];
        if byte == 0 {
            // Stop byte
            self.pos += 1;
            // Reset current_field_id for the next struct if needed, 
            // but in Parquet we don't strictly need to reset it 
            // since a struct parsing loop creates a new ThriftReader or we handle it.
            // Actually, wait, ThriftReader is passed around mutably.
            // When we enter a new struct, current_field_id should be 0.
            return None;
        }
        self.pos += 1;

        let field_delta = (byte >> 4) & 0x0F;
        let field_type = byte & 0x0F;

        if field_delta == 0 {
            // Long form: field ID follows as zigzag i16
            if let Ok(fid) = self.read_i32() {
                self.current_field_id = fid as i16;
                return Some((self.current_field_id, field_type));
            }
            return None;
        }

        self.current_field_id += field_delta as i16;
        Some((self.current_field_id, field_type))
    }

    /// Read a list header: (element_type, count)
    fn read_list_header(&mut self) -> Result<(u8, usize)> {
        let header = self.read_byte()?;
        let size_and_type = header;
        let size_hi = (size_and_type >> 4) & 0x0F;
        let elem_type = size_and_type & 0x0F;

        let count = if size_hi == 0x0F {
            // Large list: count follows as varint
            self.read_varint()? as usize
        } else {
            size_hi as usize
        };

        Ok((elem_type, count))
    }

    /// Skip a field of the given type
    fn skip_field(&mut self, field_type: u8) -> Result<()> {
        match field_type {
            1 => {
                // bool true — no data
            }
            2 => {
                // bool false — no data
            }
            3 | 4 => {
                // i8 / i16 — varint
                self.read_varint()?;
            }
            5 => {
                // i32 — varint
                self.read_varint()?;
            }
            6 => {
                // i64 — varint
                self.read_varint()?;
            }
            7 => {
                // double — 8 bytes
                self.read_bytes(8)?;
            }
            8 => {
                // binary/string
                let len = self.read_varint()? as usize;
                self.read_bytes(len)?;
            }
            9 => {
                // list
                let (_et, count) = self.read_list_header()?;
                let elem_type = _et;
                for _ in 0..count {
                    self.skip_field(elem_type)?;
                }
            }
            10 => {
                // set — same as list
                let (et, count) = self.read_list_header()?;
                for _ in 0..count {
                    self.skip_field(et)?;
                }
            }
            11 => {
                // map
                let count = self.read_varint()? as usize;
                if count > 0 {
                    let types = self.read_byte()?;
                    let key_type = (types >> 4) & 0x0F;
                    let val_type = types & 0x0F;
                    for _ in 0..count {
                        self.skip_field(key_type)?;
                        self.skip_field(val_type)?;
                    }
                }
            }
            12 => {
                // struct — read until stop byte
                let prev_field_id = self.current_field_id;
                self.current_field_id = 0;
                loop {
                    match self.read_field_header() {
                        Some((_delta, ft)) => self.skip_field(ft)?,
                        None => break,
                    }
                }
                self.current_field_id = prev_field_id;
            }
            _ => {
                // Unknown type — try varint as best effort
                self.read_varint()?;
            }
        }
        Ok(())
    }
}
