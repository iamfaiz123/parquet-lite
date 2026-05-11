#![cfg(target_arch = "wasm32")]

use crate::reader::ParquetReader;
use crate::types::Result;
use wasm_bindgen::prelude::*;

/// WASM-compatible Parquet reader that exposes data as JSON strings.
#[wasm_bindgen]
pub struct WasmParquetReader {
    reader: ParquetReader,
}

#[wasm_bindgen]
impl WasmParquetReader {
    /// Create a reader from a byte array (Uint8Array in JS).
    #[wasm_bindgen(constructor)]
    pub fn new(data: &[u8]) -> std::result::Result<WasmParquetReader, JsValue> {
        let reader =
            ParquetReader::new(data).map_err(|e| JsValue::from_str(&format!("{}", e)))?;
        Ok(WasmParquetReader { reader })
    }

    /// Get the number of rows.
    pub fn num_rows(&self) -> i32 {
        self.reader.num_rows() as i32
    }

    /// Get the number of columns.
    pub fn num_columns(&self) -> i32 {
        self.reader.num_columns() as i32
    }

    /// Get column names as a JSON array.
    pub fn column_names_json(&self) -> std::result::Result<String, JsValue> {
        let names = self.reader.column_names();
        serde_json::to_string(&names).map_err(|e| JsValue::from_str(&format!("{}", e)))
    }

    /// Get metadata as a JSON string.
    pub fn metadata_json(&self) -> std::result::Result<String, JsValue> {
        let meta = self.reader.metadata();
        let info = serde_json::json!({
            "version": meta.version,
            "num_rows": meta.num_rows,
            "num_columns": meta.num_columns,
            "schema_names": meta.schema_names,
            "created_by": meta.created_by,
            "row_groups": meta.row_groups.len(),
        });
        serde_json::to_string(&info).map_err(|e| JsValue::from_str(&format!("{}", e)))
    }

    /// Read a column as a JSON array string.
    pub fn read_column_json(&self, column_index: usize) -> std::result::Result<String, JsValue> {
        let col_data = self
            .reader
            .read_column(column_index)
            .map_err(|e| JsValue::from_str(&format!("{}", e)))?;

        let json = match col_data {
            crate::reader::ColumnData::Boolean(vals) => {
                let bools: Vec<bool> = vals.iter().map(|&b| b != 0).collect();
                serde_json::to_string(&bools)
            }
            crate::reader::ColumnData::Int32(vals) => serde_json::to_string(&vals),
            crate::reader::ColumnData::Int64(vals) => serde_json::to_string(&vals),
            crate::reader::ColumnData::Float(vals) => serde_json::to_string(&vals),
            crate::reader::ColumnData::Double(vals) => serde_json::to_string(&vals),
            crate::reader::ColumnData::ByteArray(vals) => {
                let strings: Vec<String> = vals
                    .iter()
                    .map(|v| String::from_utf8_lossy(v).to_string())
                    .collect();
                serde_json::to_string(&strings)
            }
        };

        json.map_err(|e| JsValue::from_str(&format!("{}", e)))
    }
}
