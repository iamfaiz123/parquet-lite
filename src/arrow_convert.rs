use crate::reader::ColumnData;
use crate::types::*;
use arrow_array::*;
use arrow_schema::{DataType, Field, Schema};
use arrow_array::RecordBatch;
use std::sync::Arc;

/// Converts parquet-lite column data to Apache Arrow arrays and record batches.
pub struct ArrowConverter;

impl ArrowConverter {
    /// Convert a ColumnData value to an Arrow ArrayRef.
    pub fn column_to_arrow(
        col_data: &ColumnData,
        _physical_type: ParquetType,
    ) -> Result<ArrayRef> {
        match col_data {
            ColumnData::Boolean(vals) => {
                let bools: Vec<bool> = vals.iter().map(|&v| v != 0).collect();
                Ok(Arc::new(BooleanArray::from(bools)))
            }
            ColumnData::Int32(vals) => Ok(Arc::new(Int32Array::from(vals.clone()))),
            ColumnData::Int64(vals) => Ok(Arc::new(Int64Array::from(vals.clone()))),
            ColumnData::Float(vals) => Ok(Arc::new(Float32Array::from(vals.clone()))),
            ColumnData::Double(vals) => Ok(Arc::new(Float64Array::from(vals.clone()))),
            ColumnData::ByteArray(vals) => {
                let strings: Vec<&str> = vals
                    .iter()
                    .map(|v| std::str::from_utf8(v).unwrap_or(""))
                    .collect();
                Ok(Arc::new(StringArray::from(strings)))
            }
        }
    }

    /// Get the Arrow DataType corresponding to a Parquet physical type.
    pub fn arrow_data_type(physical_type: ParquetType) -> DataType {
        match physical_type {
            ParquetType::Boolean => DataType::Boolean,
            ParquetType::Int32 => DataType::Int32,
            ParquetType::Int64 => DataType::Int64,
            ParquetType::Int96 => DataType::Int64, // timestamps stored as i64
            ParquetType::Float => DataType::Float32,
            ParquetType::Double => DataType::Float64,
            ParquetType::ByteArray => DataType::Utf8,
            ParquetType::FixedLenByteArray(len) => DataType::FixedSizeBinary(len),
        }
    }

    /// Create a RecordBatch from named Arrow arrays.
    pub fn create_record_batch(
        columns: Vec<(String, ArrayRef)>,
        _num_rows: usize,
    ) -> Result<RecordBatch> {
        if columns.is_empty() {
            return Err(ParquetError::DataError("No columns to create batch".into()));
        }

        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, arr)| Field::new(name, arr.data_type().clone(), true))
            .collect();

        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<ArrayRef> = columns.into_iter().map(|(_, arr)| arr).collect();

        RecordBatch::try_new(schema, arrays)
            .map_err(|e| ParquetError::ArrowError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int32_conversion() {
        let data = ColumnData::Int32(vec![1, 2, 3, 4, 5]);
        let arr = ArrowConverter::column_to_arrow(&data, ParquetType::Int32).unwrap();
        assert_eq!(arr.len(), 5);

        let int_arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_arr.value(0), 1);
        assert_eq!(int_arr.value(4), 5);
    }

    #[test]
    fn test_string_conversion() {
        let data = ColumnData::ByteArray(vec![
            b"hello".to_vec(),
            b"world".to_vec(),
        ]);
        let arr = ArrowConverter::column_to_arrow(&data, ParquetType::ByteArray).unwrap();
        assert_eq!(arr.len(), 2);

        let str_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_arr.value(0), "hello");
        assert_eq!(str_arr.value(1), "world");
    }

    #[test]
    fn test_record_batch_creation() {
        let cols = vec![
            (
                "id".to_string(),
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            ),
            (
                "name".to_string(),
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            ),
        ];

        let batch = ArrowConverter::create_record_batch(cols, 3).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).name(), "id");
    }
}
