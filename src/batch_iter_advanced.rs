use crate::arrow_convert::ArrowConverter;
use crate::reader::ParquetReader;
use crate::types::Result;
use arrow_array::RecordBatch;
use std::collections::HashSet;

/// Batch iterator with column projection pushdown.
///
/// Reads only selected columns and yields RecordBatches of configurable size.
pub struct SelectiveBatchIterator {
    reader: ParquetReader,
    batch_size: usize,
    current_row: usize,
    selected_columns: Vec<usize>,
}

impl SelectiveBatchIterator {
    /// Create a new iterator that reads all columns.
    pub fn new(reader: ParquetReader, batch_size: usize) -> Self {
        let num_cols = reader.num_columns();
        let selected_columns: Vec<usize> = (0..num_cols).collect();

        SelectiveBatchIterator {
            reader,
            batch_size,
            current_row: 0,
            selected_columns,
        }
    }

    /// Select specific columns by index (projection pushdown).
    pub fn with_columns(mut self, columns: Vec<usize>) -> Self {
        self.selected_columns = columns;
        self
    }

    /// Select columns by name.
    pub fn with_column_names(mut self, names: Vec<&str>) -> Self {
        let name_set: HashSet<&str> = names.into_iter().collect();
        let col_names = self.reader.column_names();

        self.selected_columns = col_names
            .iter()
            .enumerate()
            .filter(|(_, name)| name_set.contains(*name))
            .map(|(idx, _)| idx)
            .collect();

        self
    }

    /// Read the next batch of rows.
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.current_row >= self.reader.num_rows() as usize {
            return Ok(None);
        }

        let batch_end = std::cmp::min(
            self.current_row + self.batch_size,
            self.reader.num_rows() as usize,
        );
        let batch_rows = batch_end - self.current_row;

        let mut columns = Vec::new();

        for &col_idx in &self.selected_columns {
            if col_idx >= self.reader.num_columns() {
                continue;
            }

            let col_data = self.reader.read_column(col_idx)?;
            let col_meta = &self.reader.metadata().columns[col_idx];

            let arrow_array = ArrowConverter::column_to_arrow(&col_data, col_meta.physical_type)?;

            // Slice to the current batch window
            let sliced = if self.current_row == 0 && batch_rows == arrow_array.len() {
                arrow_array
            } else {
                let start = std::cmp::min(self.current_row, arrow_array.len());
                let len = std::cmp::min(batch_rows, arrow_array.len().saturating_sub(start));
                arrow_array.slice(start, len)
            };

            columns.push((col_meta.name.clone(), sliced));
        }

        self.current_row = batch_end;

        if columns.is_empty() {
            return Ok(None);
        }

        ArrowConverter::create_record_batch(columns, batch_rows).map(Some)
    }

    /// Reset the iterator to the beginning.
    pub fn reset(&mut self) {
        self.current_row = 0;
    }
}

impl Iterator for SelectiveBatchIterator {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_batch() {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
