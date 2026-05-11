use crate::types::*;

/// Per-column statistics summary
#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    pub column_name: String,
    pub physical_type: ParquetType,
    pub num_values: i64,
    pub null_count: i64,
    pub distinct_values: Option<i64>,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub size_bytes: i64,
    pub compression_ratio: f64,
}

/// Collects and formats statistics from Parquet metadata.
pub struct StatisticsCollector;

impl StatisticsCollector {
    /// Build column statistics from parsed metadata.
    pub fn from_metadata(metadata: &ParquetMetadata) -> Vec<ColumnStatistics> {
        metadata
            .columns
            .iter()
            .map(|col| ColumnStatistics {
                column_name: col.name.clone(),
                physical_type: col.physical_type,
                num_values: col.num_values,
                null_count: 0,
                distinct_values: None,
                min_value: None,
                max_value: None,
                size_bytes: col.total_uncompressed_size,
                compression_ratio: if col.total_uncompressed_size > 0 {
                    col.total_compressed_size as f64 / col.total_uncompressed_size as f64
                } else {
                    1.0
                },
            })
            .collect()
    }

    /// Print a formatted summary table to stdout.
    pub fn print_summary(metadata: &ParquetMetadata) {
        let stats = Self::from_metadata(metadata);

        println!("\n=== Parquet File Statistics ===");
        println!("Version:       {}", metadata.version);
        println!("Total Rows:    {}", metadata.num_rows);
        println!("Total Columns: {}", metadata.num_columns);
        println!("Row Groups:    {}", metadata.row_groups.len());

        let total_size: i64 = stats.iter().map(|s| s.size_bytes).sum();
        let total_compressed: i64 = metadata
            .columns
            .iter()
            .map(|c| c.total_compressed_size)
            .sum();

        println!(
            "Total Size:    {:.2} MB (compressed: {:.2} MB)\n",
            total_size as f64 / 1024.0 / 1024.0,
            total_compressed as f64 / 1024.0 / 1024.0,
        );

        println!(
            "{:<25} {:<15} {:<12} {:<12} {:<10}",
            "Column", "Type", "Values", "Size (KB)", "Ratio"
        );
        println!("{:-<74}", "");

        for stat in &stats {
            println!(
                "{:<25} {:<15} {:<12} {:<12.2} {:<10.3}",
                truncate(&stat.column_name, 24),
                format!("{:?}", stat.physical_type),
                stat.num_values,
                stat.size_bytes as f64 / 1024.0,
                stat.compression_ratio,
            );
        }

        if let Some(created_by) = &metadata.created_by {
            println!("\nCreated by: {created_by}");
        }
    }

    /// Return statistics as a formatted string (for non-stdout contexts).
    pub fn summary_string(metadata: &ParquetMetadata) -> String {
        let stats = Self::from_metadata(metadata);
        let mut out = String::new();

        out.push_str(&format!("Rows: {}, Columns: {}\n", metadata.num_rows, metadata.num_columns));

        for stat in &stats {
            out.push_str(&format!(
                "  {} ({:?}): {} values, {:.2} KB, ratio {:.3}\n",
                stat.column_name,
                stat.physical_type,
                stat.num_values,
                stat.size_bytes as f64 / 1024.0,
                stat.compression_ratio,
            ));
        }

        out
    }
}

/// Truncate a string to max_len, appending "…" if truncated.
fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}…", &s[..max_len - 1])
    }
}
