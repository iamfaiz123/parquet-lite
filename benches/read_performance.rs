use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::fs;

fn bench_read_metadata(c: &mut Criterion) {
    let data = match fs::read("test.parquet") {
        Ok(d) => d,
        Err(_) => {
            eprintln!("Skipping benchmark: test.parquet not found");
            return;
        }
    };

    c.bench_function("read_metadata", |b| {
        b.iter(|| parquet_lite::read_metadata(black_box(&data)));
    });
}

fn bench_batch_iteration(c: &mut Criterion) {
    let data = match fs::read("test.parquet") {
        Ok(d) => d,
        Err(_) => {
            eprintln!("Skipping benchmark: test.parquet not found");
            return;
        }
    };

    c.bench_function("batch_iteration_1024", |b| {
        b.iter(|| {
            let mut iter =
                parquet_lite::read_to_arrow_batches(black_box(&data), 1024).unwrap();
            while let Some(_batch) = iter.next() {
                // consume
            }
        });
    });

    c.bench_function("batch_iteration_4096", |b| {
        b.iter(|| {
            let mut iter =
                parquet_lite::read_to_arrow_batches(black_box(&data), 4096).unwrap();
            while let Some(_batch) = iter.next() {
                // consume
            }
        });
    });
}

criterion_group!(benches, bench_read_metadata, bench_batch_iteration);
criterion_main!(benches);
