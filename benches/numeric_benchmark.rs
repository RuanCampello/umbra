use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use std::str::FromStr;
use umbra::db::Numeric;

fn bench_numeric_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("Numeric Operations");

    let short1 = Numeric::from(12345u64);
    let short2 = Numeric::from(67890u64);

    let long1 = Numeric::from_str("100000000000000000000").unwrap();
    let long2 = Numeric::from_str("200000000000000000000").unwrap();

    // Short + Short
    group.bench_function("add_short_short", |b| {
        b.iter(|| {
            let res = black_box(&short1) + black_box(&short2);
            black_box(res);
        })
    });

    // Short + Long
    group.bench_function("add_short_long", |b| {
        b.iter(|| {
            let res = black_box(&short1) + black_box(&long1);
            black_box(res);
        })
    });

    // Long + Long
    group.bench_function("add_long_long", |b| {
        b.iter(|| {
            let res = black_box(&long1) + black_box(&long2);
            black_box(res);
        })
    });

    // Compare Short vs Short
    group.bench_function("cmp_short_short", |b| {
        b.iter(|| {
            let res = black_box(&short1) < black_box(&short2);
            black_box(res);
        })
    });

    // Compare Short vs Long
    group.bench_function("cmp_short_long", |b| {
        b.iter(|| {
            let res = black_box(&short1) < black_box(&long1);
            black_box(res);
        })
    });

    group.finish();
}

criterion_group!(benches, bench_numeric_ops);
criterion_main!(benches);
