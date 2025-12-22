use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use fake::{Fake, Faker};
use std::hint::black_box;
use std::str::FromStr;
use umbra::Uuid;

fn benchmark_uuid_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("Uuid::from_str");

    let inputs: Vec<String> = (0..5)
        .map(|_| {
            let v: u128 = Faker.fake();
            let hex = format!("{:032x}", v);
            format!(
                "{}-{}-{}-{}-{}",
                &hex[0..8],
                &hex[8..12],
                &hex[12..16],
                &hex[16..20],
                &hex[20..32]
            )
        })
        .collect();

    for (i, input) in inputs.iter().enumerate() {
        group.bench_with_input(BenchmarkId::new("random_sample", i), input, |b, input| {
            b.iter(|| {
                let uuid = Uuid::from_str(black_box(input)).unwrap();
                black_box(uuid);
            })
        });
    }

    group.finish();
}

fn benchmark_uuid_display(c: &mut Criterion) {
    let mut group = c.benchmark_group("Uuid::to_string");

    let uuids = [
        Uuid::from_str("00000000-0000-0000-0000-000000000000").unwrap(),
        Uuid::from_str("123e4567-e89b-12d3-a456-426614174000").unwrap(),
        Uuid::from_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
        Uuid::from_str("f47ac10b-58cc-4372-a567-0e02b2c3d479").unwrap(),
        Uuid::from_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap(),
    ];

    for (idx, uuid) in uuids.iter().enumerate() {
        group.bench_with_input(BenchmarkId::new("uuid", idx), uuid, |b, uuid| {
            b.iter(|| {
                let s = black_box(uuid).to_string();
                black_box(s);
            })
        });
    }

    group.finish();
}

fn benchmark_uuid_eq(c: &mut Criterion) {
    let mut group = c.benchmark_group("Uuid::eq");

    let a = Uuid::from_str("d111ff02-e19f-4e6c-ac44-5804f72f7e8d").unwrap();
    let b = Uuid::from_str("d111ff02-e19f-4e6c-ac44-5804f72f7e8d").unwrap();
    let c_uuid = Uuid::from_str("a0000000-0000-0000-0000-000000000000").unwrap();

    group.bench_function("equal", |bench| {
        bench.iter(|| {
            let eq = black_box(a) == black_box(b);
            black_box(eq);
        })
    });

    group.bench_function("not_equal", |bench| {
        bench.iter(|| {
            let eq = black_box(a) == black_box(c_uuid);
            black_box(eq);
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_uuid_parse,
    benchmark_uuid_display,
    benchmark_uuid_eq
);
criterion_main!(benches);
