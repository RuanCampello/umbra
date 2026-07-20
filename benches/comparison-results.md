# Umbra comparison benchmark

Median microseconds per operation (± median absolute deviation), 10000 users / 30000 orders, single connection, each statement parsed and executed end to end. Lower is better, **Best** is the fastest median in the row.

Read this with the setup in mind:
- Umbra and SQLite run **in memory**, Postgres is the local **on-disk** server, so it pays client/server round-trips on every call and a durable fsync on every write — its write rows are not comparable to the embedded engines'.
- Every engine indexes `age`/`status`/`user_id`, so column-filtered scans are a like-for-like comparison.

## Basic Operations

| Operation | Umbra (µs) | SQLite (µs) | Postgres (µs) | Best |
|:----------|----------:|----------:|----------:|:-----|
| SELECT by ID | 9.30 ±0.29 | 4.08 ±0.04 | 97.28 ±2.13 | SQLite |
| SELECT by column (exact) | 119.99 ±0.98 | 48.22 ±0.24 | 230.28 ±24.45 | SQLite |
| SELECT by column (range) | 1714.34 ±26.28 | 601.58 ±10.60 | 1057.20 ±57.88 | SQLite |
| SELECT complex | 5955.71 ±375.46 | 1160.36 ±15.51 | 976.86 ±45.90 | Postgres |
| SELECT * (full scan) | 11033.85 ±1571.63 | 1215.03 ±11.64 | 4603.71 ±261.37 | SQLite |
| Aggregation (GROUP BY) | 7603.98 ±686.15 | 2865.75 ±31.13 | 1770.39 ±28.10 | Postgres |
| UPDATE by ID | 14.73 ±0.45 | 3.01 ±0.04 | 466.72 ±22.86 | SQLite |
| UPDATE complex | 6467.08 ±813.86 | 940.36 ±17.98 | 979.08 ±37.47 | SQLite |
| INSERT single | 13.90 ±1.09 | 7.58 ±0.48 | 468.18 ±13.66 | SQLite |
| DELETE by ID | 15.13 ±1.99 | 3.53 ±0.12 | 452.57 ±14.21 | SQLite |

**Basic Operations score: Umbra 0, SQLite 8, Postgres 2**

## Advanced Operations

| Operation | Umbra (µs) | SQLite (µs) | Postgres (µs) | Best |
|:----------|----------:|----------:|----------:|:-----|
| INNER JOIN | 45680.30 ±2405.01 | 39.22 ±0.23 | 1865.80 ±34.10 | SQLite |
| LEFT JOIN + GROUP BY | 70076.45 ±2452.47 | 140.71 ±1.00 | 462.25 ±17.01 | SQLite |

**Advanced Operations score: Umbra 0, SQLite 2, Postgres 0**

## Bottleneck Hunters

| Operation | Umbra (µs) | SQLite (µs) | Postgres (µs) | Best |
|:----------|----------:|----------:|----------:|:-----|
| LIKE prefix | 2677.85 ±170.97 | 19.46 ±0.12 | 117.91 ±2.20 | SQLite |
| LIKE contains | 3063.96 ±108.85 | 258.64 ±4.56 | 482.67 ±8.57 | SQLite |
| OR conditions (3) | 204.87 ±3.62 | 31.01 ±1.63 | 143.20 ±5.11 | SQLite |
| IN list (7 values) | 431.06 ±5.28 | 29.37 ±0.15 | 144.17 ±3.67 | SQLite |
| BETWEEN (non-indexed) | 2526.45 ±172.46 | 17.26 ±0.14 | 117.08 ±3.39 | SQLite |
| OFFSET pagination (5000) | 5067.06 ±161.61 | 52.03 ±0.55 | 1355.27 ±17.59 | SQLite |
| Multi-col ORDER BY (3) | 7542.44 ±368.76 | 787.53 ±5.48 | 163.74 ±2.98 | Postgres |
| Multi aggregates (5) | 3743.59 ±134.47 | 994.87 ±7.37 | 915.19 ±10.66 | Postgres |
| GROUP BY (2 columns) | 5811.73 ±142.44 | 3731.70 ±43.28 | 1581.59 ±22.31 | Postgres |
| Math expressions | 2426.14 ±137.72 | 9.90 ±0.09 | 100.10 ±8.56 | SQLite |
| String concat (CONCAT) | 2450.09 ±129.93 | 15.01 ±0.61 | 106.97 ±13.74 | SQLite |
| COALESCE + IS NOT NULL | 2455.10 ±148.58 | 8.76 ±0.09 | 111.00 ±2.00 | SQLite |
| Large result (no LIMIT) | 3473.62 ±171.35 | 819.13 ±5.18 | 1617.45 ±30.42 | SQLite |

**Bottleneck Hunters score: Umbra 0, SQLite 10, Postgres 3**

## Summary

| Engine | Wins |
|:-------|-----:|
| Umbra | 0 |
| SQLite | 20 |
| Postgres | 5 |

## Correctness

Every compared read returned the same row count across engines.
