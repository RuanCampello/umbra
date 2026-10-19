# Umbra comparison benchmark

Median microseconds per operation (± median absolute deviation), 10000 users / 30000 orders, single connection, each statement parsed and executed end to end. Lower is better, **Best** is the fastest median in the row.
            Read this with the setup in mind:
            - Umbra and SQLite run **in memory**, Postgres is the local **on-disk** server, so it pays client/server round-trips on every call and a durable fsync on every write — its write rows are not comparable to the embedded engines'.
            - Every engine indexes `age`/`status`/`user_id`, so column-filtered scans are a like-for-like comparison.
            - Umbra and Postgres decode every column of every returned row; SQLite steps the matching rows without materialising their columns. Umbra therefore does at least as much per-row work as SQLite on every read, the comparison never flatters it.
        ## Basic Operations

| Operation | Umbra (µs) | SQLite (µs) | Postgres (µs) | Best |
|:----------|----------:|----------:|----------:|:-----|
| SELECT by ID | 6.34 ±0.10 | 4.40 ±0.03 | 62.25 ±0.71 | SQLite |
| SELECT by column (exact) | 27.86 ±0.15 | 51.81 ±0.62 | 135.11 ±1.67 | Umbra |
| SELECT by column (range) | 329.23 ±5.41 | 632.21 ±4.21 | 617.42 ±7.45 | Umbra |
| SELECT complex | 603.41 ±4.69 | 765.97 ±6.27 | 529.21 ±4.95 | Postgres |
| SELECT * (full scan) | 1345.42 ±3.66 | 2134.51 ±21.66 | 2693.52 ±48.97 | Umbra |
| Aggregation (GROUP BY) | 833.77 ±13.50 | 1862.79 ±15.29 | 1125.77 ±9.76 | Umbra |
| UPDATE by ID | 2.63 ±0.08 | 2.42 ±0.02 | 470.50 ±13.84 | SQLite |
| UPDATE complex | 505.28 ±13.11 | 579.51 ±2.84 | 719.85 ±18.45 | Umbra |
| INSERT single | 5.09 ±0.75 | 3.79 ±0.04 | 441.73 ±8.71 | SQLite |
| DELETE by ID | 2.83 ±0.22 | 4.42 ±0.07 | 444.32 ±8.78 | Umbra |

**Basic Operations score: Umbra 6, SQLite 3, Postgres 1**

## Advanced Operations

| Operation | Umbra (µs) | SQLite (µs) | Postgres (µs) | Best |
|:----------|----------:|----------:|----------:|:-----|
| INNER JOIN | 55.82 ±0.68 | 30.94 ±0.33 | 1119.74 ±21.61 | SQLite |
| LEFT JOIN + GROUP BY | 80.94 ±1.28 | 99.89 ±1.09 | 259.96 ±4.21 | Umbra |

**Advanced Operations score: Umbra 1, SQLite 1, Postgres 0**

## Bottleneck Hunters

| Operation | Umbra (µs) | SQLite (µs) | Postgres (µs) | Best |
|:----------|----------:|----------:|----------:|:-----|
| LIKE prefix | 21.99 ±0.57 | 31.80 ±0.24 | 104.68 ±1.16 | Umbra |
| LIKE contains | 365.66 ±4.59 | 234.01 ±2.17 | 403.74 ±3.63 | SQLite |
| OR conditions (3) | 24.56 ±0.79 | 39.58 ±0.25 | 132.59 ±2.78 | Umbra |
| IN list (7 values) | 28.98 ±0.68 | 39.59 ±0.18 | 131.27 ±1.39 | Umbra |
| BETWEEN (non-indexed) | 24.41 ±0.17 | 29.69 ±0.31 | 110.82 ±5.01 | Umbra |
| OFFSET pagination (5000) | 102.44 ±2.25 | 58.93 ±0.52 | 1192.38 ±9.96 | SQLite |
| Multi-col ORDER BY (3) | 59.75 ±0.90 | 703.47 ±7.82 | 160.57 ±5.73 | Umbra |
| Multi aggregates (5) | 625.02 ±4.94 | 868.59 ±6.89 | 745.98 ±5.38 | Umbra |
| GROUP BY (2 columns) | 984.45 ±17.16 | 3205.29 ±32.87 | 1379.39 ±8.48 | Umbra |
| Math expressions | 26.60 ±0.59 | 14.36 ±0.10 | 90.80 ±0.94 | SQLite |
| String concat (CONCAT) | 23.07 ±0.43 | 15.86 ±0.36 | 90.93 ±0.83 | SQLite |
| COALESCE + IS NOT NULL | 17.72 ±0.10 | 12.81 ±0.06 | 84.80 ±1.22 | SQLite |
| Large result (no LIMIT) | 837.52 ±11.39 | 1135.01 ±11.17 | 1405.07 ±19.34 | Umbra |

**Bottleneck Hunters score: Umbra 8, SQLite 5, Postgres 0**

## Specialised (UUID / enum / numeric / JSON / sorting)

| Operation | Umbra (µs) | SQLite (µs) | Postgres (µs) | Best |
|:----------|----------:|----------:|----------:|:-----|
| Sort by UUID (top 100) | 645.72 ±4.99 | 584.57 ±2.65 | 605.89 ±5.62 | SQLite |
| Sort by numeric DESC (top 100) | 760.35 ±10.05 | 481.52 ±2.76 | 1299.03 ±9.65 | SQLite |
| Extreme full sort (weight) | 2336.20 ±11.03 | 2653.56 ±32.54 | 2940.63 ±33.77 | Umbra |
| UUID point lookup | 179.48 ±1.55 | 324.02 ±2.83 | 388.84 ±5.92 | Umbra |
| Numeric aggregation (SUM/AVG/MIN/MAX) | 880.15 ±7.07 | 770.92 ±6.26 | 1031.66 ±18.79 | SQLite |
| Numeric filter + arithmetic | 18.94 ±0.15 | 13.49 ±0.17 | 102.27 ±1.82 | SQLite |
| Enum GROUP BY | 818.24 ±14.26 | 2059.77 ±20.70 | 1491.01 ±15.20 | Umbra |
| Enum filter | 32.19 ±0.37 | 50.74 ±0.38 | 141.79 ±2.15 | Umbra |
| JSON field filter | 127.55 ±0.91 | 97.74 ±0.49 | 113.95 ±2.84 | SQLite |
| JSON field projection | 41.37 ±0.39 | 30.87 ±0.17 | 79.41 ±1.05 | SQLite |

**Specialised (UUID / enum / numeric / JSON / sorting) score: Umbra 4, SQLite 6, Postgres 0**

## Summary

| Engine | Wins |
|:-------|-----:|
| Umbra | 19 |
| SQLite | 15 |
| Postgres | 1 |

## Correctness

Every compared read returned the same row count across engines.

---

# Umbra concurrency benchmark

2000 operations per worker, 80% reads / 20% writes, writes concentrated on a 256-row hot set to force write-write contention. Reads spread over 10000 rows. Throughput counts only successful operations ÷ wall time; latency percentiles include every attempt.
        Concurrency-control notes (read these before trusting the numbers):
        - **Umbra** and **Postgres** are MVCC: readers take a consistent snapshot and never block, writers race optimistically, a write-write conflict aborts and is counted as a write error, not retried.
        - **SQLite** has no MVCC, in shared-cache memory it serialises writers behind the database lock (`busy_timeout` 5s). It runs with `read_uncommitted` so readers do not block on the writer, a weaker guarantee (dirty reads) than the snapshot isolation umbra and Postgres give, and one that works in SQLite's favour here.
        - Umbra & SQLite are in memory, Postgres is the local on-disk server (a round-trip per call), so Postgres's absolute throughput is not comparable to the embedded engines', read it for scaling, not level.
    | Engine | Threads | Throughput (ops/s) | p50 (µs) | p99 (µs) | Write errors |
|:-------|-------:|-------------------:|--------:|--------:|-------------:|
| Umbra | 1 | 201280 | 4.21 | 11.94 | 0.0% |
| Umbra | 2 | 361922 | 4.70 | 11.66 | 0.0% |
| Umbra | 4 | 654595 | 5.20 | 14.47 | 0.0% |
| Umbra | 8 | 950548 | 6.35 | 19.27 | 0.0% |
| SQLite | 1 | 255523 | 3.06 | 7.72 | 0.0% |
| SQLite | 2 | 179863 | 5.71 | 80.93 | 0.0% |
| SQLite | 4 | 158288 | 7.74 | 185.36 | 0.0% |
| SQLite | 8 | 158081 | 22.72 | 322.84 | 0.0% |
| Postgres | 1 | 4937 | 121.51 | 643.45 | 0.0% |
| Postgres | 2 | 8480 | 140.22 | 843.75 | 0.0% |
| Postgres | 4 | 15888 | 138.24 | 894.85 | 0.0% |
| Postgres | 8 | 30902 | 133.75 | 893.70 | 0.0% |

## Scaling (throughput vs single thread)

| Engine | 1× | 2× | 4× | 8× |
|:-------|------:|------:|------:|------:|
| Umbra | 1.00× | 1.80× | 3.25× | 4.72× |
| SQLite | 1.00× | 0.70× | 0.62× | 0.62× |
| Postgres | 1.00× | 1.72× | 3.22× | 6.26× |
