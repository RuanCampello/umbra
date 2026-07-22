# Umbra comparison benchmark

Median microseconds per operation (± median absolute deviation), 10000 users / 30000 orders, single connection, each statement parsed and executed end to end. Lower is better, **Best** is the fastest median in the row.
            Read this with the setup in mind:
            - Umbra and SQLite run **in memory**, Postgres is the local **on-disk** server, so it pays client/server round-trips on every call and a durable fsync on every write — its write rows are not comparable to the embedded engines'.
            - Every engine indexes `age`/`status`/`user_id`, so column-filtered scans are a like-for-like comparison.
            - Umbra and Postgres decode every column of every returned row; SQLite steps the matching rows without materialising their columns. Umbra therefore does at least as much per-row work as SQLite on every read, the comparison never flatters it.
        ## Basic Operations

| Operation | Umbra (µs) | SQLite (µs) | Postgres (µs) | Best |
|:----------|----------:|----------:|----------:|:-----|
| SELECT by ID | 12.90 ±0.95 | 4.31 ±0.10 | 97.38 ±1.08 | SQLite |
| SELECT by column (exact) | 106.42 ±1.55 | 51.62 ±0.44 | 264.50 ±4.74 | SQLite |
| SELECT by column (range) | 1548.08 ±13.69 | 631.19 ±5.18 | 1082.19 ±24.00 | SQLite |
| SELECT complex | 4929.10 ±180.75 | 1167.22 ±13.76 | 931.45 ±43.44 | Postgres |
| SELECT * (full scan) | 5705.56 ±40.46 | 1298.33 ±8.99 | 4115.95 ±17.42 | SQLite |
| Aggregation (GROUP BY) | 5735.12 ±186.02 | 2858.72 ±35.01 | 1804.87 ±45.75 | Postgres |
| UPDATE by ID | 13.48 ±0.83 | 2.52 ±0.05 | 447.82 ±7.51 | SQLite |
| UPDATE complex | 4461.36 ±178.52 | 639.95 ±3.51 | 832.99 ±54.20 | SQLite |
| INSERT single | 8.99 ±0.56 | 4.38 ±0.06 | 429.38 ±15.63 | SQLite |
| DELETE by ID | 15.28 ±1.08 | 3.47 ±0.19 | 442.35 ±11.12 | SQLite |

**Basic Operations score: Umbra 0, SQLite 8, Postgres 2**

## Advanced Operations

| Operation | Umbra (µs) | SQLite (µs) | Postgres (µs) | Best |
|:----------|----------:|----------:|----------:|:-----|
| INNER JOIN | 18505.26 ±1226.50 | 40.65 ±0.36 | 1915.01 ±56.02 | SQLite |
| LEFT JOIN + GROUP BY | 67672.78 ±4636.67 | 150.78 ±5.56 | 442.01 ±3.72 | SQLite |

**Advanced Operations score: Umbra 0, SQLite 2, Postgres 0**

## Bottleneck Hunters

| Operation | Umbra (µs) | SQLite (µs) | Postgres (µs) | Best |
|:----------|----------:|----------:|----------:|:-----|
| LIKE prefix | 174.92 ±5.97 | 27.82 ±0.27 | 200.26 ±27.79 | SQLite |
| LIKE contains | 2074.72 ±14.68 | 339.38 ±4.23 | 654.05 ±6.31 | SQLite |
| OR conditions (3) | 241.72 ±6.03 | 41.59 ±0.29 | 206.72 ±3.90 | SQLite |
| IN list (7 values) | 511.40 ±6.23 | 40.20 ±0.22 | 205.84 ±3.08 | SQLite |
| BETWEEN (non-indexed) | 190.61 ±6.90 | 24.81 ±0.19 | 173.06 ±9.48 | SQLite |
| OFFSET pagination (5000) | 2679.06 ±14.27 | 69.69 ±0.55 | 1896.67 ±17.77 | SQLite |
| Multi-col ORDER BY (3) | 6042.89 ±43.49 | 1016.42 ±4.93 | 237.22 ±7.31 | Postgres |
| Multi aggregates (5) | 3167.75 ±177.52 | 934.47 ±6.34 | 840.47 ±6.50 | Postgres |
| GROUP BY (2 columns) | 4422.05 ±63.28 | 3455.51 ±17.48 | 1433.68 ±22.99 | Postgres |
| Math expressions | 101.87 ±8.90 | 9.43 ±0.05 | 93.37 ±1.29 | SQLite |
| String concat (CONCAT) | 111.08 ±3.32 | 14.55 ±0.12 | 94.02 ±1.69 | SQLite |
| COALESCE + IS NOT NULL | 96.41 ±2.29 | 8.43 ±0.09 | 88.22 ±1.48 | SQLite |
| Large result (no LIMIT) | 2597.77 ±35.37 | 844.20 ±4.77 | 1545.23 ±15.73 | SQLite |

**Bottleneck Hunters score: Umbra 0, SQLite 10, Postgres 3**

## Specialised (UUID / enum / numeric / JSON / sorting)

| Operation | Umbra (µs) | SQLite (µs) | Postgres (µs) | Best |
|:----------|----------:|----------:|----------:|:-----|
| Sort by UUID (top 100) | 2771.26 ±17.25 | 661.49 ±4.68 | 718.92 ±26.02 | SQLite |
| Sort by numeric DESC (top 100) | 3220.72 ±36.30 | 537.53 ±3.46 | 1340.78 ±25.56 | SQLite |
| Extreme full sort (weight) | 4379.56 ±60.31 | 2820.04 ±11.24 | 3242.84 ±145.47 | SQLite |
| UUID point lookup | 2394.16 ±35.90 | 364.21 ±3.57 | 433.28 ±14.22 | SQLite |
| Numeric aggregation (SUM/AVG/MIN/MAX) | 3360.59 ±37.95 | 852.21 ±3.82 | 1132.97 ±9.43 | SQLite |
| Numeric filter + arithmetic | — | 11.30 ±0.12 | 100.75 ±1.60 | SQLite |
| Enum GROUP BY | 4000.28 ±245.14 | 2262.47 ±13.07 | 1592.48 ±11.43 | Postgres |
| Enum filter | 285.17 ±4.21 | 36.56 ±0.27 | 188.58 ±3.99 | SQLite |
| JSON field filter | 317.70 ±8.66 | 107.97 ±1.64 | 138.48 ±19.87 | SQLite |
| JSON field projection | 144.36 ±3.14 | 31.81 ±0.40 | 85.76 ±3.39 | SQLite |

**Specialised (UUID / enum / numeric / JSON / sorting) score: Umbra 0, SQLite 9, Postgres 1**

## Summary

| Engine | Wins |
|:-------|-----:|
| Umbra | 0 |
| SQLite | 29 |
| Postgres | 6 |

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
| Umbra | 1 | 106871 | 7.89 | 18.32 | 0.0% |
| Umbra | 2 | 214242 | 8.04 | 21.03 | 0.0% |
| Umbra | 4 | 348862 | 9.01 | 34.80 | 0.0% |
| Umbra | 8 | 393854 | 14.26 | 56.62 | 0.0% |
| SQLite | 1 | 223398 | 3.13 | 9.92 | 0.0% |
| SQLite | 2 | 185573 | 5.54 | 87.42 | 0.0% |
| SQLite | 4 | 184902 | 9.84 | 104.89 | 0.0% |
| SQLite | 8 | 161887 | 21.11 | 364.37 | 0.0% |
| Postgres | 1 | 5416 | 107.28 | 552.75 | 0.0% |
| Postgres | 2 | 9206 | 114.62 | 830.04 | 0.0% |
| Postgres | 4 | 16502 | 124.41 | 883.28 | 0.0% |
| Postgres | 8 | 31772 | 132.90 | 866.96 | 0.0% |

## Scaling (throughput vs single thread)

| Engine | 1× | 2× | 4× | 8× |
|:-------|------:|------:|------:|------:|
| Umbra | 1.00× | 2.00× | 3.26× | 3.69× |
| SQLite | 1.00× | 0.83× | 0.83× | 0.72× |
| Postgres | 1.00× | 1.70× | 3.05× | 5.87× |
