<div align="center">
    <h1>Umbra</h1>
    <h3>(/ˈʌm.brə/)</h3>
    <img alt="Logo" src=".github/umbra-icon.png" width="250" height="250" />
    <h5>Self-contained, shadowy, and slightly suspicious database</h5>

![Rust](https://img.shields.io/badge/rust-00?style=for-the-badge&logo=rust&color=d62828&link=https%3A%2F%2Fwww.rust-lang.org)
![Linux](https://img.shields.io/badge/linux-0?style=for-the-badge&logo=linux&logoColor=fff&color=f77f00)
![MacOS](https://img.shields.io/badge/macos-0?style=for-the-badge&logo=macos&logoColor=fff&color=313244)

</div>

---

## What this is

Umbra is a relational database written in Rust. It began in the southern-hemisphere summer of December 2024, when a
heatwave and a lack of better ideas produced the obvious conclusion: build a database. It has since been rebuilt twice,
which is roughly the expected number.

It is a study project, not a product. The aim is to implement and measure the decisions a real DBMS has to make, from
isolation levels to crash recovery, and to check the results against systems that know what they are doing. PostgreSQL
and SQLite appear throughout, as references and as test oracles. They are not competitors, and nobody should migrate
anything.

A few facts about the current engine:

- **No runtime dependencies.** Only the Rust standard library.
- **No `unsafe` in the storage engine.**
- **Multi-version concurrency control in memory**, with three isolation levels and writers that never block readers.
- **Durable commits.** A commit is acknowledged only after its log record has been synced to disk.
- **Larger-than-memory tables.** Cold blocks of rows are evicted to disk under a memory budget. Indexes stay in memory (this might change).

---

## Getting started

```bash
cargo run --release -- data 8000
```

| argument | meaning |
|---|---|
| `data.db` | directory holding the database, created if missing |
| `8000` | TCP port, default `8000`. The server listens on `127.0.0.1` only |
| `512M`, `4G` (optional) | memory budget for table data. Defaults to half of the physical memory or the cgroup limit. A bare number is read as a count of 1,024-row blocks |

Then connect with `usql`, a shell in the spirit of `psql`:

```bash
cargo run --release --package usql -- 8000
```

---

## How it works

### Storage

Rows live in memory, in blocks of 1,024 slots, each block guarded by its own latch. Every slot holds a chain of versions.
Each version is stamped with the timestamp of the transaction that committed it. An uncommitted version carries its
writer's id with the top bit set, which is larger than any timestamp, so no other transaction can see it

The scheme comes from HyPer (Neumann, Mühlbauer and Kemper, SIGMOD 2015), and the commit protocol from Hekaton
(Larson et al., PVLDB 2011).

### Transactions

| level | snapshot taken | prevents | allows |
|---|---|---|---|
| `READ COMMITTED` | per statement | dirty writes and reads | non-repeatable reads, phantoms, write skew |
| `SNAPSHOT` (default, `REPEATABLE READ` is an alias) | per transaction | the above, lost updates, read skew | write skew |
| `SERIALIZABLE` | per transaction, reads validated at commit | all of the above | nothing |


### Queries

Plans are chosen by rules, not by a cost model. There are no statistics (yet).
Index nested-loop joins when the inner side has an index, hash joins otherwise. Index order serves `ORDER BY`.

---

## SQL support

### Types

#### Integers

| type | range | notes |
|---|---|---|
| `SMALLINT` | −2¹⁵ to 2¹⁵−1 | |
| `INTEGER` / `INT` | −2³¹ to 2³¹−1 | |
| `BIGINT` | −2⁶³ to 2⁶³−1 | |
| `SMALLINT UNSIGNED` | 0 to 2¹⁶−1 | MySQL's syntax, rather than PostgreSQL's check constraint |
| `INTEGER UNSIGNED` | 0 to 2³²−1 | |
| `BIGINT UNSIGNED` | 0 to 2⁶³−1 | integers are 64-bit signed inside, so the top half is out of reach |
| `SMALLSERIAL`, `SERIAL`, `BIGSERIAL` | 1 to the signed maximum | backed by a sequence |

Arithmetic is exact. Overflow is an error (`integer out of range`), not a wrap. As in PostgreSQL, a serial value taken by
a transaction that rolls back is gone for good.

#### Floating point and `NUMERIC`

| type | precision | notes |
|---|---|---|
| `REAL` | about 7 significant digits | stored at single precision |
| `DOUBLE PRECISION` | about 15 significant digits | |
| `NUMERIC(p, s)` / `DECIMAL` | arbitrary | base-10,000 digits, as in PostgreSQL. A value with more decimal places than `s` is rejected |

Mixing exact and approximate numbers gives an approximate result. A decimal literal such as `0.1` is a
`DOUBLE PRECISION`, unless it meets a `NUMERIC` column. This is a deliberate difference from PostgreSQL, where it would
be `NUMERIC`.

```sql
CREATE TABLE entropy_watch (
    event_id SERIAL PRIMARY KEY,
    observed_at TIMESTAMP,
    shadow_density REAL,
    void_pressure NUMERIC(10, 4)
);

SELECT observed_at FROM entropy_watch WHERE shadow_density > 69; -- 69 compares as a float
```

#### Other types

- `VARCHAR(n)`, `TEXT`, `BOOLEAN`
- `DATE`, `TIME`, `TIMESTAMP`, `INTERVAL`
- `UUID` (version 4)

#### JSONB

Documents are stored in a compact binary form. Object literals can be written directly in SQL, with unquoted keys and
comments, and paths are read with dots:

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    metadata JSONB
);

INSERT INTO users (name, metadata) VALUES
    ('Carol', {profile: {lang: 'en', hobbies: ['lurking', 'flying']}, active: true});

SELECT metadata.profile.lang FROM users;          -- 'en'
SELECT metadata.profile.hobbies[*] FROM users;    -- every element
SELECT metadata.profile.hobbies[-1] FROM users;   -- 'flying', counting from the end
SELECT metadata.missing FROM users;               -- NULL, not an error
SELECT name FROM users WHERE metadata.active = true;
```

### Constraints

- `PRIMARY KEY`, `UNIQUE`, enforced under concurrency
- `DEFAULT` with a constant expression
- `CHECK` at column and table level
- `FOREIGN KEY` / `REFERENCES` with `RESTRICT`, `NO ACTION` and `ON DELETE CASCADE`. Checks never wait for locks. Of two
  conflicting transactions, the one that commits second fails
- `NOT NULL` is the default, like Rust. A column that may hold `NULL` says so with `NULLABLE`

That last one departs from the SQL standard on purpose. A column is only nullable when someone decided it should be.

```sql
CREATE TABLE crypts (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE,
    depth INT DEFAULT 6 CHECK (depth > 0)
);

CREATE TABLE bats (
    id SERIAL PRIMARY KEY,
    crypt INT NULLABLE REFERENCES crypts(id) ON DELETE CASCADE,
    name TEXT
);
```

### Statements

- `CREATE TABLE`, `DROP TABLE`, `CREATE [UNIQUE] INDEX` on one or more columns
- `INSERT` (several rows at once, only with column list), `UPDATE`, `DELETE`. `INSERT` and `UPDATE` take
  `RETURNING`
- `SELECT` with `WHERE`, `ORDER BY`, `LIMIT` / `OFFSET`, `DISTINCT`, `GROUP BY`, `HAVING`, column and table aliases
- joins: inner, left, right, full, cross and comma joins, on any condition
- `UNION`, `INTERSECT` and `EXCEPT`, with or without `ALL`
- `CASE`, `CAST`, `BETWEEN`, `IN`, `LIKE`, `IS [NOT] NULL`, `%`
- subqueries in `IN`, `EXISTS` and as scalars. Correlated subqueries are refused with an error, except `EXISTS` over an
  equality, which becomes a semi-join.
- `EXPLAIN`

### Transactions

- `BEGIN` / `START TRANSACTION [ISOLATION LEVEL ...]`, `COMMIT`, `ROLLBACK`
- `SAVEPOINT`, `ROLLBACK TO [SAVEPOINT]`, `RELEASE [SAVEPOINT]`
- `SET TRANSACTION ISOLATION LEVEL ...` and `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ...`
- `CHECKPOINT`

A statement that fails while executing aborts the transaction. After that, only `COMMIT` (which rolls back),
`ROLLBACK` or `ROLLBACK TO SAVEPOINT` is accepted.

### Functions

| kind | functions |
|---|---|
| aggregate | `COUNT(*)`, `COUNT(expr)`, `COUNT(DISTINCT expr)`, `SUM`, `AVG`, `MIN`, `MAX` |
| numeric | `ABS`, `SQRT`, `POWER`, `TRUNC(x)`, `TRUNC(x, n)`, `SIGN` |
| text | `CONCAT`, `SUBSTRING(s FROM start FOR length)`, `POSITION(sub IN s)`, `ASCII`, `LIKE` with `%` and `_` |
| other | `COALESCE`, `TYPEOF`, `EXTRACT(field FROM value)` |

`SUBSTRING` counts from 1. `CONCAT` does not accept `NULL`, so wrap nullable arguments in `COALESCE`.

### Not supported

`ALTER TABLE`, `DROP INDEX`, views, triggers, stored procedures, `WITH`, window functions, upserts, prepared statements,
`SELECT ... FOR UPDATE`, deferrable constraints, `LOWER` / `UPPER` / `LENGTH` / `TRIM`. Some of these may arrive (I think). 

---

## Testing

```bash
cargo test --release
```

Besides unit and integration tests, the suite includes:

- the anomaly cases from Hermitage, each with a negative control showing the test fails at a weaker isolation level
- a history checker in the style of Elle, which searches randomised list-append histories for dependency cycles
- crash and fault injection over a simulated file system: a crash after every I/O operation, with unsynced bytes lost,
  torn, garbled or reordered
- `kill -9` against the binary under load
- metamorphic SQL fuzzing (NoREC and TLP)
- sqllogictest records whose expected results were produced by PostgreSQL
- TPC-C with its consistency conditions checked at every isolation level, and again after `SIGKILL`

Differential fuzzing against PostgreSQL runs when a local server is reachable (`UMBRA_BENCH_PG`, by default
`host=/run/postgresql user=postgres dbname=postgres`). `UMBRA_FUZZ_SEEDS` raises the number of fuzzing seeds.

Benchmarks, including YCSB against SQLite and PostgreSQL:

```bash
cargo bench --bench ycsb
```

---

## Licensing

Umbra is licensed under the **GNU Affero General Public License v3.0 or later (AGPL-3.0-or-later)**.

This project includes portions of code derived from third-party projects licensed under the MIT License. Attributions
and licence texts for those components are in [`THIRD_PARTY_NOTICES`](./THIRD_PARTY_NOTICES.md).

---

## See also

- **Wire protocol:** for writing a client, or for the curious, see [`PROTOCOL.md`](./PROTOCOL.md)
