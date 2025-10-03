<div align="center">
    <h1>Umbra</h1>
    <h3>(/ˈʌm.brə/)</h3>
    <img alt="Logo" src=".github/umbra-icon.png" width="250" height="250" />
    <h5>Self-contained, shadowy, and slightly suspicious database :bat: </h5>

![Rust](https://img.shields.io/badge/rust-00?style=for-the-badge&logo=rust&color=d62828&link=https%3A%2F%2Fwww.rust-lang.org)
![Linux](https://img.shields.io/badge/linux-0?style=for-the-badge&logo=linux&logoColor=fff&color=f77f00)
![MacOS](https://img.shields.io/badge/macos-0?style=for-the-badge&logo=macos&logoColor=fff&color=313244)

</div>

---

## ✨ Not PostgreSQL. Not SQLite. Something... _else_

This project takes certain... liberties with database paradigms. Born from the existential void of a southern hemisphere
summer (December 2024–January 2025, when ennui struck like the heatwave), Umbra cosplays as PostgreSQL with its query
planner (proper cost-based optimisation using expression trees, darling—we’re civilised), while harbouring SQLite’s
scandalous little secrets under the bonnet (_ahem_ `RowId`, you disreputable little implementation detail).

_The bat is non-negotiable._

Let's be clear: **this isn't a replacement for anything,** unless you're replacing sanity with chaos, which
case—welcome. The focus here is the **learning process**, read as "the screams of debugging hand-rolled date parsing at
3AM."

---

## Getting Started

```bash
cargo run -- file.db 8000
```

`file.db`: The path to your Umbra database file (will be created if it doesn't exist).
`8000`: The TCP port the server will listen on.

#### Use the `usql` CLI (like `psql`, but darker)

```bash
cargo run --package usql -- 8000
```

Connects to the Umbra server running on port `8000`.

> `usql` is your shadowy shell into the Umbra world. It understands SQL and the void.

---

## Implementation Status

### 🧾 Types

#### :one: **Integer Types**

The beating heart of databases—and poor life decisions.

Umbra supports both **signed** and **unsigned** integer types, as well as their attention-seeking cousins: the **serial** pseudo-types. These `SERIAL` types aren’t real—they're syntactic sugar that auto-magically generate sequences behind the scenes (just like PostgreSQL, but with fewer emotional boundaries).

| Type                 | Range       | Notes                                         |
|----------------------|-------------|-----------------------------------------------|
| `SMALLINT`           | ±2¹⁵        | Petite regrets                                |
| `INTEGER`          | ±2³¹      | Standard regret capacity                      |
| `BIGINT`             | ±2⁶³        | When regular regrets aren't enough                             |
| `SMALLINT UNSIGNED`  | 0 → 2¹⁶−1   | For when you're cautiously hopeful            |
| `INTEGER UNSIGNED`   | 0 → 2³²−1   | Delusional optimism                           |
| `BIGINT UNSIGNED`    | 0 → 2⁶⁴−1   | Sheer madness                                 |
| `SMALLSERIAL`        | 1 → 2¹⁵−1   | Small but permanent mistakes    |
| `SERIAL`             | 1 → 2³¹−1   | Commitment issues               |
| `BIGSERIAL`          | 1 → 2⁶³−1   | Lifelong consequences            |

> [!NOTE]  
> **Unsigned Integers**  
> PostgreSQL demands check constraints, SQLite shrugs, but Umbra embraces MySQL's blunt [syntax](https://dev.mysql.com/doc/refman/8.4/en/numeric-type-syntax.html) for that.

> [!IMPORTANT]  
> **`SERIAL` Types are forgetful**  
> Much like PostgreSQL, Umbra's serial values never look back. 
> Once generated—even if your transaction fails—they’re [gone](https://www.postgresql.org/docs/17/functions-sequence.html).

```sql
CREATE TABLE cursed_items (
    item_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    darkness_level SMALLINT UNSIGNED,
    soul_count BIGINT UNSIGNED
);
```

#### :1234: **Floating-Point Types**

When integers just won’t cut it and you need *approximate truths*, floating-point types step in—like unreliable narrators in a numerical novel.

| Type             | Precision              | Notes                                                                 |
|------------------|-------------------------|-----------------------------------------------------------------------|
| `REAL`             | ~7 decimal digits       | Single-precision daydreams. Fast, vague, and prone to making things up. |
| `DOUBLE PRECISION` | ~15 decimal digits      | Double the bits, double the confidence. For those who still don’t trust `REAL`. |

> [!WARNING]  
> **Floating-Point Lies**  
> They're fast. They're fun. They're wrong. Read more about that [here](https://medium.com/@Carl_Maxwell/understanding-f32-floating-point-9d7e3604ab97).
> If you're just plotting wiggly shadows over time, `REAL` is your friend.

```sql
CREATE TABLE entropy_watch (
    event_id SERIAL PRIMARY KEY,
    time TIMESTAMP,
    shadow_density REAL,
    void_pressure DOUBLE PRECISION
);
```

> [!TIP]  
> **Casting Happens**  
> Umbra will happily let you mix floats and integers. But you might want to think twice before comparing them for equality. That way lies madness.
> Also, insert will **NOT** coerce types, it'll strictly verify the types based on the defined schema.

```sql
SELECT time FROM entropy_watch WHERE shadow_density > 69; -- here we implicitly cast 69 to a float, so you can query without hustle
```

#### 🧬 **UUID Type**

When numbers aren't _unique_ enough and strings are too _sincere_, you reach for the `UUID`. Specifically: **version 4**, because determinism is for spreadsheets.

| Type   | Description                              | Notes                                             |
|--------|------------------------------------------|---------------------------------------------------|
| `UUID` | Universally Unique Identifier (v4 only)  | Random, stateless, and perfect for plausible deniability. |

```sql
CREATE TABLE shadow_agents (
    agent_id UUID PRIMARY KEY,
    codename VARCHAR(255),
    clearance_level SMALLINT
);
```

- [x] `VARCHAR`
- [x] `BOOLEAN`
- [ ] `DECIMAL`
- [x] `TEXT` (the loquacious one)
- [x] Temporal types (because time flies when... I ran out of jokes)
    - [x] `DATE`
    - [x] `TIME`
    - [x] `TIMESTAMP` (precision: "ish")

### 🔗 Constraints
- [x] `PRIMARY KEY`
  - [x] Auto-incrementing (via `SERIAL`, no manual gear-shifting required)
- [x] `UNIQUE` (because duplicates are tacky)
- [x] `NULLABLE` (explicit nullability, because I believe in clarity)
- [ ] `FOREIGN KEY` (relationships are hard)
- [ ] `CHECK`

> [!IMPORTANT]  
> **Nullability Philosophy**  
> Unlike SQL standard where columns are nullable by default, requiring `NOT NULL` to prevent it.
> Umbra follows Rust philosophy: columns are **not null** by default, unless explicit marked as `NULLABLE`.
> This makes null handling intentional and not condescending.

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),              -- non-null by default
    email VARCHAR(255) NULLABLE,    -- explicitly nullable
);
```

### ⚙️ Operations
#### 💼 **Table Operations**
- [x] `CREATE TABLE` *(where hope begins)*
- [x] `DROP TABLE` *(where hope ends)*
- [ ] `ALTER TABLE`

#### 🔍 SELECT Operations
- [x] `WHERE` (basic filtering, no existential crises)
- [x] `ORDER BY` (implemented after herding literal bats)
- [x] `BETWEEN` (for when life exists within bounds)
- [ ] `LIMIT`/`OFFSET` (self-restraint coming soon™)
- [x] Column aliases (SELECT tomb AS grave - identity is fluid)
- [ ] Table aliases (`FROM crypts AS c` - naming things is hard)
- [ ] `JOIN` (relationships require couples therapy)
- [x] `GROUP BY` (aggregation is a social construct)
- [ ] `HAVING` 

#### 💸 **Transactions**
- [x] `BEGIN` 
- [x] `COMMIT` 
- [x] `ROLLBACK`

#### 📊 **Indexes**
- [x] Unique indexes *(exclusivity is key)*
- [ ] Non-unique indexes *(for the masses)*
- [ ] Partial indexes *(discrimination coming soon)*

### 🧮 Aggregate Functions

Now available: _numerical seances_. Umbra can perform ritualistic summoning of truths across rows using standard SQL aggregate functions. Useful for divining patterns, spotting anomalies, or just counting how many regrets you’ve inserted into a table.

| Function      | Description                                          | Example                                          |
| ------------- | ---------------------------------------------------- | ------------------------------------------------ |
| `COUNT(*)`    | How many? (*all of them*)                            | `SELECT COUNT(*) FROM curses;`                   |
| `SUM(expr)`   | Add them all up (*tally your sins*)                  | `SELECT SUM(darkness_level) FROM cursed_items;`  |
| `AVG(expr)`   | Arithmetic mean (*because median is too mainstream*) | `SELECT AVG(void_pressure) FROM entropy_watch;`  |
| `MIN(expr)`   | The lowest value (*bottom of the abyss*)             | `SELECT MIN(shadow_density) FROM entropy_watch;` |
| `MAX(expr)`   | The highest value (*loftiest darkness*)              | `SELECT MAX(soul_count) FROM cursed_items;`      |

### ➗ Mathematical Functions  

For when you need to quantify the abyss. Basic arithmetic operations (`+`, `-`, `*`, `/`) work as expected, but these functions handle the *unnatural* calculations.  

| Function       | Description                                      | Example                                      |
|----------------|--------------------------------------------------|----------------------------------------------|
| `ABS(x)`       | Absolute value (*distance from zero*)            | `SELECT ABS(-666) → 666`                     |
| `SQRT(x)`      | Square root (*measure of diagonal despair*)      | `SELECT SQRT(2) → 1.414...`                  |
| `POWER(x, y)`  | Exponentiation (*x raised to yth torment*)       | `SELECT POWER(2, 10) → 1024`                 |
| `TRUNC(x)`     | Amputate decimals (*integer-only suffering*)     | `SELECT TRUNC(3.14159) → 3`                  |
| `TRUNC(x, n)`  | Precision mutilation (*n decimal digits*)        | `SELECT TRUNC(3.14159, 2) → 3.14`            |
| `SIGN(x)`      | Returns -1, 0, or 1 (*the algebra of alignment*)| `SELECT SIGN(-13) → -1`                      |

> [!WARNING]  
> **Division by Zero**  
> Attempting to divide by zero will summon an error—because some voids should remain unexplored.

### 🔤 **String Functions**

Words are powerful. Here they’re _dangerous_.

| Function                   | Description                                              | Example                                                   |
|----------------------------|----------------------------------------------------------|-----------------------------------------------------------|
| `LIKE`                     | Pattern matching with `%` and `_`                        | `WHERE name LIKE 'Umb%'`                                  |
| `CONCAT(a, b)`             | Smashes values into one long string                      | `CONCAT('bat', 'man') → 'batman'`                         |
| `SUBSTRING(str FROM x FOR y)` | Extracts a slice, PostgreSQL-style                  | `SUBSTRING(name FROM 1 FOR 1) → first letter of name`     |
| `POSITION(substr IN str)` | Finds the starting index of a substring                  | `POSITION('e' IN 'shadow') → 2`                           |
| `ASCII(char)`              | Returns the ASCII code of a single character             | `ASCII('A') → 65`                                         |

> [!NOTE]  
> **SUBSTRING Syntax**  
> Umbra uses PostgreSQL-style `SUBSTRING(string FROM start FOR length)`.  
> - `FROM` and `FOR` are both optional, but at least one must be present.  
> - Positions are 1-based (because zero-based indexing is for the living).

```sql
-- give me just the first letter of each name
SELECT name, SUBSTRING(name FROM 1 FOR 1) FROM customers;

-- or everything after the third character
SELECT SUBSTRING(name FROM 4) FROM customers;

-- or only the first 3 characters, starting from the beginning
SELECT SUBSTRING(name FOR 3) FROM customers;
```


### *✨ Just So You Know*

- **Documentation**: [Here be dragons](https://ruancampello.github.io/umbra-documentation/) (*and possibly bats*)
- **Protocol**: If you're implementing a client, or are just curious how the shadowy bits work under the hood, check the binary wire format [here](./PROTOCOL.md).
- **Philosophy**: *"Compiling is victory. Running is a miracle."*

