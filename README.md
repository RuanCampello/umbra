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

## Implementation Status

### 🧾 Types

#### 🔢 **Numeric Types**

Integers: the beating heart of databases—and poor life decisions.

Umbra supports both **signed** and **unsigned** integer types, as well as their attention-seeking cousins: the **serial** pseudo-types. These `SERIAL` types aren’t real—they're syntactic sugar that auto-magically generate sequences behind the scenes (just like PostgreSQL, but with fewer emotional boundaries).

| Type                 | Status | Range       | Notes                                         |
|----------------------|--------|-------------|-----------------------------------------------|
| `SMALLINT`           | ✅     | ±2¹⁵        | Petite regrets                                |
| `INTEGER`          | ✅     | ±2³¹      | Standard regret capacity                      |
| `BIGINT`             | ✅     | ±2⁶³        | When regular regrets aren't enough                             |
| `SMALLINT UNSIGNED`  | ✅     | 0 → 2¹⁶−1   | For when you're cautiously hopeful            |
| `INTEGER UNSIGNED`   | ✅     | 0 → 2³²−1   | Delusional optimism                           |
| `BIGINT UNSIGNED`    | ✅     | 0 → 2⁶⁴−1   | Sheer madness                                 |
| `SMALLSERIAL`        | ✅     | 1 → 2¹⁵−1   | Small but permanent mistakes    |
| `SERIAL`             | ✅     | 1 → 2³¹−1   | Commitment issues               |
| `BIGSERIAL`          | ✅     | 1 → 2⁶³−1   | Lifelong consequences            |

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

- [x] `VARCHAR`
- [x] `BOOLEAN`
- [ ] `DECIMAL`
- [ ] `TEXT`
- [x] Temporal types (because time flies when... I ran out of jokes)
    - [x] `DATE`
    - [x] `TIME`
    - [x] `TIMESTAMP` (precision: "ish")

### 🔗 Constraints
- [x] `PRIMARY KEY`
  - [x] Auto-incrementing (via `SERIAL`, no manual gear-shifting required)
- [x] `UNIQUE` (because duplicates are tacky)
- [ ] `FOREIGN KEY` (relationships are hard)
- [ ] `CHECK`
- [ ] `NOT NULL` (I'm very chill about emptiness)

### ⚙️ Operations
#### 💼 **Table Operations**
- [x] `CREATE TABLE` *(where hope begins)*
- [x] `DROP TABLE` *(where hope ends)*
- [ ] `ALTER TABLE`

#### 🔍 SELECT Operations
- [x] `WHERE` (basic filtering, no existential crises)
- [x] `ORDER BY` (implemented after herding literal bats)
- [ ] `LIMIT`/`OFFSET` (self-restraint coming soon™)
- [ ] Table aliases (`FROM crypts AS c` - naming things is hard)
- [ ] `JOIN` (relationships require couples therapy)
- [ ] `GROUP BY` (aggregation is a social construct)
- [ ] `HAVING` 

#### 💸 **Transactions**
- [x] `BEGIN` 
- [x] `COMMIT` 
- [x] `ROLLBACK`

#### 📊 **Indexes**
- [x] Unique indexes *(exclusivity is key)*
- [ ] Non-unique indexes *(for the masses)*
- [ ] Partial indexes *(discrimination coming soon)*

### 🧮 Basic Functions
- [ ] `COUNT`
- [ ] `AVG`
- [ ] `SUM`

### *✨ Just So You Know*

- **Documentation**: [Here be dragons](https://ruancampello.github.io/umbra-documentation/) (*and possibly bats*)
- **Philosophy**: *"Compiling is victory. Running is a miracle."*

