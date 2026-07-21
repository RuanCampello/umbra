use crate::dataset;

pub struct Scenario {
    pub category: Category,
    pub label: &'static str,
    pub workload: Workload,
    pub iterations: usize,
}

#[derive(Clone, Copy, PartialEq)]
pub enum Category {
    Basic,
    Advanced,
    Bottleneck,
    Specialised,
}

pub enum Workload {
    Read(&'static str),
    /// A read whose SQL is built per engine (by [`Engine::name`]) — for cases
    /// whose syntax diverges, e.g. JSON extraction (`tags.region` vs
    /// `json_extract` vs `->>`).
    ReadEach(fn(&str) -> String),
    Write(fn(usize) -> String),
}

impl Category {
    pub const fn title(self) -> &'static str {
        match self {
            Self::Basic => "Basic Operations",
            Self::Advanced => "Advanced Operations",
            Self::Bottleneck => "Bottleneck Hunters",
            Self::Specialised => "Specialised (UUID / enum / numeric / JSON / sorting)",
        }
    }
}

/// Engine-specific extraction of a JSON object key, all returning text.
fn json_extract(engine: &str, key: &str) -> String {
    match engine {
        "Umbra" => format!("tags.{key}"),
        "Postgres" => format!("tags->>'{key}'"),
        _ => format!("json_extract(tags, '$.{key}')"),
    }
}

fn json_region_filter(engine: &str) -> String {
    format!(
        "SELECT label FROM records WHERE {} = 'north' LIMIT 100",
        json_extract(engine, "region")
    )
}

fn json_region_projection(engine: &str) -> String {
    format!(
        "SELECT {} FROM records LIMIT 100",
        json_extract(engine, "region")
    )
}

fn uuid_lookup(_engine: &str) -> String {
    format!(
        "SELECT * FROM records WHERE ext_id = '{}'",
        dataset::sample_uuid()
    )
}

pub fn scenarios() -> Vec<Scenario> {
    vec![
        // basic operations
        Scenario {
            category: Category::Basic,
            label: "SELECT by ID",
            workload: Workload::Read("SELECT * FROM users WHERE id = 5000"),
            iterations: 300,
        },
        Scenario {
            category: Category::Basic,
            label: "SELECT by column (exact)",
            workload: Workload::Read("SELECT * FROM users WHERE age = 42"),
            iterations: 300,
        },
        Scenario {
            category: Category::Basic,
            label: "SELECT by column (range)",
            workload: Workload::Read("SELECT * FROM users WHERE age >= 30 AND age <= 40"),
            iterations: 200,
        },
        Scenario {
            category: Category::Basic,
            label: "SELECT complex",
            workload: Workload::Read(
                "SELECT id, name, balance FROM users \
                 WHERE age >= 25 AND age <= 45 AND active = true \
                 ORDER BY balance DESC LIMIT 100",
            ),
            iterations: 200,
        },
        Scenario {
            category: Category::Basic,
            label: "SELECT * (full scan)",
            workload: Workload::Read("SELECT * FROM users"),
            iterations: 40,
        },
        Scenario {
            category: Category::Basic,
            label: "Aggregation (GROUP BY)",
            workload: Workload::Read("SELECT age, COUNT(*), AVG(balance) FROM users GROUP BY age"),
            iterations: 100,
        },

        // advanced operations
        Scenario {
            category: Category::Advanced,
            label: "INNER JOIN",
            workload: Workload::Read(
                "SELECT u.name, o.amount FROM users AS u \
                 INNER JOIN orders AS o ON u.id = o.user_id \
                 WHERE o.status = 'completed' LIMIT 100",
            ),
            iterations: 40,
        },
        Scenario {
            category: Category::Advanced,
            label: "LEFT JOIN + GROUP BY",
            workload: Workload::Read(
                "SELECT u.name, COUNT(o.id), SUM(o.amount) FROM users AS u \
                 LEFT JOIN orders AS o ON u.id = o.user_id \
                 GROUP BY u.id, u.name LIMIT 100",
            ),
            iterations: 20,
        },

        // bottleneck hunters
        Scenario {
            category: Category::Bottleneck,
            label: "LIKE prefix",
            workload: Workload::Read("SELECT * FROM users WHERE name LIKE 'User_1%' LIMIT 100"),
            iterations: 200,
        },
        Scenario {
            category: Category::Bottleneck,
            label: "LIKE contains",
            workload: Workload::Read("SELECT * FROM users WHERE email LIKE '%50%' LIMIT 100"),
            iterations: 200,
        },
        Scenario {
            category: Category::Bottleneck,
            label: "OR conditions (3)",
            workload: Workload::Read(
                "SELECT * FROM users WHERE age = 25 OR age = 50 OR age = 75 LIMIT 100",
            ),
            iterations: 200,
        },
        Scenario {
            category: Category::Bottleneck,
            label: "IN list (7 values)",
            workload: Workload::Read(
                "SELECT * FROM users WHERE age IN (20, 25, 30, 35, 40, 45, 50) LIMIT 100",
            ),
            iterations: 200,
        },
        Scenario {
            category: Category::Bottleneck,
            label: "BETWEEN (non-indexed)",
            workload: Workload::Read(
                "SELECT * FROM users WHERE balance BETWEEN 25000 AND 75000 LIMIT 100",
            ),
            iterations: 200,
        },
        Scenario {
            category: Category::Bottleneck,
            label: "OFFSET pagination (5000)",
            workload: Workload::Read("SELECT * FROM users ORDER BY id LIMIT 100 OFFSET 5000"),
            iterations: 200,
        },
        Scenario {
            category: Category::Bottleneck,
            label: "Multi-col ORDER BY (3)",
            workload: Workload::Read(
                "SELECT * FROM users ORDER BY age DESC, balance ASC, name LIMIT 100",
            ),
            iterations: 100,
        },
        Scenario {
            category: Category::Bottleneck,
            label: "Multi aggregates (5)",
            workload: Workload::Read(
                "SELECT COUNT(*), SUM(balance), AVG(balance), MIN(balance), MAX(balance) FROM users",
            ),
            iterations: 200,
        },
        Scenario {
            category: Category::Bottleneck,
            label: "GROUP BY (2 columns)",
            workload: Workload::Read(
                "SELECT age, active, COUNT(*), AVG(balance) FROM users GROUP BY age, active",
            ),
            iterations: 100,
        },
        Scenario {
            category: Category::Bottleneck,
            label: "Math expressions",
            workload: Workload::Read(
                "SELECT name, balance * 1.1, ABS(balance - 50000) FROM users LIMIT 100",
            ),
            iterations: 200,
        },
        Scenario {
            category: Category::Bottleneck,
            label: "String concat (CONCAT)",
            workload: Workload::Read(
                "SELECT CONCAT(name, ' (', email, ')') FROM users LIMIT 100",
            ),
            iterations: 200,
        },
        Scenario {
            category: Category::Bottleneck,
            label: "COALESCE + IS NOT NULL",
            workload: Workload::Read(
                "SELECT name, COALESCE(balance, 0) FROM users WHERE balance IS NOT NULL LIMIT 100",
            ),
            iterations: 200,
        },
        Scenario {
            category: Category::Bottleneck,
            label: "Large result (no LIMIT)",
            workload: Workload::Read("SELECT id, name, balance FROM users WHERE active = true"),
            iterations: 40,
        },

        Scenario {
            category: Category::Specialised,
            label: "Sort by UUID (top 100)",
            workload: Workload::Read("SELECT ext_id FROM records ORDER BY ext_id LIMIT 100"),
            iterations: 60,
        },
        Scenario {
            category: Category::Specialised,
            label: "Sort by numeric DESC (top 100)",
            workload: Workload::Read("SELECT id, amount FROM records ORDER BY amount DESC LIMIT 100"),
            iterations: 60,
        },
        Scenario {
            category: Category::Specialised,
            label: "Extreme full sort (weight)",
            workload: Workload::Read("SELECT id FROM records ORDER BY weight"),
            iterations: 40,
        },
        Scenario {
            category: Category::Specialised,
            label: "UUID point lookup",
            workload: Workload::ReadEach(uuid_lookup),
            iterations: 200,
        },
        Scenario {
            category: Category::Specialised,
            label: "Numeric aggregation (SUM/AVG/MIN/MAX)",
            workload: Workload::Read(
                "SELECT SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM records",
            ),
            iterations: 100,
        },
        Scenario {
            category: Category::Specialised,
            label: "Numeric filter + arithmetic",
            workload: Workload::Read(
                "SELECT id, amount * 1.085 FROM records WHERE amount > 50000 LIMIT 100",
            ),
            iterations: 200,
        },
        Scenario {
            category: Category::Specialised,
            label: "Enum GROUP BY",
            workload: Workload::Read(
                "SELECT kind, COUNT(*), AVG(amount) FROM records GROUP BY kind",
            ),
            iterations: 100,
        },
        Scenario {
            category: Category::Specialised,
            label: "Enum filter",
            workload: Workload::Read("SELECT * FROM records WHERE kind = 'books' LIMIT 100"),
            iterations: 200,
        },
        Scenario {
            category: Category::Specialised,
            label: "JSON field filter",
            workload: Workload::ReadEach(json_region_filter),
            iterations: 100,
        },
        Scenario {
            category: Category::Specialised,
            label: "JSON field projection",
            workload: Workload::ReadEach(json_region_projection),
            iterations: 100,
        },

        // writes (last, so they never disturb a read scenario)
        Scenario {
            category: Category::Basic,
            label: "UPDATE by ID",
            workload: Workload::Write(|i| {
                format!("UPDATE users SET balance = 100.0 WHERE id = {}", 1 + i % dataset::users())
            }),
            iterations: 300,
        },
        Scenario {
            category: Category::Basic,
            label: "UPDATE complex",
            workload: Workload::Write(|_| {
                "UPDATE users SET balance = 500.0 WHERE age >= 27 AND age <= 28 AND active = true"
                    .to_string()
            }),
            iterations: 100,
        },
        Scenario {
            category: Category::Basic,
            label: "INSERT single",
            workload: Workload::Write(|i| {
                // a private high id range, so inserts never collide with the
                // dataset or with each other across warmup and measurement
                let id = 10_000_000 + i;
                format!(
                    "INSERT INTO users (id, name, email, age, balance, active, created_at) \
                     VALUES ({id}, 'User_{id}', 'user{id}@example.com', 30, 500.0, true, '2024-01-01 00:00:00')"
                )
            }),
            iterations: 300,
        },
        Scenario {
            category: Category::Basic,
            label: "DELETE by ID",
            workload: Workload::Write(|i| format!("DELETE FROM users WHERE id = {}", 1 + i)),
            iterations: 300,
        },
    ]
}
