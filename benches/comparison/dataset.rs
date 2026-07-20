//! Deterministic, ClickBench-inspired dataset

use std::env;

struct Rng {
    state: u64,
}

pub const STATUSES: [&str; 4] = ["pending", "completed", "shipped", "cancelled"];

pub const SCHEMA: [&str; 2] = [
    "CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name VARCHAR(64),
        email VARCHAR(128),
        age INT,
        balance REAL,
        active BOOLEAN,
        created_at VARCHAR(32)
    )",
    "CREATE TABLE orders (
        id INTEGER PRIMARY KEY,
        user_id INT,
        amount REAL,
        status VARCHAR(16),
        order_date VARCHAR(32)
    )",
];

pub const SECONDARY_INDEXES: [&str; 4] = [
    "CREATE INDEX idx_users_age ON users(age)",
    "CREATE INDEX idx_users_active ON users(active)",
    "CREATE INDEX idx_orders_user_id ON orders(user_id)",
    "CREATE INDEX idx_orders_status ON orders(status)",
];

pub fn users() -> usize {
    env::var("UMBRA_BENCH_ROWS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(10_000)
}

pub fn orders() -> usize {
    users() * 3
}

pub fn inserts() -> Vec<String> {
    let (users, orders) = (users(), orders());
    let mut statements = Vec::with_capacity(users + orders);
    let mut rng = Rng::new(0x0DDB_A11C_0FFE_E5EE);

    for id in 1..=users {
        let age = rng.range(18, 80);
        let balance = rng.money(0.0, 100_000.0);
        let active = rng.probability() < 0.7;
        statements.push(format!(
            "INSERT INTO users (id, name, email, age, balance, active, created_at) \
             VALUES ({id}, 'User_{id}', 'user{id}@example.com', {age}, {balance:.2}, {active}, '2024-01-01 00:00:00')"
        ));
    }

    for id in 1..=orders {
        let user_id = rng.range(1, users as i64 + 1);
        let amount = rng.money(10.0, 1_000.0);
        let status = STATUSES[(rng.next_u64() % STATUSES.len() as u64) as usize];
        statements.push(format!(
            "INSERT INTO orders (id, user_id, amount, status, order_date) \
             VALUES ({id}, {user_id}, {amount:.2}, '{status}', '2024-01-15')"
        ));
    }

    statements
}

impl Rng {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn range(&mut self, low: i64, high: i64) -> i64 {
        let span = (high - low) as u64;
        low + (self.next_u64() % span) as i64
    }

    fn money(&mut self, low: f64, high: f64) -> f64 {
        ((low + self.probability() * (high - low)) * 100.0).round() / 100.0
    }

    fn probability(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}
