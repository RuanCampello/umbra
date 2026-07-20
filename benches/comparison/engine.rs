use crate::dataset;
use std::env;
use umbra::db::MvccDatabase;

pub struct Umbra {
    db: MvccDatabase,
}

pub struct Sqlite {
    connection: rusqlite::Connection,
}

pub struct Postgres {
    client: postgres::Client,
}

pub trait Engine {
    fn name<'s>(&self) -> &'s str;
    fn exec(&mut self, sql: &str);
    fn query(&mut self, sql: &str) -> Result<usize, String>;
    fn write(&mut self, sql: &str) -> Result<(), String>;
    fn load(&mut self, indexed: bool) {
        for ddl in dataset::SCHEMA {
            self.exec(ddl);
        }
        if indexed {
            for index in dataset::SECONDARY_INDEXES {
                self.exec(index);
            }
        }
        for insert in dataset::inserts() {
            self.exec(&insert);
        }
    }
}

impl Umbra {
    pub fn setup() -> Self {
        let mut umbra = Self {
            db: MvccDatabase::in_memory().expect("umbra opens in memory"),
        };
        umbra.load(true);
        umbra
    }
}

impl Sqlite {
    pub fn setup() -> Self {
        let mut sqlite = Self {
            connection: rusqlite::Connection::open_in_memory().expect("sqlite opens in memory"),
        };
        sqlite.load(true);
        sqlite
    }
}

impl Postgres {
    pub fn setup() -> Option<Self> {
        let target = env::var("UMBRA_BENCH_PG")
            .unwrap_or_else(|_| "host=/run/postgresql user=postgres dbname=postgres".into());

        let mut client = postgres::Client::connect(&target, postgres::NoTls).ok()?;
        client
            .batch_execute("DROP TABLE IF EXISTS users, orders")
            .expect("postgres resets its schema");

        let mut postgres = Self { client };
        postgres.load(true);
        Some(postgres)
    }
}

impl Engine for Umbra {
    fn name<'s>(&self) -> &'s str {
        "Umbra"
    }

    fn exec(&mut self, sql: &str) {
        self.db
            .exec(&terminated(sql))
            .expect("umbra runs the statement");
    }

    fn query(&mut self, sql: &str) -> Result<usize, String> {
        self.db
            .exec(&terminated(sql))
            .map(|result| result.tuples.len())
            .map_err(|err| format!("{err:?}"))
    }

    fn write(&mut self, sql: &str) -> Result<(), String> {
        self.db
            .exec(&terminated(sql))
            .map(|_| ())
            .map_err(|err| format!("{err:?}"))
    }
}

fn terminated(sql: &str) -> String {
    format!("{sql};")
}

impl Engine for Sqlite {
    fn name<'s>(&self) -> &'s str {
        "SQLite"
    }

    fn exec(&mut self, sql: &str) {
        self.connection
            .execute(sql, [])
            .expect("sqlite runs the statement");
    }

    fn query(&mut self, sql: &str) -> Result<usize, String> {
        let mut statement = self
            .connection
            .prepare(sql)
            .map_err(|err| err.to_string())?;
        let count = statement
            .query_map([], |_| Ok(()))
            .map_err(|err| err.to_string())?
            .count();
        Ok(count)
    }

    fn write(&mut self, sql: &str) -> Result<(), String> {
        self.connection
            .execute(sql, [])
            .map(|_| ())
            .map_err(|err| err.to_string())
    }
}

impl Engine for Postgres {
    fn name<'s>(&self) -> &'s str {
        "Postgres"
    }

    fn exec(&mut self, sql: &str) {
        self.client
            .batch_execute(sql)
            .expect("postgres runs the statement");
    }

    fn query(&mut self, sql: &str) -> Result<usize, String> {
        self.client
            .query(sql, &[])
            .map(|rows| rows.len())
            .map_err(|err| err.to_string())
    }

    fn write(&mut self, sql: &str) -> Result<(), String> {
        self.client
            .execute(sql, &[])
            .map(|_| ())
            .map_err(|err| err.to_string())
    }
}
