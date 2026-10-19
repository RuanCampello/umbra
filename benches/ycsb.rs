//! YCSB core workloads (Cooper et al., SoCC 2010) against umbra, SQLite and PostgreSQL
//!
//! `umbra` runs in process like SQLite, `umbra (tcp)` behind its own server and wire protocol,
//! a round trip per statement like PostgreSQL: compare reads within each pair
//!
//! `UMBRA_YCSB_RECORDS`, `UMBRA_YCSB_SECONDS`, `UMBRA_YCSB_THREADS` (comma separated) and
//! `UMBRA_YCSB_ONLY` (workload letters) shrink or focus a run

use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use umbra::db::MvccDatabase;

const FIELDS: usize = 10;
const FIELD_LENGTH: usize = 100;
const OUTPUT: &str = "benches/ycsb-results.md";

struct Rng(u64);

/// YCSB's `ZipfianGenerator` (Gray et al., SIGMOD 1994) over `0..items`
struct Zipfian {
    items: u64,
    theta: f64,
    alpha: f64,
    zetan: f64,
    eta: f64,
}

#[derive(Clone, Copy)]
struct Workload {
    name: char,
    read: f64,
    update: f64,
    insert: f64,
    scan: f64,
    read_modify_write: f64,
    latest: bool,
}

struct Shared {
    records: u64,
    inserted: AtomicU64,
    zipfian: Zipfian,
}

#[derive(Clone)]
struct Cell {
    operations: u64,
    failures: u64,
    elapsed: Duration,
}

enum Operation {
    Read,
    Update,
    Insert,
    Scan,
    ReadModifyWrite,
}

trait Connection: Send {
    fn execute(&mut self, sql: &str) -> Result<(), String>;
    fn query(&mut self, sql: &str) -> Result<usize, String>;
}

trait Engine {
    fn name(&self) -> &'static str;
    fn connect(&self) -> Box<dyn Connection>;
}

struct Umbra {
    db: MvccDatabase,
    dir: std::path::PathBuf,
}

struct UmbraServer {
    port: u16,
}

struct Wire {
    stream: std::net::TcpStream,
}

struct Sqlite {
    path: std::path::PathBuf,
}

struct Postgres {
    target: String,
}

const WORKLOADS: [Workload; 6] = [
    Workload {
        name: 'A',
        read: 0.5,
        update: 0.5,
        insert: 0.0,
        scan: 0.0,
        read_modify_write: 0.0,
        latest: false,
    },
    Workload {
        name: 'B',
        read: 0.95,
        update: 0.05,
        insert: 0.0,
        scan: 0.0,
        read_modify_write: 0.0,
        latest: false,
    },
    Workload {
        name: 'C',
        read: 1.0,
        update: 0.0,
        insert: 0.0,
        scan: 0.0,
        read_modify_write: 0.0,
        latest: false,
    },
    Workload {
        name: 'D',
        read: 0.95,
        update: 0.0,
        insert: 0.05,
        scan: 0.0,
        read_modify_write: 0.0,
        latest: true,
    },
    Workload {
        name: 'E',
        read: 0.0,
        update: 0.0,
        insert: 0.05,
        scan: 0.95,
        read_modify_write: 0.0,
        latest: false,
    },
    Workload {
        name: 'F',
        read: 0.5,
        update: 0.0,
        insert: 0.0,
        scan: 0.0,
        read_modify_write: 0.5,
        latest: false,
    },
];

fn main() {
    let records = setting("UMBRA_YCSB_RECORDS", 50_000);
    let seconds = setting("UMBRA_YCSB_SECONDS", 2);
    // a desktop's background load moves single cells by tens of percent, for longer than a cell: the median of whole rounds
    let repeats = setting("UMBRA_YCSB_REPEAT", 1);
    let threads: Vec<_> = std::env::var("UMBRA_YCSB_THREADS")
        .map(|levels| levels.split(',').map(|level| level.trim().parse().unwrap()).collect())
        .unwrap_or_else(|_| vec![1, 4, 8]);
    let only = std::env::var("UMBRA_YCSB_ONLY").unwrap_or_else(|_| "ABCDEF".into());

    let mut engines: Vec<Box<dyn Engine>> = vec![
        Box::new(Umbra::setup()),
        Box::new(UmbraServer::setup()),
        Box::new(Sqlite::setup()),
    ];
    match Postgres::setup() {
        Some(postgres) => engines.push(Box::new(postgres)),
        _ => eprintln!("postgresql unreachable, comparing umbra and SQLite only"),
    }

    let mut table = String::new();
    let names: Vec<_> = engines.iter().map(|engine| engine.name()).collect();
    writeln!(table, "| workload | threads | {} |", names.join(" | ")).unwrap();
    writeln!(table, "|:--|--:|{}", "--:|".repeat(names.len())).unwrap();

    let workloads: Vec<_> =
        WORKLOADS.iter().filter(|workload| only.contains(workload.name)).collect();
    // cells[workload][level][engine]: one measurement per round, the rounds minutes apart
    let mut cells: Vec<Vec<Vec<Vec<_>>>> =
        vec![
            vec![(0..engines.len()).map(|_| Vec::new()).collect(); threads.len()];
            workloads.len()
        ];

    for round in 0..repeats {
        for (position, workload) in workloads.iter().enumerate() {
            for (column, engine) in engines.iter().enumerate() {
                eprintln!("round {round}: loading {records} records into {}", engine.name());
                load(engine.as_ref(), records);
                let shared = Arc::new(Shared {
                    records,
                    inserted: AtomicU64::new(records),
                    zipfian: Zipfian::new(records, 0.99),
                });

                for (level, &count) in threads.iter().enumerate() {
                    // dirty pages an earlier cell left would be written back during this one's syncs
                    let _ = std::process::Command::new("sync").status();
                    let duration = Duration::from_secs(seconds);
                    let cell = run(engine.as_ref(), **workload, &shared, count, duration);
                    let (load_name, engine_name) = (workload.name, engine.name());

                    eprintln!(
                        "  {load_name} {engine_name} @ {count}: {:.0} ops/s",
                        cell.throughput()
                    );
                    cells[position][level][column].push(cell);
                }
            }
        }
    }

    for (position, workload) in workloads.iter().enumerate() {
        for (level, engines) in cells[position].iter_mut().enumerate() {
            let row: Vec<_> = engines
                .iter_mut()
                .map(|rounds| {
                    rounds.sort_by(|a, b| a.throughput().total_cmp(&b.throughput()));
                    let cell = &rounds[rounds.len() / 2];
                    match cell.failures {
                        0 => format!("{:.0}", cell.throughput()),
                        failures => format!(
                            "{:.0} ({:.1}% failed)",
                            cell.throughput(),
                            100.0 * failures as f64 / (failures + cell.operations) as f64
                        ),
                    }
                })
                .collect();
            writeln!(table, "| {} | {} | {} |", workload.name, threads[level], row.join(" | "))
                .unwrap();
        }
    }

    let report = format!(
        "# YCSB\n\n{records} records of {FIELDS} fields × {FIELD_LENGTH} bytes, {seconds} s per cell, \
         successful operations per second (median of {repeats} rounds). Every engine durable (umbra group commit with fdatasync, \
         SQLite WAL `synchronous=FULL`, PostgreSQL defaults), same SQL text for all, zipfian θ = 0.99 \
         over hashed keys. Each workload starts from a fresh load.\n\n{table}"
    );
    std::fs::write(OUTPUT, &report).expect("the report is written");
    println!("{report}");
}

fn setting(name: &str, default: u64) -> u64 {
    std::env::var(name).map(|value| value.parse().unwrap()).unwrap_or(default)
}

fn load(engine: &dyn Engine, records: u64) {
    let mut connection = engine.connect();
    let _ = connection.execute("DROP TABLE usertable");
    let fields: Vec<_> = (0..FIELDS)
        .map(|field| format!("field{field} VARCHAR({FIELD_LENGTH}) NOT NULL"))
        .collect();

    let sql = format!(
        "CREATE TABLE usertable (ycsb_key VARCHAR(32) NOT NULL PRIMARY KEY, {})",
        fields.join(", ")
    );
    connection.execute(&sql).unwrap();

    let mut rng = Rng(7);
    for batch in (0..records).collect::<Vec<_>>().chunks(500) {
        let rows: Vec<_> = batch.iter().map(|&number| row(number, &mut rng)).collect();
        connection.execute("BEGIN TRANSACTION").unwrap();
        let sql = format!("INSERT INTO usertable VALUES {}", rows.join(", "));
        connection.execute(&sql).unwrap();
        connection.execute("COMMIT").unwrap();
    }
}

fn run(
    engine: &dyn Engine,
    workload: Workload,
    shared: &Arc<Shared>,
    threads: usize,
    duration: Duration,
) -> Cell {
    let barrier = Arc::new(Barrier::new(threads + 1));
    let stop = Arc::new(AtomicBool::new(false));

    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..threads)
            .map(|thread| {
                let mut connection = engine.connect();
                let (barrier, stop, shared) =
                    (Arc::clone(&barrier), Arc::clone(&stop), Arc::clone(shared));

                scope.spawn(move || {
                    let mut rng = Rng(0x9e37_79b9 + thread as u64);
                    let (mut operations, mut failures) = (0, 0);
                    barrier.wait();

                    while !stop.load(Ordering::Relaxed) {
                        match operate(connection.as_mut(), workload, &shared, &mut rng) {
                            Ok(()) => operations += 1,
                            Err(_) => failures += 1,
                        }
                    }

                    (operations, failures)
                })
            })
            .collect();

        barrier.wait();
        let start = Instant::now();
        std::thread::sleep(duration);
        stop.store(true, Ordering::Relaxed);

        let (operations, failures) = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .fold((0, 0), |(operations, failures), (o, f)| (operations + o, failures + f));

        Cell { operations, failures, elapsed: start.elapsed() }
    })
}

fn operate(
    connection: &mut dyn Connection,
    workload: Workload,
    shared: &Shared,
    rng: &mut Rng,
) -> Result<(), String> {
    let key = |rng: &mut Rng| {
        let inserted = shared.inserted.load(Ordering::Relaxed);
        let number = match workload.latest {
            true => inserted - 1 - shared.zipfian.next(rng).min(inserted - 1),
            false => fnv(shared.zipfian.next(rng)) % shared.records,
        };
        format!("user{}", fnv(number))
    };

    match workload.choose(rng) {
        Operation::Read => connection
            .query(&format!("SELECT * FROM usertable WHERE ycsb_key = '{}'", key(rng)))
            .map(drop),
        Operation::Update => {
            let field = rng.below(FIELDS as u64);
            let value = text(rng);
            connection.execute(&format!(
                "UPDATE usertable SET field{field} = '{value}' WHERE ycsb_key = '{}'",
                key(rng)
            ))
        },
        Operation::Insert => {
            let number = shared.inserted.fetch_add(1, Ordering::Relaxed);
            connection.execute(&format!("INSERT INTO usertable VALUES {}", row(number, rng)))
        },
        Operation::Scan => {
            let length = 1 + rng.below(100);
            let sql = format!(
                "SELECT * FROM usertable WHERE ycsb_key >= '{}' ORDER BY ycsb_key LIMIT {length}",
                key(rng)
            );
            connection.query(&sql).map(drop)
        },
        Operation::ReadModifyWrite => {
            let key = key(rng);
            let field = rng.below(FIELDS as u64);
            let value = text(rng);
            connection.execute("BEGIN TRANSACTION")?;
            let result = connection
                .query(&format!("SELECT * FROM usertable WHERE ycsb_key = '{key}'"))
                .and_then(|_| {
                    connection.execute(&format!(
                        "UPDATE usertable SET field{field} = '{value}' WHERE ycsb_key = '{key}'"
                    ))
                })
                .and_then(|_| connection.execute("COMMIT"));
            if result.is_err() {
                let _ = connection.execute("ROLLBACK");
            }
            result
        },
    }
}

fn row(number: u64, rng: &mut Rng) -> String {
    let fields: Vec<_> = (0..FIELDS).map(|_| format!("'{}'", text(rng))).collect();
    format!("('user{}', {})", fnv(number), fields.join(", "))
}

fn text(rng: &mut Rng) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    (0..FIELD_LENGTH)
        .map(|_| ALPHABET[rng.below(ALPHABET.len() as u64) as usize] as char)
        .collect()
}

fn fnv(value: u64) -> u64 {
    value.to_le_bytes().iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ *byte as u64).wrapping_mul(0x100_0000_01b3)
    })
}

impl Cell {
    fn throughput(&self) -> f64 {
        self.operations as f64 / self.elapsed.as_secs_f64()
    }
}

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x2d35_8dcc_aa6c_78a5);
        let product = u128::from(self.0) * u128::from(self.0 ^ 0x8bb8_4b93_962e_acc9);
        (product as u64) ^ (product >> 64) as u64
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

impl Zipfian {
    fn new(items: u64, theta: f64) -> Self {
        let zeta = |n: u64| (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum::<f64>();
        let zetan = zeta(items);
        let eta = (1.0 - (2.0 / items as f64).powf(1.0 - theta)) / (1.0 - zeta(2) / zetan);
        Self { items, theta, alpha: 1.0 / (1.0 - theta), zetan, eta }
    }

    fn next(&self, rng: &mut Rng) -> u64 {
        let u = rng.unit();
        let uz = u * self.zetan;
        match uz {
            uz if uz < 1.0 => 0,
            uz if uz < 1.0 + 0.5f64.powf(self.theta) => 1,
            _ => ((self.items as f64) * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as u64,
        }
    }
}

impl Workload {
    fn choose(&self, rng: &mut Rng) -> Operation {
        let mut point = rng.unit();
        for (share, operation) in [
            (self.read, Operation::Read),
            (self.update, Operation::Update),
            (self.insert, Operation::Insert),
            (self.scan, Operation::Scan),
            (self.read_modify_write, Operation::ReadModifyWrite),
        ] {
            match point < share {
                true => return operation,
                false => point -= share,
            }
        }
        Operation::Read
    }
}

impl Umbra {
    fn setup() -> Self {
        let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("umbra-ycsb");
        let _ = std::fs::remove_dir_all(&dir);
        Self { db: MvccDatabase::init(&dir).unwrap(), dir }
    }
}

impl UmbraServer {
    fn setup() -> Self {
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("umbra-ycsb-server");
        let _ = std::fs::remove_dir_all(&dir);

        let address = std::net::SocketAddr::from(([127, 0, 0, 1], port));
        std::thread::spawn(move || umbra::tcp::server::start(address, dir).unwrap());
        Self { port }
    }
}

impl Sqlite {
    fn setup() -> Self {
        let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("umbra-ycsb.sqlite");
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
        Self { path }
    }
}

impl Postgres {
    fn setup() -> Option<Self> {
        let target = std::env::var("UMBRA_BENCH_PG")
            .unwrap_or_else(|_| "host=/run/postgresql user=postgres dbname=postgres".into());
        postgres::Client::connect(&target, postgres::NoTls).ok()?;
        Some(Self { target })
    }
}

impl Engine for Umbra {
    fn name(&self) -> &'static str {
        "umbra"
    }

    fn connect(&self) -> Box<dyn Connection> {
        Box::new(self.db.session())
    }
}

impl Engine for UmbraServer {
    fn name(&self) -> &'static str {
        "umbra (tcp)"
    }

    fn connect(&self) -> Box<dyn Connection> {
        for _ in 0..250 {
            if let Ok(stream) = std::net::TcpStream::connect(("127.0.0.1", self.port)) {
                stream.set_nodelay(true).unwrap();
                return Box::new(Wire { stream });
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!("the umbra server did not start")
    }
}

impl Engine for Sqlite {
    fn name(&self) -> &'static str {
        "SQLite"
    }

    fn connect(&self) -> Box<dyn Connection> {
        let connection = rusqlite::Connection::open(&self.path).unwrap();
        connection.busy_timeout(Duration::from_secs(5)).unwrap();
        connection
            .execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL;")
            .unwrap();
        Box::new(connection)
    }
}

impl Engine for Postgres {
    fn name(&self) -> &'static str {
        "PostgreSQL"
    }

    fn connect(&self) -> Box<dyn Connection> {
        Box::new(postgres::Client::connect(&self.target, postgres::NoTls).unwrap())
    }
}

impl Connection for MvccDatabase {
    fn execute(&mut self, sql: &str) -> Result<(), String> {
        self.exec(&format!("{sql};")).map(drop).map_err(|error| error.to_string())
    }

    fn query(&mut self, sql: &str) -> Result<usize, String> {
        self.exec(&format!("{sql};"))
            .map(|set| set.tuples.len())
            .map_err(|error| error.to_string())
    }
}

impl Wire {
    fn send(&mut self, sql: &str) -> Result<umbra::tcp::Response, String> {
        use std::io::{Read, Write};

        let sql = format!("{sql};");
        let mut exchange = || -> std::io::Result<Vec<u8>> {
            self.stream.write_all(&(sql.len() as u32).to_le_bytes())?;
            self.stream.write_all(sql.as_bytes())?;
            let mut len = [0u8; 4];
            self.stream.read_exact(&mut len)?;
            let mut content = vec![0u8; u32::from_le_bytes(len) as usize];
            self.stream.read_exact(&mut content)?;
            Ok(content)
        };

        let content = exchange().map_err(|error| error.to_string())?;
        match umbra::tcp::deserialize(&content).map_err(|error| format!("{error:?}"))? {
            umbra::tcp::Response::Err(message) => Err(message),
            response => Ok(response),
        }
    }
}

impl Connection for Wire {
    fn execute(&mut self, sql: &str) -> Result<(), String> {
        self.send(sql).map(drop)
    }

    fn query(&mut self, sql: &str) -> Result<usize, String> {
        match self.send(sql)? {
            umbra::tcp::Response::QuerySet(set) => Ok(set.tuples.len()),
            _ => Ok(0),
        }
    }
}

impl Connection for rusqlite::Connection {
    fn execute(&mut self, sql: &str) -> Result<(), String> {
        rusqlite::Connection::execute(self, sql, [])
            .map(drop)
            .map_err(|error| error.to_string())
    }

    fn query(&mut self, sql: &str) -> Result<usize, String> {
        let mut statement = self.prepare(sql).map_err(|error| error.to_string())?;
        let columns = statement.column_count();
        let rows = statement
            .query_map([], |row| {
                for column in 0..columns {
                    std::hint::black_box(row.get_ref(column)?);
                }
                Ok(())
            })
            .map_err(|error| error.to_string())?;
        Ok(rows.count())
    }
}

impl Connection for postgres::Client {
    fn execute(&mut self, sql: &str) -> Result<(), String> {
        self.batch_execute(sql).map_err(|error| error.to_string())
    }

    fn query(&mut self, sql: &str) -> Result<usize, String> {
        self.simple_query(sql)
            .map(|messages| messages.len())
            .map_err(|error| error.to_string())
    }
}

impl Drop for Umbra {
    fn drop(&mut self) {
        let _ = self.db.close();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
