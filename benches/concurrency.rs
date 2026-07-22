#[path = "comparison/dataset.rs"]
mod dataset;

use rusqlite::{Connection, OpenFlags};
use std::fmt::Write as _;
use std::fs;
use std::time::{Duration, Instant};
use umbra::db::MvccDatabase;

const OPS_PER_WORKER: usize = 2000;
const READ_RATIO: f64 = 0.8;
const THREAD_LEVELS: [usize; 4] = [1, 2, 4, 8];
const OUTPUT: &str = "benches/comparison-results.md";
const SECTION_HEADING: &str = "# Umbra concurrency benchmark";
const SQLITE_URI: &str = "file:umbra_concurrency?mode=memory&cache=shared";

struct Rng {
    state: u64,
}

struct WorkerResult {
    latencies: Vec<f64>,
    errors: usize,
}

struct Measurement {
    engine: &'static str,
    threads: usize,
    throughput: f64,
    p50: f64,
    p99: f64,
    error_rate: f64,
}

struct UmbraWorker {
    session: MvccDatabase,
}

struct SqliteWorker {
    connection: Connection,
}

struct PostgresWorker {
    client: postgres::Client,
}

struct UmbraCluster {
    owner: MvccDatabase,
}

struct SqliteCluster {
    _keeper: Connection,
}

struct PostgresCluster {
    target: String,
}

trait Worker: Send {
    fn read(&mut self, id: usize) -> bool;
    fn write(&mut self, id: usize) -> bool;
}

trait Cluster {
    fn name(&self) -> &'static str;
    fn worker(&self) -> Box<dyn Worker>;
}

fn main() {
    let users = dataset::users();
    eprintln!("loading {users} users into each engine for the concurrency run...");

    let mut clusters: Vec<Box<dyn Cluster>> = vec![
        Box::new(UmbraCluster::setup()),
        Box::new(SqliteCluster::setup()),
    ];
    match PostgresCluster::setup() {
        Some(postgres) => clusters.push(Box::new(postgres)),
        None => eprintln!("postgres unreachable, comparing umbra vs SQLite only"),
    }

    let mut measurements = Vec::new();
    for cluster in &clusters {
        for threads in THREAD_LEVELS {
            eprintln!("  {} @ {threads} threads", cluster.name());
            measurements.push(run_level(cluster.as_ref(), threads, users));
        }
    }

    let section = render(&measurements);
    publish(&section);
    eprintln!("\nconcurrency section appended to {OUTPUT}\n");
    println!("{section}");
}

fn publish(section: &str) {
    let existing = fs::read_to_string(OUTPUT).unwrap_or_default();
    let mut head = match existing.find(SECTION_HEADING) {
        Some(at) => existing[..at].trim_end(),
        None => existing.trim_end(),
    };
    head = head.strip_suffix("---").unwrap_or(head).trim_end();

    let mut out = String::with_capacity(head.len() + section.len() + 16);
    if !head.is_empty() {
        out.push_str(head);
        out.push_str("\n\n---\n\n");
    }
    out.push_str(section);
    fs::write(OUTPUT, out).expect("the concurrency section is appended to the comparison report");
}

fn run_level(cluster: &dyn Cluster, threads: usize, users: usize) -> Measurement {
    let workers: Vec<Box<dyn Worker>> = (0..threads).map(|_| cluster.worker()).collect();

    let start = Instant::now();
    let handles: Vec<_> = workers
        .into_iter()
        .enumerate()
        .map(|(worker_id, mut worker)| {
            std::thread::spawn(move || run_worker(worker.as_mut(), worker_id as u64, users))
        })
        .collect();

    let mut latencies = Vec::with_capacity(threads * OPS_PER_WORKER);
    let mut errors = 0;
    for handle in handles {
        let result = handle.join().expect("a worker thread never panics");
        latencies.extend(result.latencies);
        errors += result.errors;
    }
    let elapsed = start.elapsed();

    let total = threads * OPS_PER_WORKER;
    let succeeded = total - errors;
    latencies.sort_by(|a, b| a.partial_cmp(b).expect("latencies are never NaN"));

    Measurement {
        engine: cluster.name(),
        threads,
        throughput: succeeded as f64 / elapsed.as_secs_f64(),
        p50: percentile(&latencies, 0.50),
        p99: percentile(&latencies, 0.99),
        error_rate: errors as f64 / total as f64,
    }
}

fn run_worker(worker: &mut dyn Worker, seed: u64, users: usize) -> WorkerResult {
    let mut rng = Rng::new(0x51D5_7EED ^ seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let hot = hot_set(users);

    let mut latencies = Vec::with_capacity(OPS_PER_WORKER);
    let mut errors = 0;
    for _ in 0..OPS_PER_WORKER {
        let read = rng.probability() < READ_RATIO;
        let start = Instant::now();
        // writes hit a hot set so write-write concurrency control is exercised
        let ok = match read {
            true => worker.read(rng.range(1, users as i64 + 1) as usize),
            false => worker.write(rng.range(1, hot as i64 + 1) as usize),
        };
        latencies.push(start.elapsed().as_nanos() as f64 / 1_000.0);
        if !ok {
            errors += 1;
        }
    }

    WorkerResult { latencies, errors }
}

fn hot_set(users: usize) -> usize {
    users.min(256)
}

fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (quantile * (sorted.len() - 1) as f64).round() as usize;
    sorted[rank]
}

fn render(measurements: &[Measurement]) -> String {
    let mut out = String::new();
    out.push_str("# Umbra concurrency benchmark\n\n");
    let reads = READ_RATIO * 100.0;
    let writes = (1.0 - READ_RATIO) * 100.0;
    let hot = hot_set(dataset::users());
    let spread = dataset::users();
    out.push_str(&format!(
        r#"{OPS_PER_WORKER} operations per worker, {reads:.0}% reads / {writes:.0}% writes, writes concentrated on a {hot}-row hot set to force write-write contention. Reads spread over {spread} rows. Throughput counts only successful operations ÷ wall time; latency percentiles include every attempt.
        Concurrency-control notes (read these before trusting the numbers):
        - **Umbra** and **Postgres** are MVCC: readers take a consistent snapshot and never block, writers race optimistically, a write-write conflict aborts and is counted as a write error, not retried.
        - **SQLite** has no MVCC, in shared-cache memory it serialises writers behind the database lock (`busy_timeout` 5s). It runs with `read_uncommitted` so readers do not block on the writer, a weaker guarantee (dirty reads) than the snapshot isolation umbra and Postgres give, and one that works in SQLite's favour here.
        - Umbra & SQLite are in memory, Postgres is the local on-disk server (a round-trip per call), so Postgres's absolute throughput is not comparable to the embedded engines', read it for scaling, not level.
    "#));

    out.push_str(
        "| Engine | Threads | Throughput (ops/s) | p50 (µs) | p99 (µs) | Write errors |\n",
    );
    out.push_str("|:-------|-------:|-------------------:|--------:|--------:|-------------:|\n");
    for m in measurements {
        let _ = writeln!(
            out,
            "| {} | {} | {:.0} | {:.2} | {:.2} | {:.1}% |",
            m.engine,
            m.threads,
            m.throughput,
            m.p50,
            m.p99,
            m.error_rate * 100.0,
        );
    }

    out.push_str("\n## Scaling (throughput vs single thread)\n\n");
    out.push_str("| Engine |");
    for threads in THREAD_LEVELS {
        let _ = write!(out, " {threads}× |");
    }
    out.push_str("\n|:-------|");
    for _ in THREAD_LEVELS {
        out.push_str("------:|");
    }
    out.push('\n');
    for engine in ["Umbra", "SQLite", "Postgres"] {
        let base = measurements
            .iter()
            .find(|m| m.engine == engine && m.threads == THREAD_LEVELS[0]);
        let Some(base) = base else {
            continue;
        };
        let _ = write!(out, "| {engine} |");
        for threads in THREAD_LEVELS {
            match measurements
                .iter()
                .find(|m| m.engine == engine && m.threads == threads)
            {
                Some(m) => {
                    let _ = write!(out, " {:.2}× |", m.throughput / base.throughput);
                }
                None => out.push_str(" — |"),
            }
        }
        out.push('\n');
    }

    out
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
        low + (self.next_u64() % (high - low) as u64) as i64
    }

    fn probability(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

impl UmbraCluster {
    fn setup() -> Self {
        let mut owner = MvccDatabase::in_memory().expect("umbra opens in memory");
        owner
            .exec(&format!("{};", dataset::SCHEMA[0]))
            .expect("users schema");
        for insert in dataset::user_inserts() {
            owner.exec(&format!("{insert};")).expect("users load");
        }
        Self { owner }
    }
}

impl SqliteCluster {
    fn setup() -> Self {
        let keeper = sqlite_connection();
        keeper
            .execute(dataset::SCHEMA[0], [])
            .expect("users schema");
        for insert in dataset::user_inserts() {
            keeper.execute(&insert, []).expect("users load");
        }
        Self { _keeper: keeper }
    }
}

impl PostgresCluster {
    fn setup() -> Option<Self> {
        let target = std::env::var("UMBRA_BENCH_PG")
            .unwrap_or_else(|_| "host=/run/postgresql user=postgres dbname=postgres".into());

        let mut client = postgres::Client::connect(&target, postgres::NoTls).ok()?;
        client
            .batch_execute("DROP TABLE IF EXISTS users, orders, records")
            .expect("postgres resets its schema");
        client
            .batch_execute(dataset::SCHEMA[0])
            .expect("users schema");
        for insert in dataset::user_inserts() {
            client.batch_execute(&insert).expect("users load");
        }

        Some(Self { target })
    }
}

impl Cluster for UmbraCluster {
    fn name(&self) -> &'static str {
        "Umbra"
    }

    fn worker(&self) -> Box<dyn Worker> {
        Box::new(UmbraWorker {
            session: self.owner.session(),
        })
    }
}

impl Cluster for SqliteCluster {
    fn name(&self) -> &'static str {
        "SQLite"
    }

    fn worker(&self) -> Box<dyn Worker> {
        Box::new(SqliteWorker {
            connection: sqlite_connection(),
        })
    }
}

impl Cluster for PostgresCluster {
    fn name(&self) -> &'static str {
        "Postgres"
    }

    fn worker(&self) -> Box<dyn Worker> {
        let client = postgres::Client::connect(&self.target, postgres::NoTls)
            .expect("postgres accepts another connection");
        Box::new(PostgresWorker { client })
    }
}

impl Worker for UmbraWorker {
    fn read(&mut self, id: usize) -> bool {
        self.session
            .exec(&format!("SELECT balance FROM users WHERE id = {id};"))
            .is_ok()
    }

    fn write(&mut self, id: usize) -> bool {
        self.session
            .exec(&format!(
                "UPDATE users SET balance = 100.0 WHERE id = {id};"
            ))
            .is_ok()
    }
}

impl Worker for SqliteWorker {
    fn read(&mut self, id: usize) -> bool {
        self.connection
            .query_row("SELECT balance FROM users WHERE id = ?1", [id], |_| Ok(()))
            .is_ok()
    }

    fn write(&mut self, id: usize) -> bool {
        self.connection
            .execute("UPDATE users SET balance = 100.0 WHERE id = ?1", [id])
            .is_ok()
    }
}

impl Worker for PostgresWorker {
    fn read(&mut self, id: usize) -> bool {
        self.client
            .query("SELECT balance FROM users WHERE id = $1", &[&(id as i32)])
            .is_ok()
    }

    fn write(&mut self, id: usize) -> bool {
        self.client
            .execute(
                "UPDATE users SET balance = 100.0 WHERE id = $1",
                &[&(id as i32)],
            )
            .is_ok()
    }
}

fn sqlite_connection() -> Connection {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_SHARED_CACHE;
    let connection =
        Connection::open_with_flags(SQLITE_URI, flags).expect("sqlite shared-cache connection");
    connection
        .busy_timeout(Duration::from_secs(5))
        .expect("busy timeout");
    connection
        .pragma_update(None, "read_uncommitted", true)
        .expect("shared-cache readers do not block on the writer");
    connection
}
