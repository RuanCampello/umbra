//! TPC-C consistency (TPC-C v5.11, clause 3.3.2) after concurrent New-Order, Payment and Delivery
//! transactions, in process at every isolation level and on the server binary killed mid-run
//!
//! Reduced scale and schema: no items or stock, amounts in cents. Primary keys fold the composite
//! key into one integer (there is no composite PRIMARY KEY). The transactions find orders through
//! composite unique indexes, as TPC-C's access paths are. The database starts empty, so
//! condition 11 (which counts the 2,100 orders of the initial population) does not apply, the other eleven are checked

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use umbra::db::MvccDatabase;
use umbra::sql::statement::Value;
use umbra::tcp::{self, Response};

const WAREHOUSES: i64 = 3;
const DISTRICTS: i64 = 10;
const CUSTOMERS: i64 = 20;

trait Session: Send {
    fn run(&mut self, sql: &str) -> Result<Vec<Vec<Value>>, String>;
}

struct Rng(u64);

struct Client {
    stream: TcpStream,
}

struct Server {
    child: std::process::Child,
    port: u16,
    dir: PathBuf,
}

struct Totals {
    warehouse_ytd: Vec<(i64, i64)>,
    districts: Vec<Vec<i64>>,
    customers: Vec<Vec<i64>>,
    orders: Vec<Vec<Option<i64>>>,
    new_orders: Vec<Vec<i64>>,
    lines: Vec<Vec<Option<i64>>>,
    history: Vec<Vec<i64>>,
}

#[test]
fn consistent_after_concurrent_transactions() {
    for level in ["READ COMMITTED", "SNAPSHOT", "SERIALIZABLE"] {
        let db = MvccDatabase::in_memory().unwrap();
        populate(&mut db.session());

        let stop = Arc::new(AtomicBool::new(false));
        std::thread::scope(|scope| {
            for worker in 0..6 {
                let mut session = db.session();
                let stop = Arc::clone(&stop);
                scope.spawn(move || drive(&mut session, level, worker, 120, &stop));
            }
        });

        check(&mut db.session(), level);
    }
}

#[test]
fn consistent_after_the_server_is_killed() {
    let port = free_port();
    let dir = std::env::temp_dir().join(format!("umbra-tpcc-{port}"));
    let _ = std::fs::remove_dir_all(&dir);

    let mut server = Server::start(&dir, port);
    populate(&mut server.client());

    let stop = Arc::new(AtomicBool::new(false));
    std::thread::scope(|scope| {
        for worker in 0..6 {
            let mut client = server.client();
            let stop = Arc::clone(&stop);
            scope.spawn(move || drive(&mut client, "SNAPSHOT", worker, usize::MAX, &stop));
        }

        std::thread::sleep(Duration::from_millis(1500));
        server.kill();
        stop.store(true, Ordering::Relaxed);
    });

    let server = Server::start(&dir, port);
    check(&mut server.client(), "after SIGKILL");
}

fn populate(db: &mut dyn Session) {
    for ddl in [
        "CREATE TABLE warehouse (w_id INT PRIMARY KEY, w_ytd BIGINT)",
        "CREATE TABLE district (d_id INT PRIMARY KEY, d_w_id INT, d_ytd BIGINT, d_next_o_id INT)",
        "CREATE TABLE customer (c_id INT PRIMARY KEY, c_d_id INT, c_balance BIGINT, c_ytd_payment BIGINT)",
        "CREATE TABLE orders (o_key BIGINT PRIMARY KEY, o_id INT, o_d_id INT, o_c_id INT, o_carrier_id INT NULLABLE, o_ol_cnt INT)",
        "CREATE UNIQUE INDEX orders_district ON orders (o_d_id, o_id)",
        "CREATE TABLE new_order (no_key BIGINT PRIMARY KEY, no_o_id INT, no_d_id INT)",
        "CREATE UNIQUE INDEX new_order_district ON new_order (no_d_id, no_o_id)",
        "CREATE TABLE order_line (ol_key BIGINT PRIMARY KEY, ol_o_key BIGINT, ol_d_id INT, ol_amount INT, ol_delivery_d INT NULLABLE)",
        "CREATE INDEX order_line_order ON order_line(ol_o_key)",
        "CREATE TABLE history (h_id SERIAL PRIMARY KEY, h_c_id INT, h_d_id INT, h_w_id INT, h_amount INT)",
    ] {
        db.run(ddl).unwrap();
    }

    for warehouse in 1..=WAREHOUSES {
        db.run(&format!("INSERT INTO warehouse VALUES ({warehouse}, 0)")).unwrap();
        for district in districts(warehouse) {
            db.run(&format!("INSERT INTO district VALUES ({district}, {warehouse}, 0, 1)"))
                .unwrap();
            let customers: Vec<_> = customers(district)
                .map(|customer| format!("({customer}, {district}, 0, 0)"))
                .collect();
            db.run(&format!("INSERT INTO customer VALUES {}", customers.join(", ")))
                .unwrap();
        }
    }
}

/// runs the TPC-C mix until `count` transactions committed or `stop` is set, retrying any that fails
fn drive(db: &mut dyn Session, level: &str, worker: u64, count: usize, stop: &AtomicBool) {
    let mut rng = Rng(0x5eed + worker);
    let mut committed = 0;

    while committed < count && !stop.load(Ordering::Relaxed) {
        let warehouse = 1 + rng.below(WAREHOUSES as u64) as i64;
        let district = warehouse * 100 + rng.below(DISTRICTS as u64) as i64;
        let customer = district * 1000 + rng.below(CUSTOMERS as u64) as i64;

        let outcome =
            db.run(&format!("START TRANSACTION ISOLATION LEVEL {level}")).and_then(|_| {
                match rng.below(100) {
                    0..45 => new_order(db, district, customer, 1 + rng.below(10) as i64, &mut rng),
                    45..88 => {
                        payment(db, warehouse, district, customer, 1 + rng.below(5000) as i64)
                    },
                    _ => delivery(db, warehouse, 1 + rng.below(10) as i64),
                }
                .and_then(|_| db.run("COMMIT"))
            });

        match outcome {
            Ok(_) => committed += 1,
            _ => {
                let _ = db.run("ROLLBACK");
            },
        }
    }
}

fn new_order(
    db: &mut dyn Session,
    district: i64,
    customer: i64,
    lines: i64,
    rng: &mut Rng,
) -> Result<(), String> {
    let next = db.run(&format!("SELECT d_next_o_id FROM district WHERE d_id = {district}"))?;
    let order = number(&next[0][0]);
    db.run(&format!(
        "UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_id = {district}"
    ))?;

    let key = district * 100_000 + order;
    db.run(&format!(
        "INSERT INTO orders VALUES ({key}, {order}, {district}, {customer}, NULL, {lines})"
    ))?;
    db.run(&format!("INSERT INTO new_order VALUES ({key}, {order}, {district})"))?;

    let rows: Vec<_> = (0..lines)
        .map(|line| {
            format!("({}, {key}, {district}, {}, NULL)", key * 16 + line, 1 + rng.below(10_000))
        })
        .collect();
    db.run(&format!("INSERT INTO order_line VALUES {}", rows.join(", "))).map(drop)
}

fn payment(
    db: &mut dyn Session,
    warehouse: i64,
    district: i64,
    customer: i64,
    amount: i64,
) -> Result<(), String> {
    db.run(&format!(
        "UPDATE warehouse SET w_ytd = w_ytd + {amount} WHERE w_id = {warehouse}"
    ))?;
    db.run(&format!("UPDATE district SET d_ytd = d_ytd + {amount} WHERE d_id = {district}"))?;
    db.run(&format!(
        "UPDATE customer SET c_balance = c_balance - {amount}, c_ytd_payment = c_ytd_payment + {amount} WHERE c_id = {customer}"
    ))?;
    db.run(&format!(
        "INSERT INTO history (h_c_id, h_d_id, h_w_id, h_amount) VALUES ({customer}, {district}, {warehouse}, {amount})"
    ))
    .map(drop)
}

/// delivers the oldest new order of every district of `warehouse`
fn delivery(db: &mut dyn Session, warehouse: i64, carrier: i64) -> Result<(), String> {
    for district in districts(warehouse) {
        let oldest = db.run(&format!(
            "SELECT no_o_id FROM new_order WHERE no_d_id = {district} ORDER BY no_o_id LIMIT 1"
        ))?;
        let Some(row) = oldest.first() else {
            continue;
        };
        let order = number(&row[0]);
        let key = district * 100_000 + order;

        // under READ COMMITTED a concurrent delivery may have taken it: the delete then touches nothing
        let deleted = db.run(&format!(
            "DELETE FROM new_order WHERE no_d_id = {district} AND no_o_id = {order}"
        ))?;
        if number(&deleted[0][0]) != 1 {
            return Err("the order was delivered concurrently".into());
        }

        let found = db.run(&format!(
            "SELECT o_c_id FROM orders WHERE o_d_id = {district} AND o_id = {order}"
        ))?;
        let customer = number(&found[0][0]);
        db.run(&format!("UPDATE orders SET o_carrier_id = {carrier} WHERE o_d_id = {district} AND o_id = {order}"))?;
        db.run(&format!("UPDATE order_line SET ol_delivery_d = 1 WHERE ol_o_key = {key}"))?;
        let total = number(
            &db.run(&format!("SELECT SUM(ol_amount) FROM order_line WHERE ol_o_key = {key}"))?[0]
                [0],
        );
        db.run(&format!(
            "UPDATE customer SET c_balance = c_balance + {total} WHERE c_id = {customer}"
        ))?;
    }

    Ok(())
}

fn check(db: &mut dyn Session, label: &str) {
    let totals = Totals::read(db);
    let sum = |rows: &[Vec<i64>], key: usize, value: usize, wanted: i64| -> i64 {
        rows.iter().filter(|row| row[key] == wanted).map(|row| row[value]).sum()
    };

    for &(warehouse, ytd) in &totals.warehouse_ytd {
        let districts: i64 =
            totals.districts.iter().filter(|d| d[1] == warehouse).map(|d| d[2]).sum();
        assert_eq!(ytd, districts, "{label}: condition 1, warehouse {warehouse}");
        assert_eq!(
            ytd,
            sum(&totals.history, 2, 3, warehouse),
            "{label}: condition 8, warehouse {warehouse}"
        );
    }

    for district in &totals.districts {
        let (id, ytd, next) = (district[0], district[2], district[3]);
        let orders: Vec<_> = totals.orders.iter().filter(|order| order[2] == Some(id)).collect();
        let pending: Vec<_> =
            totals.new_orders.iter().filter(|row| row[2] == id).map(|row| row[1]).collect();

        let highest = orders.iter().filter_map(|order| order[1]).max().unwrap_or(0);
        assert_eq!(next - 1, highest, "{label}: condition 2, district {id}");
        if let (Some(&low), Some(&high)) = (pending.iter().min(), pending.iter().max()) {
            assert_eq!(high, highest, "{label}: condition 2, district {id}");
            assert_eq!(high - low + 1, pending.len() as i64, "{label}: condition 3, district {id}");
        }

        let ordered: i64 = orders.iter().filter_map(|order| order[5]).sum();
        let lines = totals.lines.iter().filter(|line| line[2] == Some(id)).count() as i64;
        assert_eq!(ordered, lines, "{label}: condition 4, district {id}");
        assert_eq!(ytd, sum(&totals.history, 1, 3, id), "{label}: condition 9, district {id}");
    }

    for order in &totals.orders {
        let key = order[0].unwrap();
        let undelivered = totals.new_orders.iter().any(|row| row[0] == key);
        assert_eq!(order[4].is_none(), undelivered, "{label}: condition 5, order {key}");

        let lines: Vec<_> = totals.lines.iter().filter(|line| line[1] == Some(key)).collect();
        assert_eq!(order[5], Some(lines.len() as i64), "{label}: condition 6, order {key}");
        for line in lines {
            assert_eq!(line[4].is_none(), order[4].is_none(), "{label}: condition 7, order {key}");
        }
    }

    for customer in &totals.customers {
        let (id, balance, paid) = (customer[0], customer[2], customer[3]);
        let delivered: i64 = totals
            .orders
            .iter()
            .filter(|order| order[3] == Some(id) && order[4].is_some())
            .flat_map(|order| totals.lines.iter().filter(move |line| line[1] == order[0]))
            .map(|line| line[3].unwrap())
            .sum();

        assert_eq!(
            balance,
            delivered - sum(&totals.history, 0, 3, id),
            "{label}: condition 10, customer {id}"
        );
        assert_eq!(balance + paid, delivered, "{label}: condition 12, customer {id}");
    }

    assert!(
        !totals.orders.is_empty() && !totals.history.is_empty(),
        "{label}: the run committed nothing"
    );
}

impl Totals {
    fn read(db: &mut dyn Session) -> Self {
        let mut all = |sql: &str| -> Vec<Vec<Option<i64>>> {
            let rows = db.run(sql).unwrap();
            rows.iter().map(|row| row.iter().map(optional).collect()).collect()
        };
        let strict = |rows: Vec<Vec<Option<i64>>>| -> Vec<Vec<i64>> {
            rows.into_iter()
                .map(|row| row.into_iter().map(Option::unwrap).collect())
                .collect()
        };

        Self {
            warehouse_ytd: strict(all("SELECT w_id, w_ytd FROM warehouse"))
                .into_iter()
                .map(|row| (row[0], row[1]))
                .collect(),
            districts: strict(all("SELECT d_id, d_w_id, d_ytd, d_next_o_id FROM district")),
            customers: strict(all("SELECT c_id, c_d_id, c_balance, c_ytd_payment FROM customer")),
            orders: all("SELECT o_key, o_id, o_d_id, o_c_id, o_carrier_id, o_ol_cnt FROM orders"),
            new_orders: strict(all("SELECT no_key, no_o_id, no_d_id FROM new_order")),
            lines: all(
                "SELECT ol_key, ol_o_key, ol_d_id, ol_amount, ol_delivery_d FROM order_line",
            ),
            history: strict(all("SELECT h_c_id, h_d_id, h_w_id, h_amount FROM history")),
        }
    }
}

fn districts(warehouse: i64) -> impl Iterator<Item = i64> {
    (0..DISTRICTS).map(move |district| warehouse * 100 + district)
}

fn customers(district: i64) -> impl Iterator<Item = i64> {
    (0..CUSTOMERS).map(move |customer| district * 1000 + customer)
}

fn number(value: &Value) -> i64 {
    optional(value).expect("a number")
}

fn optional(value: &Value) -> Option<i64> {
    match value {
        Value::Number(n) => Some(*n),
        Value::Numeric(n) => Some(n.to_string().parse().expect("an integral numeric")),
        Value::Null => None,
        other => panic!("not an integer: {other:?}"),
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

impl Rng {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 = self.0.wrapping_add(0x2d35_8dcc_aa6c_78a5);
        let product = u128::from(self.0) * u128::from(self.0 ^ 0x8bb8_4b93_962e_acc9);
        ((product as u64) ^ (product >> 64) as u64) % bound
    }
}

impl Server {
    fn start(dir: &Path, port: u16) -> Self {
        let child = std::process::Command::new(env!("CARGO_BIN_EXE_umbra"))
            .arg(dir)
            .arg(port.to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();

        Self { child, port, dir: dir.to_path_buf() }
    }

    fn client(&self) -> Client {
        let address = SocketAddr::from(([127, 0, 0, 1], self.port));
        for _ in 0..250 {
            if let Ok(stream) = TcpStream::connect(address) {
                stream.set_nodelay(true).unwrap();
                return Client { stream };
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!("the server did not start on port {}", self.port)
    }

    fn kill(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}

impl Session for MvccDatabase {
    fn run(&mut self, sql: &str) -> Result<Vec<Vec<Value>>, String> {
        self.exec(&format!("{sql};"))
            .map(|set| set.tuples)
            .map_err(|error| error.to_string())
    }
}

impl Session for Client {
    fn run(&mut self, sql: &str) -> Result<Vec<Vec<Value>>, String> {
        let sql = format!("{sql};");
        let exchange = |stream: &mut TcpStream| -> std::io::Result<Vec<u8>> {
            stream.write_all(&(sql.len() as u32).to_le_bytes())?;
            stream.write_all(sql.as_bytes())?;
            let mut len = [0u8; 4];
            stream.read_exact(&mut len)?;
            let mut content = vec![0u8; u32::from_le_bytes(len) as usize];
            stream.read_exact(&mut content)?;
            Ok(content)
        };

        let content = exchange(&mut self.stream).map_err(|error| error.to_string())?;
        match tcp::deserialize(&content).map_err(|error| format!("{error:?}"))? {
            Response::QuerySet(set) => Ok(set.tuples),
            Response::Empty(affected) => Ok(vec![vec![Value::Number(affected as i64)]]),
            Response::Err(message) => Err(message),
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
