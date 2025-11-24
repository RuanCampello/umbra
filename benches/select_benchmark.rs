use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rusqlite::Connection;
use std::fs;
use std::hint::black_box;
use std::time::Duration;
use umbra::db::Database;

const SETUP_SQL: &str = r#"
    CREATE TABLE customers (
        customer_id SERIAL PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        email VARCHAR(100),
        country VARCHAR(50),
        registration_date DATE
    );
    
    CREATE TABLE products (
        product_id SERIAL PRIMARY KEY,
        product_name VARCHAR(100),
        category VARCHAR(50),
        unit_price REAL,
        stock_quantity INT
    );
    
    CREATE TABLE orders (
        order_id SERIAL PRIMARY KEY,
        customer_id INT,
        order_date DATE,
        total_amount REAL,
        status VARCHAR(20)
    );
    
    CREATE TABLE order_items (
        item_id SERIAL PRIMARY KEY,
        order_id INT,
        product_id INT,
        quantity INT,
        item_price REAL
    );
"#;

fn generate_test_data(num_customers: usize, num_products: usize, num_orders: usize) -> Vec<String> {
    let mut statements = Vec::new();

    for i in 1..=num_customers {
        statements.push(format!(
            "INSERT INTO customers (first_name, last_name, email, country, registration_date) VALUES ('Customer{}', 'Last{}', 'customer{}@test.com', 'Country{}', '2023-{:02}-{:02}');",
            i, i, i, i % 10, (i % 12) + 1, (i % 28) + 1
        ));
    }

    for i in 1..=num_products {
        statements.push(format!(
            "INSERT INTO products (product_name, category, unit_price, stock_quantity) VALUES ('Product{}', 'Category{}', {}, {});",
            i, i % 5, 10.0 + (i as f64 * 0.5), 100 + (i * 10)
        ));
    }

    for i in 1..=num_orders {
        let customer_id = (i % num_customers) + 1;
        statements.push(format!(
            "INSERT INTO orders (customer_id, order_date, total_amount, status) VALUES ({}, '2024-{:02}-{:02}', {}, '{}');",
            customer_id,
            (i % 12) + 1,
            (i % 28) + 1,
            50.0 + (i as f64 * 10.0),
            if i % 3 == 0 { "completed" } else if i % 3 == 1 { "pending" } else { "shipped" }
        ));

        let items_count = 2 + (i % 4);
        for j in 0..items_count {
            let product_id = ((i + j) % num_products) + 1;
            statements.push(format!(
                "INSERT INTO order_items (order_id, product_id, quantity, item_price) VALUES ({}, {}, {}, {});",
                i, product_id, 1 + (j % 5), 15.0 + (j as f64 * 3.0)
            ));
        }
    }

    statements
}

fn setup_umbra(db: &mut Database<std::fs::File>, statements: &[String]) {
    let tables: Vec<&str> = SETUP_SQL
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    for table in tables {
        let stmt = format!("{};", table);
        db.exec(&stmt).expect("Failed to create table in Umbra");
    }

    for stmt in statements {
        db.exec(stmt).expect("Failed to insert data in Umbra");
    }
}

fn setup_sqlite(conn: &Connection, statements: &[String]) {
    for table in SETUP_SQL.split(';').filter(|s| !s.trim().is_empty()) {
        conn.execute(table, [])
            .expect("Failed to create table in SQLite");
    }

    for stmt in statements {
        conn.execute(stmt, [])
            .expect("Failed to insert data in SQLite");
    }
}

fn benchmark_simple_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("Simple Join");
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(5));

    let query = r#"
        SELECT c.first_name, c.last_name, o.order_id, o.total_amount
        FROM customers AS c
        INNER JOIN orders AS o ON c.customer_id = o.customer_id
        WHERE o.status = 'completed'
        ORDER BY o.total_amount DESC;
    "#;

    for size in [100, 500, 1000].iter() {
        let statements = generate_test_data(*size, 50, *size * 2);

        let _ = fs::remove_file("bench_umbra.db");
        let mut umbra_db = Database::init("bench_umbra.db").unwrap();
        setup_umbra(&mut umbra_db, &statements);

        group.bench_with_input(BenchmarkId::new("Umbra", size), size, |b, _| {
            b.iter(|| {
                let query = black_box(query);
                println!("query: {query}");
                let result = match umbra_db.exec(query) {
                    Ok(res) => res,
                    Err(err) => {
                        eprintln!("{err}");
                        panic!()
                    }
                };
                black_box(result);
            });
        });
        drop(umbra_db);

        let _ = fs::remove_file("bench_sqlite.db");
        let sqlite_conn = Connection::open("bench_sqlite.db").unwrap();
        setup_sqlite(&sqlite_conn, &statements);

        group.bench_with_input(BenchmarkId::new("SQLite", size), size, |b, _| {
            b.iter(|| {
                let mut stmt = sqlite_conn.prepare(query).unwrap();
                let rows: Vec<_> = stmt.query_map([], |_| Ok(())).unwrap().collect();
                black_box(rows);
            });
        });
        drop(sqlite_conn);
    }

    group.finish();
}

fn benchmark_complex_join_with_aggregation(c: &mut Criterion) {
    let mut group = c.benchmark_group("Complex Join with Aggregation");
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(5));

    let query = r#"
        SELECT 
            c.country,
            COUNT(o.order_id) AS total_orders,
            SUM(o.total_amount) AS total_revenue,
            AVG(o.total_amount) AS avg_order_value
        FROM customers AS c
        LEFT JOIN orders AS o ON c.customer_id = o.customer_id
        WHERE o.status = 'completed'
        GROUP BY c.country
        ORDER BY total_revenue DESC;
    "#;

    for size in [100, 500, 1000].iter() {
        let statements = generate_test_data(*size, 50, *size * 2);

        let _ = fs::remove_file("bench_umbra.db");
        let mut umbra_db = Database::init("bench_umbra.db").unwrap();
        setup_umbra(&mut umbra_db, &statements);

        group.bench_with_input(BenchmarkId::new("Umbra", size), size, |b, _| {
            b.iter(|| {
                let result = umbra_db.exec(black_box(query)).unwrap();
                black_box(result);
            });
        });

        drop(umbra_db);

        let _ = fs::remove_file("bench_sqlite.db");
        let sqlite_conn = Connection::open("bench_sqlite.db").unwrap();
        setup_sqlite(&sqlite_conn, &statements);

        group.bench_with_input(BenchmarkId::new("SQLite", size), size, |b, _| {
            b.iter(|| {
                let mut stmt = sqlite_conn.prepare(query).unwrap();
                let rows: Vec<_> = stmt.query_map([], |_| Ok(())).unwrap().collect();
                black_box(rows);
            });
        });

        drop(sqlite_conn);
    }

    group.finish();
}

fn benchmark_multiple_joins(c: &mut Criterion) {
    let mut group = c.benchmark_group("Multiple Joins");
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(5));

    let query = r#"
        SELECT 
            c.first_name,
            c.last_name,
            o.order_id,
            o.order_date,
            p.product_name,
            p.category,
            oi.quantity,
            oi.item_price
        FROM customers AS c
        INNER JOIN orders AS o ON c.customer_id = o.customer_id
        INNER JOIN order_items AS oi ON o.order_id = oi.order_id
        INNER JOIN products AS p ON oi.product_id = p.product_id
        WHERE p.category = 'Category1' AND o.status = 'completed'
        ORDER BY o.order_date DESC;
    "#;

    for size in [100, 500, 1000].iter() {
        let statements = generate_test_data(*size, 50, *size * 2);

        let _ = fs::remove_file("bench_umbra.db");
        let mut umbra_db = Database::init("bench_umbra.db").unwrap();
        setup_umbra(&mut umbra_db, &statements);

        group.bench_with_input(BenchmarkId::new("Umbra", size), size, |b, _| {
            b.iter(|| {
                let result = umbra_db.exec(black_box(query)).unwrap();
                black_box(result);
            });
        });

        drop(umbra_db);

        let _ = fs::remove_file("bench_sqlite.db");
        let sqlite_conn = Connection::open("bench_sqlite.db").unwrap();
        setup_sqlite(&sqlite_conn, &statements);

        group.bench_with_input(BenchmarkId::new("SQLite", size), size, |b, _| {
            b.iter(|| {
                let mut stmt = sqlite_conn.prepare(query).unwrap();
                let rows: Vec<_> = stmt.query_map([], |_| Ok(())).unwrap().collect();
                black_box(rows);
            });
        });

        drop(sqlite_conn);
    }

    group.finish();
}

fn benchmark_join_with_group_by(c: &mut Criterion) {
    let mut group = c.benchmark_group("Join with Group By");
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(5));

    let query = r#"
        SELECT 
            p.category,
            COUNT(oi.item_id) AS items_sold,
            SUM(oi.quantity) AS total_quantity,
            SUM(oi.item_price) AS total_revenue
        FROM products AS p
        INNER JOIN order_items AS oi ON p.product_id = oi.product_id
        INNER JOIN orders AS o ON oi.order_id = o.order_id
        WHERE o.status = 'completed'
        GROUP BY p.category
        ORDER BY total_revenue DESC;
    "#;

    for size in [100, 500, 1000].iter() {
        let statements = generate_test_data(*size, 50, *size * 2);

        let _ = fs::remove_file("bench_umbra.db");
        let mut umbra_db = Database::init("bench_umbra.db").unwrap();
        setup_umbra(&mut umbra_db, &statements);

        group.bench_with_input(BenchmarkId::new("Umbra", size), size, |b, _| {
            b.iter(|| {
                let result = umbra_db.exec(black_box(query)).unwrap();
                black_box(result);
            });
        });
        drop(umbra_db);

        let _ = fs::remove_file("bench_sqlite.db");
        let sqlite_conn = Connection::open("bench_sqlite.db").unwrap();
        setup_sqlite(&sqlite_conn, &statements);

        group.bench_with_input(BenchmarkId::new("SQLite", size), size, |b, _| {
            b.iter(|| {
                let mut stmt = sqlite_conn.prepare(query).unwrap();
                let rows: Vec<_> = stmt.query_map([], |_| Ok(())).unwrap().collect();
                black_box(rows);
            });
        });
        drop(sqlite_conn);
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_simple_join,
    benchmark_complex_join_with_aggregation,
    benchmark_multiple_joins,
    benchmark_join_with_group_by
);
criterion_main!(benches);

