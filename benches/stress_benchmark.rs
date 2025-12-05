use std::fs;
use std::path::Path;
use std::time::Instant;
use umbra::db::Database;

const DB_PATH: &str = "stress_test.db";

fn main() {
    if Path::new(DB_PATH).exists() {
        fs::remove_file(DB_PATH).unwrap();
    }

    println!("Initialising Database...");
    let mut db = Database::init(DB_PATH).expect("Failed to init DB");

    db.exec(
        "CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            department_id INT,
            salary INT
        );",
    )
    .expect("Failed to create users table");

    db.exec(
        "CREATE TABLE departments (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            region VARCHAR(50)
        );",
    )
    .expect("Failed to create departments table");

    let user_count = 50_000;
    let dept_count = 100;

    println!("Inserting {} departments...", dept_count);
    db.exec("BEGIN TRANSACTION;").unwrap();
    for i in 1..=dept_count {
        let region = if i % 2 == 0 { "East" } else { "West" };
        db.exec(&format!(
            "INSERT INTO departments (name, region) VALUES ('Dept_{}', '{}');",
            i, region
        ))
        .unwrap();
    }
    db.exec("COMMIT;").unwrap();

    println!("Inserting {} users...", user_count);
    db.exec("BEGIN TRANSACTION;").unwrap();
    for i in 1..=user_count {
        let dept_id = (i % dept_count) + 1;
        let salary = (i * 100) % 50000 + 20000;
        db.exec(&format!(
            "INSERT INTO users (name, department_id, salary) VALUES ('User_{}', {}, {});",
            i, dept_id, salary
        ))
        .unwrap();
    }
    db.exec("COMMIT;").unwrap();

    // this query forces:
    // - full scan on users
    // - hash join (users -> departments)
    // - aggregation (group by region)
    // - sorting (order by avg_salary)
    let query = r#"
        SELECT 
            d.region, 
            COUNT(u.id) as user_count, 
            AVG(u.salary) as avg_salary
        FROM users AS u
        JOIN departments AS d ON u.department_id = d.id
        GROUP BY d.region
        ORDER BY avg_salary DESC;
    "#;

    println!("Running stress queries...");
    let start = Instant::now();
    let iterations = 20;

    for i in 0..iterations {
        let result = db.exec(query);
        if let Err(e) = result {
            eprintln!("Query failed iteration {}: {:?}", i, e);
        }
    }

    println!("Done. Time taken: {:.2?}", start.elapsed());

    if Path::new(DB_PATH).exists() {
        fs::remove_file(DB_PATH).unwrap();
    }
}
