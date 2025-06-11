use criterion::{criterion_group, criterion_main, Criterion};
use fake::{
    faker::{
        address::en::{CountryName, Latitude, Longitude, ZipCode},
        boolean::en::Boolean,
        chrono::en::{Date, Time},
        internet::en::{FreeEmail, IPv4, MACAddress, UserAgent, Username},
        lorem::en::Sentence,
    },
    Dummy, Fake, Faker,
};
use rusqlite::Connection;
use std::{cell::RefCell, fs::File, hint::black_box};
use umbra::db::Database;

thread_local! {
    static DISK_DB: RefCell<Option<Database<File>>> = RefCell::new(None);
}

#[derive(Debug, Dummy)]
pub struct Record {
    #[dummy(faker = "Username()")]
    pub username: String,
    #[dummy(faker = "FreeEmail()")]
    pub email: String,
    #[dummy(faker = "Date()")]
    pub signup_ts: String,
    #[dummy(faker = "Time()")]
    pub last_login_ts: String,

    #[dummy(faker = "Boolean(50)")]
    pub is_active: bool,

    #[dummy(faker = "Sentence(1..70)")]
    pub bio: String,
    #[dummy(faker = "ZipCode()")]
    pub zip_code: String,
    #[dummy(faker = "CountryName()")]
    pub country: String,

    #[dummy(faker = "Latitude()")]
    pub latitude: f64,
    #[dummy(faker = "Longitude()")]
    pub longitude: f64,

    #[dummy(faker = "IPv4()")]
    pub ip_address: String,
    #[dummy(faker = "MACAddress()")]
    pub mac_address: String,
    #[dummy(faker = "UserAgent()")]
    pub user_agent: String,

    //#[dummy(faker = "Uuidv4()")]
    //pub session_token: String,
    //#[dummy(faker = "Uuidv5()")]
    //pub api_key: String,
    #[dummy(faker = "Sentence(1..20)")]
    pub preferences: String,
    #[dummy(faker = "Sentence(1..20)")]
    pub notes: String,
}

const CREATE_TABLE: &str = r#"
    CREATE TABLE records (
        id SERIAL PRIMARY KEY,
        username VARCHAR(100),
        email VARCHAR(255),
        signup_ts DATE,
        last_login_ts TIME,
        is_active BOOLEAN,
        bio VARCHAR(1000),
        zip_code VARCHAR(20),
        country VARCHAR(500),
        latitude REAL,
        longitude REAL,
        ip_address VARCHAR(50),
        mac_address VARCHAR(50),
        user_agent VARCHAR(255),
        preferences VARCHAR(255),
        notes VARCHAR(255)
    );
"#;

fn with_disk_database_mut<F, R>(f: F) -> R
where
    F: FnOnce(&mut Database<File>) -> R,
{
    DISK_DB.with(|cell| {
        let mut borrowed = cell.borrow_mut();
        if borrowed.is_none() {
            *borrowed = Some(
                Database::init("benchmark.db").expect("Could not initialise benchmark database"),
            );
        }
        f(borrowed.as_mut().unwrap())
    })
}

fn insert_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Insert");
    group.throughput(criterion::Throughput::Elements(1));

    let record: Record = Faker.fake();
    with_disk_database_mut(|db| {
        db.exec(CREATE_TABLE).expect("Could not create table");

        group.bench_function("single_insert", |b| {
            b.iter(|| insert_record(db, &record));
        });

        db.exec("DROP TABLE records;")
            .expect("Could not drop table after benchmark");
    });

    let connection = Connection::open("benchmark-sqlite.db")
        .expect("Could not initialise benchmark database for sqlite");
    connection
        .execute(CREATE_TABLE, [])
        .expect("Could not create sqlite table");

    group.bench_function("single_insert_sqlite", |b| {
        b.iter(|| insert_record_sqlite(&connection, &record))
    });

    connection
        .execute("DROP TABLE records;", [])
        .expect("Could not drop table after benchmark");
}

fn select_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Select");
    let records: Vec<Record> = (0..1000).map(|_| Faker.fake()).collect();

    const QUERY: &str = r#"
        SELECT * FROM records 
        WHERE is_active = true 
        AND (country = 'United States' OR country = 'Canada' OR country = 'United Kingdom')
        AND signup_ts > '2023-01-01'
        AND latitude > 20.0 AND latitude < 50.0;
    "#;

    with_disk_database_mut(|db| {
        db.exec(CREATE_TABLE).expect("Table creation failed");
        records.iter().for_each(|record| insert_record(db, record));

        group.bench_function("select_with_filters", |b| {
            b.iter(|| {
                db.exec(black_box(QUERY))
                    .expect("Failed to select from database");
            });
        });
        db.exec("DROP TABLE records;").expect("Failed to cleanup");
    });

    let connection = Connection::open("benchmark-sqlite.db")
        .expect("Could not initialise benchmark database for sqlite");
    connection
        .execute(CREATE_TABLE, [])
        .expect("Could not create sqlite table");

    records
        .iter()
        .for_each(|record| insert_record_sqlite(&connection, &record));

    group.bench_function("select_with_filters_sqlite", |b| {
        b.iter(|| {
            connection
                .execute(QUERY, [])
                .expect("Failed to select from sqlite database");
        });
    });
    connection
        .execute("DROP TABLE records;", [])
        .expect("Failed to cleanup");
}

fn insert_record(db: &mut Database<File>, record: &Record) {
    let Record {
        username,
        email,
        signup_ts,
        last_login_ts,
        is_active,
        bio,
        zip_code,
        country,
        latitude,
        longitude,
        ip_address,
        mac_address,
        user_agent,
        preferences,
        notes,
    } = record;
    let country = country.replace("'", "");

    let input = format!(
        "INSERT INTO records (
            username, email, signup_ts, last_login_ts, is_active, bio,
            zip_code, country, latitude, longitude, ip_address,
            mac_address, user_agent, preferences, notes
        ) VALUES (
            '{username}', '{email}', '{signup_ts}', '{last_login_ts}', {is_active}, '{bio}',
            '{zip_code}', '{country}', {latitude}, {longitude}, '{ip_address}',
            '{mac_address}', '{user_agent}', '{preferences}', '{notes}'
        );"
    );

    db.exec(input.as_str())
        .expect("Failed to insert into database table");
}

fn insert_record_sqlite(conn: &Connection, record: &Record) {
    conn.execute(
        "INSERT INTO records (
            username, email, signup_ts, last_login_ts, is_active, bio,
            zip_code, country, latitude, longitude, ip_address,
            mac_address, user_agent, preferences, notes
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6,
            ?7, ?8, ?9, ?10, ?11,
            ?12, ?13, ?14, ?15
        )",
        [
            &record.username,
            &record.email,
            &record.signup_ts,
            &record.last_login_ts,
            &record.is_active.to_string(),
            &record.bio,
            &record.zip_code,
            &record.country,
            &record.latitude.to_string(),
            &record.longitude.to_string(),
            &record.ip_address,
            &record.mac_address,
            &record.user_agent,
            &record.preferences,
            &record.notes,
        ],
    )
    .expect("Insert failed");
}

criterion_group!(benches, insert_benchmark, select_benchmark);
criterion_main!(benches);
