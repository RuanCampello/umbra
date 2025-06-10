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
use std::{cell::RefCell, fs::File};
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

    let record: Record = Faker.fake();
    let Record {
        username,
        email,
        ip_address,
        longitude,
        latitude,
        mac_address,
        mut country,
        zip_code,
        bio,
        preferences,
        is_active,
        notes,
        last_login_ts,
        signup_ts,
        user_agent,
    } = record;
    country = country.replace("'", "");
    let insert_query = format!(
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

    with_disk_database_mut(|db| {
        db.exec(CREATE_TABLE).expect("Could not create table");

        group.bench_function("single_insert", |b| {
            b.iter(|| {
                db.exec(&insert_query)
                    .expect("Could not insert into database")
            });
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
        b.iter(|| {
            connection
                .execute(insert_query.as_str(), [])
                .expect("Could not insert into sqlite database")
        })
    });

    connection
        .execute("DROP TABLE records;", [])
        .expect("Could not drop table after benchmark");
}

criterion_group!(benches, insert_benchmark);
criterion_main!(benches);
