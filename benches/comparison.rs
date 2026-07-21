#[path = "comparison/dataset.rs"]
mod dataset;
#[path = "comparison/engine.rs"]
mod engine;
#[path = "comparison/harness.rs"]
mod harness;
#[path = "comparison/report.rs"]
mod report;
#[path = "comparison/scenario.rs"]
mod scenario;

use engine::{Engine, Postgres, Sqlite, Umbra};
use report::{Cell, Report, Row};
use scenario::Workload;
use std::fs;
use std::panic::{self, AssertUnwindSafe};

const OUTPUT: &str = "benches/comparison-results.md";

fn main() {
    panic::set_hook(Box::new(|info| eprintln!("    [unsupported] {info}")));

    eprintln!("loading {} users into each engine...", dataset::users());
    let mut engines: Vec<Box<dyn Engine>> =
        vec![Box::new(Umbra::setup()), Box::new(Sqlite::setup())];
    match Postgres::setup() {
        Some(postgres) => engines.push(Box::new(postgres)),
        None => eprintln!("postgres unreachable, comparing umbra vs SQLite only"),
    }

    let names: Vec<_> = engines
        .iter()
        .map(|engine| engine.name().to_string())
        .collect();
    let mut rows = Vec::new();

    for case in scenario::scenarios() {
        eprintln!("  {}", case.label);
        let cells = engines
            .iter_mut()
            .map(|engine| measure(engine.as_mut(), &case.workload, case.iterations))
            .collect();

        rows.push(Row {
            category: case.category,
            label: case.label.to_string(),
            cells,
        });
    }

    let report = Report {
        engines: names,
        rows,
        dataset_rows: dataset::users(),
    };

    let markdown = report.render();
    fs::write(OUTPUT, &markdown).expect("the report is written");
    eprintln!("\nreport written to {OUTPUT}\n");
    println!("{markdown}");
}

fn measure(engine: &mut dyn Engine, workload: &Workload, iterations: usize) -> Cell {
    match workload {
        Workload::Read(sql) => measure_read(engine, sql, iterations),
        Workload::ReadEach(build) => {
            let sql = build(engine.name());
            measure_read(engine, &sql, iterations)
        }
        Workload::Write(build) => Cell {
            timing: Some(harness::time(iterations, |i| {
                let _ = engine.write(&build(i));
            })),
            rows: None,
        },
    }
}

fn measure_read(engine: &mut dyn Engine, sql: &str, iterations: usize) -> Cell {
    match panic::catch_unwind(AssertUnwindSafe(|| engine.query(sql))) {
        Ok(Ok(count)) => {
            let timing = panic::catch_unwind(AssertUnwindSafe(|| {
                harness::time(iterations, |_| {
                    let _ = engine.query(sql);
                })
            }));
            Cell {
                timing: timing.ok(),
                rows: Some(count),
            }
        }
        _ => Cell {
            timing: None,
            rows: None,
        },
    }
}
