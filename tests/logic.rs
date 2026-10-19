//! Runs the sqllogictest files in `tests/logic` (record format of SQLite's sqllogictest, https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki)

use std::collections::HashMap;
use std::path::Path;
use umbra::db::MvccDatabase;
use umbra::sql::statement::Value;

struct Runner {
    db: MvccDatabase,
    hash_threshold: usize,
    labels: HashMap<String, (String, String)>,
    failures: Vec<String>,
}

struct Record<'f> {
    line: usize,
    header: Vec<&'f str>,
    sql: String,
    expected: Vec<&'f str>,
}

enum Sort {
    None,
    Rows,
    Values,
}

impl Runner {
    fn new() -> Self {
        Self {
            db: MvccDatabase::in_memory().unwrap(),
            hash_threshold: 0,
            labels: HashMap::new(),
            failures: Vec::new(),
        }
    }

    fn run(&mut self, file: &Path) {
        let text = std::fs::read_to_string(file).unwrap();
        let name = file.file_name().unwrap().to_string_lossy();
        let mut lines = text.lines().enumerate().peekable();
        let mut skip = false;

        while let Some((index, line)) = lines.next() {
            let line = line.trim_end();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let header: Vec<_> = line.split_whitespace().collect();
            match header[0] {
                "halt" => break,
                "hash-threshold" => self.hash_threshold = header[1].parse().unwrap(),
                "skipif" => skip = header[1] == "umbra",
                "onlyif" => skip = header[1] != "umbra",
                "statement" | "query" => {
                    let mut sql = String::new();
                    let mut expected = Vec::new();
                    let mut in_results = false;

                    while let Some((_, next)) = lines.next_if(|(_, next)| !next.trim().is_empty()) {
                        match (in_results, next.trim_end()) {
                            (false, "----") => in_results = true,
                            (false, text) => {
                                sql.push_str(text);
                                sql.push('\n');
                            },
                            (true, _) => expected.push(next.trim_end_matches('\r')),
                        }
                    }

                    let record = Record { line: index + 1, header, sql, expected };
                    if std::mem::take(&mut skip) {
                        continue;
                    }

                    if let Err(message) = self.check(&record) {
                        self.failures
                            .push(format!("{name}:{}: {message}\n{}", record.line, record.sql));
                    }
                },
                other => panic!("{name}:{}: unknown record `{other}`", index + 1),
            }
        }
    }

    fn check(&mut self, record: &Record) -> Result<(), String> {
        let mut sql = record.sql.trim_end().to_string();
        if !sql.ends_with(';') {
            sql.push(';');
        }

        let result = self.db.exec(&sql);
        match (record.header[0], record.header[1]) {
            ("statement", "ok") => result.map(|_| ()).map_err(|error| format!("failed: {error}")),
            ("statement", "error") => match result {
                Ok(_) => Err("succeeded, expected an error".into()),
                _ => Ok(()),
            },
            ("query", types) => {
                let rows = result.map_err(|error| format!("failed: {error}"))?.tuples;
                let sort = match record.header.get(2) {
                    Some(&"rowsort") => Sort::Rows,
                    Some(&"valuesort") => Sort::Values,
                    _ => Sort::None,
                };

                let values = render(rows, types, sort)?;
                self.compare(record, values)
            },
            (kind, mode) => Err(format!("unknown record `{kind} {mode}`")),
        }
    }

    fn compare(&mut self, record: &Record, values: Vec<String>) -> Result<(), String> {
        let digest = format!("{} values hashing to {}", values.len(), md5(&values));
        if let Some(label) = record.header.get(3) {
            let previous = self
                .labels
                .entry(label.to_string())
                .or_insert_with(|| (digest.clone(), record.sql.clone()));

            if previous.0 != digest {
                return Err(format!(
                    "label {label} differs from\n{}got {digest}, expected {}",
                    previous.1, previous.0
                ));
            }
        }

        let expected: Vec<_> = record.expected.iter().flat_map(|line| line.split('\t')).collect();
        let hashed = matches!(expected.as_slice(), [line] if line.contains(" values hashing to "));
        let matches = match hashed {
            true => expected[0] == digest,
            _ => expected == values,
        };

        let thresholded = values.len() > self.hash_threshold && self.hash_threshold > 0;
        match matches {
            true => Ok(()),
            _ if !hashed && thresholded => Err(format!("got {digest}")),
            _ => Err(format!("got\n{}\nexpected\n{}", values.join("\n"), expected.join("\n"))),
        }
    }
}

fn render(rows: Vec<Vec<Value>>, types: &str, sort: Sort) -> Result<Vec<String>, String> {
    let types = types.as_bytes();
    let mut rows = rows
        .into_iter()
        .map(|row| match row.len() == types.len() {
            true => Ok(row
                .iter()
                .zip(types)
                .map(|(value, kind)| format(value, *kind))
                .collect::<Vec<_>>()),
            _ => Err(format!("{} columns, the record declares {}", row.len(), types.len())),
        })
        .collect::<Result<Vec<_>, _>>()?;

    if let Sort::Rows = sort {
        rows.sort();
    }

    let mut values: Vec<_> = rows.into_iter().flatten().collect();
    if let Sort::Values = sort {
        values.sort();
    }

    Ok(values)
}

fn format(value: &Value, kind: u8) -> String {
    match (value, kind) {
        (Value::Null, _) => "NULL".into(),
        (Value::Number(n), b'R') => format!("{:.3}", *n as f64),
        (Value::Float(f), b'R') => format!("{f:.3}"),
        (Value::Numeric(n), b'R') => format!("{:.3}", f64::from(n.clone())),
        (Value::Float(f), b'I') if f.fract() == 0.0 => format!("{}", *f as i64),
        (Value::Boolean(b), b'I') => format!("{}", *b as u8),
        (Value::String(s), _) if s.is_empty() => "(empty)".into(),
        (Value::String(s), _) => s.to_string(),
        (value, _) => value.to_string(),
    }
}

/// RFC 1321 over the values, each followed by a newline, as sqllogictest hashes results
fn md5(values: &[String]) -> String {
    const SHIFTS: [u32; 64] = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5,
        9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10,
        15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
    ];

    let constants: Vec<_> =
        (0..64).map(|i| ((i as f64 + 1.0).sin().abs() * 4294967296.0) as u32).collect();
    let mut message: Vec<_> =
        values.iter().flat_map(|value| value.bytes().chain(Some(b'\n'))).collect();
    let length = (message.len() as u64).wrapping_mul(8);
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&length.to_le_bytes());

    let mut state: [u32; 4] = [0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476];
    for block in message.chunks(64) {
        let words: Vec<u32> = block
            .chunks(4)
            .map(|word| u32::from_le_bytes(word.try_into().unwrap()))
            .collect();
        let [mut a, mut b, mut c, mut d] = state;

        for i in 0..64 {
            let (f, g) = match i / 16 {
                0 => ((b & c) | (!b & d), i),
                1 => ((d & b) | (!d & c), (5 * i + 1) % 16),
                2 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | !d), (7 * i) % 16),
            };

            let rotated = a.wrapping_add(f).wrapping_add(constants[i]).wrapping_add(words[g]);
            (a, d, c) = (d, c, b);
            b = b.wrapping_add(rotated.rotate_left(SHIFTS[i]));
        }

        for (word, part) in state.iter_mut().zip([a, b, c, d]) {
            *word = word.wrapping_add(part);
        }
    }

    state
        .iter()
        .flat_map(|word| word.to_le_bytes())
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[test]
fn md5_vectors() {
    assert_eq!(md5(&[]), "d41d8cd98f00b204e9800998ecf8427e");
    assert_eq!(md5(&["abc".into()]), "0bee89b07a248e27c83fc3d5951213c1");
    assert_eq!(md5(&["a".repeat(63)]), "18771d92fec7fc9374ef7c5a3e2ab50f");
    assert_eq!(md5(&["1".into(), "2".into(), "x".into()]), "4e21a467c5747c7866f14624026ae647");
}

#[test]
fn logic() {
    let mut files: Vec<_> = std::fs::read_dir("tests/logic")
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "slt"))
        .collect();
    files.sort();

    let mut failures = Vec::new();
    for file in files {
        let mut runner = Runner::new();
        runner.run(&file);
        failures.append(&mut runner.failures);
    }

    assert!(failures.is_empty(), "{} failures\n\n{}", failures.len(), failures.join("\n"));
}
