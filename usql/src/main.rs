use std::{
    collections::VecDeque,
    env,
    io::{Read, Write},
    net::TcpStream,
    time::Instant,
};

use rustyline::{DefaultEditor, error::ReadlineError};
use umbra::{
    db::QuerySet,
    sql::statement::Value,
    tcp::{self, Response},
};

const EXIT: &str = "quit";
const PROMPT: &str = "umbra > ";
const SQL_PROMPT: &str = "sql > ";
const SINGLE_QUOTE: &str = "string(')> ";
const DOUBLE_QUOTE: &str = "string(\") ";

const UMBRA_ASCII: &str = r#"
⠀⠀⠀⠀⣀⣤⣤⣶⣾⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣶⣶⣦⣤⣀⠀⠀⠀⠀⠀
⣀⣴⣶⣿⣿⣿⣿⣿⣿⣷⣄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣴⣿⣿⣿⣿⣿⣿⣷⣦⣄⡀
⠁⠀⠀⠈⠉⠛⣿⣿⣿⣿⣿⣷⣦⣀⢠⣆⣸⡆⢀⣤⣾⣿⣿⣿⣿⣿⠟⠋⠉⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠸⠿⠿⠿⠿⢿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠿⠿⠿⠿⠏⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠻⣿⣿⣿⣿⠿⠋⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠉⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
"#;

fn main() -> rustyline::Result<()> {
    let port = env::args()
        .nth(1)
        .expect("Port not provided")
        .parse::<u16>()
        .expect("Invalid port provided");

    let mut rsl = DefaultEditor::new()?;
    if rsl.load_history("history.usql").is_err() {
        println!("No previous history")
    };

    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    println!("Connected to {}", stream.peer_addr()?);
    println!("{}", UMBRA_ASCII);
    println!("󰭟  usql | Umbra's shadowy SQL shell.");
    println!("Type \\help for guidance, \\quit to escape the void.");

    let mut single_quote = None;
    let mut sql = String::new();
    let mut cursor = 0;
    let mut content = Vec::new();
    let mut prompt = PROMPT;

    loop {
        let line = match rsl.readline(prompt) {
            Ok(line) => line,
            Err(err) => {
                match err {
                    ReadlineError::Interrupted => println!("CTRL-C"),
                    ReadlineError::Eof => println!("CTRL-D"),
                    other => println!("Error: {other:#?}"),
                }

                break;
            }
        };

        let mut position = VecDeque::new();
        for (index, byte) in line.bytes().enumerate() {
            match byte {
                b'"' | b'\'' => match single_quote {
                    None => single_quote = Some(byte),
                    Some(opening_quote) => {
                        if opening_quote.eq(&byte) {
                            single_quote.take();
                        }
                    }
                },

                b';' if single_quote.is_none() => position.push_back(index),
                _ => {}
            }
        }

        if sql.is_empty() && line.trim().eq(EXIT) {
            break;
        }

        if line.trim().is_empty() {
            continue;
        }

        if !sql.is_empty() {
            sql.push('\n');
        }

        position.iter_mut().for_each(|pos| *pos += sql.len());
        sql.push_str(&line);

        let mut clear = false;
        let needs_continuation = position.is_empty()
            || position
                .back()
                .is_some_and(|pos| !sql[*pos + 1..].trim_end().is_empty());

        prompt = if needs_continuation {
            match single_quote {
                Some(b'"') => DOUBLE_QUOTE,
                Some(_) => SINGLE_QUOTE,
                None => SQL_PROMPT,
            }
        } else {
            rsl.add_history_entry(&sql)?;
            clear = true;
            PROMPT
        };

        while let Some(pos) = position.pop_front() {
            let statement = &sql[cursor..=pos];

            let packet_transmission = Instant::now();
            stream.write_all(&(statement.len() as u32).to_le_bytes())?;
            stream.write_all(statement.as_bytes())?;

            let mut content_len_buff = [0; 4];
            stream.read_exact(&mut content_len_buff)?;
            let content_len = u32::from_le_bytes(content_len_buff) as usize;

            content.resize(content_len, 0);
            stream.read_exact(&mut content)?;

            match tcp::deserialize(&content) {
                Ok(response) => match response {
                    Response::Err(err) => println!("{err}"),
                    Response::Empty(affected_rows) => {
                        println!("{affected_rows} ({:.2?})", packet_transmission.elapsed())
                    }
                    Response::QuerySet(query_set) => {
                        println!(
                            "{}\n{} {} ({:.2?})",
                            table(&query_set),
                            query_set.tuples.len(),
                            "row",
                            packet_transmission.elapsed()
                        )
                    }
                },
                Err(err) => println!("Decoding error: {err}"),
            }

            cursor += 1;
        }

        if clear {
            sql.clear();
            cursor = 0;
        }
    }

    rsl.save_history("history.usql")?;
    Ok(())
}

fn table(query: &QuerySet) -> String {
    let mut width: Vec<usize> = query
        .schema
        .columns
        .iter()
        .map(|col| col.name.chars().count())
        .collect();

    let rows: Vec<Vec<String>> = query
        .tuples
        .iter()
        .map(|row| {
            row.iter()
                .map(|col| match col {
                    Value::String(string) => string.replace('\n', "\\n"),
                    other => other.to_string(),
                })
                .collect()
        })
        .collect();

    (rows).iter().for_each(|row| {
        row.iter().enumerate().for_each(|(idx, col)| {
            if col.len() > width[idx] {
                width[idx] = col.len();
            }
        })
    });

    width.iter_mut().for_each(|w| *w += 2);

    let mut border = String::from('+');
    width.iter().for_each(|w| {
        (0..*w).for_each(|_| border.push('-'));
        border.push('+');
    });

    let draw_row = |row: &Vec<String>| -> String {
        let mut row_string = String::from('|');

        row.iter().enumerate().for_each(|(idx, col)| {
            row_string.push(' ');
            row_string.push_str(col);
            (0..width[idx] - col.len() - 1).for_each(|_| row_string.push(' '));
            row_string.push('|');
        });

        row_string
    };

    let mut table = String::from(&border);
    table.push('\n');
    table.push_str(&draw_row(
        &query
            .schema
            .columns
            .iter()
            .map(|col| col.name.to_string())
            .collect(),
    ));
    table.push('\n');
    table.push_str(&border);
    table.push('\n');

    rows.iter().for_each(|row| {
        table.push_str(&draw_row(row));
        table.push('\n');
    });

    if !rows.is_empty() {
        table.push_str(&border);
    }

    table
}
