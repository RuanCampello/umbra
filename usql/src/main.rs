mod commands;
mod highlight;

use crate::{
    commands::handle_command,
    highlight::{KEYWORD_COLOUR, RESET_COLOUR, STRING_COLOUR, SqlHighlighter, TIME_COLOUR},
};
use rustyline::error::ReadlineError;
use std::{
    collections::VecDeque,
    env,
    io::{Read, Write},
    net::TcpStream,
    time::Instant,
};
use umbra::tcp::{self, Response};

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

    let mut rsl = rustyline::Editor::new()?;
    rsl.set_helper(Some(SqlHighlighter));

    if rsl.load_history("history.usql").is_err() {
        println!("No previous history")
    };

    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_nodelay(true)?;
    println!("Connected to {}", stream.peer_addr()?);

    println!("{KEYWORD_COLOUR}{UMBRA_ASCII}{RESET_COLOUR}");
    println!("󰭟  usql | Umbra's shadowy SQL shell.");
    println!(
        "Type {STRING_COLOUR}/help{RESET_COLOUR} for guidance, {STRING_COLOUR}/quit{RESET_COLOUR} to escape the void."
    );

    let mut single_quote = None;
    let mut sql = String::new();
    let mut cursor = 0;
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

        if sql.is_empty() && line.trim_start().starts_with('/') {
            rsl.add_history_entry(&line)?;
            if handle_command(line.trim(), &mut stream) {
                break;
            }

            continue;
        }

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
            process_query(&mut stream, statement);
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

fn run_query(stream: &mut TcpStream, query: &str) -> Result<Response, Box<dyn std::error::Error>> {
    stream.write_all(&(query.len() as u32).to_le_bytes())?;
    stream.write_all(query.as_bytes())?;

    let mut content_len_buff = [0; 4];
    stream.read_exact(&mut content_len_buff)?;
    let content_len = u32::from_le_bytes(content_len_buff) as usize;

    let mut content = vec![0; content_len];
    stream.read_exact(&mut content)?;

    Ok(tcp::deserialize(&content)?)
}

fn process_query(stream: &mut TcpStream, statement: &str) {
    let packet_transmission = Instant::now();
    match run_query(stream, statement) {
        Ok(response) => match response {
            Response::Err(err) => println!("{err}"),
            Response::Empty(affected_rows) => {
                println!(
                    "{affected_rows} {TIME_COLOUR}({:.2?}){RESET_COLOUR}",
                    packet_transmission.elapsed()
                )
            }
            Response::QuerySet(query_set) => {
                println!(
                    "{}\n{} {} {TIME_COLOUR}({:.2?}){RESET_COLOUR}",
                    &query_set.to_string(),
                    query_set.tuples.len(),
                    plural("row", query_set.tuples.len()),
                    packet_transmission.elapsed()
                )
            }
        },
        Err(err) => println!("Error: {err}"),
    }
}

fn plural(word: &str, length: usize) -> String {
    match length == 1 {
        true => word.to_string(),
        false => format!("{word}s"),
    }
}
