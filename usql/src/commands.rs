use crate::{RESET_COLOUR, STRING_COLOUR, process_query, run_query};
use std::{io::Write, net::TcpStream};
use umbra::tcp::Response;

pub fn handle_command(command: &str, stream: &mut TcpStream) -> bool {
    let parts = command.split_whitespace().collect::<Vec<_>>();

    match parts.as_slice() {
        ["/q"] | ["/quit"] => return true,
        ["/h"] | ["/help"] | ["/?"] => {
            println!("Available commands:");
            println!("  {STRING_COLOUR}/q, /quit{RESET_COLOUR}    Exit usql");
            println!("  {STRING_COLOUR}/h, /help{RESET_COLOUR}    Show this help message");
            println!("  {STRING_COLOUR}/clear, /c{RESET_COLOUR}   Clear the terminal screen");
            println!("  {STRING_COLOUR}/dt{RESET_COLOUR}      List all tables");
            println!("  {STRING_COLOUR}/d [table]{RESET_COLOUR}    Describe a specific table");
        }
        ["/clear"] | ["/c"] => {
            print!("\x1b[2J\x1b[1;1H");
            std::io::stdout().flush().ok();
        }
        ["/d"] | ["/dt"] => {
            process_query(
                stream,
                "SELECT name FROM umbra_db_meta WHERE type = 'table';",
            );
        }
        ["/d", table_name] => describe_table(stream, table_name),
        _ => println!("Unknown command: {command}"),
    }
    false
}

fn describe_table(stream: &mut TcpStream, table_name: &str) {
    let query = format!("SELECT * FROM {table_name};");
    let result = run_query(stream, &query);

    match result {
        Ok(Response::QuerySet(query_set)) => {
            println!("Table \"{table_name}\"");

            let col_width = query_set
                .schema
                .columns
                .iter()
                .map(|c| c.name.len())
                .max()
                .unwrap_or(0)
                .max(6);
            let type_width = query_set
                .schema
                .columns
                .iter()
                .map(|c| c.data_type.to_string().len())
                .max()
                .unwrap_or(0)
                .max(4);

            println!(
                " {0:<col_width$} | {1:<type_width$} | Nullable",
                "Column", "Type"
            );
            println!("-{0:-<col_width$}-+-{0:-<type_width$}-+----------", "");
            for col in &query_set.schema.columns {
                let nullable = match col.is_nullable() {
                    false => "not null",
                    _ => "",
                };

                println!(
                    " {name:<cw$} | {kind:<tw$} | {null}",
                    name = col.name,
                    kind = col.data_type.to_string(),
                    null = nullable,
                    cw = col_width,
                    tw = type_width
                );
            }
            println!();
        }
        Ok(Response::Err(msg)) => println!("Error: {msg}"),
        Ok(_) => println!("Unexpected response type."),
        Err(e) => println!("Network error: {e}"),
    }
}
