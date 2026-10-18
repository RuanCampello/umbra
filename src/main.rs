use std::{env, net::SocketAddr};
use umbra::db::Budget;

fn main() -> umbra::Result<()> {
    let file = env::args().nth(1).expect("Database file not provided");

    let port = env::args()
        .nth(2)
        .map(|port| port.parse::<u16>().expect("Invalid port number"))
        .unwrap_or(8000);
    let address = SocketAddr::from(([127, 0, 0, 1], port));

    // `512M` or `4G` of table data, or a count of 1024-row chunks
    let budget = env::args().nth(3).map(|budget| {
        let parse = |number: &str| number.parse::<usize>().expect("Invalid memory budget");
        match budget.as_bytes().last() {
            Some(b'M') => Budget::Bytes(parse(&budget[..budget.len() - 1]) << 20),
            Some(b'G') => Budget::Bytes(parse(&budget[..budget.len() - 1]) << 30),
            _ => Budget::Chunks(parse(&budget)),
        }
    });

    umbra::tcp::server::start_with_budget(address, file, budget)
}
