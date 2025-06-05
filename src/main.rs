use std::{env, net::SocketAddr};

fn main() -> umbra::Result<()> {
    let file = env::args().nth(1).expect("Database file not provided");

    let port = env::args()
        .nth(2)
        .map(|port| port.parse::<u16>().expect("Invalid port number"))
        .unwrap_or(8000);
    let address = SocketAddr::from(([127, 0, 0, 1], port));

    umbra::tcp::server::start(address, file)
}
