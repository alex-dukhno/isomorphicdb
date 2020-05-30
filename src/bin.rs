#[macro_use]
extern crate log;
extern crate database;
extern crate pretty_env_logger;

use database::protocol;
use database::sql_handler;
use database::storage;

use std::net::TcpListener;

use futures::io;
use piper::{Arc, Mutex};
use smol::{Async, Task};

const PORT: usize = 5432;
const HOST: &str = "127.0.0.1";

fn main() -> io::Result<()> {
    let local_address = format!("{}:{}", HOST, PORT);
    pretty_env_logger::init();
    info!("Starting server on {}", local_address);

    smol::run(async {
        let storage = Arc::new(Mutex::new(storage::SledStorage::new()));
        let listener = Async::<TcpListener>::bind(local_address.as_str())?;
        info!("Listening on {}", local_address);

        loop {
            let (stream, peer_address) = listener.accept().await?;
            let client_storage = storage.clone();
            trace!("Accepted connection {}", peer_address);
            let client = Arc::new(Mutex::new(stream));

            Task::spawn(async move {
                match protocol::hand_shake::HandShake::new(client.clone(), client.clone())
                    .perform()
                    .await
                    .expect("perform hand shake with client")
                {
                    Ok(connection) => {
                        let mut handler = sql_handler::Handler::new(client_storage, connection);
                        while let Ok(true) = handler.handle_query().await {}
                    }
                    Err(e) => error!("Error establishing protocol connection {:?}", e),
                }
            })
            .detach()
        }
    })
}
