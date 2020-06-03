#[macro_use]
extern crate log;
extern crate database;
extern crate pretty_env_logger;

use database::protocol;
use database::sql_handler;
use database::storage;

use async_std::net::TcpListener;
use async_std::prelude::*;
use async_std::sync::{Arc, Mutex};
use async_std::task;

const PORT: usize = 5432;
const HOST: &str = "127.0.0.1";

fn main() {
    let local_address = format!("{}:{}", HOST, PORT);
    pretty_env_logger::init();
    info!("Starting server on {}", local_address);

    task::block_on(async {
        let storage = Arc::new(Mutex::new(storage::SledStorage::default()));
        let listener = TcpListener::bind(local_address.as_str()).await.unwrap();
        info!("Listening on {}", local_address);

        let mut incoming = listener.incoming();
        while let Some(Ok(stream)) = incoming.next().await {
            let client_storage = storage.clone();
            task::spawn(async move {
                trace!("Accepted connection {:?}", stream.peer_addr());
                match protocol::hand_shake::HandShake::new(protocol::channel::Channel::new(
                    stream.clone(),
                    stream.clone(),
                ))
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
            });
        }
    })
}
