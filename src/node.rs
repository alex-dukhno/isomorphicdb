use crate::protocol;
use crate::sql_handler;
use crate::storage;

use async_std::net::TcpListener;
use async_std::prelude::*;
use async_std::sync::{Arc, Mutex};
use async_std::task;
use std::sync::atomic::{AtomicU8, Ordering};

const PORT: usize = 5432;
const HOST: &str = "127.0.0.1";

pub const CREATED: u8 = 0;
pub const RUNNING: u8 = 1;
pub const STOPPED: u8 = 2;

pub struct Node {
    state: Arc<AtomicU8>,
}

impl Default for Node {
    fn default() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(CREATED)),
        }
    }
}

impl Node {
    pub fn state(&self) -> u8 {
        self.state.load(Ordering::SeqCst)
    }

    pub fn stop(&self) {
        self.state.store(STOPPED, Ordering::SeqCst);
    }

    pub fn start(&self) {
        let local_address = format!("{}:{}", HOST, PORT);
        trace!("Starting server on {}", local_address);

        task::block_on(async {
            let storage = Arc::new(Mutex::new(storage::relational::RelationalStorage::default()));
            let listener = TcpListener::bind(local_address.as_str()).await;
            trace!("Listening on {}", local_address);

            let listener = listener.expect("port should be open");
            self.state.store(RUNNING, Ordering::SeqCst);

            let mut incoming = listener.incoming();
            trace!("Waiting for connections");
            loop {
                let income = incoming.next();
                if self.state.load(Ordering::SeqCst) == STOPPED {
                    trace!("SHOULD BE STOPPED!");
                    break;
                }
                match income.await {
                    Some(Ok(stream)) => {
                        let state = self.state.clone();
                        let client_storage = storage.clone();
                        task::spawn(async move {
                            trace!("Accepted connection {:?}", stream.peer_addr());
                            match protocol::hand_shake::HandShake::new(
                                protocol::channel::Channel::new(stream.clone(), stream.clone()),
                            )
                            .perform()
                            .await
                            .expect("perform hand shake with client")
                            {
                                Ok(connection) => {
                                    let mut handler =
                                        sql_handler::Handler::new(client_storage, connection);
                                    while let Ok(true) = handler.handle_query().await {
                                        trace!("QUERY HANDLED!");
                                        if state.load(Ordering::SeqCst) == STOPPED {
                                            trace!("SHOULD BE STOPPED!");
                                            break;
                                        }
                                    }
                                }
                                Err(e) => error!("Error establishing protocol connection {:?}", e),
                            }
                        });
                    }
                    _ => break,
                }
            }
            trace!("WE ARE HERE!!!");
        })
    }
}
