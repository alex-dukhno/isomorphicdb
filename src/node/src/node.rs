// Copyright 2020 Alex Dukhno
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::query_listener::SmolQueryListener;
use protocol::{listener::Secure, Command, QueryListener};
use smol::Task;
use sql_engine::Handler;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, Mutex,
};

const PORT: usize = 5432;
const HOST: &str = "0.0.0.0";

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
        log::debug!("Starting server on {}", local_address);

        smol::run(async {
            let listener = SmolQueryListener::bind(local_address, Secure::none())
                .await
                .expect("open server connection");
            self.state.store(RUNNING, Ordering::SeqCst);

            let storage = Arc::new(Mutex::new(storage::frontend::FrontendStorage::default().unwrap()));

            log::debug!("waiting for connections");
            while let Ok(mut connection) = listener.accept().await.expect("no io errors") {
                if self.state() == STOPPED {
                    return;
                }
                let state = self.state.clone();
                let storage = storage.clone();
                Task::spawn(async move {
                    let mut sql_handler = Handler::new(storage);

                    log::debug!("ready to handle query");
                    loop {
                        match connection.receive().await {
                            Err(e) => {
                                log::debug!("SHOULD STOP");
                                log::error!("UNEXPECTED ERROR: {:?}", e);
                                state.store(STOPPED, Ordering::SeqCst);
                                return;
                            }
                            Ok(Err(e)) => {
                                log::debug!("SHOULD STOP");
                                log::error!("UNEXPECTED ERROR: {:?}", e);
                                state.store(STOPPED, Ordering::SeqCst);
                                return;
                            }
                            Ok(Ok(Command::Terminate)) => {
                                log::debug!("Closing connection with client");
                                break;
                            }
                            Ok(Ok(Command::Query(sql_query))) => {
                                let response = sql_handler.execute(sql_query.as_str()).expect("no system error");
                                match connection.send(response).await {
                                    Ok(()) => {}
                                    Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                }
                            }
                        }
                    }
                })
                .detach();
            }
        });
    }
}
