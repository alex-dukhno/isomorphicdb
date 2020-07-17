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
use protocol::{listener::ProtocolConfiguration, Command, QueryListener};
use smol::Task;
use sql_engine::Handler;
use std::{
    env,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Mutex,
    },
};
use storage::{backend::SledBackendStorage, frontend::FrontendStorage};

const PORT: usize = 5432;
const HOST: &str = "0.0.0.0";

pub const RUNNING: u8 = 0;
pub const STOPPED: u8 = 1;

pub fn start() {
    smol::run(async {
        let local_address = format!("{}:{}", HOST, PORT);
        log::debug!("Starting server on {}", local_address);

        let listener = SmolQueryListener::bind(local_address, protocol_configuration())
            .await
            .expect("open server connection");

        let state = Arc::new(AtomicU8::new(RUNNING));
        let storage: Arc<Mutex<FrontendStorage<SledBackendStorage>>> =
            Arc::new(Mutex::new(FrontendStorage::default().unwrap()));

        while let Ok(mut connection) = listener.accept().await.expect("no io errors") {
            if state.load(Ordering::SeqCst) == STOPPED {
                return;
            }

            let state = state.clone();
            let storage = storage.clone();
            Task::spawn(async move {
                let mut sql_handler = Handler::new(storage.clone());
                log::debug!("ready to handle query");

                loop {
                    match connection.receive().await {
                        Err(e) => {
                            log::error!("UNEXPECTED ERROR: {:?}", e);
                            state.store(STOPPED, Ordering::SeqCst);
                            return;
                        }
                        Ok(Err(e)) => {
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

fn protocol_configuration() -> ProtocolConfiguration {
    match env::var("SECURE") {
        Ok(s) => match s.to_lowercase().as_str() {
            "ssl_only" => ProtocolConfiguration::ssl_only(),
            "gssenc_only" => ProtocolConfiguration::gssenc_only(),
            "both" => ProtocolConfiguration::both(),
            _ => ProtocolConfiguration::none(),
        },
        _ => ProtocolConfiguration::none(),
    }
}
