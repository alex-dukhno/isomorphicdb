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
use sql_engine::Handler;
use std::env;
use std::sync::{Arc, Mutex};
use storage::backend::SledBackendStorage;
use storage::frontend::FrontendStorage;

const PORT: usize = 5432;
const HOST: &str = "0.0.0.0";

pub fn start() {
    let local_address = format!("{}:{}", HOST, PORT);
    log::debug!("Starting server on {}", local_address);

    let storage: Arc<Mutex<FrontendStorage<SledBackendStorage>>> =
        Arc::new(Mutex::new(FrontendStorage::default().unwrap()));

    smol::run(async {
        let secure = match env::var("SECURE") {
            Ok(s) => match s.to_lowercase().as_str() {
                "ssl_only" => Secure::ssl_only(),
                "gssenc_only" => Secure::gssenc_only(),
                "both" => Secure::both(),
                _ => Secure::none(),
            },
            _ => Secure::none(),
        };

        let listener = SmolQueryListener::bind(local_address, secure)
            .await
            .expect("open server connection");

        log::debug!("start server");
        while let Ok(mut connection) = listener.accept().await.expect("no io errors") {
            let mut sql_handler = Handler::new(storage.clone());
            match connection.receive().await {
                Err(e) => {
                    log::error!("UNEXPECTED ERROR: {:?}", e);
                    return;
                }
                Ok(Err(e)) => {
                    log::error!("UNEXPECTED ERROR: {:?}", e);
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
            listener.start().await.unwrap().unwrap();
        }
    });
}
