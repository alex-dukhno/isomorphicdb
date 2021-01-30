// Copyright 2020 - present Alex Dukhno
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

mod query_engine;

use crate::query_engine::QueryEngine;
use async_dup::Arc as AsyncArc;
use async_executor::Executor;
use async_io::Async;
use catalog::InMemoryDatabase;
use connection::ClientRequest;
use pg_model::{ConnSupervisor, ProtocolConfiguration};
use std::{
    env,
    net::TcpListener,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

const PORT: u16 = 5432;
const HOST: [u8; 4] = [0, 0, 0, 0];

const MIN_CONN_ID: i32 = 1;
const MAX_CONN_ID: i32 = 1 << 16;

pub fn start() {
    let _root_path = env::var("ROOT_PATH").map(PathBuf::from).unwrap_or_default();

    static GLOBAL: Executor<'_> = Executor::new();

    std::thread::Builder::new()
        .name("main-executor".to_owned())
        .spawn(|| loop {
            std::panic::catch_unwind(|| async_io::block_on(GLOBAL.run(futures_lite::future::pending::<()>()))).ok();
        })
        .expect("cannot spawn executor thread");

    async_io::block_on(async {
        let database = InMemoryDatabase::new();
        let listener = Async::<TcpListener>::bind((HOST, PORT)).expect("OK");

        let config = protocol_configuration();
        let conn_supervisor = Arc::new(Mutex::new(ConnSupervisor::new(MIN_CONN_ID, MAX_CONN_ID)));

        while let Ok((tcp_stream, address)) = listener.accept().await {
            let tcp_stream = AsyncArc::new(tcp_stream);
            match connection::accept_client_request(tcp_stream, address, &config, conn_supervisor.clone()).await {
                Err(io_error) => log::error!("IO error {:?}", io_error),
                Ok(Err(protocol_error)) => log::error!("protocol error {:?}", protocol_error),
                Ok(Ok(ClientRequest::Connection(mut receiver, sender))) => {
                    let mut query_engine = QueryEngine::new(sender, database.clone());
                    log::debug!("ready to handle query");
                    GLOBAL
                        .spawn(async move {
                            loop {
                                match receiver.receive().await {
                                    Err(e) => {
                                        log::error!("UNEXPECTED ERROR: {:?}", e);
                                        return;
                                    }
                                    Ok(Err(e)) => {
                                        log::error!("UNEXPECTED ERROR: {:?}", e);
                                        return;
                                    }
                                    Ok(Ok(command)) => match query_engine.execute(command) {
                                        Ok(()) => {}
                                        Err(()) => {
                                            break;
                                        }
                                    },
                                }
                            }
                        })
                        .detach();
                }
                Ok(Ok(ClientRequest::QueryCancellation(conn_id))) => {
                    // TODO: Needs to handle Cancel Request here.
                    log::debug!("cancel request of connection-{}", conn_id);
                }
            }
        }
    });
}

fn pfx_certificate_path() -> PathBuf {
    let file = env::var("PFX_CERTIFICATE_FILE").unwrap();
    let path = Path::new(&file);
    if path.is_absolute() {
        return path.to_path_buf();
    }

    let current_dir = env::current_dir().unwrap();
    current_dir.as_path().join(path)
}

fn pfx_certificate_password() -> String {
    env::var("PFX_CERTIFICATE_PASSWORD").unwrap()
}

fn protocol_configuration() -> ProtocolConfiguration {
    match env::var("SECURE") {
        Ok(s) => match s.to_lowercase().as_str() {
            "ssl_only" => ProtocolConfiguration::with_ssl(pfx_certificate_path(), pfx_certificate_password()),
            _ => ProtocolConfiguration::none(),
        },
        _ => ProtocolConfiguration::none(),
    }
}
