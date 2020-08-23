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

use async_dup::Arc as AsyncArc;
use async_io::Async;
use protocol::{Command, ProtocolConfiguration, Receiver};
use smol::{self, block_on, Task};
use sql_engine::{catalog_manager::CatalogManager, QueryExecutor};
use std::{
    env,
    net::TcpListener,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
};

const PORT: u16 = 5432;
const HOST: [u8; 4] = [0, 0, 0, 0];

pub const RUNNING: u8 = 0;
pub const STOPPED: u8 = 1;

pub fn start() {
    let persistent = env::var("PERSISTENT").is_ok();
    block_on(async {
        let storage = if persistent {
            Arc::new(CatalogManager::persistent(PathBuf::new().join("database")).unwrap())
        } else {
            Arc::new(CatalogManager::in_memory().unwrap())
        };
        let listener = Async::<TcpListener>::bind((HOST, PORT)).expect("OK");

        let state = Arc::new(AtomicU8::new(RUNNING));
        let config = protocol_configuration();

        while let Ok((tcp_stream, address)) = listener.accept().await {
            let tcp_stream = AsyncArc::new(tcp_stream);
            if let Ok((mut receiver, sender)) = protocol::hand_shake(tcp_stream, address, &config)
                .await
                .expect("no io errors")
            {
                if state.load(Ordering::SeqCst) == STOPPED {
                    return;
                }
                let state = state.clone();
                let storage = storage.clone();
                let sender = Arc::new(sender);
                let s = sender.clone();
                Task::spawn(async move {
                    let mut query_executor = QueryExecutor::new(storage.clone(), s);
                    log::debug!("ready to handle query");

                    Task::spawn(async move {
                        loop {
                            match receiver.receive().await {
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
                                Ok(Ok(Command::Continue)) => {}
                                Ok(Ok(Command::DescribeStatement(name))) => {
                                    match query_executor.describe_prepared_statement(name.as_str()) {
                                        Ok(()) => {}
                                        Err(error) => log::error!("{:?}", error),
                                    }
                                }
                                Ok(Ok(Command::Flush)) => query_executor.flush(),
                                Ok(Ok(Command::Parse(statement_name, sql_query, param_types))) => {
                                    match query_executor.parse(
                                        statement_name.as_str(),
                                        sql_query.as_str(),
                                        param_types.as_ref(),
                                    ) {
                                        Ok(()) => {}
                                        Err(error) => log::error!("{:?}", error),
                                    }
                                }
                                Ok(Ok(Command::Query(sql_query))) => match query_executor.execute(sql_query.as_str()) {
                                    Ok(()) => {}
                                    Err(error) => log::error!("{:?}", error),
                                },
                                Ok(Ok(Command::Terminate)) => {
                                    log::debug!("Closing connection with client");
                                    break;
                                }
                            }
                        }
                    })
                    .detach();
                })
                .detach();
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
