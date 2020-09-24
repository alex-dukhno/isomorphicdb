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

use std::{
    env,
    net::TcpListener,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
};

use async_dup::Arc as AsyncArc;
use async_io::Async;

use crate::query_engine::QueryEngine;
use data_manager::DataManager;
use protocol::{ProtocolConfiguration, Receiver};

const PORT: u16 = 5432;
const HOST: [u8; 4] = [0, 0, 0, 0];

pub const RUNNING: u8 = 0;
pub const STOPPED: u8 = 1;

pub fn start() {
    let persistent = env::var("PERSISTENT").is_ok();
    let root_path = env::var("ROOT_PATH").map(PathBuf::from).unwrap_or_default();
    smol::block_on(async {
        let storage = if persistent {
            Arc::new(DataManager::persistent(root_path.join("root_directory")).unwrap())
        } else {
            Arc::new(DataManager::in_memory().unwrap())
        };
        let listener = Async::<TcpListener>::bind((HOST, PORT)).expect("OK");

        let state = Arc::new(AtomicU8::new(RUNNING));
        let config = protocol_configuration();

        while let Ok((tcp_stream, address)) = listener.accept().await {
            let tcp_stream = AsyncArc::new(tcp_stream);
            match protocol::hand_shake(tcp_stream, address, &config).await {
                Err(io_error) => log::error!("IO error {:?}", io_error),
                Ok(Err(protocol_error)) => log::error!("protocol error {:?}", protocol_error),
                Ok(Ok((mut receiver, sender))) => {
                    if state.load(Ordering::SeqCst) == STOPPED {
                        return;
                    }
                    let state = state.clone();
                    let storage = storage.clone();
                    let sender = Arc::new(sender);
                    let mut query_engine = QueryEngine::new(sender.clone(), storage.clone());
                    log::debug!("ready to handle query");
                    smol::spawn(async move {
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
                                Ok(Ok(command)) => match query_engine.execute(command) {
                                    Ok(()) => {
                                        // receiver.ready_for_query().await.expect("Ok");
                                    }
                                    Err(()) => {
                                        break;
                                    }
                                },
                            }
                        }
                    })
                    .detach();
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
