// Copyright 2020 - 2021 Alex Dukhno
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

use crate::query_engine::QueryEngine;
use async_executor::Executor;
use async_io::Async;
use async_mutex::Mutex as AsyncMutex;
use futures_lite::{future, AsyncReadExt, AsyncWriteExt};
use postgres::wire_protocol::payload::BackendMessage;
use postgres::wire_protocol::{ClientRequest, ConnSupervisor, Connection, PgWireListener, ProtocolConfiguration};
use std::sync::Arc;
use std::{
    env,
    net::TcpListener,
    panic,
    path::{Path, PathBuf},
    thread,
};
use storage::Database;

mod query_engine;
mod session;

const PORT: u16 = 5432;
const HOST: [u8; 4] = [0, 0, 0, 0];

const MIN_CONN_ID: i32 = 1;
const MAX_CONN_ID: i32 = 1 << 16;

pub fn start(database: Database) {
    static NETWORK: Executor<'_> = Executor::new();

    thread::Builder::new()
        .name("network-thread".into())
        .spawn(|| loop {
            panic::catch_unwind(|| future::block_on(NETWORK.run(future::pending::<()>()))).ok();
        })
        .expect("cannot spawn executor thread");

    static WORKER: Executor<'_> = Executor::new();
    for thread_id in 0..8 {
        thread::Builder::new()
            .name(format!("worker-{}-thread", thread_id))
            .spawn(|| loop {
                panic::catch_unwind(|| future::block_on(WORKER.run(future::pending::<()>()))).ok();
            })
            .expect("cannot spawn executor thread");
    }

    async_io::block_on(async {
        let listener = Async::<TcpListener>::bind((HOST, PORT)).expect("OK");

        let config = protocol_configuration();
        let conn_supervisor = ConnSupervisor::new(MIN_CONN_ID, MAX_CONN_ID);
        let connection_manager = PgWireListener::new(listener, config, conn_supervisor);

        loop {
            let client_request = connection_manager.accept().await;
            match client_request {
                Err(io_error) => log::error!("IO error {:?}", io_error),
                Ok(Err(protocol_error)) => log::error!("protocol error {:?}", protocol_error),
                Ok(Ok(ClientRequest::Connect((mut channel, props, conn_supervisor, address)))) => {
                    channel
                        .write_all(BackendMessage::AuthenticationCleartextPassword.as_vec().as_slice())
                        .await
                        .expect("to ask for password in clear text format");
                    channel.flush().await.expect("to flush the buffer");

                    //TODO: use message decoder for Auth messages
                    let mut tag_buffer = [0u8; 1];
                    let _tag = channel.read_exact(&mut tag_buffer).await.map(|_| tag_buffer[0]);
                    let mut len_buffer = [0u8; 4];
                    let len = channel
                        .read_exact(&mut len_buffer)
                        .await
                        .map(|_| u32::from_be_bytes(len_buffer) as usize)
                        .expect("to read message length");
                    let len = len - 4;
                    let mut message_buffer = Vec::with_capacity(len);
                    message_buffer.resize(len, b'0');
                    let _message = channel
                        .read_exact(&mut message_buffer)
                        .await
                        .map(|_| message_buffer)
                        .expect("to read message body");

                    // we are ok with any password that user sent
                    channel
                        .write_all(BackendMessage::AuthenticationOk.as_vec().as_slice())
                        .await
                        .expect("Auth Ok");

                    // pretend to be a PostgreSQL version 12.4
                    channel
                        .write_all(
                            BackendMessage::ParameterStatus("server_version".to_owned(), "12.4".to_owned())
                                .as_vec()
                                .as_slice(),
                        )
                        .await
                        .expect("send server version");

                    let (conn_id, secret_key) = match conn_supervisor.alloc() {
                        Ok((c, s)) => (c, s),
                        Err(()) => {
                            eprintln!("Cannot allocate connection and its secret key");
                            return;
                        }
                    };

                    // sending connection id and its secret key if client wanted to cancel query
                    channel
                        .write_all(BackendMessage::BackendKeyData(conn_id, secret_key).as_vec().as_slice())
                        .await
                        .expect("to send connection id and secret key");

                    channel
                        .write_all(BackendMessage::ReadyForQuery.as_vec().as_slice())
                        .await
                        .expect("to notify that we ready to handle query");

                    let channel = Arc::new(AsyncMutex::new(channel));
                    let mut connection = Connection::new(conn_id, props, address, channel, conn_supervisor);

                    let mut query_engine = QueryEngine::new(Arc::new(connection.sender()), database.clone());
                    log::debug!("ready to handle query");
                    WORKER
                        .spawn(async move {
                            loop {
                                match connection.receive().await {
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
                                        Err(_) => {
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
            _ => ProtocolConfiguration::not_secure(),
        },
        _ => ProtocolConfiguration::not_secure(),
    }
}
