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
use postgres::wire_protocol::ConnectionOld;
use std::{
    env,
    io::{self, Write},
    net::TcpListener,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
};
use storage::Database;

mod query_engine;
mod session;

// const PORT: u16 = 5432;
// const HOST: [u8; 4] = [0, 0, 0, 0];

// const MIN_CONN_ID: i32 = 1;
// const MAX_CONN_ID: i32 = 1 << 16;

const READY_FOR_QUERY: u8 = b'Z';
const EMPTY_QUERY_RESPONSE: u8 = b'I';

pub fn start(database: Database) {
    let listener = TcpListener::bind("0.0.0.0:5432").expect("create listener");

    while let Ok((socket, _addr)) = listener.accept() {
        let db = database.clone();
        thread::spawn(move || -> io::Result<()> {
            use postgres::wire_protocol::connection::{Connection, Socket};

            let connection = Connection::new(Socket::from(socket));
            let connection = connection.hand_shake(None)?;
            let connection = connection.authenticate("whatever")?;
            let connection = connection.send_params(&[
                ("client_encoding", "UTF8"),
                ("DateStyle", "ISO"),
                ("integer_datetimes", "off"),
                ("server_version", "13.0"),
            ])?;
            let connection = connection.send_backend_keys(1, 1)?;
            let mut socket = connection.channel();

            socket.write_all(&[READY_FOR_QUERY, 0, 0, 0, 5, EMPTY_QUERY_RESPONSE])?;
            socket.flush()?;
            log::debug!("send ready for query");

            let connection = ConnectionOld::from(socket);

            let arc = Arc::new(Mutex::new(connection));
            let mut query_engine = QueryEngine::new(arc.clone(), db);
            log::debug!("ready to handle query");

            loop {
                let mut guard = arc.lock().unwrap();
                let result = guard.receive();
                drop(guard);
                log::debug!("{:?}", result);
                match result {
                    Err(e) => {
                        log::error!("UNEXPECTED ERROR: {:?}", e);
                        return Err(e);
                    }
                    Ok(Err(e)) => {
                        log::error!("UNEXPECTED ERROR: {:?}", e);
                        return Err(io::ErrorKind::InvalidInput.into());
                    }
                    Ok(Ok(client_request)) => match query_engine.execute(client_request) {
                        Ok(()) => {}
                        Err(_) => {
                            break Ok(());
                        }
                    },
                }
            }
        });
    }
}

#[allow(dead_code)]
fn pfx_certificate_path() -> PathBuf {
    let file = env::var("PFX_CERTIFICATE_FILE").unwrap();
    let path = Path::new(&file);
    if path.is_absolute() {
        return path.to_path_buf();
    }

    let current_dir = env::current_dir().unwrap();
    current_dir.as_path().join(path)
}

#[allow(dead_code)]
fn pfx_certificate_password() -> String {
    env::var("PFX_CERTIFICATE_PASSWORD").unwrap()
}
