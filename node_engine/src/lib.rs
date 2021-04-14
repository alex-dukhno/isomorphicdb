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
use byteorder::{BigEndian, ReadBytesExt};
use postgres::wire_protocol::Connection;
use std::convert::TryInto;
use std::io::{Read, Write};
use std::str;
use std::sync::Mutex;
use std::{
    env, io,
    net::TcpListener,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
};
use storage::Database;

mod query_engine;
mod session;

// const PORT: u16 = 5432;
// const HOST: [u8; 4] = [0, 0, 0, 0];

// const MIN_CONN_ID: i32 = 1;
// const MAX_CONN_ID: i32 = 1 << 16;

const AUTHENTICATION: u8 = b'R';
const PARAMETER_STATUS: u8 = b'S';
const READY_FOR_QUERY: u8 = b'Z';
const EMPTY_QUERY_RESPONSE: u8 = b'I';
const BACKEND_KEY_DATA: u8 = b'K';

pub fn start(database: Database) {
    let listener = TcpListener::bind("0.0.0.0:5432").expect("create listener");

    while let Ok((mut socket, _address)) = listener.accept() {
        let db = database.clone();
        thread::spawn(move || -> io::Result<()> {
            // connection hand shake
            let len = (socket.read_i32::<BigEndian>()? - 4) as usize;
            log::debug!("LEN {:?}", len);
            let mut request = vec![0; len];
            socket.read_exact(&mut request)?;
            let version = i32::from_be_bytes(request[0..4].try_into().unwrap());
            log::debug!("VERSION {:x?}", version);
            log::debug!("VERSION {:?}", version);
            log::debug!("request {:#?}", request);
            let mut message = &request[4..];
            let props = if version == 0x00_03_00_00 {
                let mut props = vec![];
                loop {
                    let key = if let Some(pos) = message.iter().position(|b| *b == 0) {
                        let key = str::from_utf8(&message[0..pos]).unwrap().to_owned();
                        message = &message[pos + 1..];
                        key
                    } else {
                        return Err(io::ErrorKind::InvalidInput.into());
                    };
                    if key.is_empty() {
                        break;
                    }
                    let value = if let Some(pos) = message.iter().position(|b| *b == 0) {
                        let value = str::from_utf8(&message[0..pos]).unwrap().to_owned();
                        message = &message[pos + 1..];
                        value
                    } else {
                        return Err(io::ErrorKind::InvalidInput.into());
                    };
                    props.push((key, value));
                }
                props
            } else if version == 80_877_103 {
                socket.write_all(&[b'N'])?;
                log::debug!("reject ssl");
                socket.flush()?;
                log::debug!("reject ssl FLUSH");
                let len = (socket.read_i32::<BigEndian>()? - 4) as usize;
                log::debug!("len {:?}", len);
                let mut request = vec![0; len];
                socket.read_exact(&mut request)?;
                let version = i32::from_be_bytes(request[0..4].try_into().unwrap());
                log::debug!("VERSION {:x?}", version);
                log::debug!("VERSION {:?}", version);
                log::debug!("request {:#?}", request);
                let mut message = &request[4..];
                if version == 0x00_03_00_00 {
                    let mut props = vec![];
                    loop {
                        let key = if let Some(pos) = message.iter().position(|b| *b == 0) {
                            let key = str::from_utf8(&message[0..pos]).unwrap().to_owned();
                            message = &message[pos + 1..];
                            key
                        } else {
                            return Err(io::ErrorKind::InvalidInput.into());
                        };
                        if key.is_empty() {
                            break;
                        }
                        let value = if let Some(pos) = message.iter().position(|b| *b == 0) {
                            let value = str::from_utf8(&message[0..pos]).unwrap().to_owned();
                            message = &message[pos + 1..];
                            value
                        } else {
                            return Err(io::ErrorKind::InvalidInput.into());
                        };
                        log::debug!("{:?} {:?}", key, value);
                        props.push((key, value));
                    }
                    props
                } else {
                    return Err(io::ErrorKind::InvalidInput.into());
                }
            } else {
                return Err(io::ErrorKind::InvalidInput.into());
            };
            log::debug!("PROPS {:?}", props);

            // authentication
            socket.write_all(&[AUTHENTICATION, 0, 0, 0, 8, 0, 0, 0, 3])?;
            socket.flush()?;
            let _tag = socket.read_u8()?;
            let len = (socket.read_i32::<BigEndian>()? - 4) as usize;
            let mut password = vec![0; len];
            socket.read_exact(&mut password)?;
            log::debug!("{:#?}", password);

            socket.write_all(&[AUTHENTICATION, 0, 0, 0, 8, 0, 0, 0, 0])?;
            socket.flush()?;

            fn create_param(key: &str, value: &str) -> Vec<u8> {
                let mut parameter_status_buff = Vec::new();
                parameter_status_buff.extend_from_slice(&[PARAMETER_STATUS]);
                let mut parameters = Vec::new();
                parameters.extend_from_slice(key.as_bytes());
                parameters.extend_from_slice(&[0]);
                parameters.extend_from_slice(value.as_bytes());
                parameters.extend_from_slice(&[0]);
                parameter_status_buff.extend_from_slice(&(4 + parameters.len() as u32).to_be_bytes());
                parameter_status_buff.extend_from_slice(parameters.as_ref());
                parameter_status_buff
            }

            socket.write_all(&create_param("client_encoding", "UTF8"))?;
            socket.write_all(&create_param("DateStyle", "ISO"))?;
            socket.write_all(&create_param("integer_datetimes", "off"))?;
            socket.write_all(&create_param("server_version", "12.4"))?;
            socket.flush()?;

            // sending connection id and its secret key if client wanted to cancel query
            let conn_id: i32 = 1;
            let secret_key: i32 = 1;
            let mut backend_key_data = vec![BACKEND_KEY_DATA, 0, 0, 0, 12];
            backend_key_data.extend_from_slice(&conn_id.to_be_bytes());
            backend_key_data.extend_from_slice(&secret_key.to_be_bytes());
            socket.write_all(&backend_key_data)?;
            socket.flush()?;

            socket.write_all(&[READY_FOR_QUERY, 0, 0, 0, 5, EMPTY_QUERY_RESPONSE])?;
            socket.flush()?;
            log::debug!("send ready for query");

            let connection = Connection::new(socket);

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
