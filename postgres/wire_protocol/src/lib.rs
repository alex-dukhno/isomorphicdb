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

use byteorder::{BigEndian, ReadBytesExt};
use std::{
    convert::TryInto,
    io,
    io::{Read, Write},
    net::TcpStream,
    str,
};

const QUERY: u8 = b'Q';
const BIND: u8 = b'B';
const CLOSE: u8 = b'C';
const DESCRIBE: u8 = b'D';
const EXECUTE: u8 = b'E';
const FLUSH: u8 = b'H';
const PARSE: u8 = b'P';
const SYNC: u8 = b'S';
const TERMINATE: u8 = b'X';

#[derive(Debug)]
pub enum Request {
    Query {
        sql: String,
    },
    Bind {
        portal_name: String,
        statement_name: String,
        query_param_formats: Vec<i16>,
        query_params: Vec<Option<Vec<u8>>>,
        result_value_formats: Vec<i16>,
    },
    ClosePortal {
        name: String,
    },
    CloseStatement {
        name: String,
    },
    DescribePortal {
        name: String,
    },
    DescribeStatement {
        name: String,
    },
    Execute {
        portal_name: String,
        max_rows: i32,
    },
    Flush,
    Parse {
        statement_name: String,
        sql: String,
        param_types: Vec<u32>,
    },
    Sync,
    Terminate,
}

pub struct Connection {
    socket: TcpStream,
}

impl Connection {
    /// Create new Connection
    pub fn new(socket: TcpStream) -> Connection {
        Connection { socket }
    }

    fn parse_client_request(&mut self) -> io::Result<Result<Request, ()>> {
        let tag = self.socket.read_u8()?;
        let len = (self.socket.read_i32::<BigEndian>()? - 4) as usize;
        let mut message = vec![0; len];
        self.socket.read_exact(&mut message)?;
        match tag {
            // Simple query flow.
            QUERY => {
                let sql = str::from_utf8(&message[0..message.len() - 1]).unwrap().to_owned();
                Ok(Ok(Request::Query { sql }))
            }

            // Extended query flow.
            BIND => {
                let portal_name = if let Some(pos) = message.iter().position(|b| *b == 0) {
                    let portal_name = str::from_utf8(&message[0..pos]).unwrap().to_owned();
                    message = message[pos + 1..].to_vec();
                    portal_name
                } else {
                    unimplemented!()
                };

                let statement_name = if let Some(pos) = message.iter().position(|b| *b == 0) {
                    let statement_name = str::from_utf8(&message[0..pos]).unwrap().to_owned();
                    message = message[pos + 1..].to_vec();
                    statement_name
                } else {
                    unimplemented!()
                };

                let param_formats_len = i16::from_be_bytes(message[0..2].try_into().unwrap());
                message = message[2..].to_vec();
                let mut query_param_formats = vec![];
                for _ in 0..param_formats_len {
                    query_param_formats.push(i16::from_be_bytes(message[0..2].try_into().unwrap()));
                    message = message[2..].to_vec();
                }

                let params_len = i16::from_be_bytes(message[0..2].try_into().unwrap());
                let mut query_params = vec![];
                message = message[2..].to_vec();
                for _ in 0..params_len {
                    let len = i32::from_be_bytes(message[0..4].try_into().unwrap());
                    message = message[4..].to_vec();
                    if len == -1 {
                        // As a special case, -1 indicates a NULL parameter value.
                        query_params.push(None);
                    } else {
                        let value = message[0..(len as usize)].to_vec();
                        query_params.push(Some(value));
                        message = message[(len as usize)..].to_vec();
                    }
                }

                let result_value_formats_len = i16::from_be_bytes(message[0..2].try_into().unwrap());
                let mut result_value_formats = vec![];
                message = message[2..].to_vec();
                for _ in 0..result_value_formats_len {
                    result_value_formats.push(i16::from_be_bytes(message[0..2].try_into().unwrap()));
                    message = message[2..].to_vec();
                }

                Ok(Ok(Request::Bind {
                    portal_name,
                    statement_name,
                    query_param_formats,
                    query_params,
                    result_value_formats,
                }))
            }
            CLOSE => {
                let first_char = message[0];
                let name = str::from_utf8(&message[1..message.len() - 1]).unwrap().to_owned();
                match first_char {
                    b'P' => Ok(Ok(Request::ClosePortal { name })),
                    b'S' => Ok(Ok(Request::CloseStatement { name })),
                    _other => unimplemented!(),
                }
            }
            DESCRIBE => {
                let first_char = message[0];
                let name = str::from_utf8(&message[1..message.len() - 1]).unwrap().to_owned();
                match first_char {
                    b'P' => Ok(Ok(Request::DescribePortal { name })),
                    b'S' => Ok(Ok(Request::DescribeStatement { name })),
                    _other => unimplemented!(),
                }
            }
            EXECUTE => {
                let portal_name = if let Some(pos) = message.iter().position(|b| *b == 0) {
                    let portal_name = str::from_utf8(&message[0..pos]).unwrap().to_owned();
                    message = message[pos + 1..].to_vec();
                    portal_name
                } else {
                    unimplemented!()
                };
                let max_rows = i32::from_be_bytes(message[0..4].try_into().unwrap());
                Ok(Ok(Request::Execute { portal_name, max_rows }))
            }
            FLUSH => Ok(Ok(Request::Flush)),
            PARSE => {
                let statement_name = if let Some(pos) = message.iter().position(|b| *b == 0) {
                    let statement_name = str::from_utf8(&message[0..pos]).unwrap().to_owned();
                    message = message[pos + 1..].to_vec();
                    statement_name
                } else {
                    unimplemented!()
                };
                let sql = if let Some(pos) = message.iter().position(|b| *b == 0) {
                    let sql = str::from_utf8(&message[0..pos]).unwrap().to_owned();
                    message = message[pos + 1..].to_vec();
                    sql
                } else {
                    unimplemented!()
                };

                let param_types_len = i16::from_be_bytes(message[0..2].try_into().unwrap());
                let mut param_types = vec![];
                message = message[2..].to_vec();
                for _ in 0..param_types_len {
                    let pg_type = u32::from_be_bytes(message[0..4].try_into().unwrap());
                    param_types.push(pg_type);
                    message = message[4..].to_vec();
                }

                Ok(Ok(Request::Parse {
                    statement_name,
                    sql,
                    param_types,
                }))
            }
            SYNC => Ok(Ok(Request::Sync)),
            TERMINATE => Ok(Ok(Request::Terminate)),

            _ => Ok(Err(())),
        }
    }

    /// Receive client messages
    pub fn receive(&mut self) -> io::Result<Result<Request, ()>> {
        let request = match self.parse_client_request() {
            Ok(Ok(request)) => request,
            Ok(Err(_err)) => return Ok(Err(())),
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                // Client disconnected the socket immediately without sending a
                // Terminate message. Considers it as a client Terminate to save
                // resource and exit smoothly.
                Request::Terminate
            }
            Err(err) => return Err(err),
        };
        Ok(Ok(request))
    }
}

impl Sender for Connection {
    fn flush(&mut self) -> io::Result<()> {
        self.socket.flush()
    }

    fn send(&mut self, message: &[u8]) -> io::Result<()> {
        self.socket.write_all(message)?;
        self.socket.flush()
    }
}

/// Trait to handle server to client query results for PostgreSQL Wire Protocol
/// connection
pub trait Sender: Send + Sync {
    /// Flushes the output stream.
    fn flush(&mut self) -> io::Result<()>;

    /// Sends response messages to client. Most of the time it is a single
    /// message, select result one of the exceptional situation
    fn send(&mut self, message: &[u8]) -> io::Result<()>;
}