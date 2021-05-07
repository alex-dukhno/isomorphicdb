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

pub mod connection;

use crate::connection::{Connection, Established, New, Securing};
use native_tls::{Identity, TlsStream};
use std::{
    convert::TryInto,
    io,
    io::{Read, Write},
    net::TcpStream,
    str,
};
use wire_protocol_payload::{InboundMessage, OutboundMessage};

pub type WireResult = std::result::Result<InboundMessage, WireError>;

#[derive(Debug)]
pub struct WireError;

pub trait WireConnection {
    fn receive(&mut self) -> io::Result<WireResult>;

    fn send(&mut self, outbound: OutboundMessage) -> io::Result<()>;
}

pub struct PgWireAcceptor<S: Securing<TcpStream, TlsStream<TcpStream>>> {
    secured: Option<S>,
}

impl<S: Securing<TcpStream, TlsStream<TcpStream>>> PgWireAcceptor<S> {
    pub fn new(secured: Option<S>) -> PgWireAcceptor<S> {
        PgWireAcceptor { secured }
    }
}

impl PgWireAcceptor<Identity> {
    pub fn accept(&self, socket: TcpStream) -> io::Result<Connection<Established, TcpStream, TlsStream<TcpStream>>> {
        let connection: Connection<New, TcpStream, TlsStream<TcpStream>> = Connection::new(socket);
        let connection = connection.hand_shake::<Identity>(self.secured.clone())?;
        let connection = connection.authenticate("whatever")?;
        let connection = connection.send_params(&[
            ("client_encoding", "UTF8"),
            ("DateStyle", "ISO"),
            ("integer_datetimes", "off"),
            ("server_version", "13.0"),
        ])?;
        let mut connection = connection.send_backend_keys(1, 1)?;
        connection.send(OutboundMessage::ReadyForQuery)?;
        Ok(connection)
    }
}

pub struct ConnectionOld {
    socket: connection::Channel<TcpStream, TlsStream<TcpStream>>,
}

impl From<connection::Channel<TcpStream, TlsStream<TcpStream>>> for ConnectionOld {
    fn from(socket: connection::Channel<TcpStream, TlsStream<TcpStream>>) -> ConnectionOld {
        ConnectionOld { socket }
    }
}

impl ConnectionOld {
    fn parse_client_request(&mut self) -> io::Result<Result<InboundMessage, ()>> {
        let tag = self.read_tag()?;
        let len = self.read_message_len()?;
        let mut message = self.read_message(len)?;
        match tag {
            // Simple query flow.
            QUERY => {
                let sql = str::from_utf8(&message[0..message.len() - 1]).unwrap().to_owned();
                Ok(Ok(InboundMessage::Query { sql }))
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

                Ok(Ok(InboundMessage::Bind {
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
                    b'P' => Ok(Ok(InboundMessage::ClosePortal { name })),
                    b'S' => Ok(Ok(InboundMessage::CloseStatement { name })),
                    _other => unimplemented!(),
                }
            }
            DESCRIBE => {
                let first_char = message[0];
                let name = str::from_utf8(&message[1..message.len() - 1]).unwrap().to_owned();
                match first_char {
                    b'P' => Ok(Ok(InboundMessage::DescribePortal { name })),
                    b'S' => Ok(Ok(InboundMessage::DescribeStatement { name })),
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
                Ok(Ok(InboundMessage::Execute { portal_name, max_rows }))
            }
            FLUSH => Ok(Ok(InboundMessage::Flush)),
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

                Ok(Ok(InboundMessage::Parse {
                    statement_name,
                    sql,
                    param_types,
                }))
            }
            SYNC => Ok(Ok(InboundMessage::Sync)),
            TERMINATE => Ok(Ok(InboundMessage::Terminate)),

            _ => Ok(Err(())),
        }
    }

    fn read_tag(&mut self) -> io::Result<u8> {
        let buff = &mut [0u8; 1];
        self.socket.read_exact(buff.as_mut())?;
        Ok(buff[0])
    }

    fn read_message_len(&mut self) -> io::Result<usize> {
        let buff = &mut [0u8; 4];
        self.socket.read_exact(buff.as_mut())?;
        Ok((i32::from_be_bytes(*buff) as usize) - 4)
    }

    fn read_message(&mut self, len: usize) -> io::Result<Vec<u8>> {
        let mut message = vec![0; len];
        self.socket.read_exact(&mut message)?;
        Ok(message)
    }

    /// Receive client messages
    pub fn receive(&mut self) -> io::Result<Result<InboundMessage, ()>> {
        let request = match self.parse_client_request() {
            Ok(Ok(request)) => request,
            Ok(Err(_err)) => return Ok(Err(())),
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                // Client disconnected the socket immediately without sending a
                // Terminate message. Considers it as a client Terminate to save
                // resource and exit smoothly.
                InboundMessage::Terminate
            }
            Err(err) => return Err(err),
        };
        Ok(Ok(request))
    }
}

impl Sender for ConnectionOld {
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
pub trait Sender {
    /// Flushes the output stream.
    fn flush(&mut self) -> io::Result<()>;

    /// Sends response messages to client. Most of the time it is a single
    /// message, select result one of the exceptional situation
    fn send(&mut self, message: &[u8]) -> io::Result<()>;
}
