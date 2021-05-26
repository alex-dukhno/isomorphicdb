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

use crate::{WireConnection, WireError, WireResult};
use native_tls::{Identity, TlsAcceptor, TlsStream};
use std::{
    collections::HashMap,
    convert::TryInto,
    fmt::{self, Debug, Formatter},
    io::{self, Read, Write},
    net::TcpStream,
    str,
};
use wire_protocol_payload::{InboundMessage, OutboundMessage, BIND, CLOSE, DESCRIBE, EXECUTE, FLUSH, PARSE, QUERY, SYNC, TERMINATE};

const ACCEPT_SSL: u8 = b'S';
const REJECT_SSL: u8 = b'N';
const AUTHENTICATION: u8 = b'R';
const PARAMETER_STATUS: u8 = b'S';
const BACKEND_KEY_DATA: u8 = b'K';

pub trait Securing<P: Plain, S: Secure>: Clone {
    #[allow(clippy::result_unit_err)]
    fn secure(self, socket: P) -> Result<S, ()>;
}

impl Securing<TcpStream, TlsStream<TcpStream>> for Identity {
    fn secure(self, socket: TcpStream) -> Result<TlsStream<TcpStream>, ()> {
        TlsAcceptor::new(self).unwrap().accept(socket).map_err(|_| ())
    }
}

pub trait Plain: Read + Write {}

pub trait Secure: Read + Write {}

pub enum Channel<P: Plain, S: Secure> {
    Plain(P),
    Secure(S),
}

impl<P: Plain, S: Secure> Channel<P, S> {
    pub fn read_tag(&mut self) -> io::Result<u8> {
        let buff = &mut [0u8; 1];
        self.read_exact(buff.as_mut())?;
        Ok(buff[0])
    }

    pub fn read_message_len(&mut self) -> io::Result<usize> {
        let buff = &mut [0u8; 4];
        self.read_exact(buff.as_mut())?;
        Ok((i32::from_be_bytes(*buff) as usize) - 4)
    }

    pub fn read_message(&mut self, len: usize) -> io::Result<Vec<u8>> {
        let mut message = vec![0; len];
        self.read_exact(&mut message)?;
        Ok(message)
    }
}

impl<P: Plain, S: Secure> Read for Channel<P, S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Channel::Plain(socket) => socket.read(buf),
            Channel::Secure(socket) => socket.read(buf),
        }
    }
}

impl<P: Plain, S: Secure> Write for Channel<P, S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Channel::Plain(socket) => socket.write(buf),
            Channel::Secure(socket) => socket.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Channel::Plain(socket) => socket.flush(),
            Channel::Secure(socket) => socket.flush(),
        }
    }
}

pub struct Connection<State, P: Plain, S: Secure> {
    channel: Channel<P, S>,
    #[allow(dead_code)]
    state: State,
}

impl<State, P: Plain, S: Secure> Debug for Connection<State, P, S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Connection")
    }
}

impl<P: Plain, S: Secure> Connection<New, P, S> {
    pub fn new(socket: P) -> Connection<New, P, S> {
        Connection {
            channel: Channel::Plain(socket),
            state: New,
        }
    }
}

impl<P: Plain, S: Secure> Connection<New, P, S> {
    pub fn hand_shake<Sec: Securing<P, S>>(self, identity: Option<Sec>) -> io::Result<Connection<HandShake, P, S>> {
        println!("hand shake started");
        let mut channel = self.channel;
        let len = channel.read_message_len()?;
        let request = channel.read_message(len)?;
        let (version, message) = Self::parse_setup(&request);
        let props = match version {
            0x00_03_00_00 => Self::parse_props(&message)?,
            80_877_103 => {
                channel = match (channel, identity) {
                    (Channel::Plain(mut socket), Some(identity)) => {
                        socket.write_all(&[ACCEPT_SSL])?;
                        println!("accepting ssl!");
                        let secure_socket = match identity.secure(socket) {
                            Ok(socket) => socket,
                            Err(()) => {
                                println!("shrug!");
                                return Err(io::ErrorKind::InvalidInput.into());
                            }
                        };
                        Channel::Secure(secure_socket)
                    }
                    (mut channel, _) => {
                        channel.write_all(&[REJECT_SSL])?;
                        channel
                    }
                };
                channel.flush()?;
                let len = channel.read_message_len()?;
                let request = channel.read_message(len)?;
                let (version, message) = Self::parse_setup(&request);
                println!("ver {:?}", version);
                println!("ver {:x?}", version);
                match version {
                    0x00_03_00_00 => Self::parse_props(&message)?,
                    _ => unimplemented!(),
                }
            }
            _ => unimplemented!(),
        };

        println!("hand shake complete");

        Ok(Connection {
            channel,
            state: HandShake {
                props: props.into_iter().collect(),
            },
        })
    }

    fn parse_props(message: &[u8]) -> io::Result<Vec<(String, String)>> {
        fn read_cstr(mut message: &[u8]) -> io::Result<(String, &[u8])> {
            if let Some(pos) = message.iter().position(|b| *b == 0) {
                let key = str::from_utf8(&message[0..pos]).unwrap().to_owned();
                message = &message[pos + 1..];
                Ok((key, message))
            } else {
                Err(io::ErrorKind::InvalidInput.into())
            }
        }

        let mut req = message;
        let mut props = vec![];
        loop {
            let (key, message) = read_cstr(req)?;
            req = message;
            if key.is_empty() {
                break;
            }
            let (value, message) = read_cstr(req)?;
            req = message;
            props.push((key, value));
        }
        Ok(props)
    }

    fn parse_setup(message: &[u8]) -> (i32, &[u8]) {
        let version = i32::from_be_bytes(message[0..4].try_into().unwrap());
        let message = &message[4..];
        (version, message)
    }
}

impl<P: Plain, S: Secure> Connection<HandShake, P, S> {
    pub fn authenticate(mut self, _password: &str) -> io::Result<Connection<Authenticated, P, S>> {
        self.channel.write_all(&[AUTHENTICATION, 0, 0, 0, 8, 0, 0, 0, 3])?;
        self.channel.flush()?;

        let _tag = self.channel.read_tag()?;
        let len = self.channel.read_message_len()?;
        let _message = self.channel.read_message(len)?;

        // we are ok with any password that user sent
        self.channel.write_all(&[AUTHENTICATION, 0, 0, 0, 8, 0, 0, 0, 0])?;
        self.channel.flush()?;

        log::debug!("auth ok");

        Ok(Connection {
            channel: self.channel,
            state: Authenticated,
        })
    }
}

impl<P: Plain, S: Secure> Connection<Authenticated, P, S> {
    pub fn send_params(mut self, params: &[(&str, &str)]) -> io::Result<Connection<AllocateBackendKey, P, S>> {
        for (key, value) in params {
            let len: i32 = 4 + (key.len() as i32) + 1 + (value.len() as i32) + 1;
            let mut buff = vec![];
            buff.extend_from_slice(&[PARAMETER_STATUS]);
            buff.extend_from_slice(&len.to_be_bytes());
            buff.extend_from_slice(key.as_bytes());
            buff.extend_from_slice(&[0]);
            buff.extend_from_slice(value.as_bytes());
            buff.extend_from_slice(&[0]);
            self.channel.write_all(&buff)?;
            self.channel.flush()?;
        }
        Ok(Connection {
            channel: self.channel,
            state: AllocateBackendKey,
        })
    }
}

impl<P: Plain, S: Secure> Connection<AllocateBackendKey, P, S> {
    pub fn send_backend_keys(mut self, conn_id: u32, conn_secret_key: u32) -> io::Result<Connection<Established, P, S>> {
        self.channel.write_all(&[BACKEND_KEY_DATA])?;
        self.channel.write_all(&12i32.to_be_bytes())?;
        self.channel.write_all(&conn_id.to_be_bytes())?;
        self.channel.write_all(&conn_secret_key.to_be_bytes())?;
        self.channel.flush()?;

        Ok(Connection {
            channel: self.channel,
            state: Established,
        })
    }
}

impl<P: Plain, S: Secure> Connection<Established, P, S> {
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
        self.channel.read_exact(buff.as_mut())?;
        Ok(buff[0])
    }

    fn read_message_len(&mut self) -> io::Result<usize> {
        let buff = &mut [0u8; 4];
        self.channel.read_exact(buff.as_mut())?;
        Ok((i32::from_be_bytes(*buff) as usize) - 4)
    }

    fn read_message(&mut self, len: usize) -> io::Result<Vec<u8>> {
        let mut message = vec![0; len];
        self.channel.read_exact(&mut message)?;
        Ok(message)
    }
}

impl<P: Plain, S: Secure> WireConnection for Connection<Established, P, S> {
    fn receive(&mut self) -> io::Result<WireResult> {
        let request = match self.parse_client_request() {
            Ok(Ok(request)) => request,
            Ok(Err(_err)) => return Ok(Err(WireError)),
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

    fn send(&mut self, outbound: OutboundMessage) -> io::Result<()> {
        let buff: Vec<u8> = outbound.into();
        self.channel.write_all(&buff)?;
        self.channel.flush()
    }
}

#[derive(Debug)]
pub struct New;

#[derive(Debug)]
pub struct HandShake {
    props: HashMap<String, String>,
}

#[derive(Debug)]
pub struct Authenticated;

#[derive(Debug)]
pub struct AllocateBackendKey;

#[derive(Debug)]
pub struct Established;

impl Plain for TcpStream {}

impl Secure for TlsStream<TcpStream> {}

#[cfg(test)]
mod tests;
