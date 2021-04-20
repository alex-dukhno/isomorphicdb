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

use native_tls::{Identity, TlsAcceptor, TlsStream};
use std::{
    collections::HashMap,
    convert::TryInto,
    fmt::{self, Debug, Formatter},
    io::{self, Read, Write},
    net::TcpStream,
    str,
};

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
    pub fn send_backend_keys(
        mut self,
        conn_id: u32,
        conn_secret_key: u32,
    ) -> io::Result<Connection<Established, P, S>> {
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
    pub fn channel(self) -> Channel<P, S> {
        self.channel
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
