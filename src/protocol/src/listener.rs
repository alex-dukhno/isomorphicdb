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

use crate::{
    messages::{Encryption, Message},
    Channel, Connection, Error, Params, Result, Version, VERSION_1, VERSION_2, VERSION_3, VERSION_CANCEL,
    VERSION_GSSENC, VERSION_SSL,
};
use async_trait::async_trait;
use byteorder::{ByteOrder, NetworkEndian};
use futures_util::io::{self, AsyncReadExt, AsyncWriteExt};
use itertools::Itertools;
use std::{net::SocketAddr, pin::Pin};

/// Listener trait that use underline network to `accept` queries from clients
#[async_trait]
pub trait QueryListener {
    /// ServerChannel that listens to client connections and creates bidirectional `Channels`
    type ServerChannel: ServerListener + Unpin + Send + Sync;

    /// accepts incoming client connections
    async fn accept(&self) -> io::Result<Result<Connection>> {
        let (mut socket, address) = self.server_channel().tcp_channel().await?;
        log::debug!("ADDRESS {:?}", address);

        loop {
            let len = read_len(&mut socket).await?;
            let message = read_message(len, &mut socket).await?;
            log::debug!("MESSAGE FOR TEST = {:#?}", message);

            match decode_startup(message) {
                Ok(ClientHandshake::Startup(version, params)) => {
                    socket
                        .write_all(Message::AuthenticationCleartextPassword.as_vec().as_slice())
                        .await?;
                    let mut buffer = [0u8; 1];
                    let tag = socket.read_exact(&mut buffer).await.map(|_| buffer[0]);
                    log::debug!("client message response tag {:?}", tag);
                    log::debug!("waiting for authentication response");
                    let len = read_len(&mut socket).await?;
                    let _message = read_message(len, &mut socket).await?;
                    socket.write_all(Message::AuthenticationOk.as_vec().as_slice()).await?;

                    socket
                        .write_all(
                            Message::ParameterStatus("client_encoding".to_owned(), "UTF8".to_owned())
                                .as_vec()
                                .as_slice(),
                        )
                        .await?;

                    socket
                        .write_all(
                            Message::ParameterStatus("DateStyle".to_owned(), "ISO".to_owned())
                                .as_vec()
                                .as_slice(),
                        )
                        .await?;

                    return Ok(Ok(Connection::new((version, params), socket)));
                }
                Ok(ClientHandshake::SslRequest) => {
                    if self.configuration().ssl_support() {
                        socket.write_all(Encryption::AcceptSsl.into()).await?;
                        socket = self.server_channel().tls_channel(socket).await?;
                    } else {
                        socket.write_all(Encryption::RejectSsl.into()).await?;
                    }
                }
                Ok(ClientHandshake::GssEncryptRequest) => return Ok(Err(Error::UnsupportedRequest)),
                Err(error) => return Ok(Err(error)),
            }
        }
    }

    /// returns configuration of accepting or rejecting secure connections from
    /// clients
    fn configuration(&self) -> &ProtocolConfiguration;

    /// returns implementation of `ServerChannel`
    fn server_channel(&self) -> &Self::ServerChannel;
}

/// Trait that uses underline network protocol to establish bidirectional
/// protocol channels
#[async_trait]
pub trait ServerListener {
    /// returns bidirectional TCP channel with client socket address
    async fn tcp_channel(&self) -> io::Result<(Pin<Box<dyn Channel>>, SocketAddr)>;
    /// returns bidirectional TLS channel
    async fn tls_channel(&self, tcp_channel: Pin<Box<dyn Channel>>) -> io::Result<Pin<Box<dyn Channel>>>;
}

/// Struct to configure possible secure providers for client-server communication
/// PostgreSQL Wire Protocol supports `ssl`/`tls` and `gss` encryption
#[allow(dead_code)]
pub struct ProtocolConfiguration {
    ssl: bool,
    gssenc: bool,
}

#[allow(dead_code)]
impl ProtocolConfiguration {
    /// Creates configuration that support neither `ssl` nor `gss` encryption
    pub fn none() -> Self {
        Self {
            ssl: false,
            gssenc: false,
        }
    }

    /// Creates configuration that support only `ssl`
    pub fn ssl_only() -> Self {
        Self {
            ssl: true,
            gssenc: false,
        }
    }

    /// Creates configuration that support only `gss` encryption
    pub fn gssenc_only() -> Self {
        Self {
            ssl: false,
            gssenc: true,
        }
    }

    /// Creates configuration that support both `ssl` and `gss` encryption
    pub fn both() -> Self {
        Self {
            ssl: true,
            gssenc: true,
        }
    }

    /// returns `true` if support `ssl` connection
    fn ssl_support(&self) -> bool {
        self.ssl
    }

    /// returns `true` if support `gss` encrypted connection
    fn gssenc_support(&self) -> bool {
        self.gssenc
    }
}

enum ClientHandshake {
    SslRequest,
    GssEncryptRequest,
    Startup(Version, Params),
}

fn decode_startup(message: Vec<u8>) -> Result<ClientHandshake> {
    let version = NetworkEndian::read_i32(&message);
    log::debug!("VERSION FOR TEST = {:#?}", version);

    match version {
        VERSION_1 => Err(Error::UnsupportedVersion),
        VERSION_2 => Err(Error::UnsupportedVersion),
        VERSION_3 => {
            let params = message[4..]
                .split(|b| *b == 0)
                .filter(|b| !b.is_empty())
                .map(|b| std::str::from_utf8(b).unwrap().to_owned())
                .tuples()
                .collect::<Params>();
            Ok(ClientHandshake::Startup(version, params))
        }
        VERSION_CANCEL => Err(Error::UnsupportedVersion),
        VERSION_GSSENC => Ok(ClientHandshake::GssEncryptRequest),
        VERSION_SSL => Ok(ClientHandshake::SslRequest),
        _ => Err(Error::UnrecognizedVersion),
    }
}

async fn read_len<RW>(socket: &mut RW) -> io::Result<usize>
where
    RW: AsyncReadExt + AsyncWriteExt + Unpin,
{
    let mut buffer = [0u8; 4];
    let len = socket
        .read_exact(&mut buffer)
        .await
        .map(|_| NetworkEndian::read_u32(&buffer) as usize)?;
    Ok(len - 4)
}

async fn read_message<RW>(len: usize, socket: &mut RW) -> io::Result<Vec<u8>>
where
    RW: AsyncReadExt + AsyncWriteExt + Unpin,
{
    let mut buffer = Vec::with_capacity(len);
    buffer.resize(len, b'0');
    socket.read_exact(&mut buffer).await.map(|_| buffer)
}
