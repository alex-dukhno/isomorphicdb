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

#![deny(missing_docs)]
//! API for backend implementation of PostgreSQL Wire Protocol
extern crate log;

use crate::{
    messages::{BackendMessage, Encryption, FrontendMessage},
    results::QueryResult,
    sql_types::PostgreSqlType,
};
use async_mutex::Mutex as AsyncMutex;
use async_native_tls::TlsStream;
use async_trait::async_trait;
use blocking::Unblock;
use byteorder::{ByteOrder, NetworkEndian};
use futures_lite::{
    future::block_on,
    io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ErrorKind},
};
use itertools::Itertools;
use std::{
    fs::File,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// Module contains backend messages that could be send by server implementation
/// to a client
pub mod messages;
/// Module contains functionality to represent query result
pub mod results;
/// Module contains functionality to represent SQL type system
pub mod sql_types;

/// Protocol version
pub type Version = i32;
/// Connection key-value params
pub type Params = Vec<(String, String)>;
/// Protocol operation result
pub type Result<T> = std::result::Result<T, Error>;

/// Version 1 of the protocol
pub const VERSION_1: Version = 0x10000;
/// Version 2 of the protocol
pub const VERSION_2: Version = 0x20000;
/// Version 3 of the protocol
pub const VERSION_3: Version = 0x30000;
/// Client initiate cancel of a command
pub const VERSION_CANCEL: Version = (1234 << 16) + 5678;
/// Client initiate `ssl` connection
pub const VERSION_SSL: Version = (1234 << 16) + 5679;
/// Client initiate `gss` encrypted connection
pub const VERSION_GSSENC: Version = (1234 << 16) + 5680;

/// `Error` type in protocol `Result`. Indicates that something went not well
#[derive(Debug, PartialEq)]
pub enum Error {
    /// Indicates that incoming data is invalid
    InvalidInput(String),
    /// Indicates that incoming data can't be parsed as UTF-8 string
    InvalidUtfString,
    /// Indicates that frontend message is not supported
    UnsupportedFrontendMessage,
    /// Indicates that protocol version is not supported
    UnsupportedVersion,
    /// Indicates that client request is not supported
    UnsupportedRequest,
    /// Indicates that during handshake client sent unrecognized protocol version
    UnrecognizedVersion,
}

/// Result of handling incoming bytes from a client
#[derive(Debug, PartialEq)]
pub enum Command {
    /// Nothing needs to handle on client, just to receive next message
    Continue,
    /// Client commands to describe a prepared statement
    DescribeStatement(String),
    /// Client commands to flush the output stream
    Flush,
    /// Client commands to prepare a statement for execution
    Parse(String, String, Vec<PostgreSqlType>),
    /// Client commands to execute a `Query`
    Query(String),
    /// Client commands to terminate current connection
    Terminate,
}

/// Perform `PostgreSql` wire protocol hand shake to establish connection with
/// a client based on `config` parameters and using `stream` as a medium to
/// communicate
/// As a result of operation returns tuple of `Receiver` and `Sender`
/// that have to be used to communicate with the client on performing commands
pub async fn hand_shake<RW>(
    stream: RW,
    address: SocketAddr,
    config: &ProtocolConfiguration,
) -> io::Result<Result<(impl Receiver, impl Sender)>>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    log::debug!("ADDRESS {:?}", address);

    let mut channel = Channel::Plain(stream);
    loop {
        let mut buffer = [0u8; 4];
        let len = channel
            .read_exact(&mut buffer)
            .await
            .map(|_| NetworkEndian::read_u32(&buffer) as usize)?;
        let len = len - 4;
        let mut buffer = Vec::with_capacity(len);
        buffer.resize(len, b'0');
        let message = channel.read_exact(&mut buffer).await.map(|_| buffer)?;
        log::debug!("MESSAGE FOR TEST = {:#?}", message);

        match decode_startup(message) {
            Ok(ClientHandshake::Startup(version, params)) => {
                channel
                    .write_all(BackendMessage::AuthenticationCleartextPassword.as_vec().as_slice())
                    .await?;
                let mut buffer = [0u8; 1];
                let tag = channel.read_exact(&mut buffer).await.map(|_| buffer[0]);
                log::debug!("client message response tag {:?}", tag);
                log::debug!("waiting for authentication response");
                let mut buffer = [0u8; 4];
                let len = channel
                    .read_exact(&mut buffer)
                    .await
                    .map(|_| NetworkEndian::read_u32(&buffer) as usize)?;
                let len = len - 4;
                let mut buffer = Vec::with_capacity(len);
                buffer.resize(len, b'0');
                let _message = channel.read_exact(&mut buffer).await.map(|_| buffer)?;
                channel
                    .write_all(BackendMessage::AuthenticationOk.as_vec().as_slice())
                    .await?;

                channel
                    .write_all(
                        BackendMessage::ParameterStatus("client_encoding".to_owned(), "UTF8".to_owned())
                            .as_vec()
                            .as_slice(),
                    )
                    .await?;

                channel
                    .write_all(
                        BackendMessage::ParameterStatus("DateStyle".to_owned(), "ISO".to_owned())
                            .as_vec()
                            .as_slice(),
                    )
                    .await?;

                channel
                    .write_all(
                        BackendMessage::ParameterStatus("integer_datetimes".to_owned(), "off".to_owned())
                            .as_vec()
                            .as_slice(),
                    )
                    .await?;

                log::debug!("Send ready_for_query message");
                channel
                    .write_all(BackendMessage::ReadyForQuery.as_vec().as_slice())
                    .await?;

                let channel = Arc::new(AsyncMutex::new(channel));
                return Ok(Ok((
                    RequestReceiver::new((version, params.clone()), channel.clone()),
                    ResponseSender::new((version, params), channel),
                )));
            }
            Ok(ClientHandshake::SslRequest) => {
                channel = match channel {
                    Channel::Plain(mut channel) if config.ssl_support() => {
                        channel.write_all(Encryption::AcceptSsl.into()).await?;
                        Channel::Secure(tls_channel(channel, config).await?)
                    }
                    _ => {
                        channel.write_all(Encryption::RejectSsl.into()).await?;
                        channel
                    }
                };
            }
            Ok(ClientHandshake::GssEncryptRequest) => return Ok(Err(Error::UnsupportedRequest)),
            Err(error) => return Ok(Err(error)),
        }
    }
}

async fn tls_channel<RW>(tcp_channel: RW, config: &ProtocolConfiguration) -> io::Result<TlsStream<RW>>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    match config.ssl_config() {
        Some((path, password)) => {
            match async_native_tls::accept(Unblock::new(File::open(path)?), password, tcp_channel).await {
                Ok(socket) => Ok(socket),
                Err(_err) => Err(io::Error::from(ErrorKind::ConnectionAborted)),
            }
        }
        None => Err(io::Error::from(io::ErrorKind::ConnectionAborted)),
    }
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

struct RequestReceiver<RW: AsyncRead + AsyncWrite + Unpin> {
    properties: (Version, Params),
    channel: Arc<AsyncMutex<Channel<RW>>>,
}

impl<RW: AsyncRead + AsyncWrite + Unpin> RequestReceiver<RW> {
    /// Creates new Connection with properties and read-write socket
    pub(crate) fn new(properties: (Version, Params), channel: Arc<AsyncMutex<Channel<RW>>>) -> RequestReceiver<RW> {
        RequestReceiver { properties, channel }
    }

    /// connection properties tuple
    pub fn properties(&self) -> &(Version, Params) {
        &(self.properties)
    }
}

#[async_trait]
impl<RW: AsyncRead + AsyncWrite + Unpin> Receiver for RequestReceiver<RW> {
    async fn receive(&mut self) -> io::Result<Result<Command>> {
        // Parses the one-byte tag.
        let mut buffer = [0u8; 1];
        let tag = self
            .channel
            .lock()
            .await
            .read_exact(&mut buffer)
            .await
            .map(|_| buffer[0])?;
        log::debug!("TAG {:?}", tag);

        // Parses the frame length.
        let mut buffer = [0u8; 4];
        let len = self
            .channel
            .lock()
            .await
            .read_exact(&mut buffer)
            .await
            .map(|_| NetworkEndian::read_u32(&buffer))?;

        // Parses the frame data.
        let mut buffer = Vec::with_capacity(len as usize - 4);
        buffer.resize(len as usize - 4, b'0');
        self.channel.lock().await.read_exact(&mut buffer).await?;

        let message = match FrontendMessage::decode(tag, &buffer) {
            Ok(msg) => msg,
            Err(err) => return Ok(Err(err)),
        };
        log::debug!("MESSAGE {:?}", message);

        match message {
            FrontendMessage::DescribeStatement { name } => Ok(Ok(Command::DescribeStatement(name))),
            FrontendMessage::Flush => Ok(Ok(Command::Flush)),
            FrontendMessage::Parse {
                statement_name,
                sql,
                param_types,
            } => Ok(Ok(Command::Parse(statement_name, sql, param_types))),
            FrontendMessage::Query { sql } => Ok(Ok(Command::Query(sql))),
            FrontendMessage::Terminate => Ok(Ok(Command::Terminate)),
            _ => Ok(Ok(Command::Continue)),
        }
    }
}

/// Trait to handle client to server commands for PostgreSQL Wire Protocol connection
#[async_trait]
pub trait Receiver: Send + Sync {
    /// receives and decodes a command from remote client
    async fn receive(&mut self) -> io::Result<Result<Command>>;
}

struct ResponseSender<RW: AsyncRead + AsyncWrite + Unpin> {
    properties: (Version, Params),
    channel: Arc<AsyncMutex<Channel<RW>>>,
}

impl<RW: AsyncRead + AsyncWrite + Unpin> Clone for ResponseSender<RW> {
    fn clone(&self) -> Self {
        Self {
            properties: (self.properties.0, self.properties.1.clone()),
            channel: self.channel.clone(),
        }
    }
}

impl<RW: AsyncRead + AsyncWrite + Unpin> ResponseSender<RW> {
    /// Creates new Connection with properties and read-write socket
    pub(crate) fn new(properties: (Version, Params), channel: Arc<AsyncMutex<Channel<RW>>>) -> ResponseSender<RW> {
        ResponseSender { properties, channel }
    }
}

impl<RW: AsyncRead + AsyncWrite + Unpin> Sender for ResponseSender<RW> {
    fn flush(&self) -> io::Result<()> {
        block_on(async {
            self.channel.lock().await.flush().await.expect("OK");
        });

        Ok(())
    }

    fn send(&self, query_result: QueryResult) -> io::Result<()> {
        log::debug!("Sending {:?}", query_result);
        block_on(async {
            match query_result {
                Ok(event) => {
                    let messages: Vec<BackendMessage> = event.into();
                    for message in messages {
                        log::debug!("{:?}", message);
                        self.channel
                            .lock()
                            .await
                            .write_all(message.as_vec().as_slice())
                            .await
                            .expect("OK");
                    }
                }
                Err(error) => {
                    let message: BackendMessage = error.into();
                    log::debug!("{:?}", message);
                    self.channel
                        .lock()
                        .await
                        .write_all(message.as_vec().as_slice())
                        .await
                        .expect("OK");
                }
            }
            log::debug!("end of the command is sent");
        });
        Ok(())
    }
}

/// Trait to handle server to client query results for PostgreSQL Wire Protocol
/// connection
pub trait Sender: Send + Sync {
    /// Flushes the output stream.
    fn flush(&self) -> io::Result<()>;

    /// Sends response messages to client. Most of the time it is a single
    /// message, select result one of the exceptional situation
    fn send(&self, query_result: QueryResult) -> io::Result<()>;
}

impl<RW: AsyncRead + AsyncWrite + Unpin> PartialEq for RequestReceiver<RW> {
    fn eq(&self, other: &Self) -> bool {
        self.properties().eq(other.properties())
    }
}

pub(crate) enum Channel<RW: AsyncRead + AsyncWrite + Unpin> {
    Plain(RW),
    Secure(TlsStream<RW>),
}

unsafe impl<RW: AsyncRead + AsyncWrite + Unpin> Send for Channel<RW> {}
unsafe impl<RW: AsyncRead + AsyncWrite + Unpin> Sync for Channel<RW> {}

impl<RW: AsyncRead + AsyncWrite + Unpin> AsyncRead for Channel<RW> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Channel::Plain(tcp) => Pin::new(tcp).poll_read(cx, buf),
            Channel::Secure(tls) => Pin::new(tls).poll_read(cx, buf),
        }
    }
}

impl<RW: AsyncRead + AsyncWrite + Unpin> AsyncWrite for Channel<RW> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Channel::Plain(tcp) => Pin::new(tcp).poll_write(cx, buf),
            Channel::Secure(tls) => Pin::new(tls).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Channel::Plain(tcp) => Pin::new(tcp).poll_flush(cx),
            Channel::Secure(tls) => Pin::new(tls).poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Channel::Plain(tcp) => Pin::new(tcp).poll_close(cx),
            Channel::Secure(tls) => Pin::new(tls).poll_close(cx),
        }
    }
}

/// Struct to configure possible secure providers for client-server communication
/// PostgreSQL Wire Protocol supports `ssl`/`tls` and `gss` encryption
pub struct ProtocolConfiguration {
    ssl_conf: Option<(PathBuf, String)>,
}

#[allow(dead_code)]
impl ProtocolConfiguration {
    /// Creates configuration that support neither `ssl` nor `gss` encryption
    pub fn none() -> Self {
        Self { ssl_conf: None }
    }

    /// Creates configuration that support only `ssl`
    pub fn with_ssl(cert: PathBuf, password: String) -> Self {
        Self {
            ssl_conf: Some((cert, password)),
        }
    }

    /// returns `true` if support `ssl` connection
    fn ssl_support(&self) -> bool {
        self.ssl_conf.is_some()
    }

    /// cert file and its password
    fn ssl_config(&self) -> Option<&(PathBuf, String)> {
        self.ssl_conf.as_ref()
    }

    /// returns `true` if support `gss` encrypted connection
    fn gssenc_support(&self) -> bool {
        false
    }
}

enum ClientHandshake {
    SslRequest,
    GssEncryptRequest,
    Startup(Version, Params),
}

#[cfg(test)]
mod tests;
