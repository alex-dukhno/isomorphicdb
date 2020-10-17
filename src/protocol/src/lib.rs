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

#![warn(missing_docs)]
//! API for backend implementation of PostgreSQL Wire Protocol
extern crate log;

use crate::{
    hand_shake::{Process, Request, Status},
    messages::{BackendMessage, Encryption, FrontendMessage},
    pgsql_types::{PostgreSqlFormat, PostgreSqlType},
    results::QueryResult,
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
use rand::Rng;
use std::{
    collections::{HashMap, VecDeque},
    fmt::{self, Debug, Display, Formatter},
    fs::File,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

mod hand_shake;
/// Module contains backend messages that could be send by server implementation
/// to a client
pub mod messages;
/// Module contains functionality to represent SQL type system
pub mod pgsql_types;
/// Module contains functionality to represent query result
pub mod results;
/// Module contains functionality to represent server side client session
pub mod session;
/// Module contains functionality to hold data about `PreparedStatement`
pub mod statement;

/// Connection ID
pub type ConnId = i32;
/// Connection secret key
pub type ConnSecretKey = i32;
/// Protocol version
pub type Version = i32;
/// Connection key-value params
pub type Params = Vec<(String, String)>;
/// Protocol operation result
pub type Result<T> = std::result::Result<T, Error>;

/// Version 1 of the protocol
pub(crate) const VERSION_1_CODE: Code = Code(0x00_01_00_00);
/// Version 2 of the protocol
pub(crate) const VERSION_2_CODE: Code = Code(0x00_02_00_00);
/// Version 3 of the protocol
pub(crate) const VERSION_3_CODE: Code = Code(0x00_03_00_00);
/// Client initiate cancel of a command
pub(crate) const CANCEL_REQUEST_CODE: Code = Code(8087_7102);
/// Client initiate `ssl` connection
pub(crate) const SSL_REQUEST_CODE: Code = Code(8087_7103);
/// Client initiate `gss` encrypted connection
#[allow(dead_code)]
pub(crate) const GSSENC_REQUEST_CODE: Code = Code(8087_7104);

/// Client Request Code
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub struct Code(i32);

impl Display for Code {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.0 {
            8087_7102 => write!(f, "Cancel Request"),
            8087_7103 => write!(f, "SSL Request"),
            8087_7104 => write!(f, "GSSENC Request"),
            _ => write!(
                f,
                "Version {}.{} Request",
                (self.0 >> 16) as i16,
                (self.0 & 0x00_00_FF_FF) as i16
            ),
        }
    }
}

impl From<Code> for Vec<u8> {
    fn from(code: Code) -> Vec<u8> {
        code.0.to_be_bytes().to_vec()
    }
}

#[cfg(test)]
mod code_display_tests {
    use super::*;

    #[test]
    fn version_one_request() {
        assert_eq!(VERSION_1_CODE.to_string(), "Version 1.0 Request");
    }

    #[test]
    fn version_two_request() {
        assert_eq!(VERSION_2_CODE.to_string(), "Version 2.0 Request");
    }

    #[test]
    fn version_three_request() {
        assert_eq!(VERSION_3_CODE.to_string(), "Version 3.0 Request");
    }

    #[test]
    fn cancel_request() {
        assert_eq!(CANCEL_REQUEST_CODE.to_string(), "Cancel Request")
    }

    #[test]
    fn ssl_request() {
        assert_eq!(SSL_REQUEST_CODE.to_string(), "SSL Request")
    }

    #[test]
    fn gssenc_request() {
        assert_eq!(GSSENC_REQUEST_CODE.to_string(), "GSSENC Request")
    }
}

/// Client request accepted from a client
pub enum ClientRequest {
    /// Connection to perform queries
    Connection(Box<dyn Receiver>, Arc<dyn Sender>),
    /// Connection to cancel queries of another client
    QueryCancellation(ConnId),
}

/// `Error` type in protocol `Result`. Indicates that something went not well
#[derive(Debug, PartialEq)]
pub enum Error {
    /// Indicates that the current count of active connections is full
    ConnectionIdExhausted,
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
    /// Indicates that connection verification is failed
    VerificationFailed,
}

/// Result of handling incoming bytes from a client
#[derive(Debug, PartialEq)]
pub enum Command {
    /// Client commands to bind a prepared statement to a portal
    Bind {
        /// The destination portal. An empty string selects the unnamed
        /// portal. The portal can later be executed with the `Execute` command.
        portal_name: String,
        /// The source prepared statement. An empty string selects the unnamed
        /// prepared statement.
        statement_name: String,
        /// The formats used to encode the parameters.
        param_formats: Vec<PostgreSqlFormat>,
        /// The value of each parameter.
        raw_params: Vec<Option<Vec<u8>>>,
        /// The desired formats for the columns in the result set.
        result_formats: Vec<PostgreSqlFormat>,
    },
    /// Nothing needs to handle on client, just to receive next message
    Continue,
    /// Client commands to describe a prepared statement
    DescribeStatement {
        /// The name of the prepared statement to describe.
        name: String,
    },
    /// Client commands to execute a portal
    Execute {
        /// The name of the portal to execute.
        portal_name: String,
        /// The maximum number of rows to return before suspending.
        ///
        /// 0 or negative means infinite.
        max_rows: i32,
    },
    /// Client commands to flush the output stream
    Flush,
    /// Client commands to prepare a statement for execution
    Parse {
        /// The name of the prepared statement to create. An empty string
        /// specifies the unnamed prepared statement.
        statement_name: String,
        /// The SQL to parse.
        sql: String,
        /// The number of specified parameter data types can be less than the
        /// number of parameters specified in the query.
        param_types: Vec<PostgreSqlType>,
    },
    /// Client commands to execute a `Query`
    Query {
        /// The SQL to execute.
        sql: String,
    },
    /// Client commands to terminate current connection
    Terminate,
}

/// Manages allocation of Connection IDs and secret keys.
pub struct ConnSupervisor {
    next_id: ConnId,
    max_id: ConnId,
    free_ids: VecDeque<ConnId>,
    current_mapping: HashMap<ConnId, ConnSecretKey>,
}

impl ConnSupervisor {
    /// Creates a new Connection Supervisor.
    pub fn new(min_id: ConnId, max_id: ConnId) -> Self {
        Self {
            next_id: min_id,
            max_id,
            free_ids: VecDeque::new(),
            current_mapping: HashMap::new(),
        }
    }

    /// Allocates a new Connection ID and secret key.
    fn alloc(&mut self) -> Result<(ConnId, ConnSecretKey)> {
        let conn_id = self.generate_conn_id()?;
        let secret_key = rand::thread_rng().gen();
        self.current_mapping.insert(conn_id, secret_key);
        Ok((conn_id, secret_key))
    }

    /// Releases a Connection ID back to the pool.
    fn free(&mut self, conn_id: ConnId) {
        if self.current_mapping.remove(&conn_id).is_some() {
            self.free_ids.push_back(conn_id);
        }
    }

    /// Validates whether the secret key matches the specified Connection ID.
    fn verify(&self, conn_id: ConnId, secret_key: ConnSecretKey) -> bool {
        match self.current_mapping.get(&conn_id) {
            Some(s) => *s == secret_key,
            None => false,
        }
    }

    fn generate_conn_id(&mut self) -> Result<ConnId> {
        match self.free_ids.pop_front() {
            Some(id) => Ok(id),
            None => {
                let id = self.next_id;
                if id > self.max_id {
                    return Err(Error::ConnectionIdExhausted);
                }

                self.next_id += 1;
                Ok(id)
            }
        }
    }
}

/// Perform `PostgreSql` wire protocol to accept request and establish
/// connection with a client based on `config` parameters and using `stream` as
/// a medium to communicate
/// As a result of operation returns tuple of `Receiver` and `Sender`
/// that have to be used to communicate with the client on performing commands
pub async fn accept_client_request<RW: 'static>(
    stream: RW,
    address: SocketAddr,
    config: &ProtocolConfiguration,
    conn_supervisor: Arc<Mutex<ConnSupervisor>>,
) -> io::Result<Result<ClientRequest>>
where
    RW: AsyncRead + AsyncWrite + Unpin,
{
    log::debug!("address {:?}", address);

    let mut channel = Channel::Plain(stream);
    let mut process = Process::start();
    let mut current: Option<Vec<u8>> = None;
    loop {
        match process.next_stage(current.as_deref()) {
            Ok(Status::Requesting(Request::Buffer(len))) => {
                let mut local = Vec::with_capacity(len);
                local.resize(len, b'0');
                local = channel.read_exact(&mut local).await.map(|_| local)?;
                current = Some(local);
            }
            Ok(Status::Requesting(Request::UpgradeToSsl)) => {
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
                current = None
            }
            Ok(Status::Cancel(conn_id, secret_key)) => {
                return if conn_supervisor.lock().unwrap().verify(conn_id, secret_key) {
                    Ok(Ok(ClientRequest::QueryCancellation(conn_id)))
                } else {
                    Ok(Err(Error::VerificationFailed))
                }
            }
            Ok(Status::Done(props)) => {
                channel
                    .write_all(BackendMessage::AuthenticationCleartextPassword.as_vec().as_slice())
                    .await?;
                channel.flush().await?;
                let mut tag_buffer = [0u8; 1];
                let tag = channel.read_exact(&mut tag_buffer).await.map(|_| tag_buffer[0]);
                log::debug!("client message response tag {:?}", tag);
                log::debug!("waiting for authentication response");
                let mut len_buffer = [0u8; 4];
                let len = channel
                    .read_exact(&mut len_buffer)
                    .await
                    .map(|_| NetworkEndian::read_u32(&len_buffer) as usize)?;
                let len = len - 4;
                let mut message_buffer = Vec::with_capacity(len);
                message_buffer.resize(len, b'0');
                let _message = channel.read_exact(&mut message_buffer).await.map(|_| message_buffer)?;
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

                channel
                    .write_all(
                        BackendMessage::ParameterStatus("server_version".to_owned(), "12.4".to_owned())
                            .as_vec()
                            .as_slice(),
                    )
                    .await?;

                let (conn_id, secret_key) = match conn_supervisor.lock().unwrap().alloc() {
                    Ok((c, s)) => (c, s),
                    Err(e) => return Ok(Err(e)),
                };

                log::debug!("start service on connection-{}", conn_id);
                channel
                    .write_all(BackendMessage::BackendKeyData(conn_id, secret_key).as_vec().as_slice())
                    .await?;

                log::debug!("send ready_for_query message");
                channel
                    .write_all(BackendMessage::ReadyForQuery.as_vec().as_slice())
                    .await?;

                let channel = Arc::new(AsyncMutex::new(channel));
                return Ok(Ok(ClientRequest::Connection(
                    Box::new(RequestReceiver::new(
                        conn_id,
                        props.clone(),
                        channel.clone(),
                        conn_supervisor,
                    )),
                    Arc::new(ResponseSender::new((VERSION_3_CODE.0, props), channel)),
                )));
            }
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

struct RequestReceiver<RW: AsyncRead + AsyncWrite + Unpin> {
    conn_id: ConnId,
    properties: Params,
    channel: Arc<AsyncMutex<Channel<RW>>>,
    conn_supervisor: Arc<Mutex<ConnSupervisor>>,
}

impl<RW: AsyncRead + AsyncWrite + Unpin> RequestReceiver<RW> {
    /// Creates a new connection
    pub(crate) fn new(
        conn_id: ConnId,
        properties: Params,
        channel: Arc<AsyncMutex<Channel<RW>>>,
        conn_supervisor: Arc<Mutex<ConnSupervisor>>,
    ) -> RequestReceiver<RW> {
        RequestReceiver {
            conn_id,
            properties,
            channel,
            conn_supervisor,
        }
    }

    /// connection properties tuple
    pub fn properties(&self) -> &Params {
        &self.properties
    }

    async fn read_frontend_message(&self) -> io::Result<Result<FrontendMessage>> {
        // Parses the one-byte tag.
        let mut buffer = [0u8; 1];
        let tag = self
            .channel
            .lock()
            .await
            .read_exact(&mut buffer)
            .await
            .map(|_| buffer[0])?;
        log::debug!("client request message tag {:?}", tag);

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

        match FrontendMessage::decode(tag, &buffer) {
            Ok(msg) => Ok(Ok(msg)),
            Err(err) => Ok(Err(err)),
        }
    }
}

#[async_trait]
impl<RW: AsyncRead + AsyncWrite + Unpin> Receiver for RequestReceiver<RW> {
    async fn receive(&mut self) -> io::Result<Result<Command>> {
        let message = match self.read_frontend_message().await {
            Ok(Ok(message)) => message,
            Ok(Err(err)) => return Ok(Err(err)),
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                // Client disconnected the socket immediately without sending a
                // Terminate message. Considers it as a client Terminate to save
                // resource and exit smoothly.
                log::debug!("client disconnected immediately");
                FrontendMessage::Terminate
            }
            Err(err) => return Err(err),
        };
        log::debug!("client request message {:?}", message);

        match message {
            FrontendMessage::Bind {
                portal_name,
                statement_name,
                param_formats,
                raw_params,
                result_formats,
            } => Ok(Ok(Command::Bind {
                portal_name,
                statement_name,
                param_formats,
                raw_params,
                result_formats,
            })),
            FrontendMessage::DescribeStatement { name } => Ok(Ok(Command::DescribeStatement { name })),
            FrontendMessage::Execute { portal_name, max_rows } => Ok(Ok(Command::Execute { portal_name, max_rows })),
            FrontendMessage::Flush => Ok(Ok(Command::Flush)),
            FrontendMessage::Parse {
                statement_name,
                sql,
                param_types,
            } => Ok(Ok(Command::Parse {
                statement_name,
                sql,
                param_types,
            })),
            FrontendMessage::Query { sql } => Ok(Ok(Command::Query { sql })),
            FrontendMessage::Terminate => Ok(Ok(Command::Terminate)),
            FrontendMessage::Sync => Ok(Ok(Command::Continue)),
            FrontendMessage::DescribePortal { name: _ } => Ok(Ok(Command::Continue)),
            FrontendMessage::CloseStatement { name: _ } => Ok(Ok(Command::Continue)),
            FrontendMessage::ClosePortal { name: _ } => Ok(Ok(Command::Continue)),
            FrontendMessage::Setup { .. } => Ok(Ok(Command::Continue)),
            FrontendMessage::SslRequest => Ok(Ok(Command::Continue)),
            FrontendMessage::GssencRequest => Ok(Ok(Command::Continue)),
        }
    }
}

impl<RW: AsyncRead + AsyncWrite + Unpin> Drop for RequestReceiver<RW> {
    fn drop(&mut self) {
        self.conn_supervisor.lock().unwrap().free(self.conn_id);
        log::debug!("stop service of connection-{}", self.conn_id);
    }
}

/// Trait to handle client to server commands for PostgreSQL Wire Protocol connection
#[async_trait]
pub trait Receiver: Send + Sync {
    /// receives and decodes a command from remote client
    async fn receive(&mut self) -> io::Result<Result<Command>>;
}

struct ResponseSender<RW: AsyncRead + AsyncWrite + Unpin> {
    #[allow(dead_code)]
    properties: (Version, Params),
    channel: Arc<AsyncMutex<Channel<RW>>>,
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
        log::debug!("[{:?}] query result sent to client", query_result);
        block_on(async {
            let message: BackendMessage = query_result.into();
            log::debug!("response message {:?}", message);
            self.channel
                .lock()
                .await
                .write_all(message.as_vec().as_slice())
                .await
                .expect("OK");
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

#[cfg(test)]
mod tests;
