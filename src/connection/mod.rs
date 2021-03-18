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

use crate::session::Command;
use async_mutex::Mutex as AsyncMutex;
use futures_lite::{future::block_on, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use postgres::{
    query_response::QueryResult,
    wire_protocol::{BackendMessage, ConnId, ConnSecretKey, FrontendMessage, MessageDecoder, MessageDecoderStatus},
};
use rand::Rng;
use std::{
    collections::{HashMap, VecDeque},
    io,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

mod async_native_tls;
pub mod manager;
pub mod network;

type Props = Vec<(String, String)>;

pub struct Connection {
    id: ConnId,
    #[allow(dead_code)]
    client_props: Props,
    #[allow(dead_code)]
    address: SocketAddr,
    channel: Arc<AsyncMutex<Channel>>,
    supervisor: ConnSupervisor,
    sender: Arc<ResponseSender>,
}

impl Connection {
    pub fn new(
        id: ConnId,
        client_props: Props,
        address: SocketAddr,
        channel: Arc<AsyncMutex<Channel>>,
        supervisor: ConnSupervisor,
    ) -> Connection {
        let sender = Arc::new(ResponseSender::new(channel.clone()));
        Connection {
            id,
            client_props,
            address,
            channel,
            supervisor,
            sender,
        }
    }

    pub fn sender(&self) -> Arc<ResponseSender> {
        self.sender.clone()
    }

    async fn read_frontend_message(&mut self) -> io::Result<Result<FrontendMessage, ()>> {
        let mut current: Option<Vec<u8>> = None;
        let mut message_decoder = MessageDecoder::default();
        loop {
            log::debug!("Read bytes from connection {:?}", current);
            match message_decoder.next_stage(current.take().as_deref()) {
                Ok(MessageDecoderStatus::Requesting(len)) => {
                    let mut buffer = vec![b'0'; len];
                    self.channel.lock().await.read_exact(&mut buffer).await?;
                    current = Some(buffer);
                }
                Ok(MessageDecoderStatus::Done(message)) => return Ok(Ok(message)),
                Err(error) => {
                    log::error!("{}", error);
                    return Ok(Err(()));
                }
            }
        }
    }

    pub async fn receive(&mut self) -> io::Result<Result<Command, ()>> {
        let message = match self.read_frontend_message().await {
            Ok(Ok(message)) => message,
            Ok(Err(_err)) => return Ok(Err(())),
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
            FrontendMessage::DescribePortal { name } => Ok(Ok(Command::DescribePortal { name })),
            FrontendMessage::CloseStatement { name: _ } => Ok(Ok(Command::Continue)),
            FrontendMessage::ClosePortal { name: _ } => Ok(Ok(Command::Continue)),
            FrontendMessage::Setup { .. } => Ok(Ok(Command::Continue)),
            FrontendMessage::SslRequest => Ok(Ok(Command::Continue)),
            FrontendMessage::GssencRequest => Ok(Ok(Command::Continue)),
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.supervisor.free(self.id);
        log::debug!("stop service of connection-{}", self.id);
    }
}

/// Client request accepted from a client
pub enum ClientRequest {
    /// Connection to perform queries
    Connect(Connection),
    /// Connection to cancel queries of another client
    QueryCancellation(ConnId),
}

pub struct ResponseSender {
    channel: Arc<AsyncMutex<Channel>>,
}

impl ResponseSender {
    pub(crate) fn new(channel: Arc<AsyncMutex<Channel>>) -> ResponseSender {
        ResponseSender { channel }
    }
}

impl Sender for ResponseSender {
    fn flush(&self) -> io::Result<()> {
        block_on(async {
            self.channel.lock().await.flush().await.expect("OK");
        });

        Ok(())
    }

    fn send(&self, query_result: QueryResult) -> io::Result<()> {
        block_on(async {
            let message: BackendMessage = match query_result {
                Ok(event) => event.into(),
                Err(error) => error.into(),
            };
            log::debug!("response message {:?}", message);
            self.channel
                .lock()
                .await
                .write_all(message.as_vec().as_slice())
                .await
                .expect("OK");
            log::trace!("end of the command is sent");
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

pub enum Channel {
    Plain(network::Stream),
    Secure(network::SecureStream),
}

impl AsyncRead for Channel {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Channel::Plain(tcp) => Pin::new(tcp).poll_read(cx, buf),
            Channel::Secure(tls) => Pin::new(tls).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Channel {
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

#[cfg(test)]
mod tests;

/// Manages allocation of Connection IDs and secret keys.
#[derive(Clone)]
pub struct ConnSupervisor {
    inner: Arc<Mutex<ConnSupervisorInner>>,
}

impl ConnSupervisor {
    /// Creates a new Connection Supervisor.
    pub fn new(min_id: ConnId, max_id: ConnId) -> ConnSupervisor {
        ConnSupervisor {
            inner: Arc::new(Mutex::new(ConnSupervisorInner::new(min_id, max_id))),
        }
    }

    /// Allocates a new Connection ID and secret key.
    pub fn alloc(&self) -> Result<(ConnId, ConnSecretKey), ()> {
        self.inner.lock().unwrap().alloc()
    }

    /// Releases a Connection ID back to the pool.
    pub fn free(&self, conn_id: ConnId) {
        self.inner.lock().unwrap().free(conn_id);
    }

    /// Validates whether the secret key matches the specified Connection ID.
    pub fn verify(&self, conn_id: ConnId, secret_key: ConnSecretKey) -> bool {
        self.inner.lock().unwrap().verify(conn_id, secret_key)
    }
}

struct ConnSupervisorInner {
    next_id: ConnId,
    max_id: ConnId,
    free_ids: VecDeque<ConnId>,
    current_mapping: HashMap<ConnId, ConnSecretKey>,
}

impl ConnSupervisorInner {
    /// Creates a new Connection Supervisor.
    pub fn new(min_id: ConnId, max_id: ConnId) -> ConnSupervisorInner {
        ConnSupervisorInner {
            next_id: min_id,
            max_id,
            free_ids: VecDeque::new(),
            current_mapping: HashMap::new(),
        }
    }

    /// Allocates a new Connection ID and secret key.
    fn alloc(&mut self) -> Result<(ConnId, ConnSecretKey), ()> {
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

    fn generate_conn_id(&mut self) -> Result<ConnId, ()> {
        match self.free_ids.pop_front() {
            Some(id) => Ok(id),
            None => {
                let id = self.next_id;
                if id > self.max_id {
                    return Err(());
                }

                self.next_id += 1;
                Ok(id)
            }
        }
    }
}

/// Accepting or Rejecting SSL connection
pub enum Encryption {
    /// Accept SSL connection from client
    AcceptSsl,
    /// Reject SSL connection from client
    RejectSsl,
}

impl Into<&'_ [u8]> for Encryption {
    fn into(self) -> &'static [u8] {
        match self {
            Self::AcceptSsl => &[b'S'],
            Self::RejectSsl => &[b'N'],
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
        log::debug!("ALEX SSL!!!");
        Self {
            ssl_conf: Some((cert, password)),
        }
    }

    /// returns `true` if support `ssl` connection
    pub fn ssl_support(&self) -> bool {
        self.ssl_conf.is_some()
    }

    /// cert file and its password
    pub fn ssl_config(&self) -> Option<&(PathBuf, String)> {
        self.ssl_conf.as_ref()
    }

    /// returns `true` if support `gss` encrypted connection
    pub fn gssenc_support(&self) -> bool {
        false
    }
}
