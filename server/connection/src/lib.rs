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

use async_mutex::Mutex as AsyncMutex;
use async_native_tls::TlsStream;
use blocking::Unblock;
use byteorder::{ByteOrder, NetworkEndian};
use futures_lite::{future::block_on, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use pg_model::{results::QueryResult, Command, ConnSupervisor, Encryption, ProtocolConfiguration};
use pg_wire::{
    BackendMessage, ConnId, Error, FrontendMessage, HandShakeProcess, HandShakeRequest, HandShakeStatus,
    MessageDecoder, MessageDecoderStatus, Result,
};
use std::{
    fs::File,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

type Props = Vec<(String, String)>;

/// Client request accepted from a client
pub enum ClientRequest {
    /// Connection to perform queries
    Connection(Box<dyn Receiver>, Arc<dyn Sender>),
    /// Connection to cancel queries of another client
    QueryCancellation(ConnId),
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
    let mut process = HandShakeProcess::start();
    let mut current: Option<Vec<u8>> = None;
    loop {
        match process.next_stage(current.as_deref()) {
            Ok(HandShakeStatus::Requesting(HandShakeRequest::Buffer(len))) => {
                let mut local = vec![b'0'; len];
                local = channel.read_exact(&mut local).await.map(|_| local)?;
                current = Some(local);
            }
            Ok(HandShakeStatus::Requesting(HandShakeRequest::UpgradeToSsl)) => {
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
            Ok(HandShakeStatus::Cancel(conn_id, secret_key)) => {
                return if conn_supervisor.lock().unwrap().verify(conn_id, secret_key) {
                    Ok(Ok(ClientRequest::QueryCancellation(conn_id)))
                } else {
                    Ok(Err(Error::VerificationFailed))
                }
            }
            Ok(HandShakeStatus::Done(props)) => {
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
                    Arc::new(ResponseSender::new(props, channel)),
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
                Err(_err) => Err(io::Error::from(io::ErrorKind::ConnectionAborted)),
            }
        }
        None => Err(io::Error::from(io::ErrorKind::ConnectionAborted)),
    }
}

struct RequestReceiver<RW: AsyncRead + AsyncWrite + Unpin> {
    conn_id: ConnId,
    properties: Props,
    channel: Arc<AsyncMutex<Channel<RW>>>,
    conn_supervisor: Arc<Mutex<ConnSupervisor>>,
    message_decoder: MessageDecoder,
}

impl<RW: AsyncRead + AsyncWrite + Unpin> RequestReceiver<RW> {
    /// Creates a new connection
    pub(crate) fn new(
        conn_id: ConnId,
        properties: Props,
        channel: Arc<AsyncMutex<Channel<RW>>>,
        conn_supervisor: Arc<Mutex<ConnSupervisor>>,
    ) -> RequestReceiver<RW> {
        RequestReceiver {
            conn_id,
            properties,
            channel,
            conn_supervisor,
            message_decoder: MessageDecoder::new(),
        }
    }

    /// connection properties tuple
    pub fn properties(&self) -> &Props {
        &self.properties
    }

    async fn read_frontend_message(&mut self) -> io::Result<Result<FrontendMessage>> {
        let mut current: Option<Vec<u8>> = None;
        loop {
            log::debug!("Read bytes from connection {:?}", current);
            match self.message_decoder.next_stage(current.take().as_deref()) {
                Ok(MessageDecoderStatus::Requesting(len)) => {
                    let mut buffer = vec![b'0'; len];
                    self.channel.lock().await.read_exact(&mut buffer).await?;
                    current = Some(buffer);
                }
                Ok(MessageDecoderStatus::Decoding) => {}
                Ok(MessageDecoderStatus::Done(message)) => return Ok(Ok(message)),
                Err(error) => return Ok(Err(error)),
            }
        }
    }
}

#[async_trait::async_trait]
impl<RW: AsyncRead + AsyncWrite + Unpin> Receiver for RequestReceiver<RW> {
    // TODO: currently it uses protocol::Result
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
            FrontendMessage::DescribePortal { name } => Ok(Ok(Command::DescribePortal { name })),
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
#[async_trait::async_trait]
pub trait Receiver: Send + Sync {
    /// receives and decodes a command from remote client
    async fn receive(&mut self) -> io::Result<Result<Command>>;
}

struct ResponseSender<RW: AsyncRead + AsyncWrite + Unpin> {
    #[allow(dead_code)]
    properties: Props,
    channel: Arc<AsyncMutex<Channel<RW>>>,
}

impl<RW: AsyncRead + AsyncWrite + Unpin> ResponseSender<RW> {
    /// Creates new Connection with properties and read-write socket
    pub(crate) fn new(properties: Props, channel: Arc<AsyncMutex<Channel<RW>>>) -> ResponseSender<RW> {
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

#[cfg(test)]
mod tests;
