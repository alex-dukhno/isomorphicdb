// Copyright 2020 - present Alex Dukhno
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

use crate::pg_model::{Command, ConnSupervisor};
use async_mutex::Mutex as AsyncMutex;
use futures_lite::{future::block_on, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use postgres::{
    query_response::QueryResult,
    wire_protocol::{BackendMessage, ConnId, FrontendMessage, MessageDecoder, MessageDecoderStatus},
};
use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

mod async_native_tls;
pub mod manager;
pub mod network;

type Props = Vec<(String, String)>;

pub struct Connection {
    pub receiver: Box<RequestReceiver>,
    pub sender: Arc<ResponseSender>,
}

/// Client request accepted from a client
pub enum ClientRequest {
    /// Connection to perform queries
    Connect(Connection),
    /// Connection to cancel queries of another client
    QueryCancellation(ConnId),
}

pub struct RequestReceiver {
    conn_id: ConnId,
    properties: Props,
    channel: Arc<AsyncMutex<Channel>>,
    conn_supervisor: ConnSupervisor,
}

impl RequestReceiver {
    /// Creates a new connection
    pub(crate) fn new(
        conn_id: ConnId,
        properties: Props,
        channel: Arc<AsyncMutex<Channel>>,
        conn_supervisor: ConnSupervisor,
    ) -> RequestReceiver {
        RequestReceiver {
            conn_id,
            properties,
            channel,
            conn_supervisor,
        }
    }

    /// connection properties tuple
    pub fn properties(&self) -> &Props {
        &self.properties
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
}

impl RequestReceiver {
    // TODO: currently it uses protocol::Result
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

impl Drop for RequestReceiver {
    fn drop(&mut self) {
        self.conn_supervisor.free(self.conn_id);
        log::debug!("stop service of connection-{}", self.conn_id);
    }
}

pub struct ResponseSender {
    #[allow(dead_code)]
    properties: Props,
    channel: Arc<AsyncMutex<Channel>>,
}

impl ResponseSender {
    /// Creates new Connection with properties and read-write socket
    pub(crate) fn new(properties: Props, channel: Arc<AsyncMutex<Channel>>) -> ResponseSender {
        ResponseSender { properties, channel }
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

impl PartialEq for RequestReceiver {
    fn eq(&self, other: &Self) -> bool {
        self.properties().eq(other.properties())
    }
}

pub(crate) enum Channel {
    Plain(network::Stream),
    Secure(network::SecureStream),
}

unsafe impl Send for Channel {}

unsafe impl Sync for Channel {}

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
