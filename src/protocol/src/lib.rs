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

#[cfg(test)]
mod tests;

use crate::messages::Message;
use byteorder::{ByteOrder, NetworkEndian};
use futures_util::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    task::{Context, Poll},
};
use std::{io, pin::Pin};

use crate::results::QueryResult;
pub use listener::{QueryListener, ServerListener};

/// Module contains functionality to listen to incoming client connections and
/// queries
pub mod listener;
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
    /// Indicates that incoming query can't be parsed as UTF-8 string
    QueryIsNotValidUtfString,
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
    /// Client commands to execute a `Query`
    Query(String),
    /// Client commands to terminate current connection
    Terminate,
}

/// Abstract trait for bidirectional TCP or TLS channel
pub trait Channel: Sync + Send {
    /// used for implementing trait AsyncRead
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>>;
    /// used for implementing trait AsyncWrite
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>>;
    /// used for implementing trait AsyncWrite
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>>;
    /// used for implementing trait AsyncWrite
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>>;
}

impl AsyncRead for dyn Channel {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        self.poll_read(cx, buf)
    }
}

impl AsyncWrite for dyn Channel {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_close(cx)
    }
}

/// Structure to handle client-server PostgreSQL Wire Protocol connection
pub struct Connection {
    properties: (Version, Params),
    channel: Pin<Box<dyn Channel>>,
}

impl Connection {
    /// Creates new Connection with properties and read-write socket
    pub fn new(properties: (Version, Params), channel: Pin<Box<dyn Channel>>) -> Connection {
        Connection { properties, channel }
    }

    /// connection properties tuple
    pub fn properties(&self) -> &(Version, Params) {
        &(self.properties)
    }

    async fn send_ready_for_query(&mut self) -> io::Result<Result<()>> {
        log::debug!("send ready for query message");
        self.channel
            .write_all(Message::ReadyForQuery.as_vec().as_slice())
            .await?;
        Ok(Ok(()))
    }

    /// receives and decodes a command from remote client
    pub async fn receive(&mut self) -> io::Result<Result<Command>> {
        self.send_ready_for_query().await?.expect("to send ready for query");
        let mut buffer = [0u8; 1];
        let tag = self.channel.read_exact(&mut buffer).await.map(|_| buffer[0])?;
        if b'X' == tag {
            Ok(Ok(Command::Terminate))
        } else {
            let mut buffer = [0u8; 4];
            let len = self
                .channel
                .read_exact(&mut buffer)
                .await
                .map(|_| NetworkEndian::read_u32(&buffer))?;
            let mut buffer = Vec::with_capacity(len as usize - 4);
            buffer.resize(len as usize - 4, b'0');
            let sql_buff = self.channel.read_exact(&mut buffer).await.map(|_| buffer)?;
            log::debug!("FOR TEST sql = {:?}", sql_buff);
            let sql = match String::from_utf8(sql_buff[..sql_buff.len() - 1].to_vec()) {
                Ok(sql) => sql,
                Err(_e) => return Ok(Err(Error::QueryIsNotValidUtfString)),
            };
            log::debug!("SQL = {}", sql);
            Ok(Ok(Command::Query(sql)))
        }
    }

    /// Sends response messages to client. Most of the time it is a single
    /// message, select result one of the exceptional situation
    pub async fn send(&mut self, query_result: QueryResult) -> io::Result<()> {
        let messages: Vec<Message> = query_result.map_or_else(|event| event.into(), |err| err.into());
        for message in messages {
            log::debug!("{:?}", message);
            self.channel.write_all(message.as_vec().as_slice()).await?;
        }
        log::debug!("end of the command is sent");
        Ok(())
    }
}

impl PartialEq for Connection {
    fn eq(&self, other: &Self) -> bool {
        self.properties().eq(other.properties())
    }
}

/// Struct description of metadata that describes how client should interpret
/// outgoing selected data
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ColumnMetadata {
    /// name of the column that was specified in query
    pub name: String,
    /// PostgreSQL data type id
    pub type_id: i32,
    /// PostgreSQL data type size
    pub type_size: i16,
}

impl ColumnMetadata {
    /// Creates new column metadata
    pub fn new(name: String, type_id: i32, type_size: i16) -> Self {
        Self {
            name,
            type_id,
            type_size,
        }
    }
}
