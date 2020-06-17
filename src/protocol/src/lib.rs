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

extern crate log;

use crate::messages::Message;
use byteorder::{ByteOrder, NetworkEndian};
use bytes::BytesMut;
use futures::{AsyncReadExt, AsyncWriteExt};
use std::io;

pub use listener::QueryListener;
pub use listener::ServerListener;

pub mod listener;
pub mod messages;

pub type Version = i32;
pub type Params = Vec<(String, String)>;
pub type Result<T> = std::result::Result<T, Error>;

pub const VERSION_1: Version = 0x10000;
pub const VERSION_2: Version = 0x20000;
pub const VERSION_3: Version = 0x30000;
pub const VERSION_CANCEL: Version = (1234 << 16) + 5678;
pub const VERSION_SSL: Version = (1234 << 16) + 5679;
pub const VERSION_GSSENC: Version = (1234 << 16) + 5680;

#[derive(Debug, PartialEq)]
pub enum Error {
    QueryIsNotValidUtfString,
    UnsupportedVersion,
    UnsupportedRequest,
    UnrecognizedVersion,
}

#[derive(Debug, PartialEq)]
pub enum Command {
    Query(String),
    Terminate,
}

pub struct Connection<RW: AsyncReadExt + AsyncWriteExt + Unpin> {
    properties: (Version, Params, SslMode),
    socket: RW,
}

impl<RW: AsyncReadExt + AsyncWriteExt + Unpin> Connection<RW> {
    pub fn new(properties: (Version, Params, SslMode), socket: RW) -> Connection<RW> {
        Connection { properties, socket }
    }

    pub fn properties(&self) -> &(Version, Params, SslMode) {
        &(self.properties)
    }

    async fn send_ready_for_query(&mut self) -> io::Result<Result<()>> {
        log::debug!("send ready for query message");
        self.socket
            .write_all(Message::ReadyForQuery.as_vec().as_slice())
            .await?;
        Ok(Ok(()))
    }

    pub async fn read_query(&mut self) -> io::Result<Result<Command>> {
        self.send_ready_for_query().await?.expect("to send ready for query");
        let mut buffer = [0u8; 1];
        let tag = self.socket.read_exact(&mut buffer).await.map(|_| buffer[0])?;
        if b'X' == tag {
            Ok(Ok(Command::Terminate))
        } else {
            let mut buffer = [0u8; 4];
            let len = self
                .socket
                .read_exact(&mut buffer)
                .await
                .map(|_| NetworkEndian::read_u32(&buffer))?;
            let mut buffer = BytesMut::with_capacity(len as usize - 4);
            buffer.resize(len as usize - 4, b'0');
            let sql_buff = self.socket.read_exact(&mut buffer).await.map(|_| buffer)?;
            log::debug!("FOR TEST sql = {:?}", sql_buff);
            let sql = match String::from_utf8(sql_buff[..sql_buff.len() - 1].to_vec()) {
                Ok(sql) => sql,
                Err(_e) => return Ok(Err(Error::QueryIsNotValidUtfString)),
            };
            log::debug!("SQL = {}", sql);
            Ok(Ok(Command::Query(sql)))
        }
    }

    pub async fn send_response(&mut self, messages: Vec<Message>) -> io::Result<()> {
        for message in messages {
            log::debug!("{:?}", message);
            self.socket.write_all(message.as_vec().as_slice()).await?;
        }
        log::debug!("end of the command is sent");
        Ok(())
    }
}

impl<RW: AsyncReadExt + AsyncWriteExt + Unpin> PartialEq for Connection<RW> {
    fn eq(&self, other: &Self) -> bool {
        self.properties().eq(other.properties())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Field {
    pub name: String,
    pub type_id: i32,
    pub type_size: i16,
}

impl Field {
    pub fn new(name: String, type_id: i32, type_size: i16) -> Self {
        Self {
            name,
            type_id,
            type_size,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum SslMode {
    Require,
    Disable,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    mod connection {
        use super::*;

        #[cfg(test)]
        mod read_query {
            use super::*;
            use test_helpers::async_io;

            #[async_std::test]
            async fn read_termination_command() -> io::Result<()> {
                let test_case = async_io::TestCase::with_content(vec![&[88], &[0, 0, 0, 4]]).await;
                let mut connection = Connection::new((VERSION_3, vec![], SslMode::Disable), test_case);

                let query = connection.read_query().await?;

                assert_eq!(query, Ok(Command::Terminate));

                Ok(())
            }

            #[async_std::test]
            async fn read_query_successfully() -> io::Result<()> {
                let test_case = async_io::TestCase::with_content(vec![&[81], &[0, 0, 0, 14], b"select 1;\0"]).await;
                let mut connection = Connection::new((VERSION_3, vec![], SslMode::Disable), test_case.clone());

                let query = connection.read_query().await?;

                assert_eq!(query, Ok(Command::Query("select 1;".to_owned())));

                let actual_content = test_case.read_result().await;
                let mut expected_content = BytesMut::new();
                expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
                assert_eq!(actual_content, expected_content);

                Ok(())
            }

            #[async_std::test]
            async fn unexpected_eof_when_read_type_code_of_query_request() {
                let test_case = async_io::TestCase::with_content(vec![]).await;
                let mut connection = Connection::new((VERSION_3, vec![], SslMode::Disable), test_case);

                let query = connection.read_query().await;

                assert!(query.is_err());
            }

            #[async_std::test]
            async fn unexpected_eof_when_read_length_of_query() {
                let test_case = async_io::TestCase::with_content(vec![&[81]]).await;
                let mut connection = Connection::new((VERSION_3, vec![], SslMode::Disable), test_case);

                let query = connection.read_query().await;

                assert!(query.is_err());
            }

            #[async_std::test]
            async fn unexpected_eof_when_query_string() {
                let test_case = async_io::TestCase::with_content(vec![&[81], &[0, 0, 0, 14], b"sel;\0"]).await;
                let mut connection = Connection::new((VERSION_3, vec![], SslMode::Disable), test_case);

                let query = connection.read_query().await;

                assert!(query.is_err());
            }
        }
    }
}
