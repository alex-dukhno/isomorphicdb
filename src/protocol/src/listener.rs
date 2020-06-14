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

use crate::{messages::Message, supported_version, Connection, Params, Result, SslMode, Version};
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Buf, BytesMut};
use futures::io::{self, AsyncReadExt, AsyncWriteExt};
use smol::Async;
use std::net::{TcpListener, TcpStream};

pub struct QueryListener {
    listener: Async<TcpListener>,
}

impl QueryListener {
    pub async fn bind<A: ToString>(addr: A) -> io::Result<QueryListener> {
        let listener = Async::<TcpListener>::bind(addr)?;
        Ok(QueryListener::new(listener))
    }

    fn new(listener: Async<TcpListener>) -> QueryListener {
        QueryListener { listener }
    }

    pub async fn accept(&self) -> io::Result<Result<Connection<Async<TcpStream>>>> {
        let (socket, address) = self.listener.accept().await?;
        log::debug!("ADDRESS {:?}", address);
        hand_shake(socket).await
    }
}

async fn hand_shake<RW: AsyncReadExt + AsyncWriteExt + Unpin>(mut socket: RW) -> io::Result<Result<Connection<RW>>> {
    let mut buffer = [0u8; 4];
    let len = socket
        .read_exact(&mut buffer)
        .await
        .map(|_| NetworkEndian::read_u32(&buffer))?;
    let mut buffer = BytesMut::with_capacity(len as usize - 4);
    buffer.resize(len as usize - 4, b'0');
    let mut message = socket.read_exact(&mut buffer).await.map(|_| buffer)?;
    let version = NetworkEndian::read_i32(message.bytes());
    message.advance(4);
    let state: State = if version == supported_version() {
        let parsed = message
            .bytes()
            .split(|b| *b == 0)
            .filter(|b| !b.is_empty())
            .map(|b| String::from_utf8(b.to_vec()).unwrap())
            .collect::<Vec<String>>();
        let mut params = vec![];
        let mut i = 0;
        while i < parsed.len() {
            params.push((parsed[i].clone(), parsed[i + 1].clone()));
            i += 2;
        }
        message.advance(message.remaining());
        log::debug!("Version {}\nparams = {:?}", version, params);
        State::Completed(version, params, SslMode::Disable)
    } else {
        State::InProgress(SslMode::Require)
    };

    match state {
        State::InProgress(ssl_mode) => {
            socket.write_all(Message::Notice.as_vec().as_slice()).await?;
            let mut buffer = [0u8; 4];
            let len = socket
                .read_exact(&mut buffer)
                .await
                .map(|_| NetworkEndian::read_u32(&buffer))?;
            let mut buffer = BytesMut::with_capacity(len as usize - 4);
            buffer.resize(len as usize - 4, b'0');
            let mut message = socket.read_exact(&mut buffer).await.map(|_| buffer)?;
            let version = NetworkEndian::read_i32(message.bytes());
            message.advance(4);
            let parsed = message
                .bytes()
                .split(|b| *b == 0)
                .filter(|b| !b.is_empty())
                .map(|b| String::from_utf8(b.to_vec()).unwrap())
                .collect::<Vec<String>>();
            let mut params = vec![];
            let mut i = 0;
            while i < parsed.len() {
                params.push((parsed[i].clone(), parsed[i + 1].clone()));
                i += 2;
            }
            message.advance(message.remaining());
            socket
                .write_all(Message::AuthenticationCleartextPassword.as_vec().as_slice())
                .await?;
            let mut buffer = [0u8; 1];
            let tag = socket.read_exact(&mut buffer).await.map(|_| buffer[0]);
            log::debug!("client message response tag {:?}", tag);
            log::debug!("waiting for authentication response");
            let mut buffer = [0u8; 4];
            let len = socket
                .read_exact(&mut buffer)
                .await
                .map(|_| NetworkEndian::read_u32(&buffer))?;
            let mut buffer = BytesMut::with_capacity(len as usize - 4);
            buffer.resize(len as usize - 4, b'0');
            let _message = socket.read_exact(&mut buffer).await.map(|_| buffer)?;
            socket.write_all(Message::AuthenticationOk.as_vec().as_slice()).await?;
            Ok(Ok(Connection::new((version, params, ssl_mode), socket)))
        }
        State::Completed(version, params, ssl_mode) => {
            socket.write_all(Message::AuthenticationOk.as_vec().as_slice()).await?;
            Ok(Ok(Connection::new((version, params, ssl_mode), socket)))
        }
    }
}

#[derive(Debug)]
enum State {
    Completed(Version, Params, SslMode),
    InProgress(SslMode),
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_helpers::{async_io, pg_frontend};

    #[cfg(test)]
    mod hand_shake {
        use super::*;

        #[async_std::test]
        async fn trying_read_from_empty_stream() {
            let test_case = async_io::TestCase::with_content(vec![]).await;

            let error = hand_shake(test_case).await;

            assert!(error.is_err());
        }

        #[cfg(test)]
        mod rust_postgres {
            use super::*;
            use crate::VERSION_3;

            #[async_std::test]
            async fn trying_read_setup_message() {
                let test_case = async_io::TestCase::with_content(vec![&[0, 0, 0, 57]]).await;

                let error = hand_shake(test_case).await;

                assert!(error.is_err());
            }

            #[async_std::test]
            async fn successful_connection_handshake() -> io::Result<()> {
                let test_case = async_io::TestCase::with_content(vec![
                    pg_frontend::Message::SslDisabled.as_vec().as_slice(),
                    pg_frontend::Message::Setup(vec![
                        ("client_encoding", "UTF8"),
                        ("timezone", "UTC"),
                        ("user", "postgres"),
                    ])
                    .as_vec()
                    .as_slice(),
                ])
                .await;

                let connection = hand_shake(test_case.clone()).await?.expect("connection is open");

                assert_eq!(
                    connection.properties(),
                    &(
                        VERSION_3,
                        vec![
                            ("client_encoding".to_owned(), "UTF8".to_owned()),
                            ("timezone".to_owned(), "UTC".to_owned()),
                            ("user".to_owned(), "postgres".to_owned())
                        ],
                        SslMode::Disable
                    )
                );

                let actual_content = test_case.read_result().await;
                let mut expected_content = BytesMut::new();
                expected_content.extend_from_slice(Message::AuthenticationOk.as_vec().as_slice());

                assert_eq!(actual_content, expected_content);

                Ok(())
            }
        }

        #[cfg(test)]
        mod psql_client {
            use super::*;

            #[async_std::test]
            async fn trying_read_only_length_of_ssl_message() {
                let test_case = async_io::TestCase::with_content(vec![&[0, 0, 0, 8]]).await;

                let error = hand_shake(test_case).await;

                assert!(error.is_err());
            }

            #[async_std::test]
            async fn sending_notice_after_reading_ssl_message() {
                let test_case =
                    async_io::TestCase::with_content(vec![pg_frontend::Message::SslRequired.as_vec().as_slice()]).await;

                let error = hand_shake(test_case.clone()).await;

                assert!(error.is_err());

                let actual_content = test_case.read_result().await;
                let mut expected_content = BytesMut::new();
                expected_content.extend_from_slice(Message::Notice.as_vec().as_slice());

                assert_eq!(actual_content, expected_content);
            }

            #[async_std::test]
            async fn successful_connection_handshake() -> io::Result<()> {
                let test_case = async_io::TestCase::with_content(vec![
                    pg_frontend::Message::SslRequired.as_vec().as_slice(),
                    pg_frontend::Message::Setup(vec![
                        ("user", "username"),
                        ("database", "database_name"),
                        ("application_name", "psql"),
                        ("client_encoding", "UTF8"),
                    ])
                    .as_vec()
                    .as_slice(),
                    pg_frontend::Message::Password("123").as_vec().as_slice(),
                ])
                .await;

                let connection = hand_shake(test_case.clone()).await?;

                assert!(connection.is_ok());

                let actual_content = test_case.read_result().await;
                let mut expected_content = BytesMut::new();
                expected_content.extend_from_slice(Message::Notice.as_vec().as_slice());
                expected_content.extend_from_slice(Message::AuthenticationCleartextPassword.as_vec().as_slice());
                expected_content.extend_from_slice(Message::AuthenticationOk.as_vec().as_slice());

                assert_eq!(actual_content, expected_content);

                Ok(())
            }
        }
    }
}
