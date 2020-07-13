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

use crate::messages::Encryption;
use crate::{
    messages::Message, Connection, Error, Params, Result, Version, VERSION_1, VERSION_2, VERSION_3, VERSION_CANCEL,
    VERSION_GSSENC, VERSION_SSL,
};
use async_trait::async_trait;
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Buf, BytesMut};
use futures_util::{
    io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    task::{Context, Poll},
};
use itertools::Itertools;
use std::net::SocketAddr;
use std::pin::Pin;

/// Listener trait that use underline network to `accept` queries from clients
#[async_trait]
pub trait QueryListener {
    /// some docs
    type ServerChannel: ServerListener + Unpin + Send + Sync + 'static;

    /// accepts incoming client connections
    #[allow(clippy::if_same_then_else)]
    async fn accept<RW>(&self) -> io::Result<Result<Connection<Channel<RW>>>>
    where
        RW: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync + 'static,
    {
        let (mut socket, address): (Channel<RW>, SocketAddr) = self.server_channel().tcp_channel().await?;
        log::debug!("ADDRESS {:?}", address);

        loop {
            let len = read_len(&mut socket).await?;
            let message = read_message(len, &mut socket).await?;
            log::debug!("MESSAGE FOR TEST = {:#?}", message);
            match decode_startup(message) {
                Ok(ClientHandshake::SslRequest) => {
                    if self.secure().ssl_support() {
                        socket.write_all(Encryption::AcceptSsl.into()).await?;
                        socket = self.server_channel().tls_channel(socket).await?;
                    } else {
                        socket.write_all(Encryption::RejectSsl.into()).await?;
                    }
                }
                Ok(ClientHandshake::GssEncryptRequest) => return Ok(Err(Error::UnsupportedRequest)),
                Ok(ClientHandshake::Startup(version, params)) => {
                    return Ok(Ok(Connection::new((version, params), socket)))
                }
                Err(error) => return Ok(Err(error)),
            }
        }
    }

    /// handles a new connection from a client, returns true if the server
    /// should continue running, otherwise the server should exit.
    fn handle_connection<RW>(&self, connection: Connection<Channel<RW>>) -> bool
    where
        RW: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync + 'static;

    /// returns implementation of `ServerChannel`
    fn server_channel(&self) -> &Self::ServerChannel;

    /// returns configuration of accepting or rejecting secure connections from
    /// clients
    fn secure(&self) -> &Secure;
}

/// Trait that uses underline network protocol to establish bidirectional
/// protocol channels
#[async_trait]
pub trait ServerListener {
    /// returns bidirectional TCP channel with client and socket address
    async fn tcp_channel<RW>(&self) -> io::Result<(Channel<RW>, SocketAddr)>
    where
        RW: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync + 'static;
    /// returns bidirectional TLS channel with client and socket address
    async fn tls_channel<RW>(&self, tcp_socket: Channel<RW>) -> io::Result<Channel<RW>>
    where
        RW: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync + 'static;
}

///
pub enum Channel<RW: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync + 'static> {
    ///
    Plain(RW),
    ///
    Secured(RW),
}

impl<RW: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static> AsyncRead for Channel<RW> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Channel::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Channel::Secured(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl<RW: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static> AsyncWrite for Channel<RW> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Channel::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Channel::Secured(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Channel::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Channel::Secured(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Channel::Plain(stream) => Pin::new(stream).poll_close(cx),
            Channel::Secured(stream) => Pin::new(stream).poll_close(cx),
        }
    }
}

/// Struct to configure possible secure providers for client-server communication
/// PostgreSQL Wire Protocol supports `ssl`/`tls` and `gss` encryption
pub struct Secure {
    ssl: bool,
    gssenc: bool,
}

impl Secure {
    /// Creates configuration that support neither `ssl` nor `gss` encryption
    pub fn none() -> Secure {
        Secure {
            ssl: false,
            gssenc: false,
        }
    }

    /// Creates configuration that support only `ssl`
    pub fn ssl_only() -> Secure {
        Secure {
            ssl: true,
            gssenc: false,
        }
    }

    /// Creates configuration that support only `gss` encryption
    pub fn gssenc_only() -> Secure {
        Secure {
            ssl: false,
            gssenc: true,
        }
    }

    /// Creates configuration that support both `ssl` and `gss` encryption
    pub fn both() -> Secure {
        Secure {
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

async fn read_message<RW>(len: usize, socket: &mut RW) -> io::Result<BytesMut>
where
    RW: AsyncReadExt + AsyncWriteExt + Unpin,
{
    let mut buffer = BytesMut::with_capacity(len);
    buffer.resize(len, b'0');
    socket.read_exact(&mut buffer).await.map(|_| buffer)
}

fn decode_startup(mut message: BytesMut) -> Result<ClientHandshake> {
    let version = NetworkEndian::read_i32(message.bytes());
    log::debug!("VERSION FOR TEST = {:#?}", version);
    message.advance(4);
    match version {
        VERSION_2 => Err(Error::UnsupportedVersion),
        VERSION_1 => Err(Error::UnsupportedVersion),
        VERSION_3 => {
            let params = message
                .bytes()
                .split(|b| *b == 0)
                .filter(|b| !b.is_empty())
                .map(|b| std::str::from_utf8(b).unwrap().to_owned())
                .tuples()
                .collect::<Params>();
            Ok(ClientHandshake::Startup(version, params))
        }
        VERSION_CANCEL => Err(Error::UnsupportedVersion),
        VERSION_SSL => Ok(ClientHandshake::SslRequest),
        VERSION_GSSENC => Ok(ClientHandshake::GssEncryptRequest),
        _ => Err(Error::UnrecognizedVersion),
    }
}

enum ClientHandshake {
    SslRequest,
    GssEncryptRequest,
    Startup(Version, Params),
}

async fn create_ssl_connection<RW>(mut socket: RW) -> io::Result<Connection<RW>>
where
    RW: AsyncReadExt + AsyncWriteExt + Unpin,
{
    let len = read_len(&mut socket).await?;
    log::debug!("LEN = {:?}", len);
    let mut message = read_message(len, &mut socket).await?;
    log::debug!("MESSAGE FOR TEST = {:#?}", message);
    let version = NetworkEndian::read_i32(message.bytes());
    message.advance(4);
    let parsed = {
        message
            .bytes()
            .split(|b| *b == 0)
            .filter(|b| !b.is_empty())
            .map(|b| std::str::from_utf8(b).unwrap().to_owned())
            .tuples()
            .collect::<Params>()
    };

    message.advance(message.remaining());
    log::debug!("MESSAGE FOR TEST = {:#?}", parsed);
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

    Ok(Connection::new((version, parsed), socket))
}

async fn create_version_3_connection<RW>(mut socket: RW, mut message: BytesMut) -> io::Result<Connection<RW>>
where
    RW: AsyncReadExt + AsyncWriteExt + Unpin,
{
    let parsed = message
        .bytes()
        .split(|b| *b == 0)
        .filter(|b| !b.is_empty())
        .map(|b| std::str::from_utf8(b).unwrap().to_owned())
        .tuples()
        .collect::<Params>();

    message.advance(message.remaining());
    log::debug!("Version {}\nparams = {:?}", VERSION_3, parsed);

    socket.write_all(Message::AuthenticationOk.as_vec().as_slice()).await?;
    Ok(Connection::new((VERSION_3, parsed), socket))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Version;
    use std::net::{IpAddr, Ipv4Addr};
    use test_helpers::{async_io, pg_frontend};

    struct MockQueryListener {
        server_listener: MockServerListener,
        secure: Secure,
    }

    impl MockQueryListener {
        fn new(tcp_test_case: async_io::TestCase, tls_test_case: async_io::TestCase, secure: Secure) -> Self {
            Self {
                server_listener: MockServerListener::new(tcp_test_case, tls_test_case),
                secure,
            }
        }
    }

    #[async_trait]
    impl QueryListener for MockQueryListener {
        type ServerChannel = MockServerListener;

        #[allow(clippy::clone_on_copy)]
        fn handle_connection<RW>(&self, mut connection: Connection<RW>) -> bool
        where
            RW: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync + 'static,
        {
            let properties = connection.properties();
            false
        }

        fn server_channel(&self) -> &Self::ServerChannel {
            &self.server_listener
        }

        fn secure(&self) -> &Secure {
            &self.secure
        }
    }

    struct MockServerListener {
        tcp_test_case: async_io::TestCase,
        tls_test_case: async_io::TestCase,
    }

    impl MockServerListener {
        fn new(tcp_test_case: async_io::TestCase, tls_test_case: async_io::TestCase) -> MockServerListener {
            MockServerListener {
                tcp_test_case,
                tls_test_case,
            }
        }
    }

    #[async_trait]
    impl ServerListener for MockServerListener {
        async fn tcp_channel<RW>(&self) -> io::Result<(Channel<RW>, SocketAddr)>
        where
            RW: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync + 'static,
        {
            Ok((
                Channel::Plain(self.tcp_test_case.clone()),
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5432),
            ))
        }

        async fn tls_channel<RW>(&self, _tcp_socket: Channel<RW>) -> io::Result<Channel<RW>>
        where
            RW: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync + 'static,
        {
            Ok(Channel::Secured(self.tls_test_case.clone()))
        }
    }

    #[cfg(test)]
    mod hand_shake {
        use super::*;

        #[async_std::test]
        async fn trying_read_from_empty_stream() {
            let tcp_test_case = async_io::TestCase::with_content(vec![]).await;
            let tls_test_case = async_io::TestCase::with_content(vec![]).await;

            let callback = |_| false;
            let listener = MockQueryListener::new(tcp_test_case, tls_test_case, Secure::none());

            let result = listener.start().await;
            assert!(result.is_err());
        }

        #[cfg(test)]
        mod rust_postgres {
            use super::*;
            use crate::VERSION_3;

            #[async_std::test]
            async fn trying_read_setup_message() {
                let tcp_test_case = async_io::TestCase::with_content(vec![&[0, 0, 0, 57]]).await;
                let tls_test_case = async_io::TestCase::with_content(vec![]).await;

                let callback = |_| false;
                let listener = MockQueryListener::new(tcp_test_case, tls_test_case, Secure::none());

                let result = listener.start().await;
                assert!(result.is_err());
            }

            #[async_std::test]
            async fn successful_connection_handshake() -> io::Result<()> {
                let tcp_test_case = async_io::TestCase::with_content(vec![
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
                let tls_test_case = async_io::TestCase::with_content(vec![]).await;

                let listener = MockQueryListener::new(tcp_test_case.clone(), tls_test_case, Secure::none());
                let result = listener.start().await?;
                assert!(result.is_ok());

                let actual_content = tcp_test_case.read_result().await;
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
                let tcp_test_case = async_io::TestCase::with_content(vec![&[0, 0, 0, 8]]).await;
                let tls_test_case = async_io::TestCase::with_content(vec![]).await;

                let callback = |_| false;
                let listener = MockQueryListener::new(tcp_test_case, tls_test_case, Secure::none());

                let result = listener.start().await;
                assert!(result.is_err());
            }

            #[async_std::test]
            async fn sending_reject_notification_for_none_secure() {
                let tcp_test_case =
                    async_io::TestCase::with_content(vec![pg_frontend::Message::SslRequired.as_vec().as_slice()]).await;
                let tls_test_case = async_io::TestCase::with_content(vec![]).await;

                let callback = |_| false;
                let listener = MockQueryListener::new(tcp_test_case.clone(), tls_test_case, Secure::none());
                let result = listener.start().await;
                assert!(result.is_err());

                let actual_content = tcp_test_case.read_result().await;
                let mut expected_content = BytesMut::new();
                expected_content.extend_from_slice(Encryption::RejectSsl.into());
                assert_eq!(actual_content, expected_content);
            }

            #[async_std::test]
            async fn sending_accept_notification_for_ssl_only_secure() {
                let tcp_test_case =
                    async_io::TestCase::with_content(vec![pg_frontend::Message::SslRequired.as_vec().as_slice()]).await;
                let tls_test_case = async_io::TestCase::with_content(vec![]).await;

                let callback = |_| false;
                let listener = MockQueryListener::new(tcp_test_case.clone(), tls_test_case, Secure::ssl_only());
                let result = listener.start().await;
                assert!(result.is_err());

                let actual_content = tcp_test_case.read_result().await;
                let mut expected_content = BytesMut::new();
                expected_content.extend_from_slice(Encryption::AcceptSsl.into());
                assert_eq!(actual_content, expected_content);
            }

            #[async_std::test]
            async fn successful_connection_handshake_for_none_secure() -> io::Result<()> {
                let tcp_test_case = async_io::TestCase::with_content(vec![
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
                let tls_test_case = async_io::TestCase::with_content(vec![]).await;

                let listener = MockQueryListener::new(tcp_test_case.clone(), tls_test_case, Secure::none());
                let result = listener.start().await;
                assert!(result.is_ok());

                let actual_content = tcp_test_case.read_result().await;
                let mut expected_content = BytesMut::new();
                expected_content.extend_from_slice(Encryption::RejectSsl.into());
                expected_content.extend_from_slice(Message::AuthenticationCleartextPassword.as_vec().as_slice());
                expected_content.extend_from_slice(Message::AuthenticationOk.as_vec().as_slice());
                expected_content.extend_from_slice(
                    Message::ParameterStatus("client_encoding".to_owned(), "UTF8".to_owned())
                        .as_vec()
                        .as_slice(),
                );
                expected_content.extend_from_slice(
                    Message::ParameterStatus("DateStyle".to_owned(), "ISO".to_owned())
                        .as_vec()
                        .as_slice(),
                );
                assert_eq!(actual_content, expected_content);

                Ok(())
            }

            #[async_std::test]
            async fn successful_connection_handshake_for_ssl_only_secure() -> io::Result<()> {
                let tcp_test_case =
                    async_io::TestCase::with_content(vec![pg_frontend::Message::SslRequired.as_vec().as_slice()]).await;
                let tls_test_case = async_io::TestCase::with_content(vec![
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

                let listener = MockQueryListener::new(tcp_test_case.clone(), tls_test_case.clone(), Secure::ssl_only());
                let result = listener.start().await;
                assert!(result.is_ok());

                let tcp_actual_content = tcp_test_case.read_result().await;
                let mut tcp_expected_content = BytesMut::new();
                tcp_expected_content.extend_from_slice(Encryption::AcceptSsl.into());
                assert_eq!(tcp_actual_content, tcp_expected_content);

                let tls_actual_content = tls_test_case.read_result().await;
                let mut tls_expected_content = BytesMut::new();
                tls_expected_content.extend_from_slice(Message::AuthenticationCleartextPassword.as_vec().as_slice());
                tls_expected_content.extend_from_slice(Message::AuthenticationOk.as_vec().as_slice());
                tls_expected_content.extend_from_slice(
                    Message::ParameterStatus("client_encoding".to_owned(), "UTF8".to_owned())
                        .as_vec()
                        .as_slice(),
                );
                tls_expected_content.extend_from_slice(
                    Message::ParameterStatus("DateStyle".to_owned(), "ISO".to_owned())
                        .as_vec()
                        .as_slice(),
                );
                assert_eq!(tls_actual_content, tls_expected_content);

                Ok(())
            }
        }
    }
}
