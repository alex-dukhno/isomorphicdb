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
    listener::ProtocolConfiguration,
    messages::{Encryption, Message},
    tests::{async_io, pg_frontend, MockChannel},
    Channel, QueryListener, ServerListener, VERSION_3,
};
use async_trait::async_trait;
use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
};

struct MockQueryListener {
    configuration: ProtocolConfiguration,
    server_listener: MockServerListener,
}

impl MockQueryListener {
    fn new(
        configuration: ProtocolConfiguration,
        tcp_test_case: async_io::TestCase,
        tls_test_case: async_io::TestCase,
    ) -> Self {
        Self {
            configuration,
            server_listener: MockServerListener::new(tcp_test_case, tls_test_case),
        }
    }
}

#[async_trait]
impl QueryListener for MockQueryListener {
    type ServerChannel = MockServerListener;

    fn configuration(&self) -> &ProtocolConfiguration {
        &self.configuration
    }

    fn server_channel(&self) -> &Self::ServerChannel {
        &self.server_listener
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
    async fn tcp_channel(&self) -> io::Result<(Pin<Box<dyn Channel>>, SocketAddr)> {
        Ok((
            Box::pin(MockChannel::new(self.tcp_test_case.clone())),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5432),
        ))
    }

    async fn tls_channel(&self, _tcp_channel: Pin<Box<dyn Channel>>) -> io::Result<Pin<Box<dyn Channel>>> {
        Ok(Box::pin(MockChannel::new(self.tls_test_case.clone())))
    }
}

#[async_std::test]
async fn trying_read_from_empty_stream() {
    let tcp_test_case = async_io::TestCase::with_content(vec![]).await;
    let tls_test_case = async_io::TestCase::with_content(vec![]).await;

    let listener = MockQueryListener::new(ProtocolConfiguration::none(), tcp_test_case, tls_test_case);

    let result = listener.accept().await;
    assert!(result.is_err());
}

#[cfg(test)]
mod rust_postgres {
    use super::*;

    #[async_std::test]
    async fn trying_read_setup_message() {
        let tcp_test_case = async_io::TestCase::with_content(vec![&[0, 0, 0, 57]]).await;
        let tls_test_case = async_io::TestCase::with_content(vec![]).await;

        let listener = MockQueryListener::new(ProtocolConfiguration::none(), tcp_test_case, tls_test_case);

        let result = listener.accept().await;
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

        let listener = MockQueryListener::new(ProtocolConfiguration::none(), tcp_test_case.clone(), tls_test_case);
        let connection = listener.accept().await?.expect("connection is open");
        assert_eq!(
            connection.properties(),
            &(
                VERSION_3,
                vec![
                    ("client_encoding".to_owned(), "UTF8".to_owned()),
                    ("timezone".to_owned(), "UTC".to_owned()),
                    ("user".to_owned(), "postgres".to_owned())
                ],
            )
        );

        let actual_content = tcp_test_case.read_result().await;
        let mut expected_content = Vec::new();
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

        let listener = MockQueryListener::new(ProtocolConfiguration::none(), tcp_test_case, tls_test_case);
        let result = listener.accept().await;
        assert!(result.is_err());
    }

    #[async_std::test]
    async fn sending_reject_notification_for_none_secure() {
        let tcp_test_case =
            async_io::TestCase::with_content(vec![pg_frontend::Message::SslRequired.as_vec().as_slice()]).await;
        let tls_test_case = async_io::TestCase::with_content(vec![]).await;

        let listener = MockQueryListener::new(ProtocolConfiguration::none(), tcp_test_case.clone(), tls_test_case);
        let result = listener.accept().await;
        assert!(result.is_err());

        let actual_content = tcp_test_case.read_result().await;
        let mut expected_content = Vec::new();
        expected_content.extend_from_slice(Encryption::RejectSsl.into());
        assert_eq!(actual_content, expected_content);
    }

    #[async_std::test]
    async fn sending_accept_notification_for_ssl_only_secure() {
        let tcp_test_case =
            async_io::TestCase::with_content(vec![pg_frontend::Message::SslRequired.as_vec().as_slice()]).await;
        let tls_test_case = async_io::TestCase::with_content(vec![]).await;

        let listener = MockQueryListener::new(ProtocolConfiguration::ssl_only(), tcp_test_case.clone(), tls_test_case);

        let result = listener.accept().await;
        assert!(result.is_err());

        let actual_content = tcp_test_case.read_result().await;
        let mut expected_content = Vec::new();
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

        let listener = MockQueryListener::new(ProtocolConfiguration::none(), tcp_test_case.clone(), tls_test_case);
        let result = listener.accept().await;
        assert!(result.is_ok());

        let actual_content = tcp_test_case.read_result().await;
        let mut expected_content = Vec::new();
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

        let listener = MockQueryListener::new(
            ProtocolConfiguration::ssl_only(),
            tcp_test_case.clone(),
            tls_test_case.clone(),
        );

        let result = listener.accept().await;
        assert!(result.is_ok());

        let tcp_actual_content = tcp_test_case.read_result().await;
        let mut tcp_expected_content = Vec::new();
        tcp_expected_content.extend_from_slice(&[b'S']);
        assert_eq!(tcp_actual_content, tcp_expected_content);

        let tls_actual_content = tls_test_case.read_result().await;
        let mut tls_expected_content = Vec::new();
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
