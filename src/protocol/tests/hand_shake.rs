mod async_io;
mod pg_frontend;

use futures_util::{AsyncReadExt, AsyncWriteExt};
use protocol::listener::Secure;
use protocol::{Connection, Params, QueryListener, ServerListener, SslMode, Version};
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

struct MockQueryListener<CB>
where
    CB: FnMut((Version, Params, SslMode)) -> bool + Copy,
{
    server_listener: MockServerListener,
    secure: Secure,
    callback: CB,
}

impl<CB: FnMut((Version, Params, SslMode)) -> bool + Copy> MockQueryListener<CB> {
    fn new(tcp_test_case: async_io::TestCase, tls_test_case: async_io::TestCase, secure: Secure, callback: CB) -> Self {
        Self {
            server_listener: MockServerListener::new(tcp_test_case, tls_test_case),
            secure,
            callback,
        }
    }
}

#[async_trait]
impl<CB: FnMut((Version, Params, SslMode)) -> bool + Copy> QueryListener for MockQueryListener<CB> {
    type TcpChannel = async_io::TestCase;
    type TlsChannel = async_io::TestCase;
    type ServerChannel = MockServerListener;

    #[allow(clippy::clone_on_copy)]
    fn handle_connection<RW>(&self, connection: Connection<RW>) -> bool
    where
        RW: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync + 'static,
    {
        let properties = connection.properties();
        (self.callback.clone())(properties.clone())
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
    type TcpChannel = async_io::TestCase;
    type TlsChannel = async_io::TestCase;

    async fn tcp_channel(&self) -> io::Result<(Self::TcpChannel, SocketAddr)> {
        Ok((
            self.tcp_test_case.clone(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 5432),
        ))
    }

    async fn tls_channel(&self, _tcp_socket: Self::TcpChannel) -> io::Result<Self::TlsChannel> {
        Ok(self.tls_test_case.clone())
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
        let listener = MockQueryListener::new(tcp_test_case, tls_test_case, Secure::none(), callback);

        let result = listener.start().await;
        assert!(result.is_err());
    }

    #[cfg(test)]
    mod rust_postgres {
        use super::*;
        use crate::VERSION_3;
        use bytes::BytesMut;
        use protocol::messages::Message;
        use protocol::VERSION_3;

        #[async_std::test]
        async fn trying_read_setup_message() {
            let tcp_test_case = async_io::TestCase::with_content(vec![&[0, 0, 0, 57]]).await;
            let tls_test_case = async_io::TestCase::with_content(vec![]).await;

            let callback = |_| false;
            let listener = MockQueryListener::new(tcp_test_case, tls_test_case, Secure::none(), callback);

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

            let callback = |properties| {
                assert_eq!(
                    properties,
                    (
                        VERSION_3,
                        vec![
                            ("client_encoding".to_owned(), "UTF8".to_owned()),
                            ("timezone".to_owned(), "UTC".to_owned()),
                            ("user".to_owned(), "postgres".to_owned())
                        ],
                        SslMode::Disable
                    )
                );

                false
            };

            let listener = MockQueryListener::new(tcp_test_case.clone(), tls_test_case, Secure::none(), callback);
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
        use bytes::BytesMut;
        use protocol::messages::Message;

        #[async_std::test]
        async fn trying_read_only_length_of_ssl_message() {
            let tcp_test_case = async_io::TestCase::with_content(vec![&[0, 0, 0, 8]]).await;
            let tls_test_case = async_io::TestCase::with_content(vec![]).await;

            let callback = |_| false;
            let listener = MockQueryListener::new(tcp_test_case, tls_test_case, Secure::none(), callback);

            let result = listener.start().await;
            assert!(result.is_err());
        }

        #[async_std::test]
        async fn sending_reject_notification_for_none_secure() {
            let tcp_test_case =
                async_io::TestCase::with_content(vec![pg_frontend::Message::SslRequired.as_vec().as_slice()]).await;
            let tls_test_case = async_io::TestCase::with_content(vec![]).await;

            let callback = |_| false;
            let listener = MockQueryListener::new(tcp_test_case.clone(), tls_test_case, Secure::none(), callback);
            let result = listener.start().await;
            assert!(result.is_err());

            let actual_content = tcp_test_case.read_result().await;
            let mut expected_content = BytesMut::new();
            expected_content.extend_from_slice(&[REJECT_SSL_ENCRYPTION]);
            assert_eq!(actual_content, expected_content);
        }

        #[async_std::test]
        async fn sending_accept_notification_for_ssl_only_secure() {
            let tcp_test_case =
                async_io::TestCase::with_content(vec![pg_frontend::Message::SslRequired.as_vec().as_slice()]).await;
            let tls_test_case = async_io::TestCase::with_content(vec![]).await;

            let callback = |_| false;
            let listener = MockQueryListener::new(tcp_test_case.clone(), tls_test_case, Secure::ssl_only(), callback);
            let result = listener.start().await;
            assert!(result.is_err());

            let actual_content = tcp_test_case.read_result().await;
            let mut expected_content = BytesMut::new();
            expected_content.extend_from_slice(&[ACCEPT_SSL_ENCRYPTION]);
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

            let callback = |_| false;

            let listener = MockQueryListener::new(tcp_test_case.clone(), tls_test_case, Secure::none(), callback);
            let result = listener.start().await;
            assert!(result.is_ok());

            let actual_content = tcp_test_case.read_result().await;
            let mut expected_content = BytesMut::new();
            expected_content.extend_from_slice(&[REJECT_SSL_ENCRYPTION]);
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

            let callback = |_| false;

            let listener = MockQueryListener::new(
                tcp_test_case.clone(),
                tls_test_case.clone(),
                Secure::ssl_only(),
                callback,
            );
            let result = listener.start().await;
            assert!(result.is_ok());

            let tcp_actual_content = tcp_test_case.read_result().await;
            let mut tcp_expected_content = BytesMut::new();
            tcp_expected_content.extend_from_slice(&[ACCEPT_SSL_ENCRYPTION]);
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
