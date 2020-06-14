use crate::{messages::Message, supported_version, Command, Error, Params, Result, Version};
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

async fn hand_shake<RW: AsyncReadExt + AsyncWriteExt + Unpin>(
    mut socket: RW,
) -> io::Result<Result<Connection<RW>>> {
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
            socket
                .write_all(Message::Notice.as_vec().as_slice())
                .await?;
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
            socket
                .write_all(Message::AuthenticationOk.as_vec().as_slice())
                .await?;
            Ok(Ok(Connection::new((version, params, ssl_mode), socket)))
        }
        State::Completed(version, params, ssl_mode) => {
            socket
                .write_all(Message::AuthenticationOk.as_vec().as_slice())
                .await?;
            Ok(Ok(Connection::new((version, params, ssl_mode), socket)))
        }
    }
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
        self.send_ready_for_query()
            .await?
            .expect("to send ready for query");
        let mut buffer = [0u8; 1];
        let tag = self
            .socket
            .read_exact(&mut buffer)
            .await
            .map(|_| buffer[0])?;
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
                Err(_e) => return Ok(Err(Error)),
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

                let connection = hand_shake(test_case.clone())
                    .await?
                    .expect("connection is open");

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
                    async_io::TestCase::with_content(vec![pg_frontend::Message::SslRequired
                        .as_vec()
                        .as_slice()])
                    .await;

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
                expected_content.extend_from_slice(
                    Message::AuthenticationCleartextPassword.as_vec().as_slice(),
                );
                expected_content.extend_from_slice(Message::AuthenticationOk.as_vec().as_slice());

                assert_eq!(actual_content, expected_content);

                Ok(())
            }
        }
    }

    #[cfg(test)]
    mod connection {
        use super::*;

        #[cfg(test)]
        mod read_query {
            use super::*;

            #[async_std::test]
            async fn read_termination_command() -> io::Result<()> {
                let test_case = async_io::TestCase::with_content(vec![&[88], &[0, 0, 0, 4]]).await;
                let mut connection =
                    Connection::new((supported_version(), vec![], SslMode::Disable), test_case);

                let query = connection.read_query().await?;

                assert_eq!(query, Ok(Command::Terminate));

                Ok(())
            }

            #[async_std::test]
            async fn read_query_successfully() -> io::Result<()> {
                let test_case =
                    async_io::TestCase::with_content(vec![&[81], &[0, 0, 0, 14], b"select 1;\0"])
                        .await;
                let mut connection = Connection::new(
                    (supported_version(), vec![], SslMode::Disable),
                    test_case.clone(),
                );

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
                let mut connection =
                    Connection::new((supported_version(), vec![], SslMode::Disable), test_case);

                let query = connection.read_query().await;

                assert!(query.is_err());
            }

            #[async_std::test]
            async fn unexpected_eof_when_read_length_of_query() {
                let test_case = async_io::TestCase::with_content(vec![&[81]]).await;
                let mut connection =
                    Connection::new((supported_version(), vec![], SslMode::Disable), test_case);

                let query = connection.read_query().await;

                assert!(query.is_err());
            }

            #[async_std::test]
            async fn unexpected_eof_when_query_string() {
                let test_case =
                    async_io::TestCase::with_content(vec![&[81], &[0, 0, 0, 14], b"sel;\0"]).await;
                let mut connection =
                    Connection::new((supported_version(), vec![], SslMode::Disable), test_case);

                let query = connection.read_query().await;

                assert!(query.is_err());
            }
        }
    }
}
