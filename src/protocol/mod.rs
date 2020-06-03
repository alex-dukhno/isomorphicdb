pub mod channel;
pub mod connection;
pub mod hand_shake;
pub mod messages;

use byteorder::{ByteOrder, NetworkEndian};
use bytes::Buf;
use std::fmt::{self, Display, Formatter};

pub type Result<T> = std::result::Result<T, Error>;

pub fn supported_version() -> Version {
    Version::new(3, 0)
}

#[derive(Debug, PartialEq)]
pub struct Error;

#[derive(Debug, PartialEq)]
pub enum Command {
    Query(String),
    Terminate,
}

#[derive(Debug, PartialEq)]
pub struct Version {
    major: u16,
    minor: u16,
}

impl Version {
    pub fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

impl<B: Buf> From<&mut B> for Version {
    fn from(bytes: &mut B) -> Self {
        let raw = NetworkEndian::read_u32(bytes.bytes());
        bytes.advance(4);
        let major = (raw >> 16) as u16;
        let minor = (raw & 0xffff) as u16;
        Self::new(major, minor)
    }
}

#[derive(Debug, PartialEq)]
pub struct Params(pub Vec<(String, String)>);

impl<B: Buf> From<&mut B> for Params {
    fn from(bytes: &mut B) -> Self {
        let parsed = bytes
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
        bytes.advance(bytes.remaining());
        Params(params)
    }
}

#[derive(Debug, PartialEq)]
pub enum SslMode {
    Require,
    Disable,
}

#[cfg(test)]
mod compatibility {
    use super::*;
    use crate::protocol::{channel::Channel, hand_shake::HandShake, messages::*};
    use async_std::io;
    use bytes::BytesMut;
    use test_helpers::{async_io, frontend};

    #[async_std::test]
    async fn trying_read_from_empty_stream() -> io::Result<()> {
        let test_case = async_io::TestCase::with_content(vec![]).await;

        let hand_shake = HandShake::new(
            test_case.clone(),
            test_case.clone(),
            Channel::new(test_case.clone(), test_case.clone()),
        );

        let error = hand_shake.perform().await;

        assert!(error.is_err());

        Ok(())
    }

    #[cfg(test)]
    mod rust_postgres {
        use super::*;

        #[async_std::test]
        async fn trying_read_setup_message() -> io::Result<()> {
            let test_case = async_io::TestCase::with_content(vec![&[0, 0, 0, 57]]).await;

            let hand_shake = HandShake::new(
                test_case.clone(),
                test_case.clone(),
                Channel::new(test_case.clone(), test_case.clone()),
            );

            let error = hand_shake.perform().await;

            assert!(error.is_err());

            Ok(())
        }

        #[async_std::test]
        async fn successful_connection_handshake() -> io::Result<()> {
            let test_case = async_io::TestCase::with_content(vec![
                frontend::Message::SslDisabled.as_vec().as_slice(),
                frontend::Message::Setup(vec![
                    ("client_encoding", "UTF8"),
                    ("timezone", "UTC"),
                    ("user", "postgres"),
                ])
                .as_vec()
                .as_slice(),
            ])
            .await;

            let hand_shake = HandShake::new(
                test_case.clone(),
                test_case.clone(),
                Channel::new(test_case.clone(), test_case.clone()),
            );

            let connection = hand_shake.perform().await?.expect("connection is open");

            assert_eq!(
                connection.properties(),
                &(
                    Version::new(3, 0),
                    Params(vec![
                        ("client_encoding".to_owned(), "UTF8".to_owned()),
                        ("timezone".to_owned(), "UTC".to_owned()),
                        ("user".to_owned(), "postgres".to_owned())
                    ]),
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
        async fn trying_read_only_length_of_ssl_message() -> io::Result<()> {
            let test_case = async_io::TestCase::with_content(vec![&[0, 0, 0, 8]]).await;

            let hand_shake = HandShake::new(
                test_case.clone(),
                test_case.clone(),
                Channel::new(test_case.clone(), test_case.clone()),
            );

            let error = hand_shake.perform().await;

            assert!(error.is_err());

            Ok(())
        }

        #[async_std::test]
        async fn sending_notice_after_reading_ssl_message() -> io::Result<()> {
            let test_case = async_io::TestCase::with_content(vec![frontend::Message::SslRequired
                .as_vec()
                .as_slice()])
            .await;

            let hand_shake = HandShake::new(
                test_case.clone(),
                test_case.clone(),
                Channel::new(test_case.clone(), test_case.clone()),
            );

            let error = hand_shake.perform().await;

            assert!(error.is_err());

            let actual_content = test_case.read_result().await;
            let mut expected_content = BytesMut::new();
            expected_content.extend_from_slice(Message::Notice.as_vec().as_slice());

            assert_eq!(actual_content, expected_content);

            Ok(())
        }

        #[async_std::test]
        async fn successful_connection_handshake() -> io::Result<()> {
            let test_case = async_io::TestCase::with_content(vec![
                frontend::Message::SslRequired.as_vec().as_slice(),
                frontend::Message::Setup(vec![
                    ("user", "username"),
                    ("database", "database_name"),
                    ("application_name", "psql"),
                    ("client_encoding", "UTF8"),
                ])
                .as_vec()
                .as_slice(),
                frontend::Message::Password("123").as_vec().as_slice(),
            ])
            .await;

            let hand_shake = HandShake::new(
                test_case.clone(),
                test_case.clone(),
                Channel::new(test_case.clone(), test_case.clone()),
            );
            let connection = hand_shake.perform().await?;

            assert!(connection.is_ok());

            let actual_content = test_case.read_result().await;
            let mut expected_content = BytesMut::new();
            expected_content.extend_from_slice(Message::Notice.as_vec().as_slice());
            expected_content
                .extend_from_slice(Message::AuthenticationCleartextPassword.as_vec().as_slice());
            expected_content.extend_from_slice(Message::AuthenticationOk.as_vec().as_slice());

            assert_eq!(actual_content, expected_content);

            Ok(())
        }
    }
}
