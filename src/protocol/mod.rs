pub mod connection;
pub mod hand_shake;
pub mod messages;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, PartialEq)]
pub struct Error;

#[derive(Debug, PartialEq)]
pub enum Command {
    Query(String),
    Terminate,
}

#[cfg(test)]
mod compatibility {
    #[cfg(test)]
    mod psql_client {
        use crate::protocol::hand_shake::HandShake;
        use crate::protocol::messages::*;
        use async_std::io;
        use bytes::BytesMut;
        use test_helpers::async_io;

        #[async_std::test]
        async fn successful_connection_handshake() -> io::Result<()> {
            let test_case = async_io::TestCase::with_content(vec![
                &[0, 0, 0, 8],
                &[4, 210, 22, 47],
                &[0, 0, 0, 89],
                &[0, 3, 0, 0],
                b"user\0username\0database\0database_name\0application_name\0psql\0client_encoding\0UTF8\0\0",
                &[112],
                &[0, 0, 0, 8],
                b"123\0"
            ]).await;

            let hand_shake = HandShake::new(test_case.clone(), test_case.clone());
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
