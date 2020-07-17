mod async_io;

use bytes::BytesMut;
use protocol::messages::Message;
use protocol::Command;
use protocol::Connection;
use protocol::SslMode;
use protocol::VERSION_3;
use std::io;

#[cfg(test)]
mod connection {
    use super::*;

    #[cfg(test)]
    mod read_query {
        use super::*;

        #[async_std::test]
        async fn read_termination_command() -> io::Result<()> {
            let test_case = async_io::TestCase::with_content(vec![&[88], &[0, 0, 0, 4]]).await;
            let mut connection = Connection::new((VERSION_3, vec![], SslMode::Disable), test_case);

            let query = connection.receive().await?;

            assert_eq!(query, Ok(Command::Terminate));

            Ok(())
        }

        #[async_std::test]
        async fn read_query_successfully() -> io::Result<()> {
            let test_case = async_io::TestCase::with_content(vec![&[81], &[0, 0, 0, 14], b"select 1;\0"]).await;
            let mut connection = Connection::new((VERSION_3, vec![], SslMode::Disable), test_case.clone());

            let query = connection.receive().await?;

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

            let query = connection.receive().await;

            assert!(query.is_err());
        }

        #[async_std::test]
        async fn unexpected_eof_when_read_length_of_query() {
            let test_case = async_io::TestCase::with_content(vec![&[81]]).await;
            let mut connection = Connection::new((VERSION_3, vec![], SslMode::Disable), test_case);

            let query = connection.receive().await;

            assert!(query.is_err());
        }

        #[async_std::test]
        async fn unexpected_eof_when_query_string() {
            let test_case = async_io::TestCase::with_content(vec![&[81], &[0, 0, 0, 14], b"sel;\0"]).await;
            let mut connection = Connection::new((VERSION_3, vec![], SslMode::Disable), test_case);

            let query = connection.receive().await;

            assert!(query.is_err());
        }
    }
}
