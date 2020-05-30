use piper::{Arc, Mutex};
use smol::Async;

pub mod connection;
pub mod hand_shake;
pub mod messages;

pub type Result<T> = std::result::Result<T, Error>;
pub type Stream<I> = Arc<Mutex<Async<I>>>;

#[derive(Debug, PartialEq)]
pub struct Error;

#[derive(Debug, PartialEq)]
pub enum Command {
    Query(String),
    Terminate,
}

#[cfg(test)]
mod compatibility {
    use super::*;
    use crate::protocol::messages::*;
    use std::fs::File;
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::NamedTempFile;

    fn empty_file() -> NamedTempFile {
        NamedTempFile::new().expect("Failed to create tempfile")
    }

    fn file_with(content: Vec<&[u8]>) -> File {
        let mut file = empty_file();
        for bytes in content {
            file.write(bytes);
        }
        file.seek(SeekFrom::Start(0));
        file.into_file()
    }

    fn stream(file: File) -> Stream<File> {
        Arc::new(Mutex::new(
            Async::new(file).expect("Failed to create asynchronous stream"),
        ))
    }

    #[cfg(test)]
    mod psql_client {
        use super::*;
        use crate::protocol::hand_shake::HandShake;
        use bytes::BytesMut;
        use futures::io;
        use std::io::Read;

        #[async_std::test]
        async fn successful_connection_handshake() -> io::Result<()> {
            let write_content = empty_file();
            let mut path = write_content.reopen().expect("reopen file");
            let mut hand_shake = HandShake::new(
                stream(file_with(vec![
                    &[0, 0, 0, 8],
                    &[4, 210, 22, 47],
                    &[0, 0, 0, 89],
                    &[0, 3, 0, 0],
                    b"user\0username\0database\0database_name\0application_name\0psql\0client_encoding\0UTF8\0\0",
                    &[112],
                    &[0, 0, 0, 8],
                    b"123\0"
                ])),
                stream(write_content.into_file()),
            );
            let connection = hand_shake.perform().await?;
            assert!(connection.is_ok());
            let mut content = Vec::new();
            path.read_to_end(&mut content);
            let mut expected_content = BytesMut::new();
            expected_content.extend_from_slice(Message::Notice.as_vec().as_slice());
            expected_content
                .extend_from_slice(Message::AuthenticationCleartextPassword.as_vec().as_slice());
            expected_content.extend_from_slice(Message::AuthenticationOk.as_vec().as_slice());

            assert_eq!(content, expected_content.to_vec());
            Ok(())
        }
    }
}
