use crate::protocol::messages::Message;
use crate::protocol::{connection::Connection, Error, Result, Stream};
use byteorder::{ByteOrder, NetworkEndian};
use bytes::BytesMut;
use futures::io::{self, AsyncReadExt, AsyncWriteExt, ErrorKind};
use std::fmt::{self, Display, Formatter};
use std::io::{Read, Write};

pub struct HandShake<
    R: Read + Send + Sync + Unpin + 'static,
    W: Write + Send + Sync + Unpin + 'static,
> {
    reader: Stream<R>,
    writer: Stream<W>,
}

impl<R: Read + Send + Sync + Unpin + 'static, W: Write + Send + Sync + Unpin + 'static>
    HandShake<R, W>
{
    pub fn new(reader: Stream<R>, writer: Stream<W>) -> Self {
        Self { reader, writer }
    }

    pub async fn perform(mut self) -> io::Result<Result<Connection<R, W>>> {
        self.hand_ssl().await?;
        self.send_notice().await?;
        self.read_startup_message().await?;
        self.send_authentication_request().await?;
        self.handle_authentication_response().await
    }

    async fn read_len(&mut self) -> io::Result<Result<i32>> {
        let mut len_buff = [0u8; 4];
        match self.reader.read_exact(&mut len_buff).await {
            Ok(_) => {
                let len = NetworkEndian::read_i32(&mut len_buff);
                trace!("message length {}", len);
                Ok(Ok(len))
            }
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                trace!("Unexpected EOF {:?}", e);
                Ok(Err(Error))
            }
            Err(e) => {
                error!("{:?}", e);
                Err(e)
            }
        }
    }

    async fn hand_ssl(&mut self) -> io::Result<Result<SslMode>> {
        let len_read = self.read_len().await?;
        match len_read {
            Err(e) => Ok(Err(e)),
            Ok(len) => {
                let mut ssl_buff = [0u8; 4];
                match self.reader.read_exact(&mut ssl_buff).await {
                    Ok(_) => {
                        let ssl = NetworkEndian::read_u32(&mut ssl_buff);
                        trace!("ssl = {}", ssl);
                        Ok(Ok(SslMode::Require))
                    }
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                        trace!("Unexpected EOF {:?}", e);
                        Ok(Err(Error))
                    }
                    Err(e) => {
                        error!("{:?}", e);
                        Err(e)
                    }
                }
            }
        }
    }

    async fn send_notice(&mut self) -> io::Result<usize> {
        trace!("send notice tag");
        self.writer.write(Message::Notice.as_vec().as_slice()).await
    }

    async fn read_startup_message(&mut self) -> io::Result<Result<Setup>> {
        let len_read = self.read_len().await?;
        match len_read {
            Err(e) => Ok(Err(e)),
            Ok(len) => {
                let mut setup_message_buff = BytesMut::with_capacity((len as usize) - 4);
                setup_message_buff.extend(0..((len as u8) - 4));
                match self.reader.read_exact(&mut setup_message_buff).await {
                    Ok(_) => {
                        let version =
                            Version::from(NetworkEndian::read_u32(&mut setup_message_buff));
                        let parsed = setup_message_buff[4..setup_message_buff.len()]
                            .to_vec()
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
                        Ok(Ok(Setup(version, Params(params))))
                    }
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                        trace!("Unexpected EOF {:?}", e);
                        Ok(Err(Error))
                    }
                    Err(e) => {
                        error!("{:?}", e);
                        Err(e)
                    }
                }
            }
        }
    }

    async fn send_authentication_request(&mut self) -> io::Result<usize> {
        trace!("send authentication request tag");
        self.writer
            .write(Message::AuthenticationCleartextPassword.as_vec().as_slice())
            .await
    }

    async fn handle_authentication_response(&mut self) -> io::Result<Result<Connection<R, W>>> {
        trace!("waiting for authentication response");
        let mut p_tag_buff = [0u8; 1];
        match self.reader.read_exact(&mut p_tag_buff).await {
            Ok(_) => {
                let p = p_tag_buff[0];
                trace!("authentication response tag {}", p);
                let len_read = self.read_len().await?;
                match len_read {
                    Err(e) => Ok(Err(e)),
                    Ok(len) => {
                        let mut password_buff = BytesMut::with_capacity(len as usize);
                        password_buff.extend(0..((len as u8) - 4));
                        match self.reader.read_exact(&mut password_buff).await {
                            Ok(_) => {
                                self.writer
                                    .write(Message::AuthenticationOk.as_vec().as_slice())
                                    .await;
                                Ok(Ok(Connection::new(
                                    self.reader.clone(),
                                    self.writer.clone(),
                                )))
                            }
                            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                                trace!("Unexpected EOF {:?}", e);
                                Ok(Err(Error))
                            }
                            Err(e) => {
                                error!("{:?}", e);
                                Err(e)
                            }
                        }
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                trace!("Unexpected EOF {:?}", e);
                Ok(Err(Error))
            }
            Err(e) => {
                error!("{:?}", e);
                Err(e)
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Version {
    major: u16,
    minor: u16,
}

impl Version {
    pub fn from(raw: u32) -> Self {
        let major = (raw >> 16) as u16;
        let minor = (raw & 0xffff) as u16;
        Self { major, minor }
    }

    pub fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

#[derive(Debug, PartialEq)]
pub struct Params(pub Vec<(String, String)>);

#[derive(Debug, PartialEq)]
pub struct Setup(pub Version, pub Params);

#[derive(Debug, PartialEq)]
pub enum SslMode {
    Require,
}

#[cfg(test)]
mod tests {
    use super::*;
    use piper::{Arc, Mutex};
    use smol::Async;
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
        file.seek(SeekFrom::Start(0))
            .expect("set position at the beginning of a file");
        file.into_file()
    }

    fn stream(file: File) -> Stream<File> {
        Arc::new(Mutex::new(
            Async::new(file).expect("Failed to create asynchronous stream"),
        ))
    }

    #[cfg(test)]
    mod length {
        use super::*;

        #[async_std::test]
        async fn read_length_from_closed_stream() -> io::Result<()> {
            let mut hand_shake = HandShake::new(
                stream(empty_file().into_file()),
                stream(empty_file().into_file()),
            );
            let len = hand_shake.read_len().await?;
            assert_eq!(len, Err(Error));
            Ok(())
        }

        #[async_std::test]
        async fn read_length_from_open_stream() -> io::Result<()> {
            let mut hand_shake = HandShake::new(
                stream(file_with(vec![&[1u8; 4]])),
                stream(empty_file().into_file()),
            );
            let len = hand_shake.read_len().await?;
            assert_eq!(len, Ok(NetworkEndian::read_i32(&[1u8; 4])));
            Ok(())
        }
    }

    #[async_std::test]
    async fn send_notice() -> io::Result<()> {
        let write_content = empty_file();
        let mut path = write_content.reopen().expect("reopen file");
        let mut hand_shake = HandShake::new(
            stream(empty_file().into_file()),
            stream(write_content.into_file()),
        );
        hand_shake.send_notice().await?;
        let mut content = Vec::new();
        path.read_to_end(&mut content)?;
        let mut expected_content = BytesMut::new();
        expected_content.extend_from_slice(Message::Notice.as_vec().as_slice());
        assert_eq!(expected_content, content);
        Ok(())
    }

    #[cfg(test)]
    mod secure {
        use super::*;

        #[async_std::test]
        async fn successful_read_ssl_request() -> io::Result<()> {
            let mut hand_shake = HandShake::new(
                stream(file_with(vec![&[0, 0, 0, 8], &[4, 210, 22, 47]])),
                stream(empty_file().into_file()),
            );
            let ssl_mode = hand_shake.hand_ssl().await?;
            assert_eq!(ssl_mode, Ok(SslMode::Require));
            Ok(())
        }

        #[async_std::test]
        async fn unexpected_eof_when_read_length_of_ssl_request() -> io::Result<()> {
            let mut hand_shake = HandShake::new(
                stream(file_with(vec![&[]])),
                stream(empty_file().into_file()),
            );
            let ssl_mode = hand_shake.hand_ssl().await?;
            assert_eq!(ssl_mode, Err(Error));
            Ok(())
        }

        #[async_std::test]
        async fn unexpected_eof_when_read_ssl_request() -> io::Result<()> {
            let mut hand_shake = HandShake::new(
                stream(file_with(vec![&[0, 0, 0, 8]])),
                stream(empty_file().into_file()),
            );
            let ssl_mode = hand_shake.hand_ssl().await?;
            assert_eq!(ssl_mode, Err(Error));
            Ok(())
        }
    }

    #[cfg(test)]
    mod setup_message {
        use super::*;

        #[async_std::test]
        async fn unexpected_eof_when_read_length_of_setup_message() -> io::Result<()> {
            let mut hand_shake = HandShake::new(
                stream(file_with(vec![&[]])),
                stream(empty_file().into_file()),
            );
            let setup_message = hand_shake.read_startup_message().await?;
            assert_eq!(setup_message, Err(Error));
            Ok(())
        }

        #[async_std::test]
        async fn read_setup_message() -> io::Result<()> {
            let mut hand_shake = HandShake::new(
                stream(file_with(vec![&[0, 0, 0, 89],
                                      &[0, 3, 0, 0],
                                      b"user\0username\0database\0database_name\0application_name\0psql\0client_encoding\0UTF8\0\0"])),
                stream(empty_file().into_file()),
            );
            let setup_message = hand_shake.read_startup_message().await?;
            assert_eq!(
                setup_message,
                Ok(Setup(
                    Version::new(3, 0),
                    Params(vec![
                        ("user".to_owned(), "username".to_owned()),
                        ("database".to_owned(), "database_name".to_owned()),
                        ("application_name".to_owned(), "psql".to_owned()),
                        ("client_encoding".to_owned(), "UTF8".to_owned())
                    ])
                ))
            );
            Ok(())
        }

        #[async_std::test]
        async fn unexpected_eof_when_read_setup_message() -> io::Result<()> {
            let mut hand_shake = HandShake::new(
                stream(file_with(vec![&[0, 0, 0, 89], &[0, 3, 0, 0]])),
                stream(empty_file().into_file()),
            );
            let setup_message = hand_shake.read_startup_message().await?;
            assert_eq!(setup_message, Err(Error));
            Ok(())
        }
    }

    #[cfg(test)]
    mod authentication {
        use super::*;

        #[async_std::test]
        async fn successful_authentication() -> io::Result<()> {
            let write_content = empty_file();
            let mut path = write_content.reopen()?;
            let mut hand_shake = HandShake::new(
                stream(file_with(vec![&[112], &[0, 0, 0, 8], b"123\0"])),
                stream(write_content.into_file()),
            );
            hand_shake.send_authentication_request().await?;
            let authentication = hand_shake.handle_authentication_response().await?;
            assert!(authentication.is_ok());

            let mut content = Vec::new();
            path.read_to_end(&mut content);
            let mut expected_content = BytesMut::new();
            expected_content
                .extend_from_slice(Message::AuthenticationCleartextPassword.as_vec().as_slice());
            expected_content.extend_from_slice(Message::AuthenticationOk.as_vec().as_slice());
            Ok(())
        }

        #[async_std::test]
        async fn unexpected_eof_when_read_authentication_tag() -> io::Result<()> {
            let mut hand_shake =
                HandShake::new(stream(file_with(vec![])), stream(empty_file().into_file()));
            hand_shake.send_authentication_request().await?;
            let authentication = hand_shake.handle_authentication_response().await?;
            assert!(authentication.is_err());
            Ok(())
        }

        #[async_std::test]
        async fn unexpected_eof_when_read_authentication_message_length() -> io::Result<()> {
            let mut hand_shake = HandShake::new(
                stream(file_with(vec![&[112]])),
                stream(empty_file().into_file()),
            );
            hand_shake.send_authentication_request().await?;
            let authentication = hand_shake.handle_authentication_response().await?;
            assert!(authentication.is_err());
            Ok(())
        }

        #[async_std::test]
        async fn unexpected_eof_when_read_authentication_message_password() -> io::Result<()> {
            let mut hand_shake = HandShake::new(
                stream(file_with(vec![&[112], &[0, 0, 0, 8], b"1\0"])),
                stream(empty_file().into_file()),
            );
            hand_shake.send_authentication_request().await?;
            let authentication = hand_shake.handle_authentication_response().await?;
            assert!(authentication.is_err());
            Ok(())
        }
    }
}
