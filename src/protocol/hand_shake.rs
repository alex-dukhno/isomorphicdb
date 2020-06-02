use crate::protocol::{
    connection::Connection, messages::Message, supported_version, Error, Params, Result, SslMode,
    Version,
};
use async_std::io::{self, Read, Write};
use byteorder::{ByteOrder, NetworkEndian};
use bytes::BytesMut;
use futures::io::{AsyncReadExt, AsyncWriteExt};

enum State {
    Completed(Version, Params, SslMode),
    InProgress(SslMode),
}

pub struct HandShake<
    R: Read + Send + Sync + Unpin + 'static,
    W: Write + Send + Sync + Unpin + 'static,
> {
    reader: R,
    writer: W,
}

impl<R: Read + Send + Sync + Unpin + 'static, W: Write + Send + Sync + Unpin + 'static>
    HandShake<R, W>
{
    pub fn new(reader: R, writer: W) -> Self {
        Self { reader, writer }
    }

    async fn read_tag(&mut self) -> io::Result<Result<u8>> {
        let mut buffer = [0u8; 1];
        match self.reader.read_exact(&mut buffer).await {
            Ok(()) => {
                let tag = buffer[0];
                trace!("message length {}", tag);
                Ok(Ok(tag))
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

    async fn read_message_len(&mut self) -> io::Result<Result<i32>> {
        let mut buffer = [0u8; 4];
        match self.reader.read_exact(&mut buffer).await {
            Ok(()) => {
                let len = NetworkEndian::read_i32(&buffer);
                trace!("message length {}", len);
                Ok(Ok(len))
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

    async fn read_message(&mut self, len: i32) -> io::Result<Result<BytesMut>> {
        let mut message = BytesMut::with_capacity((len as usize) - 4);
        message.extend(0..((len as u8) - 4));
        match self.reader.read_exact(&mut message).await {
            Ok(()) => Ok(Ok(message)),
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

    async fn send_message(&mut self, message: Message) -> io::Result<()> {
        self.writer.write_all(message.as_vec().as_slice()).await?;
        Ok(())
    }

    pub async fn perform(mut self) -> io::Result<Result<Connection<R, W>>> {
        let len_read = self.read_message_len().await?;
        let state: State = match len_read {
            Err(e) => return Ok(Err(e)),
            Ok(len) => match self.read_message(len).await? {
                Err(e) => return Ok(Err(e)),
                Ok(mut message) => {
                    let version = Version::from(&mut message);
                    if version == supported_version() {
                        let params = Params::from(&mut message);
                        trace!("Version {}\nparams = {:?}", version, params);
                        State::Completed(version, params, SslMode::Disable)
                    } else {
                        State::InProgress(SslMode::Require)
                    }
                }
            },
        };
        match state {
            State::InProgress(ssl_mode) => {
                self.send_message(Message::Notice).await?;
                let len_read = self.read_message_len().await?;
                let (version, params) = match len_read {
                    Err(e) => return Ok(Err(e)),
                    Ok(len) => match self.read_message(len).await? {
                        Err(e) => return Ok(Err(e)),
                        Ok(mut message) => {
                            let version = Version::from(&mut message);
                            let params = Params::from(&mut message);
                            (version, params)
                        }
                    },
                };
                self.send_message(Message::AuthenticationCleartextPassword)
                    .await?;
                let tag = self.read_tag().await?;
                trace!("client message response tag {:?}", tag);
                trace!("waiting for authentication response");
                let len_read = self.read_message_len().await?;
                match len_read {
                    Err(e) => return Ok(Err(e)),
                    Ok(len) => match self.read_message(len).await? {
                        Err(e) => return Ok(Err(e)),
                        Ok(_message) => {
                            self.send_message(Message::AuthenticationOk).await?;
                            Ok(Ok(Connection::new(
                                self.reader,
                                self.writer,
                                (version, params, ssl_mode),
                            )))
                        }
                    },
                }
            }
            State::Completed(version, params, ssl_mode) => {
                self.writer
                    .write_all(Message::AuthenticationOk.as_vec().as_slice())
                    .await?;
                Ok(Ok(Connection::new(
                    self.reader,
                    self.writer,
                    (version, params, ssl_mode),
                )))
            }
        }
    }
}
