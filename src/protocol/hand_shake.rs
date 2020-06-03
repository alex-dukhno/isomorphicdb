use crate::protocol::{
    channel::Channel, connection::Connection, messages::Message, supported_version, Params, Result,
    SslMode, Version,
};
use async_std::io::{self, Read, Write};

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
    channel: Channel<R, W>,
}

impl<R: Read + Send + Sync + Unpin + 'static, W: Write + Send + Sync + Unpin + 'static>
    HandShake<R, W>
{
    pub fn new(reader: R, writer: W, channel: Channel<R, W>) -> Self {
        Self {
            reader,
            writer,
            channel,
        }
    }

    pub async fn perform(mut self) -> io::Result<Result<Connection<R, W>>> {
        let len_read = self.channel.read_message_len().await?;
        let state: State = match len_read {
            Err(e) => return Ok(Err(e)),
            Ok(len) => match self.channel.receive_message(len).await? {
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
                self.channel.send_message(Message::Notice).await?;
                let len_read = self.channel.read_message_len().await?;
                let (version, params) = match len_read {
                    Err(e) => return Ok(Err(e)),
                    Ok(len) => match self.channel.receive_message(len).await? {
                        Err(e) => return Ok(Err(e)),
                        Ok(mut message) => {
                            let version = Version::from(&mut message);
                            let params = Params::from(&mut message);
                            (version, params)
                        }
                    },
                };
                self.channel
                    .send_message(Message::AuthenticationCleartextPassword)
                    .await?;
                let tag = self.channel.read_tag().await?;
                trace!("client message response tag {:?}", tag);
                trace!("waiting for authentication response");
                let len_read = self.channel.read_message_len().await?;
                match len_read {
                    Err(e) => Ok(Err(e)),
                    Ok(len) => match self.channel.receive_message(len).await? {
                        Err(e) => Ok(Err(e)),
                        Ok(_message) => {
                            self.channel.send_message(Message::AuthenticationOk).await?;
                            Ok(Ok(Connection::new(
                                self.reader,
                                self.writer,
                                (version, params, ssl_mode),
                                self.channel,
                            )))
                        }
                    },
                }
            }
            State::Completed(version, params, ssl_mode) => {
                self.channel.send_message(Message::AuthenticationOk).await?;
                Ok(Ok(Connection::new(
                    self.reader,
                    self.writer,
                    (version, params, ssl_mode),
                    self.channel,
                )))
            }
        }
    }
}
