use crate::{
    channel::Channel, connection::Connection, messages::Message, supported_version, Params, Result,
    SslMode, Version,
};
use futures::io::{self, AsyncRead, AsyncWrite};

enum State {
    Completed(Version, Params, SslMode),
    InProgress(SslMode),
}

pub struct HandShake<
    R: AsyncRead + Send + Sync + Unpin + 'static,
    W: AsyncWrite + Send + Sync + Unpin + 'static,
> {
    channel: Channel<R, W>,
}

impl<
        R: AsyncRead + Send + Sync + Unpin + 'static,
        W: AsyncWrite + Send + Sync + Unpin + 'static,
    > HandShake<R, W>
{
    pub fn new(channel: Channel<R, W>) -> Self {
        Self { channel }
    }

    pub async fn perform(mut self) -> io::Result<Result<Connection<R, W>>> {
        let len = self.channel.read_message_len().await?;
        let mut message = self.channel.receive_message(len).await?;
        let version = Version::from(&mut message);
        let state: State = if version == supported_version() {
            let params = Params::from(&mut message);
            trace!("Version {}\nparams = {:?}", version, params);
            State::Completed(version, params, SslMode::Disable)
        } else {
            State::InProgress(SslMode::Require)
        };

        match state {
            State::InProgress(ssl_mode) => {
                self.channel.send_message(Message::Notice).await?;
                let len = self.channel.read_message_len().await?;
                let mut message = self.channel.receive_message(len).await?;
                let version = Version::from(&mut message);
                let params = Params::from(&mut message);
                self.channel
                    .send_message(Message::AuthenticationCleartextPassword)
                    .await?;
                let tag = self.channel.read_tag().await?;
                trace!("client message response tag {:?}", tag);
                trace!("waiting for authentication response");
                let len = self.channel.read_message_len().await?;
                let _message = self.channel.receive_message(len).await?;
                self.channel.send_message(Message::AuthenticationOk).await?;
                Ok(Ok(Connection::new(
                    (version, params, ssl_mode),
                    self.channel,
                )))
            }
            State::Completed(version, params, ssl_mode) => {
                self.channel.send_message(Message::AuthenticationOk).await?;
                Ok(Ok(Connection::new(
                    (version, params, ssl_mode),
                    self.channel,
                )))
            }
        }
    }
}
