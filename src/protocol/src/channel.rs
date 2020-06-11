use crate::messages::Message;
use byteorder::{ByteOrder, NetworkEndian};
use bytes::BytesMut;
use futures::io::{self, AsyncReadExt, AsyncWriteExt};

#[derive(Debug)]
pub struct Channel<
    R: AsyncReadExt + Send + Sync + Unpin + 'static,
    W: AsyncWriteExt + Send + Sync + Unpin + 'static,
> {
    reader: R,
    writer: W,
}

impl<
        R: AsyncReadExt + Send + Sync + Unpin + 'static,
        W: AsyncWriteExt + Send + Sync + Unpin + 'static,
    > Channel<R, W>
{
    pub fn new(reader: R, writer: W) -> Self {
        Self { reader, writer }
    }

    pub async fn read_tag(&mut self) -> io::Result<u8> {
        let mut buffer = [0u8; 1];
        self.reader.read_exact(&mut buffer).await.map(|_| buffer[0])
    }

    pub async fn read_message_len(&mut self) -> io::Result<u32> {
        let mut buffer = [0u8; 4];
        self.reader
            .read_exact(&mut buffer)
            .await
            .map(|_| NetworkEndian::read_u32(&buffer))
    }

    pub async fn receive_message(&mut self, len: u32) -> io::Result<BytesMut> {
        let mut buffer = BytesMut::with_capacity(len as usize - 4);
        buffer.resize(len as usize - 4, b'0');
        self.reader.read_exact(&mut buffer).await.map(|_| buffer)
    }

    pub async fn send_message(&mut self, message: Message) -> io::Result<()> {
        self.writer.write_all(message.as_vec().as_slice()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::Message;
    use test_helpers::{async_io, frontend};

    #[async_std::test]
    async fn read_tag_from_empty_stream() {
        let test_case = async_io::TestCase::empty().await;
        let mut channel = Channel::new(test_case.clone(), test_case.clone());

        let tag = channel.read_tag().await;

        assert!(tag.is_err());
    }

    #[async_std::test]
    async fn read_tag() -> io::Result<()> {
        let test_case = async_io::TestCase::with_content(vec![&[8]]).await;
        let mut channel = Channel::new(test_case.clone(), test_case.clone());

        let tag = channel.read_tag().await?;

        assert_eq!(tag, 8);

        Ok(())
    }

    #[async_std::test]
    async fn read_length_from_empty_stream() {
        let test_case = async_io::TestCase::empty().await;
        let mut channel = Channel::new(test_case.clone(), test_case.clone());

        let len = channel.read_message_len().await;

        assert!(len.is_err());
    }

    #[async_std::test]
    async fn read_message_length() -> io::Result<()> {
        let test_case = async_io::TestCase::with_content(vec![&[0, 0, 8, 0]]).await;
        let mut channel = Channel::new(test_case.clone(), test_case.clone());

        let len = channel.read_message_len().await?;

        assert_eq!(len, 8 * 256);

        Ok(())
    }

    #[async_std::test]
    async fn read_message() -> io::Result<()> {
        let full_message = frontend::Message::SslRequired.as_vec();
        let test_case = async_io::TestCase::with_content(vec![full_message.as_slice()]).await;
        let mut channel = Channel::new(test_case.clone(), test_case.clone());

        let len = channel.read_message_len().await?;
        let message = channel.receive_message(len).await?;

        assert_eq!(message, BytesMut::from(&full_message[4..]));

        Ok(())
    }

    #[async_std::test]
    async fn read_message_from_empty_stream() -> io::Result<()> {
        let test_case = async_io::TestCase::with_content(vec![&[0, 0, 8, 0]]).await;
        let mut channel = Channel::new(test_case.clone(), test_case.clone());

        let len = channel.read_message_len().await?;
        let message = channel.receive_message(len).await;

        assert!(message.is_err());

        Ok(())
    }

    #[async_std::test]
    async fn write_message() -> io::Result<()> {
        let test_case = async_io::TestCase::empty().await;
        let mut channel = Channel::new(test_case.clone(), test_case.clone());

        channel.send_message(Message::AuthenticationOk).await?;

        let actual_content = test_case.read_result().await;

        assert_eq!(
            actual_content,
            Message::AuthenticationOk.as_vec().as_slice()
        );

        Ok(())
    }
}
