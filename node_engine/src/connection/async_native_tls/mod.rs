#![allow(unused_attributes)]
#![rustfmt::skip]

mod acceptor;
mod handshake;
mod std_adapter;
mod tls_stream;

pub use accept::accept;
pub use acceptor::{Error as AcceptError, TlsAcceptor};
pub use tls_stream::TlsStream;

pub use native_tls::{Certificate, Error, Identity, Protocol, Result};

mod accept {
    use super::TlsStream;
    use futures_lite::{AsyncRead, AsyncWrite};

    pub async fn accept<R, S, T>(file: R, password: S, stream: T) -> Result<TlsStream<T>, super::AcceptError>
    where
        R: AsyncRead + Unpin,
        S: AsRef<str>,
        T: AsyncRead + AsyncWrite + Unpin,
    {
        let acceptor = super::TlsAcceptor::new(file, password).await?;
        let stream = acceptor.accept(stream).await?;

        Ok(stream)
    }
}
