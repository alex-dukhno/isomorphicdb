use std::fmt;
use std::marker::Unpin;

use super::handshake::handshake;
use super::TlsStream;
use futures_lite::{AsyncRead, AsyncReadExt, AsyncWrite};

#[derive(Clone)]
pub struct TlsAcceptor(native_tls::TlsAcceptor);

/// An error returned from creating an acceptor.
#[derive(Debug)]
pub enum Error {
    /// NativeTls error.
    NativeTls(native_tls::Error),
    /// Io error.
    Io(std::io::Error),
}

impl From<native_tls::Error> for Error {
    fn from(error: native_tls::Error) -> Error {
        Error::NativeTls(error)
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Error {
        Error::Io(error)
    }
}

impl TlsAcceptor {
    /// Create a new TlsAcceptor based on an identity file and matching password.
    pub async fn new<R, S>(mut file: R, password: S) -> Result<Self, Error>
    where
        R: AsyncRead + Unpin,
        S: AsRef<str>,
    {
        let mut identity = vec![];
        file.read_to_end(&mut identity).await?;

        let identity = native_tls::Identity::from_pkcs12(&identity, password.as_ref())?;
        Ok(TlsAcceptor(native_tls::TlsAcceptor::new(identity)?))
    }

    /// Accepts a new client connection with the provided stream.
    ///
    /// This function will internally call `TlsAcceptor::accept` to connect
    /// the stream and returns a future representing the resolution of the
    /// connection operation. The returned future will resolve to either
    /// `TlsStream<S>` or `Error` depending if it's successful or not.
    ///
    /// This is typically used after a new socket has been accepted from a
    /// `TcpListener`. That socket is then passed to this function to perform
    /// the server half of accepting a client connection.
    pub async fn accept<S>(&self, stream: S) -> Result<TlsStream<S>, native_tls::Error>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let stream = handshake(move |s| self.0.accept(s), stream).await?;
        Ok(stream)
    }
}

impl fmt::Debug for TlsAcceptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsAcceptor").finish()
    }
}

impl From<native_tls::TlsAcceptor> for TlsAcceptor {
    fn from(inner: native_tls::TlsAcceptor) -> TlsAcceptor {
        TlsAcceptor(inner)
    }
}
