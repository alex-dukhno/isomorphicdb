use std::future::Future;
use std::io::{Read, Write};
use std::marker::Unpin;
use std::pin::Pin;
use std::ptr::null_mut;
use std::task::{Context, Poll};

use native_tls::{Error, HandshakeError, MidHandshakeTlsStream};

use super::std_adapter::StdAdapter;
use super::TlsStream;
use futures_lite::{AsyncRead, AsyncWrite};

pub(crate) async fn handshake<F, S>(f: F, stream: S) -> Result<TlsStream<S>, Error>
where
    F: FnOnce(StdAdapter<S>) -> Result<native_tls::TlsStream<StdAdapter<S>>, HandshakeError<StdAdapter<S>>> + Unpin,
    S: AsyncRead + AsyncWrite + Unpin,
{
    let start = StartedHandshakeFuture(Some(StartedHandshakeFutureInner { f, stream }));

    match start.await {
        Err(e) => Err(e),
        Ok(StartedHandshake::Done(s)) => Ok(s),
        Ok(StartedHandshake::Mid(s)) => MidHandshake(Some(s)).await,
    }
}

struct MidHandshake<S>(Option<MidHandshakeTlsStream<StdAdapter<S>>>);

enum StartedHandshake<S> {
    Done(TlsStream<S>),
    Mid(MidHandshakeTlsStream<StdAdapter<S>>),
}

struct StartedHandshakeFuture<F, S>(Option<StartedHandshakeFutureInner<F, S>>);
struct StartedHandshakeFutureInner<F, S> {
    f: F,
    stream: S,
}

impl<F, S> Future for StartedHandshakeFuture<F, S>
where
    F: FnOnce(StdAdapter<S>) -> Result<native_tls::TlsStream<StdAdapter<S>>, HandshakeError<StdAdapter<S>>> + Unpin,
    S: Unpin,
    StdAdapter<S>: Read + Write,
{
    type Output = Result<StartedHandshake<S>, Error>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Result<StartedHandshake<S>, Error>> {
        let inner = self.0.take().expect("future polled after completion");
        let stream = StdAdapter {
            inner: inner.stream,
            context: ctx as *mut _ as *mut (),
        };

        match (inner.f)(stream) {
            Ok(mut s) => {
                s.get_mut().context = null_mut();
                Poll::Ready(Ok(StartedHandshake::Done(TlsStream::new(s))))
            }
            Err(HandshakeError::WouldBlock(mut s)) => {
                s.get_mut().context = null_mut();
                Poll::Ready(Ok(StartedHandshake::Mid(s)))
            }
            Err(HandshakeError::Failure(e)) => Poll::Ready(Err(e)),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Future for MidHandshake<S> {
    type Output = Result<TlsStream<S>, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut_self = self.get_mut();
        let mut s = mut_self.0.take().expect("future polled after completion");

        s.get_mut().context = cx as *mut _ as *mut ();
        match s.handshake() {
            Ok(stream) => Poll::Ready(Ok(TlsStream::new(stream))),
            Err(HandshakeError::Failure(e)) => Poll::Ready(Err(e)),
            Err(HandshakeError::WouldBlock(mut s)) => {
                s.get_mut().context = null_mut();
                mut_self.0 = Some(s);
                Poll::Pending
            }
        }
    }
}
