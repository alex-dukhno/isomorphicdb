// Copyright 2020 - 2021 Alex Dukhno
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::connection::async_native_tls::{self, AcceptError, TlsStream};
use async_io::Async;
use blocking::Unblock;
use futures_lite::io::{AsyncRead, AsyncWrite};
#[cfg(test)]
use std::sync::{Arc, Mutex};
use std::{
    fs::File,
    io,
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
};

pub struct Network {
    inner: NetworkInner,
}

impl Network {
    pub async fn accept(&self) -> io::Result<(Stream, SocketAddr)> {
        self.inner.accept().await
    }

    pub async fn tls_accept(
        &self,
        certificate_path: &PathBuf,
        password: &str,
        stream: Stream,
    ) -> Result<SecureStream, AcceptError> {
        self.inner.tls_accept(certificate_path, password, stream).await
    }
}

impl From<Async<TcpListener>> for Network {
    fn from(tcp: Async<TcpListener>) -> Self {
        Network {
            inner: NetworkInner::Tcp(tcp),
        }
    }
}

#[cfg(test)]
impl From<TestCase> for Network {
    fn from(test_case: TestCase) -> Self {
        Network {
            inner: NetworkInner::Mock(test_case),
        }
    }
}

enum NetworkInner {
    Tcp(Async<TcpListener>),
    #[cfg(test)]
    Mock(TestCase),
}

impl NetworkInner {
    async fn accept(&self) -> io::Result<(Stream, SocketAddr)> {
        match self {
            NetworkInner::Tcp(tcp) => tcp.accept().await.map(|(stream, addr)| (Stream::from(stream), addr)),
            #[cfg(test)]
            NetworkInner::Mock(data) => {
                use std::net::{IpAddr, Ipv4Addr};
                Ok((
                    Stream::from(data.clone()),
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1000),
                ))
            }
        }
    }

    async fn tls_accept(
        &self,
        certificate_path: &PathBuf,
        password: &str,
        stream: Stream,
    ) -> Result<SecureStream, AcceptError> {
        match self {
            NetworkInner::Tcp(_) => Ok(SecureStream::from(
                async_native_tls::accept(Unblock::new(File::open(certificate_path)?), password, stream).await?,
            )),
            #[cfg(test)]
            NetworkInner::Mock(data) => Ok(SecureStream::from(data.clone())),
        }
    }
}

pub struct SecureStream {
    inner: SecureStreamInner,
}

impl From<TlsStream<Stream>> for SecureStream {
    fn from(stream: TlsStream<Stream>) -> SecureStream {
        SecureStream {
            inner: SecureStreamInner::Tls(stream),
        }
    }
}

#[cfg(test)]
impl From<TestCase> for SecureStream {
    fn from(test_case: TestCase) -> SecureStream {
        SecureStream {
            inner: SecureStreamInner::Mock(test_case),
        }
    }
}

enum SecureStreamInner {
    Tls(TlsStream<Stream>),
    #[cfg(test)]
    Mock(TestCase),
}

impl AsyncRead for SecureStream {
    fn poll_read(self: Pin<&mut SecureStream>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        match &mut self.get_mut().inner {
            SecureStreamInner::Tls(tls) => Pin::new(tls).poll_read(cx, buf),
            #[cfg(test)]
            SecureStreamInner::Mock(data) => Pin::new(data).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for SecureStream {
    fn poll_write(self: Pin<&mut SecureStream>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match &mut self.get_mut().inner {
            SecureStreamInner::Tls(tls) => Pin::new(tls).poll_write(cx, buf),
            #[cfg(test)]
            SecureStreamInner::Mock(data) => Pin::new(data).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut SecureStream>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            SecureStreamInner::Tls(tls) => Pin::new(tls).poll_flush(cx),
            #[cfg(test)]
            SecureStreamInner::Mock(data) => Pin::new(data).poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut SecureStream>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            SecureStreamInner::Tls(tls) => Pin::new(tls).poll_close(cx),
            #[cfg(test)]
            SecureStreamInner::Mock(data) => Pin::new(data).poll_close(cx),
        }
    }
}

pub struct Stream {
    inner: StreamInner,
}

impl From<Async<TcpStream>> for Stream {
    fn from(tcp: Async<TcpStream>) -> Stream {
        Stream {
            inner: StreamInner::Tcp(tcp),
        }
    }
}

#[cfg(test)]
impl From<TestCase> for Stream {
    fn from(test_case: TestCase) -> Stream {
        Stream {
            inner: StreamInner::Mock(test_case),
        }
    }
}

enum StreamInner {
    Tcp(Async<TcpStream>),
    #[cfg(test)]
    Mock(TestCase),
}

impl AsyncRead for Stream {
    fn poll_read(self: Pin<&mut Stream>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        match &mut self.get_mut().inner {
            StreamInner::Tcp(tcp) => Pin::new(tcp).poll_read(cx, buf),
            #[cfg(test)]
            StreamInner::Mock(data) => Pin::new(data).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(self: Pin<&mut Stream>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match &mut self.get_mut().inner {
            StreamInner::Tcp(tcp) => Pin::new(tcp).poll_write(cx, buf),
            #[cfg(test)]
            StreamInner::Mock(data) => Pin::new(data).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Stream>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            StreamInner::Tcp(tcp) => Pin::new(tcp).poll_flush(cx),
            #[cfg(test)]
            StreamInner::Mock(data) => Pin::new(data).poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut Stream>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            StreamInner::Tcp(tcp) => Pin::new(tcp).poll_close(cx),
            #[cfg(test)]
            StreamInner::Mock(data) => Pin::new(data).poll_close(cx),
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
struct TestCaseInner {
    read_content: Vec<u8>,
    read_index: usize,
    write_content: Vec<u8>,
    write_index: usize,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub struct TestCase {
    inner: Arc<Mutex<TestCaseInner>>,
}

#[cfg(test)]
impl TestCase {
    pub fn new(content: Vec<&[u8]>) -> TestCase {
        TestCase {
            inner: Arc::new(Mutex::new(TestCaseInner {
                read_content: content.concat(),
                read_index: 0,
                write_content: vec![],
                write_index: 0,
            })),
        }
    }

    pub async fn read_result(&self) -> Vec<u8> {
        self.inner.lock().unwrap().write_content.clone()
    }
}

#[cfg(test)]
impl AsyncRead for TestCase {
    fn poll_read(self: Pin<&mut Self>, _cx: &mut Context, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        let mut case = self.get_mut().inner.lock().unwrap();
        if buf.len() > case.read_content.len() - case.read_index {
            Poll::Ready(Err(io::Error::from(io::ErrorKind::UnexpectedEof)))
        } else {
            for (i, item) in buf.iter_mut().enumerate() {
                *item = case.read_content[case.read_index + i];
            }
            case.read_index += buf.len();
            Poll::Ready(Ok(buf.len()))
        }
    }
}

#[cfg(test)]
impl AsyncWrite for TestCase {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        let mut case = self.get_mut().inner.lock().unwrap();
        case.write_content.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
