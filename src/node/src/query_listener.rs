// Copyright 2020 Alex Dukhno
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

use async_std::fs::File;
use async_trait::async_trait;
use futures_util::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    task::{Context, Poll},
};
use protocol::{listener::ProtocolConfiguration, Channel, QueryListener, ServerListener};
use smol::Async;
use std::{
    env, io,
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    pin::Pin,
};

#[async_trait]
impl QueryListener for SmolQueryListener {
    type ServerChannel = SmolServerListener;

    fn configuration(&self) -> &ProtocolConfiguration {
        &self.configuration
    }

    fn server_channel(&self) -> &Self::ServerChannel {
        &self.listener
    }
}

pub struct SmolServerListener {
    inner: Async<TcpListener>,
}

impl SmolServerListener {
    fn new(inner: Async<TcpListener>) -> SmolServerListener {
        SmolServerListener { inner }
    }
}

#[async_trait]
impl ServerListener for SmolServerListener {
    async fn tcp_channel(&self) -> io::Result<(Pin<Box<dyn Channel>>, SocketAddr)> {
        let (socket, address) = self.inner.accept().await?;
        Ok((Box::pin(SmolChannel::new(socket)), address))
    }

    async fn tls_channel(&self, tcp_channel: Pin<Box<dyn Channel>>) -> io::Result<Pin<Box<dyn Channel>>> {
        let key = File::open(pfx_certificate_path()).await?;
        let password = pfx_certificate_password();
        let socket = async_native_tls::accept(key, password, tcp_channel).await.unwrap();
        Ok(Box::pin(SmolChannel::new(socket)))
    }
}

pub struct SmolQueryListener {
    configuration: ProtocolConfiguration,
    listener: SmolServerListener,
}

impl SmolQueryListener {
    pub async fn bind<A: ToString>(addr: A, configuration: ProtocolConfiguration) -> io::Result<SmolQueryListener> {
        let tcp_listener = Async::<TcpListener>::bind(addr)?;
        let listener = SmolServerListener::new(tcp_listener);

        Ok(Self {
            configuration,
            listener,
        })
    }
}

struct SmolChannel<RW: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync> {
    socket: RW,
}

impl<RW: AsyncRead + AsyncWrite + Unpin + Send + Sync> SmolChannel<RW> {
    pub fn new(socket: RW) -> Self {
        Self { socket }
    }
}

impl<RW: AsyncRead + AsyncWrite + Unpin + Send + Sync> Channel for SmolChannel<RW> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        let socket = &mut self.get_mut().socket;
        Pin::new(socket).poll_read(cx, buf)
    }

    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let socket = &mut self.get_mut().socket;
        Pin::new(socket).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let socket = &mut self.get_mut().socket;
        Pin::new(socket).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let socket = &mut self.get_mut().socket;
        Pin::new(socket).poll_close(cx)
    }
}

fn pfx_certificate_path() -> PathBuf {
    let file = env::var("PFX_CERTIFICATE_FILE").unwrap();
    let path = Path::new(&file);
    if path.is_absolute() {
        return path.to_path_buf();
    }

    let current_dir = env::current_dir().unwrap();
    current_dir.as_path().join(path)
}

fn pfx_certificate_password() -> String {
    env::var("PFX_CERTIFICATE_PASSWORD").unwrap()
}
