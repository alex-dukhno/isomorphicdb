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

use async_trait::async_trait;
use protocol::{listener::Secure, QueryListener, ServerListener};
use smol::Async;
use std::{
    io,
    net::{SocketAddr, TcpListener, TcpStream},
};

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
    type Channel = Async<TcpStream>;

    async fn channel(&self) -> io::Result<(Self::Channel, SocketAddr)> {
        self.inner.accept().await
    }
}

pub struct SmolQueryListener {
    listener: SmolServerListener,
    secure: Secure,
}

impl SmolQueryListener {
    pub async fn bind<A: ToString>(addr: A, secure: Secure) -> io::Result<SmolQueryListener> {
        let listener = Async::<TcpListener>::bind(addr)?;
        Ok(SmolQueryListener::new(SmolServerListener::new(listener), secure))
    }

    fn new(listener: SmolServerListener, secure: Secure) -> SmolQueryListener {
        SmolQueryListener { listener, secure }
    }
}

#[async_trait]
impl QueryListener for SmolQueryListener {
    type Channel = Async<TcpStream>;
    type ServerChannel = SmolServerListener;

    fn server_channel(&self) -> &Self::ServerChannel {
        &self.listener
    }

    fn secure(&self) -> &Secure {
        &self.secure
    }
}
