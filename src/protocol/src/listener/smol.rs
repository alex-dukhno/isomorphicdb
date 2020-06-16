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

use crate::listener::{Secure, ServerListener};
use crate::QueryListener;
use async_trait::async_trait;
use smol::Async;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};

#[async_trait]
impl ServerListener for Async<TcpListener> {
    type Socket = Async<TcpStream>;

    async fn tcp_connection(&self) -> io::Result<(Self::Socket, SocketAddr)> {
        self.accept().await
    }
}

pub struct SmolQueryListener {
    listener: Async<TcpListener>,
    secure: Secure,
}

impl SmolQueryListener {
    pub async fn bind<A: ToString>(addr: A, secure: Secure) -> io::Result<SmolQueryListener> {
        let listener = Async::<TcpListener>::bind(addr)?;
        Ok(SmolQueryListener::new(listener, secure))
    }

    fn new(listener: Async<TcpListener>, secure: Secure) -> SmolQueryListener {
        SmolQueryListener { listener, secure }
    }
}

#[async_trait]
impl QueryListener for SmolQueryListener {
    type Socket = Async<TcpStream>;
    type ServerSocket = Async<TcpListener>;

    fn server_socket(&self) -> &Self::ServerSocket {
        &self.listener
    }

    fn secure(&self) -> &Secure {
        &self.secure
    }
}
