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

use async_native_tls::TlsStream;
use async_std::fs::File;
use async_trait::async_trait;
use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use protocol::{listener::ProtocolConfiguration, Command, Connection, QueryListener, ServerListener};
use smol::{Async, Task};
use sql_engine::Handler;
use std::{
    env, io,
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use storage::{backend::SledBackendStorage, frontend::FrontendStorage};

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
    type TcpChannel = Async<TcpStream>;
    type TlsChannel = TlsStream<Async<TcpStream>>;

    async fn tcp_channel(&self) -> io::Result<(Self::TcpChannel, SocketAddr)> {
        self.inner.accept().await
    }

    async fn tls_channel(&self, tcp_socket: Self::TcpChannel) -> io::Result<Self::TlsChannel> {
        let key = File::open(pfx_certificate_path()).await?;
        let password = pfx_certificate_password();
        Ok(async_native_tls::accept(key, password, tcp_socket).await.unwrap())
    }
}

pub struct SmolQueryListener {
    listener: SmolServerListener,
    secure: ProtocolConfiguration,
    storage: Arc<Mutex<FrontendStorage<SledBackendStorage>>>,
}

impl SmolQueryListener {
    pub async fn bind<A: ToString>(addr: A, secure: ProtocolConfiguration) -> io::Result<SmolQueryListener> {
        let tcp_listener = Async::<TcpListener>::bind(addr)?;
        let server_listener = SmolServerListener::new(tcp_listener);

        let query_listener = SmolQueryListener::new(server_listener, secure);

        Ok(query_listener)
    }

    fn new(listener: SmolServerListener, secure: ProtocolConfiguration) -> SmolQueryListener {
        Self {
            listener,
            secure,
            storage: Arc::new(Mutex::new(FrontendStorage::default().unwrap())),
        }
    }
}

#[async_trait]
impl QueryListener for SmolQueryListener {
    type TcpChannel = Async<TcpStream>;
    type TlsChannel = TlsStream<Async<TcpStream>>;
    type ServerChannel = SmolServerListener;

    fn handle_connection<RW>(&self, mut connection: Connection<RW>) -> bool
    where
        RW: AsyncReadExt + AsyncWriteExt + Unpin + Send + Sync + 'static,
    {
        let storage = self.storage.clone();
        Task::spawn(async move {
            let mut sql_handler = Handler::new(storage);

            log::debug!("ready to handle query");
            loop {
                match connection.receive().await {
                    Err(e) => {
                        log::error!("UNEXPECTED ERROR: {:?}", e);
                        break;
                    }
                    Ok(Err(e)) => {
                        log::error!("UNEXPECTED ERROR: {:?}", e);
                        break;
                    }
                    Ok(Ok(Command::Terminate)) => {
                        log::debug!("Closing connection with client");
                        break;
                    }
                    Ok(Ok(Command::Query(sql_query))) => {
                        let response = sql_handler.execute(sql_query.as_str()).expect("no system error");
                        match connection.send(response).await {
                            Ok(()) => {}
                            Err(error) => eprintln!("{:?}", error),
                        }
                    }
                }
            }
        })
        .detach();

        true
    }

    fn server_channel(&self) -> &Self::ServerChannel {
        &self.listener
    }

    fn configuration(&self) -> &ProtocolConfiguration {
        &self.secure
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
