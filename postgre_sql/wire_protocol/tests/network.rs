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

use native_tls::{Certificate, Identity, TlsConnector};
use postgres::{Client, NoTls};
use postgres_native_tls::MakeTlsConnector;
use std::{
    fs,
    net::TcpListener,
    sync::{Arc, Condvar, Mutex},
};
use wire_protocol::PgWireAcceptor;

#[ignore]
#[test]
#[allow(clippy::mutex_atomic)]
fn non_secure() {
    const PORT: &str = "5432";

    let ready = Arc::new((Mutex::new(false), Condvar::new()));
    let inner_ready = ready.clone();

    let handle = std::thread::spawn(move || {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", PORT)).unwrap();
        let (lock, cond) = &*inner_ready;
        let mut started = lock.lock().unwrap();
        *started = true;
        cond.notify_one();
        drop(started);
        let (socket, _) = listener.accept().unwrap();

        let acceptor: PgWireAcceptor<Identity> = PgWireAcceptor::new(None);
        acceptor.accept(socket)
    });

    std::thread::yield_now();
    let (lock, cond) = &*ready;
    let mut started = lock.lock().unwrap();
    while !*started {
        started = cond.wait(started).unwrap();
    }

    let client = Client::connect(format!("host=0.0.0.0 port={} user=postgre_sql password=123", PORT).as_str(), NoTls).unwrap();

    client.close().unwrap();

    assert!(handle.join().is_ok());
}

#[ignore]
#[test]
#[allow(clippy::mutex_atomic)]
fn secure() {
    const PORT: &str = "5433";

    let ready = Arc::new((Mutex::new(false), Condvar::new()));
    let inner_ready = ready.clone();

    let handle = std::thread::spawn(move || {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", PORT)).unwrap();
        let (lock, cond) = &*inner_ready;
        let mut started = lock.lock().unwrap();
        *started = true;
        cond.notify_one();
        drop(started);
        let (socket, _) = listener.accept().unwrap();

        let cert = fs::read("../../tests/fixtures/identity.pfx").unwrap();
        let cert = Identity::from_pkcs12(&cert, "password").unwrap();

        let acceptor: PgWireAcceptor<Identity> = PgWireAcceptor::new(Some(cert));
        acceptor.accept(socket)
    });

    let cert = fs::read("../../tests/fixtures/certificate.crt").unwrap();
    let cert = Certificate::from_pem(&cert).unwrap();
    let connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .add_root_certificate(cert)
        .build()
        .unwrap();
    let connector = MakeTlsConnector::new(connector);

    std::thread::yield_now();
    let (lock, cond) = &*ready;
    let mut started = lock.lock().unwrap();
    while !*started {
        started = cond.wait(started).unwrap();
    }

    let client = postgres::Client::connect(
        format!("host=0.0.0.0 port={} user=postgre_sql password=123 sslmode=require", PORT).as_str(),
        connector,
    )
    .unwrap();

    client.close().unwrap();

    assert!(handle.join().is_ok());
}
