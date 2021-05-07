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

pub mod connection;

use crate::connection::{Connection, Established, New, Securing};
use native_tls::{Identity, TlsStream};
use std::{io, net::TcpStream};
use wire_protocol_payload::{InboundMessage, OutboundMessage};

pub type WireResult = std::result::Result<InboundMessage, WireError>;

#[derive(Debug)]
pub struct WireError;

pub trait WireConnection {
    fn receive(&mut self) -> io::Result<WireResult>;

    fn send(&mut self, outbound: OutboundMessage) -> io::Result<()>;
}

pub struct PgWireAcceptor<S: Securing<TcpStream, TlsStream<TcpStream>>> {
    secured: Option<S>,
}

impl<S: Securing<TcpStream, TlsStream<TcpStream>>> PgWireAcceptor<S> {
    pub fn new(secured: Option<S>) -> PgWireAcceptor<S> {
        PgWireAcceptor { secured }
    }
}

impl PgWireAcceptor<Identity> {
    pub fn accept(&self, socket: TcpStream) -> io::Result<Connection<Established, TcpStream, TlsStream<TcpStream>>> {
        let connection: Connection<New, TcpStream, TlsStream<TcpStream>> = Connection::new(socket);
        let connection = connection.hand_shake::<Identity>(self.secured.clone())?;
        let connection = connection.authenticate("whatever")?;
        let connection = connection.send_params(&[
            ("client_encoding", "UTF8"),
            ("DateStyle", "ISO"),
            ("integer_datetimes", "off"),
            ("server_version", "13.0"),
        ])?;
        let mut connection = connection.send_backend_keys(1, 1)?;
        connection.send(OutboundMessage::ReadyForQuery)?;
        Ok(connection)
    }
}
