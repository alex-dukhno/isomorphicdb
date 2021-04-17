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

use native_tls::{HandshakeError, Identity, TlsAcceptor, TlsStream};
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::{self, Debug, Formatter};
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::str;

const ACCEPT_SSL: u8 = b'S';
const REJECT_SSL: u8 = b'N';
const AUTHENTICATION: u8 = b'R';
const PARAMETER_STATUS: u8 = b'S';
const BACKEND_KEY_DATA: u8 = b'K';

pub trait Secure<RW: Read + Write>: Clone {
    fn secure(self, socket: Socket) -> Result<RW, ()>;
}

impl Secure<SecureSocket<TlsStream<Socket>>> for Identity {
    fn secure(self, socket: Socket) -> Result<SecureSocket<TlsStream<Socket>>, ()> {
        let acceptor = TlsAcceptor::new(self).unwrap();
        let mut inter = socket;
        let socket = match acceptor.accept(inter) {
            Ok(socket) => socket,
            Err(HandshakeError::WouldBlock(e)) => {
                let mut inner = e;
                loop {
                    match inner.handshake() {
                        Ok(socket) => break socket,
                        Err(HandshakeError::WouldBlock(e)) => {
                            inner = e;
                        }
                        Err(e) => {
                            println!("2) {:?}", e);
                            return Err(());
                        }
                    }
                }
            }
            Err(e) => {
                println!("3) {:?}", e);
                return Err(());
            }
        };
        Ok(SecureSocket::from(socket))
    }
}

impl Secure<Socket> for Identity {
    fn secure(self, _socket: Socket) -> Result<Socket, ()> {
        println!("uups!!");
        Err(())
    }
}

#[cfg(test)]
impl Secure<TestData> for TestData {
    fn secure(self, _socket: Socket) -> Result<TestData, ()> {
        Ok(self)
    }
}

pub enum Channel<RW: Read + Write> {
    Plain(Socket),
    Secure(RW),
}

impl<RW: Read + Write> Channel<RW> {
    pub fn read_tag(&mut self) -> io::Result<u8> {
        let buff = &mut [0u8; 1];
        self.read_exact(buff.as_mut())?;
        Ok(buff[0])
    }

    pub fn read_message_len(&mut self) -> io::Result<usize> {
        let buff = &mut [0u8; 4];
        self.read_exact(buff.as_mut())?;
        Ok((i32::from_be_bytes(*buff) as usize) - 4)
    }

    pub fn read_message(&mut self, len: usize) -> io::Result<Vec<u8>> {
        let mut message = vec![0; len];
        self.read_exact(&mut message)?;
        Ok(message)
    }
}

impl<RW: Read + Write> Read for Channel<RW> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Channel::Plain(socket) => socket.read(buf),
            Channel::Secure(socket) => socket.read(buf),
        }
    }
}

impl<RW: Read + Write> Write for Channel<RW> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Channel::Plain(socket) => socket.write(buf),
            Channel::Secure(socket) => socket.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Channel::Plain(socket) => socket.flush(),
            Channel::Secure(socket) => socket.flush(),
        }
    }
}

pub struct Connection<S, RW: Read + Write> {
    channel: Channel<RW>,
    #[allow(dead_code)]
    state: S,
}

impl<S, RW: Read + Write> Debug for Connection<S, RW> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Connection")
    }
}

impl<RW: Read + Write> Connection<New, RW> {
    pub fn new(socket: Socket) -> Connection<New, RW> {
        Connection {
            channel: Channel::Plain(socket),
            state: New,
        }
    }
}

impl<RW: Read + Write> Connection<New, RW> {
    pub fn hand_shake<S: Secure<RW>>(self, identity: Option<S>) -> io::Result<Connection<HandShake, RW>> {
        println!("hand shake started");
        let mut channel = self.channel;
        let len = channel.read_message_len()?;
        let request = channel.read_message(len)?;
        let (version, message) = Self::parse_setup(&request);
        let props = match version {
            0x00_03_00_00 => Self::parse_props(&message)?,
            80_877_103 => {
                channel = match (channel, identity) {
                    (Channel::Plain(mut socket), Some(identity)) => {
                        socket.write_all(&[ACCEPT_SSL])?;
                        println!("accepting ssl!");
                        let secure_socket = match identity.secure(socket) {
                            Ok(socket) => socket,
                            Err(()) => {
                                println!("shrug!");
                                return Err(io::ErrorKind::InvalidInput.into());
                            }
                        };
                        Channel::Secure(secure_socket)
                    }
                    (mut channel, _) => {
                        channel.write_all(&[REJECT_SSL])?;
                        channel
                    }
                };
                channel.flush()?;
                let len = channel.read_message_len()?;
                let request = channel.read_message(len)?;
                let (version, message) = Self::parse_setup(&request);
                println!("ver {:?}", version);
                println!("ver {:x?}", version);
                match version {
                    0x00_03_00_00 => Self::parse_props(&message)?,
                    _ => unimplemented!(),
                }
            }
            _ => unimplemented!(),
        };

        println!("hand shake complete");

        Ok(Connection {
            channel,
            state: HandShake {
                props: props.into_iter().collect(),
            },
        })
    }

    fn parse_props(message: &[u8]) -> io::Result<Vec<(String, String)>> {
        fn read_cstr(mut message: &[u8]) -> io::Result<(String, &[u8])> {
            if let Some(pos) = message.iter().position(|b| *b == 0) {
                let key = str::from_utf8(&message[0..pos]).unwrap().to_owned();
                message = &message[pos + 1..];
                Ok((key, message))
            } else {
                Err(io::ErrorKind::InvalidInput.into())
            }
        }

        let mut req = message;
        let mut props = vec![];
        loop {
            let (key, message) = read_cstr(req)?;
            req = message;
            if key.is_empty() {
                break;
            }
            let (value, message) = read_cstr(req)?;
            req = message;
            props.push((key, value));
        }
        Ok(props)
    }

    fn parse_setup(message: &[u8]) -> (i32, &[u8]) {
        let version = i32::from_be_bytes(message[0..4].try_into().unwrap());
        let message = &message[4..];
        (version, message)
    }
}

impl<RW: Read + Write> Connection<HandShake, RW> {
    pub fn authenticate(mut self, _password: &str) -> io::Result<Connection<Authenticated, RW>> {
        self.channel.write_all(&[AUTHENTICATION, 0, 0, 0, 8, 0, 0, 0, 3])?;
        self.channel.flush()?;

        let _tag = self.channel.read_tag()?;
        let len = self.channel.read_message_len()?;
        let _message = self.channel.read_message(len)?;

        // we are ok with any password that user sent
        self.channel.write_all(&[AUTHENTICATION, 0, 0, 0, 8, 0, 0, 0, 0])?;
        self.channel.flush()?;

        log::debug!("auth ok");

        Ok(Connection {
            channel: self.channel,
            state: Authenticated,
        })
    }
}

impl<RW: Read + Write> Connection<Authenticated, RW> {
    pub fn send_params(mut self, params: &[(&str, &str)]) -> io::Result<Connection<AllocateBackendKey, RW>> {
        for (key, value) in params {
            let len: i32 = 4 + (key.len() as i32) + 1 + (value.len() as i32) + 1;
            let mut buff = vec![];
            buff.extend_from_slice(&[PARAMETER_STATUS]);
            buff.extend_from_slice(&len.to_be_bytes());
            buff.extend_from_slice(key.as_bytes());
            buff.extend_from_slice(&[0]);
            buff.extend_from_slice(value.as_bytes());
            buff.extend_from_slice(&[0]);
            self.channel.write_all(&buff)?;
            self.channel.flush()?;
        }
        Ok(Connection {
            channel: self.channel,
            state: AllocateBackendKey,
        })
    }
}

impl<RW: Read + Write> Connection<AllocateBackendKey, RW> {
    pub fn send_backend_keys(mut self, conn_id: u32, conn_secret_key: u32) -> io::Result<Connection<Established, RW>> {
        self.channel.write_all(&[BACKEND_KEY_DATA])?;
        self.channel.write_all(&12i32.to_be_bytes())?;
        self.channel.write_all(&conn_id.to_be_bytes())?;
        self.channel.write_all(&conn_secret_key.to_be_bytes())?;
        self.channel.flush()?;

        Ok(Connection {
            channel: self.channel,
            state: Established,
        })
    }
}

impl<RW: Read + Write> Connection<Established, RW> {
    pub fn channel(self) -> Channel<RW> {
        self.channel
    }
}

#[derive(Debug)]
pub struct New;

#[derive(Debug)]
pub struct HandShake {
    props: HashMap<String, String>,
}

#[derive(Debug)]
pub struct Authenticated;

#[derive(Debug)]
pub struct AllocateBackendKey;

#[derive(Debug)]
pub struct Established;

pub struct SecureSocket<RW: Read + Write> {
    inner: RW,
}

impl<RW: Read + Write> Read for SecureSocket<RW> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl<RW: Read + Write> Write for SecureSocket<RW> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl From<TlsStream<Socket>> for SecureSocket<TlsStream<Socket>> {
    fn from(socket: TlsStream<Socket>) -> SecureSocket<TlsStream<Socket>> {
        SecureSocket { inner: socket }
    }
}

#[cfg(test)]
impl From<TestData> for SecureSocket<TestData> {
    fn from(data: TestData) -> SecureSocket<TestData> {
        SecureSocket { inner: data }
    }
}

pub struct Socket {
    inner: SocketInner,
}

impl Debug for Socket {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Socket")
    }
}

impl Read for Socket {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.inner {
            SocketInner::Tcp(tcp_stream) => tcp_stream.read(buf),
            #[cfg(test)]
            SocketInner::Static(data) => data.read(buf),
        }
    }
}

impl Write for Socket {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match &mut self.inner {
            SocketInner::Tcp(tcp_stream) => tcp_stream.write(buf),
            #[cfg(test)]
            SocketInner::Static(data) => data.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut self.inner {
            SocketInner::Tcp(tcp_stream) => tcp_stream.flush(),
            #[cfg(test)]
            SocketInner::Static(data) => data.flush(),
        }
    }
}

impl From<TcpStream> for Socket {
    fn from(tcp_stream: TcpStream) -> Socket {
        Socket {
            inner: SocketInner::Tcp(tcp_stream),
        }
    }
}

#[cfg(test)]
impl From<TestData> for Socket {
    fn from(data: TestData) -> Socket {
        Socket {
            inner: SocketInner::Static(data),
        }
    }
}

enum SocketInner {
    Tcp(TcpStream),
    #[cfg(test)]
    Static(TestData),
}

#[cfg(test)]
use std::sync::{Arc, Mutex};

#[cfg(test)]
#[derive(Clone)]
pub struct TestData {
    inner: Arc<Mutex<DataInner>>,
}

#[cfg(test)]
impl TestData {
    pub fn new(content: Vec<&[u8]>) -> TestData {
        TestData {
            inner: Arc::new(Mutex::new(DataInner {
                read_buffer: content.concat(),
                read_index: 0,
                write_buffer: vec![],
            })),
        }
    }

    pub fn read_result(&self) -> Vec<u8> {
        self.inner.lock().unwrap().write_buffer.clone()
    }
}

#[cfg(test)]
impl Read for TestData {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.lock().unwrap().read(buf)
    }
}

#[cfg(test)]
impl Write for TestData {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.lock().unwrap().flush()
    }
}

#[cfg(test)]
struct DataInner {
    read_buffer: Vec<u8>,
    read_index: usize,
    write_buffer: Vec<u8>,
}

#[cfg(test)]
impl Read for DataInner {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.len() > self.read_buffer.len() - self.read_index {
            Err(io::Error::from(io::ErrorKind::UnexpectedEof))
        } else {
            for (i, item) in buf.iter_mut().enumerate() {
                *item = self.read_buffer[self.read_index + i];
            }
            self.read_index += buf.len();
            Ok(buf.len())
        }
    }
}

#[cfg(test)]
impl Write for DataInner {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trying_read_from_empty_stream() {
        let connection: Connection<New, TestData> = Connection::new(Socket::from(TestData::new(vec![])));

        let connection = connection.hand_shake::<TestData>(None);
        assert!(matches!(connection, Err(_)));
    }

    #[test]
    fn trying_read_only_length_of_ssl_message() {
        let connection: Connection<New, TestData> = Connection::new(Socket::from(TestData::new(vec![&[0, 0, 0, 8]])));

        let connection = connection.hand_shake::<TestData>(None);
        assert!(matches!(connection, Err(_)));
    }

    #[test]
    fn successful_connection_handshake_for_none_secure() {
        let test_data = TestData::new(vec![
            &8i32.to_be_bytes(),
            &1234i16.to_be_bytes(),
            &5679i16.to_be_bytes(),
            &89i32.to_be_bytes(),
            &3i16.to_be_bytes(),
            &0i16.to_be_bytes(),
            b"user\0",
            b"username\0",
            b"database\0",
            b"database_name\0",
            b"application_name\0",
            b"psql\0",
            b"client_encoding\0",
            b"UTF8\0",
            &[0],
        ]);

        let connection: Connection<New, TestData> = Connection::new(Socket::from(test_data.clone()));
        let connection = connection.hand_shake::<TestData>(None);

        assert!(matches!(connection, Ok(_)));

        let actual_content = test_data.read_result();
        let mut expected_content = Vec::new();
        expected_content.extend_from_slice(&[REJECT_SSL]);
        assert_eq!(actual_content, expected_content);
    }

    #[test]
    fn successful_connection_handshake_for_ssl_secure() {
        let test_data = TestData::new(vec![
            &8i32.to_be_bytes(),
            &1234i16.to_be_bytes(),
            &5679i16.to_be_bytes(),
            &89i32.to_be_bytes(),
            &3i16.to_be_bytes(),
            &0i16.to_be_bytes(),
            "user\0".as_bytes(),
            "username\0".as_bytes(),
            "database\0".as_bytes(),
            "database_name\0".as_bytes(),
            "application_name\0".as_bytes(),
            "psql\0".as_bytes(),
            "client_encoding\0".as_bytes(),
            "UTF8\0".as_bytes(),
            &[0],
        ]);

        let connection: Connection<New, TestData> = Connection::new(Socket::from(test_data.clone()));
        let connection = connection.hand_shake(Some(test_data.clone()));

        assert!(matches!(connection, Ok(_)));

        let actual_content = test_data.read_result();
        let mut expected_content = Vec::new();
        expected_content.extend_from_slice(&[ACCEPT_SSL]);
        assert_eq!(actual_content, expected_content);
    }

    #[test]
    fn authenticate() {
        let test_data = TestData::new(vec![
            &8i32.to_be_bytes(),
            &1234i16.to_be_bytes(),
            &5679i16.to_be_bytes(),
            &89i32.to_be_bytes(),
            &3i16.to_be_bytes(),
            &0i16.to_be_bytes(),
            b"user\0",
            b"username\0",
            b"database\0",
            b"database_name\0",
            b"application_name\0",
            b"psql\0",
            b"client_encoding\0",
            b"UTF8\0",
            &[0],
            &[b'p'],
            &8i32.to_be_bytes(),
            b"123\0",
        ]);

        let connection: Connection<New, TestData> = Connection::new(Socket::from(test_data.clone()));
        let connection = connection.hand_shake::<TestData>(None).unwrap();
        let connection = connection.authenticate("123");

        assert!(matches!(connection, Ok(_)));

        let actual_content = test_data.read_result();
        let mut expected_content = Vec::new();
        expected_content.extend_from_slice(&[REJECT_SSL]);
        expected_content.extend_from_slice(&[AUTHENTICATION, 0, 0, 0, 8, 0, 0, 0, 3]);
        expected_content.extend_from_slice(&[AUTHENTICATION, 0, 0, 0, 8, 0, 0, 0, 0]);
        assert_eq!(actual_content, expected_content);
    }

    #[test]
    fn send_server_params() {
        let test_data = TestData::new(vec![
            &8i32.to_be_bytes(),
            &1234i16.to_be_bytes(),
            &5679i16.to_be_bytes(),
            &89i32.to_be_bytes(),
            &3i16.to_be_bytes(),
            &0i16.to_be_bytes(),
            b"user\0",
            b"username\0",
            b"database\0",
            b"database_name\0",
            b"application_name\0",
            b"psql\0",
            b"client_encoding\0",
            b"UTF8\0",
            &[0],
            &[b'p'],
            &8i32.to_be_bytes(),
            b"123\0",
        ]);

        let connection: Connection<New, TestData> = Connection::new(Socket::from(test_data.clone()));
        let connection = connection.hand_shake::<TestData>(None).unwrap();
        let connection = connection.authenticate("123").unwrap();
        let connection = connection.send_params(&[("key1", "value1"), ("key2", "value2")]);

        assert!(matches!(connection, Ok(_)));

        let actual_content = test_data.read_result();
        let mut expected_content = Vec::new();
        expected_content.extend_from_slice(&[REJECT_SSL]);
        expected_content.extend_from_slice(&[AUTHENTICATION, 0, 0, 0, 8, 0, 0, 0, 3]);
        expected_content.extend_from_slice(&[AUTHENTICATION, 0, 0, 0, 8, 0, 0, 0, 0]);
        expected_content.extend_from_slice(&[PARAMETER_STATUS]);
        expected_content.extend_from_slice(&16i32.to_be_bytes());
        expected_content.extend_from_slice(b"key1\0");
        expected_content.extend_from_slice(b"value1\0");
        expected_content.extend_from_slice(&[PARAMETER_STATUS]);
        expected_content.extend_from_slice(&16i32.to_be_bytes());
        expected_content.extend_from_slice(b"key2\0");
        expected_content.extend_from_slice(b"value2\0");
        assert_eq!(actual_content, expected_content);
    }

    #[test]
    fn send_backend_keys() {
        let test_data = TestData::new(vec![
            &8i32.to_be_bytes(),
            &1234i16.to_be_bytes(),
            &5679i16.to_be_bytes(),
            &89i32.to_be_bytes(),
            &3i16.to_be_bytes(),
            &0i16.to_be_bytes(),
            b"user\0",
            b"username\0",
            b"database\0",
            b"database_name\0",
            b"application_name\0",
            b"psql\0",
            b"client_encoding\0",
            b"UTF8\0",
            &[0],
            &[b'p'],
            &8i32.to_be_bytes(),
            b"123\0",
        ]);

        const CONNECTION_ID: u32 = 1;
        const CONNECTION_SECRET_KEY: u32 = 1;

        let connection: Connection<New, TestData> = Connection::new(Socket::from(test_data.clone()));
        let connection = connection.hand_shake::<TestData>(None).unwrap();
        let connection = connection.authenticate("123").unwrap();
        let connection = connection
            .send_params(&[("key1", "value1"), ("key2", "value2")])
            .unwrap();
        let connection = connection.send_backend_keys(CONNECTION_ID, CONNECTION_SECRET_KEY);

        assert!(matches!(connection, Ok(_)));

        let actual_content = test_data.read_result();
        let mut expected_content = Vec::new();
        expected_content.extend_from_slice(&[REJECT_SSL]);
        expected_content.extend_from_slice(&[AUTHENTICATION, 0, 0, 0, 8, 0, 0, 0, 3]);
        expected_content.extend_from_slice(&[AUTHENTICATION, 0, 0, 0, 8, 0, 0, 0, 0]);
        expected_content.extend_from_slice(&[PARAMETER_STATUS]);
        expected_content.extend_from_slice(&16i32.to_be_bytes());
        expected_content.extend_from_slice(b"key1\0");
        expected_content.extend_from_slice(b"value1\0");
        expected_content.extend_from_slice(&[PARAMETER_STATUS]);
        expected_content.extend_from_slice(&16i32.to_be_bytes());
        expected_content.extend_from_slice(b"key2\0");
        expected_content.extend_from_slice(b"value2\0");
        expected_content.extend_from_slice(&[BACKEND_KEY_DATA]);
        expected_content.extend_from_slice(&12i32.to_be_bytes());
        expected_content.extend_from_slice(&CONNECTION_ID.to_be_bytes());
        expected_content.extend_from_slice(&CONNECTION_SECRET_KEY.to_be_bytes());
        assert_eq!(actual_content, expected_content);
    }
}
