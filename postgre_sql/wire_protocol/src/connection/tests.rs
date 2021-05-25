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

use super::*;
use std::sync::{Arc, Mutex};

impl Securing<TestData, TestData> for TestData {
    fn secure(self, _socket: TestData) -> Result<TestData, ()> {
        Ok(self)
    }
}

#[derive(Clone)]
pub struct TestData {
    inner: Arc<Mutex<DataInner>>,
}

impl Plain for TestData {}

impl Secure for TestData {}

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

impl Read for TestData {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.lock().unwrap().read(buf)
    }
}

impl Write for TestData {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.lock().unwrap().flush()
    }
}

struct DataInner {
    read_buffer: Vec<u8>,
    read_index: usize,
    write_buffer: Vec<u8>,
}

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

impl Write for DataInner {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn trying_read_from_empty_stream() {
    let socket = TestData::new(vec![]);
    let connection: Connection<New, TestData, TestData> = Connection::new(socket);

    let connection = connection.hand_shake::<TestData>(None);
    assert!(matches!(connection, Err(_)));
}

#[test]
fn trying_read_only_length_of_ssl_message() {
    let socket = TestData::new(vec![&[0, 0, 0, 8]]);
    let connection: Connection<New, TestData, TestData> = Connection::new(socket);

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

    let socket = test_data.clone();
    let connection: Connection<New, TestData, TestData> = Connection::new(socket);
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

    let socket = test_data.clone();
    let connection: Connection<New, TestData, TestData> = Connection::new(socket);
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

    let socket = test_data.clone();
    let connection: Connection<New, TestData, TestData> = Connection::new(socket);
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

    let socket = test_data.clone();
    let connection: Connection<New, TestData, TestData> = Connection::new(socket);
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

    let socket = test_data.clone();
    let connection: Connection<New, TestData, TestData> = Connection::new(socket);
    let connection = connection.hand_shake::<TestData>(None).unwrap();
    let connection = connection.authenticate("123").unwrap();
    let connection = connection.send_params(&[("key1", "value1"), ("key2", "value2")]).unwrap();
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
