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

use crate::{
    CANCEL_REQUEST_CODE, GSSENC_REQUEST_CODE, SSL_REQUEST_CODE, VERSION_1_CODE, VERSION_2_CODE, VERSION_3_CODE,
};
use std::fmt::{self, Display, Formatter};

#[derive(PartialEq)]
struct Code(i32);

impl Display for Code {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.0 {
            CANCEL_REQUEST_CODE => write!(f, "Cancel Request"),
            SSL_REQUEST_CODE => write!(f, "SSL Request"),
            GSSENC_REQUEST_CODE => write!(f, "GSSENC Request"),
            _ => write!(
                f,
                "Version {}.{} Request",
                (self.0 >> 16) as i16,
                (self.0 & 0x00_00_FF_FF) as i16
            ),
        }
    }
}

#[test]
fn version_one_request() {
    assert_eq!(Code(VERSION_1_CODE).to_string(), "Version 1.0 Request");
}

#[test]
fn version_two_request() {
    assert_eq!(Code(VERSION_2_CODE).to_string(), "Version 2.0 Request");
}

#[test]
fn version_three_request() {
    assert_eq!(Code(VERSION_3_CODE).to_string(), "Version 3.0 Request");
}

#[test]
fn cancel_request() {
    assert_eq!(Code(CANCEL_REQUEST_CODE).to_string(), "Cancel Request")
}

#[test]
fn ssl_request() {
    assert_eq!(Code(SSL_REQUEST_CODE).to_string(), "SSL Request")
}

#[test]
fn gssenc_request() {
    assert_eq!(Code(GSSENC_REQUEST_CODE).to_string(), "GSSENC Request")
}

struct Buffer<'b> {
    data: &'b [u8],
    index: usize,
}

impl<'b> From<&'b [u8]> for Buffer<'b> {
    fn from(data: &'b [u8]) -> Self {
        Buffer { data, index: 0 }
    }
}

impl<'b> Buffer<'b> {
    fn read_i32(&mut self) -> Result<i32, ()> {
        if self.data.len() - self.index < 4 {
            Err(())
        } else {
            self.index += 4;
            Ok((self.data[self.index - 4] as i32) << 24
                | (self.data[self.index - 3] as i32) << 16
                | (self.data[self.index - 2] as i32) << 8
                | (self.data[self.index - 1] as i32))
        }
    }
}

#[derive(Debug, PartialEq)]
enum Connection {
    Created,
    Setup,
}

#[derive(Debug, PartialEq)]
enum ConnectionError {
    NoFurtherState,
    InvalidData,
    NotSupportedProtocolVersion,
}

impl Connection {
    fn transform(self, buf: &[u8]) -> Result<Connection, ConnectionError> {
        match self {
            Connection::Created => {
                let mut buffer = Buffer::from(buf);
                let _len = match buffer.read_i32() {
                    Ok(len) => len,
                    Err(()) => return Err(ConnectionError::InvalidData),
                };
                let code = match buffer.read_i32() {
                    Ok(code) => Code(code),
                    Err(()) => return Err(ConnectionError::InvalidData),
                };
                log::info!("Connection Code: {}", code);
                match code {
                    Code(VERSION_1_CODE) => Err(ConnectionError::NotSupportedProtocolVersion),
                    Code(VERSION_2_CODE) => Err(ConnectionError::NotSupportedProtocolVersion),
                    _ => Ok(Connection::Setup),
                }
            }
            Connection::Setup => Err(ConnectionError::NoFurtherState),
        }
    }
}

#[test]
fn empty_payload() {
    let connection = Connection::Created;

    assert_eq!(connection.transform(&mut []), Err(ConnectionError::InvalidData));
}

#[test]
fn only_length() {
    let connection = Connection::Created;

    assert_eq!(
        connection.transform(&mut [0, 0, 0, 4]),
        Err(ConnectionError::InvalidData)
    );
}

#[test]
fn version_one_is_not_supported() {
    let connection = Connection::Created;

    let mut payload = vec![];
    payload.extend_from_slice(&[0, 0, 0, 8]);
    payload.extend_from_slice(&VERSION_1_CODE.to_be_bytes());

    assert_eq!(
        connection.transform(&mut payload),
        Err(ConnectionError::NotSupportedProtocolVersion)
    );
}

#[test]
fn version_two_is_not_supported() {
    let connection = Connection::Created;

    let mut payload = vec![];
    payload.extend_from_slice(&[0, 0, 0, 8]);
    payload.extend_from_slice(&VERSION_2_CODE.to_be_bytes());

    assert_eq!(
        connection.transform(&mut payload),
        Err(ConnectionError::NotSupportedProtocolVersion)
    );
}

#[test]
fn setup_version_three_connection() {
    let connection = Connection::Created;

    let mut payload = vec![];
    payload.extend_from_slice(&[0, 0, 0, 8]);
    payload.extend_from_slice(&VERSION_3_CODE.to_be_bytes());

    assert_eq!(connection.transform(&payload), Ok(Connection::Setup));
}

#[test]
fn setup_version_three_with_client_params() {
    let connection = Connection::Created;

    let mut payload = vec![];
    payload.extend_from_slice(&[0, 0, 0, 8]);
    payload.extend_from_slice(&VERSION_3_CODE.to_be_bytes());

    assert_eq!(connection.transform(&payload), Ok(Connection::Setup));
}
