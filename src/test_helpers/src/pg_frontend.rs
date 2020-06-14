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

use bytes::{BufMut, BytesMut};

const QUERY: u8 = b'Q';
const TERMINATE: u8 = b'X';

pub enum Message {
    Query(&'static str),
    Terminate,
    Setup(Vec<(&'static str, &'static str)>),
    SslDisabled,
    SslRequired,
    Password(&'static str),
}

impl Message {
    pub fn as_vec(&self) -> Vec<u8> {
        match self {
            Message::Query(sql) => {
                let mut buff = BytesMut::with_capacity(256);
                buff.put_u8(QUERY);
                let sql_bytes = sql.as_bytes();
                buff.put_i32(sql_bytes.len() as i32 + 4 + 1);
                buff.extend_from_slice(sql_bytes);
                buff.put_u8(0);
                buff.to_vec()
            }
            Message::Terminate => vec![TERMINATE, 0, 0, 0, 4],
            Message::Setup(params) => {
                let mut buff = BytesMut::with_capacity(512);
                buff.put_u16(3);
                buff.put_u16(0);
                for (key, value) in params {
                    buff.extend_from_slice(key.as_bytes());
                    buff.put_u8(0);
                    buff.extend_from_slice(value.as_bytes());
                    buff.put_u8(0);
                }
                buff.put_u8(0);
                let len = buff.len();
                let mut with_len = BytesMut::with_capacity(512);
                with_len.put_u32(len as u32 + 4);
                with_len.extend_from_slice(&buff);
                with_len.to_vec()
            }
            Message::SslDisabled => vec![],
            Message::SslRequired => {
                let mut buff = BytesMut::with_capacity(256);
                buff.put_u32(8);
                buff.put_u32(80_877_103);
                buff.to_vec()
            }
            Message::Password(password) => {
                let mut buff = BytesMut::with_capacity(256);
                buff.extend_from_slice(password.as_bytes());
                buff.put_u8(0);
                let mut with_len = BytesMut::with_capacity(256);
                with_len.put_u8(b'p');
                with_len.put_u32(buff.len() as u32 + 4);
                with_len.extend_from_slice(&buff);
                with_len.to_vec()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query() {
        assert_eq!(
            Message::Query("create schema schema_name;").as_vec(),
            vec![
                QUERY, 0, 0, 0, 31, 99, 114, 101, 97, 116, 101, 32, 115, 99, 104, 101, 109, 97, 32, 115, 99, 104, 101,
                109, 97, 95, 110, 97, 109, 101, 59, 0
            ]
        )
    }

    #[test]
    fn terminate() {
        assert_eq!(Message::Terminate.as_vec(), vec![TERMINATE, 0, 0, 0, 4])
    }

    #[test]
    fn setup() {
        assert_eq!(
            Message::Setup(vec![("1", "1"), ("2", "2")]).as_vec(),
            vec![0, 0, 0, 17, 0, 3, 0, 0, 49, 0, 49, 0, 50, 0, 50, 0, 0]
        )
    }

    #[test]
    fn ssl_disabled() {
        assert_eq!(Message::SslDisabled.as_vec(), vec![])
    }

    #[test]
    fn ssl_required() {
        assert_eq!(Message::SslRequired.as_vec(), vec![0, 0, 0, 8, 4, 210, 22, 47])
    }

    #[test]
    fn password() {
        assert_eq!(Message::Password("123").as_vec(), vec![112, 0, 0, 0, 8, 49, 50, 51, 0])
    }
}
