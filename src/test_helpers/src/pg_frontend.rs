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

use byteorder::{NetworkEndian, WriteBytesExt};
use iobuf::{Iobuf, RWIobuf};

const QUERY: u8 = b'Q';
const TERMINATE: u8 = b'X';
const PASSWORD: u8 = b'p';

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
                let mut buff = RWIobuf::new(256);
                buff.write_u8(QUERY);
                let sql_bytes = sql.as_bytes();
                buff.write_u32::<NetworkEndian>(sql_bytes.len() as u32 + 4 + 1);
                buff.fill(sql_bytes);
                buff.write_u8(0);
                buff.flip_lo();
                let mut r = Vec::with_capacity(buff.len() as usize);
                r.resize(buff.len() as usize, 0);
                buff.consume(&mut r);
                r
            }
            Message::Terminate => vec![TERMINATE, 0, 0, 0, 4],
            Message::Setup(params) => {
                let mut buff = RWIobuf::new(512);
                let start = buff.len();
                buff.write_u32::<NetworkEndian>(0);
                buff.write_u16::<NetworkEndian>(3);
                buff.write_u16::<NetworkEndian>(0);
                for (key, value) in params {
                    buff.fill(key.as_bytes());
                    buff.write_u8(0);
                    buff.fill(value.as_bytes());
                    buff.write_u8(0);
                }
                buff.write_u8(0);
                let end = buff.len();
                buff.flip_lo();
                buff.poke_be(0, start - end);
                let mut r = Vec::with_capacity(buff.len() as usize);
                r.resize(buff.len() as usize, 0);
                buff.consume(&mut r);
                r
            }
            Message::SslDisabled => vec![],
            Message::SslRequired => {
                let mut buff = RWIobuf::new(512);
                buff.write_u32::<NetworkEndian>(8);
                buff.write_u32::<NetworkEndian>(80_877_103);
                buff.flip_lo();
                let mut r = Vec::with_capacity(buff.len() as usize);
                r.resize(buff.len() as usize, 0);
                buff.consume(&mut r);
                r
            }
            Message::Password(password) => {
                let mut buff = RWIobuf::new(512);
                buff.write_u8(PASSWORD);
                let password_bytes = password.as_bytes();
                buff.write_u32::<NetworkEndian>(1 + 4 + password_bytes.len() as u32);
                buff.fill(password_bytes);
                buff.write_u8(0);
                buff.flip_lo();
                let mut r = Vec::with_capacity(buff.len() as usize);
                r.resize(buff.len() as usize, 0);
                buff.consume(&mut r);
                r
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
