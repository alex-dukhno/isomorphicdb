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

const QUERY: u8 = b'Q';
const TERMINATE: u8 = b'X';

pub enum Message {
    Query(&'static str),
    Terminate,
    Setup(Vec<(&'static str, &'static str)>),
    SslDisabled,
    SslRequired,
    Password(&'static str),
    CancelRequest(i32, i32),
}

impl Message {
    pub fn as_vec(&self) -> Vec<u8> {
        match self {
            Message::Query(sql) => {
                let sql_bytes = sql.as_bytes();
                let mut buff = Vec::new();
                buff.extend_from_slice(&[QUERY]);
                buff.extend_from_slice(&(sql_bytes.len() as i32 + 4 + 1).to_be_bytes());
                buff.extend_from_slice(sql_bytes);
                buff.extend_from_slice(&[0]);
                buff
            }
            Message::Terminate => vec![TERMINATE, 0, 0, 0, 4],
            Message::Setup(params) => {
                let mut buff = Vec::new();
                buff.extend_from_slice(&3u16.to_be_bytes());
                buff.extend_from_slice(&0u16.to_be_bytes());
                for (key, value) in params {
                    buff.extend_from_slice(key.as_bytes());
                    buff.extend_from_slice(&[0]);
                    buff.extend_from_slice(value.as_bytes());
                    buff.extend_from_slice(&[0]);
                }
                buff.extend_from_slice(&[0]);
                let len = buff.len();
                let mut with_len = Vec::new();
                with_len.extend_from_slice(&(len as u32 + 4).to_be_bytes());
                with_len.extend_from_slice(&buff);
                with_len
            }
            Message::SslDisabled => vec![],
            Message::SslRequired => {
                let mut buff = Vec::new();
                buff.extend_from_slice(&8u32.to_be_bytes());
                buff.extend_from_slice(&8087_7103u32.to_be_bytes());
                buff
            }
            Message::Password(password) => {
                let mut buff = Vec::new();
                buff.extend_from_slice(password.as_bytes());
                buff.extend_from_slice(&[0]);
                let mut with_len = Vec::new();
                with_len.extend_from_slice(&[b'p']);
                with_len.extend_from_slice(&(buff.len() as u32 + 4).to_be_bytes());
                with_len.extend_from_slice(&buff);
                with_len
            }
            Message::CancelRequest(conn_id, secret_key) => {
                let mut buff = Vec::new();
                buff.extend_from_slice(&16u32.to_be_bytes());
                buff.extend_from_slice(&80_877_102u32.to_be_bytes());
                buff.extend_from_slice(&conn_id.to_be_bytes());
                buff.extend_from_slice(&secret_key.to_be_bytes());
                buff
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
