use bytes::{BufMut, BytesMut};

const QUERY: u8 = b'Q';
const TERMINATE: u8 = b'X';

pub fn query(sql: &'static str) -> Message {
    Message::Query(sql.to_owned())
}

pub enum Message {
    Query(String),
    Terminate,
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query() {
        assert_eq!(
            Message::Query("create schema schema_name;".to_owned()).as_vec(),
            vec![
                QUERY, 0, 0, 0, 31, 99, 114, 101, 97, 116, 101, 32, 115, 99, 104, 101, 109, 97, 32,
                115, 99, 104, 101, 109, 97, 95, 110, 97, 109, 101, 59, 0
            ]
        )
    }

    #[test]
    fn terminate() {
        assert_eq!(Message::Terminate.as_vec(), vec![TERMINATE, 0, 0, 0, 4])
    }
}
