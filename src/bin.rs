#[macro_use]
extern crate log;
extern crate postgres_protocol;
extern crate pretty_env_logger;

use std::net::{TcpListener, TcpStream};

use byteorder::{ByteOrder, NetworkEndian};
use bytes::buf::BufExt;
use bytes::{Buf, BufMut, BytesMut};
use futures::io::{self, AsyncReadExt, AsyncWriteExt};
use piper::Arc;
use postgres_protocol::message::backend::{
    AUTHENTICATION_TAG, COMMAND_COMPLETE_TAG, DATA_ROW_TAG, EMPTY_QUERY_RESPONSE_TAG,
    NOTICE_RESPONSE_TAG, PARAMETER_STATUS_TAG, READY_FOR_QUERY_TAG, ROW_DESCRIPTION_TAG,
};
use smol::{Async, Task};
use std::fmt::{self, Display, Formatter};

const PORT: usize = 5432;
const HOST: &str = "127.0.0.1";

fn main() -> io::Result<()> {
    let local_address = format!("{}:{}", HOST, PORT);
    pretty_env_logger::init();
    info!("Starting server on {}", local_address);

    smol::run(async {
        let listener = Async::<TcpListener>::bind(local_address.as_str())?;
        info!("Listening on {}", local_address);

        loop {
            let (stream, peer_address) = listener.accept().await?;
            trace!("Accepted connection {}", peer_address);
            let client = Arc::new(stream);

            Task::spawn(async move {
                let mut connection = Connection::new(client.clone());
                connection.hand_ssl().await;
                connection.send_notice().await;
                connection.read_startup_message().await;
                connection.send_authentication_request().await;
                connection.handle_authentication_response().await;
                loop {
                    connection.send_ready_for_query().await;

                    connection.read_query().await;
                    connection.send_row_description().await;
                    connection.send_row_data().await;
                    connection.send_command_complete().await;
                }
            })
            .detach()
        }
    })
}

struct Connection {
    client: Arc<Async<TcpStream>>,
}

impl Connection {
    fn new(client: Arc<Async<TcpStream>>) -> Self {
        Self { client }
    }

    async fn hand_ssl(&mut self) -> io::Result<usize> {
        let mut len_buff = [0u8; 4];
        self.client.read_exact(&mut len_buff).await;
        let len = NetworkEndian::read_i32(&mut len_buff);
        trace!("length of startup message {}", len);
        let mut version_buff = [0u8; 4];
        self.client.read_exact(&mut version_buff).await;
        let ssl = NetworkEndian::read_u32(&mut version_buff);
        trace!("ssl = {}", ssl);
        // self.client.write(&[PARAMETER_STATUS_TAG]).await;
        Ok(1)
    }

    async fn send_notice(&mut self) -> io::Result<usize> {
        trace!("send notice tag");
        self.client.write(&[NOTICE_RESPONSE_TAG]).await
    }

    async fn read_startup_message(&mut self) {
        let mut len_buff = [0u8; 4];
        self.client.read_exact(&mut len_buff).await;
        let mut len = NetworkEndian::read_i32(&mut len_buff);
        trace!("length of startup message {}", len);
        let mut version_buff = [0u8; 4];
        self.client.read_exact(&mut version_buff).await;
        let version = Version::new(NetworkEndian::read_u32(&mut version_buff));
        trace!("version = {}", version);
        let params = {
            let mut buffer = BytesMut::with_capacity(len as usize);
            buffer.extend((0..((len as u8) - 8)));
            self.client.read_exact(&mut buffer).await;
            let params = buffer
                .to_vec()
                .split(|b| *b == 0)
                .map(|b| String::from_utf8(b.to_vec()).unwrap())
                .collect::<Vec<String>>();
            for param in params.iter() {
                trace!("param = {:?}", param);
            }
            params
        };
    }

    async fn send_authentication_request(&mut self) -> io::Result<usize> {
        trace!("send authentication request tag");
        let mut buff = BytesMut::new();
        buff.put_u8(AUTHENTICATION_TAG);
        buff.put_i32(8);
        buff.put_i32(3);
        self.client.write(buff.bytes()).await
    }

    async fn handle_authentication_response(&mut self) -> io::Result<usize> {
        trace!("waiting for authentication response");
        let mut p_tag_buff = [0u8; 1];
        self.client.read_exact(&mut p_tag_buff).await;
        let p = p_tag_buff[0];
        trace!("authentication response tag {}", p);
        let mut len_buff = [0u8; 4];
        self.client.read_exact(&mut len_buff).await;
        let len = NetworkEndian::read_i32(&mut len_buff);
        trace!("length of authentication message {}", len);
        let mut buffer = BytesMut::with_capacity(len as usize);
        buffer.extend((0..((len as u8) - 4)));
        self.client.read_exact(&mut buffer).await;
        trace!("password = {:?}", String::from_utf8(buffer.to_vec()));
        let mut buff = BytesMut::new();
        buff.put_u8(AUTHENTICATION_TAG);
        buff.put_i32(8);
        buff.put_i32(0);
        self.client.write(buff.bytes()).await
    }

    async fn send_ready_for_query(&mut self) -> io::Result<usize> {
        trace!("send ready for query message");
        let mut buff = BytesMut::new();
        buff.put_u8(READY_FOR_QUERY_TAG);
        buff.put_i32(5);
        buff.put_u8(EMPTY_QUERY_RESPONSE_TAG);
        self.client.write(buff.bytes()).await
    }

    async fn read_query(&mut self) -> io::Result<usize> {
        let mut type_code_buff = [0u8; 1];
        self.client.read_exact(&mut type_code_buff).await;
        trace!(
            "type code = {:?}",
            String::from_utf8(type_code_buff.to_vec())
        );
        let mut len_buff = [0u8; 4];
        self.client.read_exact(&mut len_buff).await;
        let len = NetworkEndian::read_i32(&len_buff);
        let mut sql_buffer = BytesMut::with_capacity(len as usize);
        sql_buffer.extend((0..((len as u8) - 4)));
        self.client.read_exact(&mut sql_buffer).await;
        let sql = String::from_utf8(sql_buffer.to_vec()).unwrap();
        trace!("SQL = {}", sql);
        Ok(1)
    }

    async fn send_row_description(&mut self) -> io::Result<usize> {
        let fields = vec![
            Field::new("c1".to_owned(), 23, 4),
            Field::new("c2".to_owned(), 23, 4),
        ];
        let mut buff = BytesMut::with_capacity(256);

        for field in fields.iter() {
            buff.put_slice(field.name.as_str().as_bytes());
            buff.put_u8(0); // end of c string
            buff.put_i32(0); // table id
            buff.put_i16(0); // column id
            buff.put_i32(field.type_id);
            buff.put_i16(field.type_size);
            buff.put_i32(-1); // type modifier
            buff.put_i16(0);
        }
        let mut len_buff = BytesMut::new();
        len_buff.put_u8(ROW_DESCRIPTION_TAG);
        len_buff.put_i32(6 + buff.len() as i32);
        len_buff.put_i16(fields.len() as i16);
        len_buff.extend_from_slice(&buff);

        self.client.write(len_buff.bytes()).await;
        trace!("row description is sent");
        Ok(1)
    }

    async fn send_row_data(&mut self) -> io::Result<usize> {
        let rows = vec![vec![1, 2], vec![3, 4], vec![5, 6]];

        for row in rows {
            let mut row_buff = BytesMut::with_capacity(256);
            for field in row.iter() {
                let as_string = format!("{}", field);
                row_buff.put_i32(as_string.len() as i32);
                row_buff.extend_from_slice(as_string.as_str().as_bytes());
            }
            let mut len_buff = BytesMut::new();
            len_buff.put_u8(DATA_ROW_TAG);
            len_buff.put_i32(6 + row_buff.len() as i32);
            len_buff.put_i16(row.len() as i16);
            len_buff.extend_from_slice(&row_buff);
            self.client.write(len_buff.bytes()).await;
            trace!("row {:?} data is sent", row);
        }
        Ok(1)
    }

    async fn send_command_complete(&mut self) -> io::Result<usize> {
        let command = b"SELECT\x00";
        let mut command_buff = BytesMut::with_capacity(256);
        command_buff.put_u8(COMMAND_COMPLETE_TAG);
        command_buff.put_i32(4 + command.len() as i32);
        command_buff.extend_from_slice(command);
        self.client.write(command_buff.bytes()).await;
        trace!("end of the command is sent");
        Ok(1)
    }
}

struct Field {
    name: String,
    type_id: i32,
    type_size: i16,
}

impl Field {
    fn new(name: String, type_id: i32, type_size: i16) -> Self {
        Self {
            name,
            type_id,
            type_size,
        }
    }
}

struct Version {
    major: u16,
    minor: u16,
}

impl Version {
    fn new(raw: u32) -> Self {
        let major = (raw >> 16) as u16;
        let minor = (raw & 0xffff) as u16;
        Self { major, minor }
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}
