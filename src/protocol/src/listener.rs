use crate::{messages::Message, supported_version, Command, Error, Params, Result, Version};
use byteorder::{ByteOrder, NetworkEndian};
use bytes::{Buf, BytesMut};
use std::{
    io::{self, Read, Write},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};

pub struct QueryListener {
    listener: TcpListener,
}

impl QueryListener {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<QueryListener> {
        let listener = TcpListener::bind(addr)?;
        Ok(QueryListener::new(listener))
    }

    fn new(listener: TcpListener) -> QueryListener {
        QueryListener { listener }
    }

    pub fn accept(&self) -> io::Result<Result<Connection<TcpStream>>> {
        let (socket, address) = self.listener.accept()?;
        debug!("ADDRESS {:?}", address);
        hand_shake(socket)
    }
}

fn hand_shake<RW: Read + Write>(mut socket: RW) -> io::Result<Result<Connection<RW>>> {
    let mut buffer = [0u8; 4];
    let len = socket
        .read_exact(&mut buffer)
        .map(|_| NetworkEndian::read_u32(&buffer))?;
    let mut buffer = BytesMut::with_capacity(len as usize - 4);
    buffer.resize(len as usize - 4, b'0');
    let mut message = socket.read_exact(&mut buffer).map(|_| buffer)?;
    let version = NetworkEndian::read_i32(message.bytes());
    message.advance(4);
    let state: State = if version == supported_version() {
        let parsed = message
            .bytes()
            .split(|b| *b == 0)
            .filter(|b| !b.is_empty())
            .map(|b| String::from_utf8(b.to_vec()).unwrap())
            .collect::<Vec<String>>();
        let mut params = vec![];
        let mut i = 0;
        while i < parsed.len() {
            params.push((parsed[i].clone(), parsed[i + 1].clone()));
            i += 2;
        }
        message.advance(message.remaining());
        debug!("Version {}\nparams = {:?}", version, params);
        State::Completed(version, params, SslMode::Disable)
    } else {
        State::InProgress(SslMode::Require)
    };

    match state {
        State::InProgress(ssl_mode) => {
            socket.write_all(Message::Notice.as_vec().as_slice())?;
            let mut buffer = [0u8; 4];
            let len = socket
                .read_exact(&mut buffer)
                .map(|_| NetworkEndian::read_u32(&buffer))?;
            let mut buffer = BytesMut::with_capacity(len as usize - 4);
            buffer.resize(len as usize - 4, b'0');
            let mut message = socket.read_exact(&mut buffer).map(|_| buffer)?;
            let version = NetworkEndian::read_i32(message.bytes());
            message.advance(4);
            let parsed = message
                .bytes()
                .split(|b| *b == 0)
                .filter(|b| !b.is_empty())
                .map(|b| String::from_utf8(b.to_vec()).unwrap())
                .collect::<Vec<String>>();
            let mut params = vec![];
            let mut i = 0;
            while i < parsed.len() {
                params.push((parsed[i].clone(), parsed[i + 1].clone()));
                i += 2;
            }
            message.advance(message.remaining());
            socket.write_all(Message::AuthenticationCleartextPassword.as_vec().as_slice())?;
            let mut buffer = [0u8; 1];
            let tag = socket.read_exact(&mut buffer).map(|_| buffer[0]);
            debug!("client message response tag {:?}", tag);
            debug!("waiting for authentication response");
            let mut buffer = [0u8; 4];
            let len = socket
                .read_exact(&mut buffer)
                .map(|_| NetworkEndian::read_u32(&buffer))?;
            debug!("len = {:?}", len);
            let mut buffer = BytesMut::with_capacity(len as usize - 4);
            buffer.resize(len as usize - 4, b'0');
            let _message = socket.read_exact(&mut buffer).map(|_| buffer)?;
            socket.write_all(Message::AuthenticationOk.as_vec().as_slice())?;
            Ok(Ok(Connection::new((version, params, ssl_mode), socket)))
        }
        State::Completed(version, params, ssl_mode) => {
            socket.write_all(Message::AuthenticationOk.as_vec().as_slice())?;
            Ok(Ok(Connection::new((version, params, ssl_mode), socket)))
        }
    }
}

pub struct Connection<RW: Read + Write> {
    properties: (Version, Params, SslMode),
    socket: RW,
}

impl<RW: Read + Write> Connection<RW> {
    pub fn new(properties: (Version, Params, SslMode), socket: RW) -> Connection<RW> {
        Connection { properties, socket }
    }

    pub fn properties(&self) -> &(Version, Params, SslMode) {
        &(self.properties)
    }

    pub fn send_ready_for_query(&mut self) -> io::Result<Result<()>> {
        debug!("send ready for query message");
        self.socket
            .write_all(Message::ReadyForQuery.as_vec().as_slice())?;
        Ok(Ok(()))
    }

    pub fn read_query(&mut self) -> io::Result<Result<Command>> {
        match self.send_ready_for_query()? {
            Ok(()) => {}
            Err(_) => return Ok(Ok(Command::Terminate)),
        }
        let mut buffer = [0u8; 1];
        let tag = self.socket.read_exact(&mut buffer).map(|_| buffer[0])?;
        if b'X' == tag {
            Ok(Ok(Command::Terminate))
        } else {
            let mut buffer = [0u8; 4];
            let len = self
                .socket
                .read_exact(&mut buffer)
                .map(|_| NetworkEndian::read_u32(&buffer))?;
            let mut buffer = BytesMut::with_capacity(len as usize - 4);
            buffer.resize(len as usize - 4, b'0');
            let sql_buff = self.socket.read_exact(&mut buffer).map(|_| buffer)?;
            debug!("FOR TEST sql = {:?}", sql_buff);
            let sql = match String::from_utf8(sql_buff[..sql_buff.len() - 1].to_vec()) {
                Ok(sql) => sql,
                Err(_e) => return Ok(Err(Error)),
            };
            debug!("SQL = {}", sql);
            Ok(Ok(Command::Query(sql)))
        }
    }

    pub fn send_row_description(&mut self, fields: Vec<Field>) -> io::Result<()> {
        self.socket.write_all(
            Message::RowDescription(
                fields
                    .into_iter()
                    .map(|f| (f.name, f.type_id, f.type_size))
                    .collect(),
            )
            .as_vec()
            .as_slice(),
        )?;
        debug!("row description is sent");
        Ok(())
    }

    pub fn send_row_data(&mut self, row: Vec<String>) -> io::Result<()> {
        self.socket
            .write_all(Message::DataRow(row).as_vec().as_slice())?;
        Ok(())
    }

    pub fn send_command_complete(&mut self, message: Message) -> io::Result<()> {
        self.socket.write_all(message.as_vec().as_slice())?;
        debug!("end of the command is sent");
        Ok(())
    }
}

impl<RW: Read + Write> PartialEq for Connection<RW> {
    fn eq(&self, other: &Self) -> bool {
        self.properties().eq(other.properties())
    }
}

#[derive(Clone)]
pub struct Field {
    pub name: String,
    pub type_id: i32,
    pub type_size: i16,
}

impl Field {
    pub fn new(name: String, type_id: i32, type_size: i16) -> Self {
        Self {
            name,
            type_id,
            type_size,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum SslMode {
    Require,
    Disable,
}

#[derive(Debug)]
enum State {
    Completed(Version, Params, SslMode),
    InProgress(SslMode),
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_helpers::{pg_frontend, sync_io};

    #[cfg(test)]
    mod hand_shake {
        use super::*;

        #[test]
        fn trying_read_from_empty_stream() {
            let (test_case, _) = sync_io::TestCase::with_content(vec![]);

            let error = hand_shake(test_case);

            assert!(error.is_err());
        }

        #[cfg(test)]
        mod rust_postgres {
            use super::*;
            use crate::VERSION_3;

            #[test]
            fn trying_read_setup_message() {
                let (test_case, _) = sync_io::TestCase::with_content(vec![&[0, 0, 0, 57]]);

                let error = hand_shake(test_case);

                assert!(error.is_err());
            }

            #[test]
            fn successful_connection_handshake() -> io::Result<()> {
                let (test_case, mut test_result) = sync_io::TestCase::with_content(vec![
                    pg_frontend::Message::SslDisabled.as_vec().as_slice(),
                    pg_frontend::Message::Setup(vec![
                        ("client_encoding", "UTF8"),
                        ("timezone", "UTC"),
                        ("user", "postgres"),
                    ])
                    .as_vec()
                    .as_slice(),
                ]);

                let connection = hand_shake(test_case)?.expect("connection is open");

                assert_eq!(
                    connection.properties(),
                    &(
                        VERSION_3,
                        vec![
                            ("client_encoding".to_owned(), "UTF8".to_owned()),
                            ("timezone".to_owned(), "UTC".to_owned()),
                            ("user".to_owned(), "postgres".to_owned())
                        ],
                        SslMode::Disable
                    )
                );

                let actual_content = test_result.read_result();
                let mut expected_content = BytesMut::new();
                expected_content.extend_from_slice(Message::AuthenticationOk.as_vec().as_slice());

                assert_eq!(actual_content, expected_content);

                Ok(())
            }
        }

        #[cfg(test)]
        mod psql_client {
            use super::*;

            #[test]
            fn trying_read_only_length_of_ssl_message() {
                let (test_case, _) = sync_io::TestCase::with_content(vec![&[0, 0, 0, 8]]);

                let error = hand_shake(test_case);

                assert!(error.is_err());
            }

            #[test]
            fn sending_notice_after_reading_ssl_message() {
                let (test_case, mut test_result) =
                    sync_io::TestCase::with_content(vec![pg_frontend::Message::SslRequired
                        .as_vec()
                        .as_slice()]);

                let error = hand_shake(test_case);

                assert!(error.is_err());

                let actual_content = test_result.read_result();
                let mut expected_content = BytesMut::new();
                expected_content.extend_from_slice(Message::Notice.as_vec().as_slice());

                assert_eq!(actual_content, expected_content);
            }

            #[test]
            fn successful_connection_handshake() -> io::Result<()> {
                let (test_case, mut test_result) = sync_io::TestCase::with_content(vec![
                    pg_frontend::Message::SslRequired.as_vec().as_slice(),
                    pg_frontend::Message::Setup(vec![
                        ("user", "username"),
                        ("database", "database_name"),
                        ("application_name", "psql"),
                        ("client_encoding", "UTF8"),
                    ])
                    .as_vec()
                    .as_slice(),
                    pg_frontend::Message::Password("123").as_vec().as_slice(),
                ]);

                let connection = hand_shake(test_case)?;

                assert!(connection.is_ok());

                let actual_content = test_result.read_result();
                let mut expected_content = BytesMut::new();
                expected_content.extend_from_slice(Message::Notice.as_vec().as_slice());
                expected_content.extend_from_slice(
                    Message::AuthenticationCleartextPassword.as_vec().as_slice(),
                );
                expected_content.extend_from_slice(Message::AuthenticationOk.as_vec().as_slice());

                assert_eq!(actual_content, expected_content);

                Ok(())
            }
        }
    }

    #[cfg(test)]
    mod connection {
        use super::*;

        #[test]
        fn send_ready_for_query() -> io::Result<()> {
            let (test_case, mut test_result) = sync_io::TestCase::empty();
            let mut connection =
                Connection::new((supported_version(), vec![], SslMode::Disable), test_case);

            let ready_for_query = connection.send_ready_for_query()?;

            assert_eq!(ready_for_query, Ok(()));

            let actual_content = test_result.read_result();
            let mut expected_content = BytesMut::new();
            expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());

            assert_eq!(actual_content, expected_content);

            Ok(())
        }

        #[cfg(test)]
        mod read_query {
            use super::*;

            #[test]
            fn read_termination_command() -> io::Result<()> {
                let (test_case, _) = sync_io::TestCase::with_content(vec![&[88], &[0, 0, 0, 4]]);
                let mut connection =
                    Connection::new((supported_version(), vec![], SslMode::Disable), test_case);

                let query = connection.read_query()?;

                assert_eq!(query, Ok(Command::Terminate));

                Ok(())
            }

            #[test]
            fn read_query_successfully() -> io::Result<()> {
                let (test_case, _) =
                    sync_io::TestCase::with_content(vec![&[81], &[0, 0, 0, 14], b"select 1;\0"]);
                let mut connection =
                    Connection::new((supported_version(), vec![], SslMode::Disable), test_case);

                let query = connection.read_query()?;

                assert_eq!(query, Ok(Command::Query("select 1;".to_owned())));

                Ok(())
            }

            #[test]
            fn unexpected_eof_when_read_type_code_of_query_request() {
                let (test_case, _) = sync_io::TestCase::with_content(vec![]);
                let mut connection =
                    Connection::new((supported_version(), vec![], SslMode::Disable), test_case);

                let query = connection.read_query();

                assert!(query.is_err());
            }

            #[test]
            fn unexpected_eof_when_read_length_of_query() {
                let (test_case, _) = sync_io::TestCase::with_content(vec![&[81]]);
                let mut connection =
                    Connection::new((supported_version(), vec![], SslMode::Disable), test_case);

                let query = connection.read_query();

                assert!(query.is_err());
            }

            #[test]
            fn unexpected_eof_when_query_string() {
                let (test_case, _) =
                    sync_io::TestCase::with_content(vec![&[81], &[0, 0, 0, 14], b"sel;\0"]);
                let mut connection =
                    Connection::new((supported_version(), vec![], SslMode::Disable), test_case);

                let query = connection.read_query();

                assert!(query.is_err());
            }
        }

        #[test]
        fn send_field_description_query() -> io::Result<()> {
            let (test_case, mut test_result) = sync_io::TestCase::empty();
            let mut connection =
                Connection::new((supported_version(), vec![], SslMode::Disable), test_case);
            let fields = vec![
                Field::new(
                    "c1".to_owned(),
                    23, // int4 type code
                    4,
                ),
                Field::new("c2".to_owned(), 23, 4),
            ];

            connection.send_row_description(fields.clone())?;

            let actual_content = test_result.read_result();
            let mut expected_content = BytesMut::new();
            expected_content.extend_from_slice(
                Message::RowDescription(
                    fields
                        .into_iter()
                        .map(|f| (f.name, f.type_id, f.type_size))
                        .collect(),
                )
                .as_vec()
                .as_slice(),
            );

            assert_eq!(actual_content, expected_content);

            Ok(())
        }

        #[test]
        fn send_rows_data() -> io::Result<()> {
            let (test_case, mut test_result) = sync_io::TestCase::empty();
            let mut connection =
                Connection::new((supported_version(), vec![], SslMode::Disable), test_case);

            let rows = vec![
                vec!["1".to_owned(), "2".to_owned()],
                vec!["3".to_owned(), "4".to_owned()],
                vec!["5".to_owned(), "6".to_owned()],
            ];
            for row in rows.iter() {
                connection.send_row_data(row.clone())?;
            }

            let actual_content = test_result.read_result();
            let mut expected_content = BytesMut::new();
            for row in rows {
                expected_content.extend_from_slice(Message::DataRow(row).as_vec().as_slice());
            }

            assert_eq!(actual_content, expected_content);

            Ok(())
        }

        #[test]
        fn send_command_complete() -> io::Result<()> {
            let (test_case, mut test_result) = sync_io::TestCase::empty();
            let mut connection =
                Connection::new((supported_version(), vec![], SslMode::Disable), test_case);
            connection.send_command_complete(Message::CommandComplete("SELECT".to_owned()))?;

            let actual_content = test_result.read_result();
            let mut expected_content = BytesMut::new();
            expected_content.extend_from_slice(
                Message::CommandComplete("SELECT".to_owned())
                    .as_vec()
                    .as_slice(),
            );
            assert_eq!(actual_content, expected_content);

            Ok(())
        }
    }
}
