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

#![deny(missing_docs)]
//! API for backend implementation of PostgreSQL Wire Protocol
extern crate log;

use crate::messages::Message;
use byteorder::{ByteOrder, NetworkEndian};
use bytes::BytesMut;
use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use std::io;

use crate::results::{QueryEvent, QueryResult};
pub use listener::{QueryListener, ServerListener};

/// Module contains functionality to listen to incoming client connections and
/// queries
pub mod listener;
/// Module contains backend messages that could be send by server implementation
/// to a client
pub mod messages;
/// Module contains functionality to represent query result
pub mod results;
/// Module contains functionality to represent SQL type system
pub mod sql_types;

/// Protocol version
pub type Version = i32;
/// Connection key-value params
pub type Params = Vec<(String, String)>;
/// Protocol operation result
pub type Result<T> = std::result::Result<T, Error>;

/// Version 1 of the protocol
pub const VERSION_1: Version = 0x10000;
/// Version 2 of the protocol
pub const VERSION_2: Version = 0x20000;
/// Version 3 of the protocol
pub const VERSION_3: Version = 0x30000;
/// Client initiate cancel of a command
pub const VERSION_CANCEL: Version = (1234 << 16) + 5678;
/// Client initiate `ssl` connection
pub const VERSION_SSL: Version = (1234 << 16) + 5679;
/// Client initiate `gss` encrypted connection
pub const VERSION_GSSENC: Version = (1234 << 16) + 5680;

/// `Error` type in protocol `Result`. Indicates that something went not well
#[derive(Debug, PartialEq)]
pub enum Error {
    /// Indicates that incoming query can't be parsed as UTF-8 string
    QueryIsNotValidUtfString,
    /// Indicates that protocol version is not supported
    UnsupportedVersion,
    /// Indicates that client request is not supported
    UnsupportedRequest,
    /// Indicates that during handshake client sent unrecognized protocol version
    UnrecognizedVersion,
}

/// Result of handling incoming bytes from a client
#[derive(Debug, PartialEq)]
pub enum Command {
    /// Client commands to execute a `Query`
    Query(String),
    /// Client commands to terminate current connection
    Terminate,
}

/// Structure to handle client-server PostgreSQL Wire Protocol connection
pub struct Connection<RW: AsyncReadExt + AsyncWriteExt + Unpin> {
    properties: (Version, Params, SslMode),
    socket: RW,
}

impl<RW: AsyncReadExt + AsyncWriteExt + Unpin> Connection<RW> {
    /// Creates new Connection with properties and read-write socket
    pub fn new(properties: (Version, Params, SslMode), socket: RW) -> Connection<RW> {
        Connection { properties, socket }
    }

    /// connection properties tuple
    pub fn properties(&self) -> &(Version, Params, SslMode) {
        &(self.properties)
    }

    async fn send_ready_for_query(&mut self) -> io::Result<Result<()>> {
        log::debug!("send ready for query message");
        self.socket
            .write_all(Message::ReadyForQuery.as_vec().as_slice())
            .await?;
        Ok(Ok(()))
    }

    /// receives and decodes a command from remote client
    pub async fn receive(&mut self) -> io::Result<Result<Command>> {
        self.send_ready_for_query().await?.expect("to send ready for query");
        let mut buffer = [0u8; 1];
        let tag = self.socket.read_exact(&mut buffer).await.map(|_| buffer[0])?;
        if b'X' == tag {
            Ok(Ok(Command::Terminate))
        } else {
            let mut buffer = [0u8; 4];
            let len = self
                .socket
                .read_exact(&mut buffer)
                .await
                .map(|_| NetworkEndian::read_u32(&buffer))?;
            let mut buffer = BytesMut::with_capacity(len as usize - 4);
            buffer.resize(len as usize - 4, b'0');
            let sql_buff = self.socket.read_exact(&mut buffer).await.map(|_| buffer)?;
            log::debug!("FOR TEST sql = {:?}", sql_buff);
            let sql = match String::from_utf8(sql_buff[..sql_buff.len() - 1].to_vec()) {
                Ok(sql) => sql,
                Err(_e) => return Ok(Err(Error::QueryIsNotValidUtfString)),
            };
            log::debug!("SQL = {}", sql);
            Ok(Ok(Command::Query(sql)))
        }
    }

    /// Sends response messages to client. Most of the time it is a single
    /// message, select result one of the exceptional situation
    pub async fn send(&mut self, query_result: QueryResult) -> io::Result<()> {
        for message in QueryResultMapper::map(query_result) {
            log::debug!("{:?}", message);
            self.socket.write_all(message.as_vec().as_slice()).await?;
        }
        log::debug!("end of the command is sent");
        Ok(())
    }
}

impl<RW: AsyncReadExt + AsyncWriteExt + Unpin> PartialEq for Connection<RW> {
    fn eq(&self, other: &Self) -> bool {
        self.properties().eq(other.properties())
    }
}

/// Struct description of metadata that describes how client should interpret
/// outgoing selected data
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ColumnMetadata {
    /// name of the column that was specified in query
    pub name: String,
    /// PostgreSQL data type id
    pub type_id: i32,
    /// PostgreSQL data type size
    pub type_size: i16,
}

impl ColumnMetadata {
    /// Creates new column metadata
    pub fn new(name: String, type_id: i32, type_size: i16) -> Self {
        Self {
            name,
            type_id,
            type_size,
        }
    }
}

/// Enum that describes possible `ssl` mode
/// possible values `Require`, `Preferred` and `Disable`
#[derive(Debug, PartialEq)]
pub enum SslMode {
    /// Client initiate connection that require `ssl` tunnel
    Require,
    /// Client initiate connection without `ssl` tunnel
    Disable,
}

struct QueryResultMapper;

impl QueryResultMapper {
    fn map(resp: QueryResult) -> Vec<Message> {
        match resp {
            Ok(QueryEvent::SchemaCreated) => vec![Message::CommandComplete("CREATE SCHEMA".to_owned())],
            Ok(QueryEvent::SchemaDropped) => vec![Message::CommandComplete("DROP SCHEMA".to_owned())],
            Ok(QueryEvent::TableCreated) => vec![Message::CommandComplete("CREATE TABLE".to_owned())],
            Ok(QueryEvent::TableDropped) => vec![Message::CommandComplete("DROP TABLE".to_owned())],
            Ok(QueryEvent::VariableSet) => vec![Message::CommandComplete("SET".to_owned())],
            Ok(QueryEvent::TransactionStarted) => vec![Message::CommandComplete("BEGIN".to_owned())],
            Ok(QueryEvent::RecordsInserted(records)) => vec![Message::CommandComplete(format!("INSERT 0 {}", records))],
            Ok(QueryEvent::RecordsSelected(projection)) => {
                let definition = projection.0;
                let description: Vec<ColumnMetadata> = definition
                    .into_iter()
                    .map(|(name, sql_type)| ColumnMetadata::new(name, sql_type.pg_oid(), sql_type.pg_len()))
                    .collect();
                let records = projection.1;
                let len = records.len();
                let mut messages = vec![Message::RowDescription(description)];
                for record in records {
                    messages.push(Message::DataRow(record));
                }
                messages.push(Message::CommandComplete(format!("SELECT {}", len)));
                messages
            }
            Ok(QueryEvent::RecordsUpdated(records)) => vec![Message::CommandComplete(format!("UPDATE {}", records))],
            Ok(QueryEvent::RecordsDeleted(records)) => vec![Message::CommandComplete(format!("DELETE {}", records))],
            Err(query_error) => query_error.into_messages(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    mod mapper {
        use super::*;
        use crate::{results::QueryError, sql_types::PostgreSqlType};

        #[test]
        fn create_schema() {
            assert_eq!(
                QueryResultMapper::map(Ok(QueryEvent::SchemaCreated)),
                vec![Message::CommandComplete("CREATE SCHEMA".to_owned())]
            )
        }

        #[test]
        fn drop_schema() {
            assert_eq!(
                QueryResultMapper::map(Ok(QueryEvent::SchemaDropped)),
                vec![Message::CommandComplete("DROP SCHEMA".to_owned())]
            )
        }

        #[test]
        fn create_table() {
            assert_eq!(
                QueryResultMapper::map(Ok(QueryEvent::TableCreated)),
                vec![Message::CommandComplete("CREATE TABLE".to_owned())]
            );
        }

        #[test]
        fn drop_table() {
            assert_eq!(
                QueryResultMapper::map(Ok(QueryEvent::TableDropped)),
                vec![Message::CommandComplete("DROP TABLE".to_owned())]
            );
        }

        #[test]
        fn insert_record() {
            let records_number = 3;
            assert_eq!(
                QueryResultMapper::map(Ok(QueryEvent::RecordsInserted(records_number))),
                vec![Message::CommandComplete(format!("INSERT 0 {}", records_number))]
            )
        }

        #[test]
        fn select_records() {
            let projection = (
                vec![
                    ("column_name_1".to_owned(), PostgreSqlType::SmallInt),
                    ("column_name_2".to_owned(), PostgreSqlType::SmallInt),
                ],
                vec![
                    vec!["1".to_owned(), "2".to_owned()],
                    vec!["3".to_owned(), "4".to_owned()],
                ],
            );
            assert_eq!(
                QueryResultMapper::map(Ok(QueryEvent::RecordsSelected(projection))),
                vec![
                    Message::RowDescription(vec![
                        ColumnMetadata::new("column_name_1".to_owned(), 21, 2),
                        ColumnMetadata::new("column_name_2".to_owned(), 21, 2)
                    ]),
                    Message::DataRow(vec!["1".to_owned(), "2".to_owned()]),
                    Message::DataRow(vec!["3".to_owned(), "4".to_owned()]),
                    Message::CommandComplete("SELECT 2".to_owned())
                ]
            );
        }

        #[test]
        fn update_records() {
            let records_number = 3;
            assert_eq!(
                QueryResultMapper::map(Ok(QueryEvent::RecordsUpdated(records_number))),
                vec![Message::CommandComplete(format!("UPDATE {}", records_number))]
            );
        }

        #[test]
        fn delete_records() {
            let records_number = 3;
            assert_eq!(
                QueryResultMapper::map(Ok(QueryEvent::RecordsDeleted(records_number))),
                vec![Message::CommandComplete(format!("DELETE {}", records_number))]
            )
        }

        #[test]
        fn schema_already_exists() {
            let schema_name = "some_table_name".to_owned();
            assert_eq!(
                QueryResultMapper::map(Err(QueryError::schema_already_exists(schema_name.clone()))),
                vec![Message::ErrorResponse(
                    Some("ERROR".to_owned()),
                    Some("42P06".to_owned()),
                    Some(format!("schema \"{}\" already exists", schema_name)),
                )]
            )
        }

        #[test]
        fn schema_does_not_exists() {
            let schema_name = "some_table_name".to_owned();
            assert_eq!(
                QueryResultMapper::map(Err(QueryError::schema_does_not_exist(schema_name.clone()))),
                vec![Message::ErrorResponse(
                    Some("ERROR".to_owned()),
                    Some("3F000".to_owned()),
                    Some(format!("schema \"{}\" does not exist", schema_name)),
                )]
            )
        }

        #[test]
        fn table_already_exists() {
            let table_name = "some_table_name".to_owned();
            assert_eq!(
                QueryResultMapper::map(Err(QueryError::table_already_exists(table_name.clone()))),
                vec![Message::ErrorResponse(
                    Some("ERROR".to_owned()),
                    Some("42P07".to_owned()),
                    Some(format!("table \"{}\" already exists", table_name)),
                )]
            )
        }

        #[test]
        fn table_does_not_exists() {
            let table_name = "some_table_name".to_owned();
            assert_eq!(
                QueryResultMapper::map(Err(QueryError::table_does_not_exist(table_name.clone()))),
                vec![Message::ErrorResponse(
                    Some("ERROR".to_owned()),
                    Some("42P01".to_owned()),
                    Some(format!("table \"{}\" does not exist", table_name)),
                )]
            )
        }

        #[test]
        fn one_column_does_not_exists() {
            assert_eq!(
                QueryResultMapper::map(Err(QueryError::column_does_not_exist(vec![
                    "column_not_in_table".to_owned()
                ]))),
                vec![Message::ErrorResponse(
                    Some("ERROR".to_owned()),
                    Some("42703".to_owned()),
                    Some("column column_not_in_table does not exist".to_owned()),
                )]
            )
        }

        #[test]
        fn multiple_columns_does_not_exists() {
            assert_eq!(
                QueryResultMapper::map(Err(QueryError::column_does_not_exist(vec![
                    "column_not_in_table1".to_owned(),
                    "column_not_in_table2".to_owned()
                ]))),
                vec![Message::ErrorResponse(
                    Some("ERROR".to_owned()),
                    Some("42703".to_owned()),
                    Some("columns column_not_in_table1, column_not_in_table2 do not exist".to_owned()),
                )]
            )
        }

        #[test]
        fn operation_is_not_supported() {
            let raw_sql_query = "some SQL query".to_owned();
            assert_eq!(
                QueryResultMapper::map(Err(QueryError::not_supported_operation(raw_sql_query.clone()))),
                vec![Message::ErrorResponse(
                    Some("ERROR".to_owned()),
                    Some("42601".to_owned()),
                    Some(format!("Currently, Query '{}' can't be executed", raw_sql_query)),
                )]
            )
        }
    }

    #[cfg(test)]
    mod connection {
        use super::*;

        #[cfg(test)]
        mod read_query {
            use super::*;
            use test_helpers::async_io;

            #[async_std::test]
            async fn read_termination_command() -> io::Result<()> {
                let test_case = async_io::TestCase::with_content(vec![&[88], &[0, 0, 0, 4]]).await;
                let mut connection = Connection::new((VERSION_3, vec![], SslMode::Disable), test_case);

                let query = connection.receive().await?;

                assert_eq!(query, Ok(Command::Terminate));

                Ok(())
            }

            #[async_std::test]
            async fn read_query_successfully() -> io::Result<()> {
                let test_case = async_io::TestCase::with_content(vec![&[81], &[0, 0, 0, 14], b"select 1;\0"]).await;
                let mut connection = Connection::new((VERSION_3, vec![], SslMode::Disable), test_case.clone());

                let query = connection.receive().await?;

                assert_eq!(query, Ok(Command::Query("select 1;".to_owned())));

                let actual_content = test_case.read_result().await;
                let mut expected_content = BytesMut::new();
                expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
                assert_eq!(actual_content, expected_content);

                Ok(())
            }

            #[async_std::test]
            async fn unexpected_eof_when_read_type_code_of_query_request() {
                let test_case = async_io::TestCase::with_content(vec![]).await;
                let mut connection = Connection::new((VERSION_3, vec![], SslMode::Disable), test_case);

                let query = connection.receive().await;

                assert!(query.is_err());
            }

            #[async_std::test]
            async fn unexpected_eof_when_read_length_of_query() {
                let test_case = async_io::TestCase::with_content(vec![&[81]]).await;
                let mut connection = Connection::new((VERSION_3, vec![], SslMode::Disable), test_case);

                let query = connection.receive().await;

                assert!(query.is_err());
            }

            #[async_std::test]
            async fn unexpected_eof_when_query_string() {
                let test_case = async_io::TestCase::with_content(vec![&[81], &[0, 0, 0, 14], b"sel;\0"]).await;
                let mut connection = Connection::new((VERSION_3, vec![], SslMode::Disable), test_case);

                let query = connection.receive().await;

                assert!(query.is_err());
            }
        }
    }
}
