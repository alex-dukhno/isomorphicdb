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

use crate::query_listener::SmolQueryListener;
use protocol::{listener::Secure, messages::Message, ColumnMetadata, Command, QueryListener};
use smol::Task;
use sql_engine::{Handler, QueryError, QueryEvent, QueryResult};
use sql_types::SqlType;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, Mutex,
};

const PORT: usize = 5432;
const HOST: &str = "127.0.0.1";

pub const CREATED: u8 = 0;
pub const RUNNING: u8 = 1;
pub const STOPPED: u8 = 2;

pub struct Node {
    state: Arc<AtomicU8>,
}

impl Default for Node {
    fn default() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(CREATED)),
        }
    }
}

impl Node {
    pub fn state(&self) -> u8 {
        self.state.load(Ordering::SeqCst)
    }

    pub fn stop(&self) {
        self.state.store(STOPPED, Ordering::SeqCst);
    }

    pub fn start(&self) {
        let local_address = format!("{}:{}", HOST, PORT);
        log::debug!("Starting server on {}", local_address);

        smol::run(async {
            let listener = SmolQueryListener::bind(local_address, Secure::none())
                .await
                .expect("open server connection");
            self.state.store(RUNNING, Ordering::SeqCst);

            let storage = Arc::new(Mutex::new(storage::frontend::FrontendStorage::default().unwrap()));

            log::debug!("waiting for connections");
            while let Ok(mut connection) = listener.accept().await.expect("no io errors") {
                if self.state() == STOPPED {
                    return;
                }
                let state = self.state.clone();
                let storage = storage.clone();
                Task::spawn(async move {
                    let mut sql_handler = Handler::new(storage);

                    log::debug!("ready to handle query");
                    loop {
                        match connection.receive().await {
                            Err(e) => {
                                log::debug!("SHOULD STOP");
                                log::error!("UNEXPECTED ERROR: {:?}", e);
                                state.store(STOPPED, Ordering::SeqCst);
                                return;
                            }
                            Ok(Err(e)) => {
                                log::debug!("SHOULD STOP");
                                log::error!("UNEXPECTED ERROR: {:?}", e);
                                state.store(STOPPED, Ordering::SeqCst);
                                return;
                            }
                            Ok(Ok(Command::Terminate)) => {
                                log::debug!("Closing connection with client");
                                break;
                            }
                            Ok(Ok(Command::Query(sql_query))) => {
                                match sql_handler.execute(sql_query.as_str()).expect("no system error") {
                                    response => {
                                        match connection.send(QueryResultMapper::map(response)).await {
                                            Ok(()) => {}
                                            Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                        }
                                    }
                                }
                            }
                        }
                    }
                })
                .detach();
            }
        });
    }
}

struct TypeConverter;

impl TypeConverter {
    fn pg_oid(sql_type: &SqlType) -> i32 {
        match sql_type {
            SqlType::Bool => 16,
            SqlType::Char(_) => 18,
            SqlType::BigInt => 20,           // PG int8
            SqlType::SmallInt => 21,         // PG int2
            SqlType::Integer => 23,          // PG int4
            SqlType::Real => 700,            // PG float4
            SqlType::DoublePrecision => 701, // PG float8
            SqlType::VarChar(_) => 1043,
            SqlType::Date => 1082,
            SqlType::Time => 1083,
            SqlType::Timestamp => 1114,
            SqlType::TimestampWithTimeZone => 1184, // PG Timestamptz
            SqlType::Interval => 1186,
            SqlType::TimeWithTimeZone => 1266, // PG Timetz
            SqlType::Decimal => 1700,          // PG Numeric & Decimal
        }
    }

    fn pg_len(sql_type: &SqlType) -> i16 {
        match sql_type {
            SqlType::Bool => 1,
            SqlType::Char(_) => 1,
            SqlType::BigInt => 8,
            SqlType::SmallInt => 2,
            SqlType::Integer => 4,
            SqlType::Real => 4,
            SqlType::DoublePrecision => 8,
            SqlType::VarChar(_) => -1,
            SqlType::Date => 4,
            SqlType::Time => 8,
            SqlType::Timestamp => 8,
            SqlType::TimestampWithTimeZone => 8,
            SqlType::Interval => 16,
            SqlType::TimeWithTimeZone => 12,
            SqlType::Decimal => -1,
        }
    }
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
            Ok(QueryEvent::RecordsInserted(records)) => vec![Message::CommandComplete(format!("INSERT 0 {}", records))],
            Ok(QueryEvent::RecordsSelected(projection)) => {
                let definition = projection.0;
                let description: Vec<ColumnMetadata> = definition
                    .into_iter()
                    .map(|(name, sql_type)| {
                        ColumnMetadata::new(name, TypeConverter::pg_oid(&sql_type), TypeConverter::pg_len(&sql_type))
                    })
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
            Err(QueryError::SchemaAlreadyExists(schema_name)) => vec![Message::ErrorResponse(
                Some("ERROR".to_owned()),
                Some("42P06".to_owned()),
                Some(format!("schema \"{}\" already exists", schema_name)),
            )],
            Err(QueryError::SchemaDoesNotExist(schema_name)) => vec![Message::ErrorResponse(
                Some("ERROR".to_owned()),
                Some("3F000".to_owned()),
                Some(format!("schema \"{}\" does not exist", schema_name)),
            )],
            Err(QueryError::TableAlreadyExists(table_name)) => vec![Message::ErrorResponse(
                Some("ERROR".to_owned()),
                Some("42P07".to_owned()),
                Some(format!("table \"{}\" already exists", table_name)),
            )],
            Err(QueryError::TableDoesNotExist(table_name)) => vec![Message::ErrorResponse(
                Some("ERROR".to_owned()),
                Some("42P01".to_owned()),
                Some(format!("table \"{}\" does not exist", table_name)),
            )],
            Err(QueryError::ColumnDoesNotExist(non_existing_columns)) => {
                let error_message = if non_existing_columns.len() > 1 {
                    format!("columns {} do not exist", non_existing_columns.join(", "))
                } else {
                    format!("column {} does not exist", non_existing_columns[0])
                };

                vec![Message::ErrorResponse(
                    Some("ERROR".to_owned()),
                    Some("42703".to_owned()),
                    Some(error_message),
                )]
            }
            Err(QueryError::NotSupportedOperation(raw_sql_query)) => vec![Message::ErrorResponse(
                Some("ERROR".to_owned()),
                Some("42601".to_owned()),
                Some(format!("Currently, Query '{}' can't be executed", raw_sql_query)),
            )],
        }
    }
}

#[cfg(test)]
mod mapper {
    use super::*;
    use sql_types::SqlType;

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
                ("column_name_1".to_owned(), SqlType::SmallInt),
                ("column_name_2".to_owned(), SqlType::SmallInt),
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
            QueryResultMapper::map(Err(QueryError::SchemaAlreadyExists(schema_name.clone()))),
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
            QueryResultMapper::map(Err(QueryError::SchemaDoesNotExist(schema_name.clone()))),
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
            QueryResultMapper::map(Err(QueryError::TableAlreadyExists(table_name.clone()))),
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
            QueryResultMapper::map(Err(QueryError::TableDoesNotExist(table_name.clone()))),
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
            QueryResultMapper::map(Err(QueryError::ColumnDoesNotExist(vec![
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
            QueryResultMapper::map(Err(QueryError::ColumnDoesNotExist(vec![
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
            QueryResultMapper::map(Err(QueryError::NotSupportedOperation(raw_sql_query.clone()))),
            vec![Message::ErrorResponse(
                Some("ERROR".to_owned()),
                Some("42601".to_owned()),
                Some(format!("Currently, Query '{}' can't be executed", raw_sql_query)),
            )]
        )
    }
}
