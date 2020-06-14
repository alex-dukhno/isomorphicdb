use protocol::{
    listener::{Field, QueryListener},
    messages::Message,
    Command,
};
use smol::Task;
use sql_engine::{Handler, QueryEvent, QueryResult};
use std::{
    sync::atomic::{AtomicU8, Ordering},
    sync::{Arc, Mutex},
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
            let listener = QueryListener::bind(local_address)
                .await
                .expect("open server connection");
            self.state.store(RUNNING, Ordering::SeqCst);

            let storage = Arc::new(Mutex::new(
                storage::frontend::FrontendStorage::default().unwrap(),
            ));

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
                        match connection.read_query().await {
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
                                log::debug!("SHOULD STOP");
                                state.store(STOPPED, Ordering::SeqCst);
                                return;
                            }
                            Ok(Ok(Command::Query(sql_query))) => match sql_handler
                                .execute(sql_query.as_str())
                                .expect("no system error")
                            {
                                Ok(QueryEvent::Terminate) => {
                                    log::debug!("SHOULD STOP");
                                    state.store(STOPPED, Ordering::SeqCst);
                                    return;
                                }
                                response => {
                                    match connection
                                        .send_response(QueryResultMapper::map(response))
                                        .await
                                    {
                                        Ok(()) => {}
                                        Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                    }
                                }
                            },
                        }
                    }
                })
                .detach();
            }
        });
    }
}

struct QueryResultMapper;

impl QueryResultMapper {
    fn map(resp: QueryResult) -> Vec<Message> {
        match resp {
            Ok(QueryEvent::SchemaCreated) => {
                vec![Message::CommandComplete("CREATE SCHEMA".to_owned())]
            }
            Ok(QueryEvent::SchemaDropped) => {
                vec![Message::CommandComplete("DROP SCHEMA".to_owned())]
            }
            Ok(QueryEvent::TableCreated) => {
                vec![Message::CommandComplete("CREATE TABLE".to_owned())]
            }
            Ok(QueryEvent::TableDropped) => vec![Message::CommandComplete("DROP TABLE".to_owned())],
            Ok(QueryEvent::RecordsInserted(records)) => {
                vec![Message::CommandComplete(format!("INSERT 0 {}", records))]
            }
            Ok(QueryEvent::RecordsSelected(projection)) => {
                let definition = projection.0;
                let description: Vec<Field> = definition
                    .into_iter()
                    .map(|name| Field::new(name, 21, 2))
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
            Ok(QueryEvent::RecordsUpdated(records)) => {
                vec![Message::CommandComplete(format!("UPDATE {}", records))]
            }
            Ok(QueryEvent::RecordsDeleted(records)) => {
                vec![Message::CommandComplete(format!("DELETE {}", records))]
            }
            Err(storage::frontend::Error::SchemaAlreadyExists(schema_name)) => {
                vec![Message::ErrorResponse(
                    Some("ERROR".to_owned()),
                    Some("42P06".to_owned()),
                    Some(format!("schema \"{}\" already exists", schema_name)),
                )]
            }
            Err(storage::frontend::Error::SchemaDoesNotExist(schema_name)) => {
                vec![Message::ErrorResponse(
                    Some("ERROR".to_owned()),
                    Some("3F000".to_owned()),
                    Some(format!("schema \"{}\" does not exist", schema_name)),
                )]
            }
            Err(storage::frontend::Error::TableAlreadyExists(table_name)) => {
                vec![Message::ErrorResponse(
                    Some("ERROR".to_owned()),
                    Some("42P07".to_owned()),
                    Some(format!("table \"{}\" already exists", table_name)),
                )]
            }
            Err(storage::frontend::Error::TableDoesNotExist(table_name)) => {
                vec![Message::ErrorResponse(
                    Some("ERROR".to_owned()),
                    Some("42P01".to_owned()),
                    Some(format!("table \"{}\" does not exist", table_name)),
                )]
            }
            Err(storage::frontend::Error::NotSupportedOperation(raw_sql_query)) => {
                vec![Message::ErrorResponse(
                    Some("ERROR".to_owned()),
                    Some("42601".to_owned()),
                    Some(format!(
                        "Currently, Query '{}' can't be executed",
                        raw_sql_query
                    )),
                )]
            }
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod mapper {
    use super::*;

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
            vec![Message::CommandComplete(format!(
                "INSERT 0 {}",
                records_number
            ))]
        )
    }

    #[test]
    fn select_records() {
        let projection = (
            vec!["column_name_1".to_owned(), "column_name_2".to_owned()],
            vec![
                vec!["1".to_owned(), "2".to_owned()],
                vec!["3".to_owned(), "4".to_owned()],
            ],
        );
        assert_eq!(
            QueryResultMapper::map(Ok(QueryEvent::RecordsSelected(projection))),
            vec![
                Message::RowDescription(vec![
                    Field::new("column_name_1".to_owned(), 21, 2),
                    Field::new("column_name_2".to_owned(), 21, 2)
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
            vec![Message::CommandComplete(format!(
                "UPDATE {}",
                records_number
            ))]
        );
    }

    #[test]
    fn delete_records() {
        let records_number = 3;
        assert_eq!(
            QueryResultMapper::map(Ok(QueryEvent::RecordsDeleted(records_number))),
            vec![Message::CommandComplete(format!(
                "DELETE {}",
                records_number
            ))]
        )
    }

    #[test]
    fn schema_already_exists() {
        let schema_name = "some_table_name".to_owned();
        assert_eq!(
            QueryResultMapper::map(Err(storage::frontend::Error::SchemaAlreadyExists(
                schema_name.clone()
            ))),
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
            QueryResultMapper::map(Err(storage::frontend::Error::SchemaDoesNotExist(
                schema_name.clone()
            ))),
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
            QueryResultMapper::map(Err(storage::frontend::Error::TableAlreadyExists(
                table_name.clone()
            ))),
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
            QueryResultMapper::map(Err(storage::frontend::Error::TableDoesNotExist(
                table_name.clone()
            ))),
            vec![Message::ErrorResponse(
                Some("ERROR".to_owned()),
                Some("42P01".to_owned()),
                Some(format!("table \"{}\" does not exist", table_name)),
            )]
        )
    }

    #[test]
    fn operation_is_not_supported() {
        let raw_sql_query = "some SQL query".to_owned();
        assert_eq!(
            QueryResultMapper::map(Err(storage::frontend::Error::NotSupportedOperation(
                raw_sql_query.clone()
            ))),
            vec![Message::ErrorResponse(
                Some("ERROR".to_owned()),
                Some("42601".to_owned()),
                Some(format!(
                    "Currently, Query '{}' can't be executed",
                    raw_sql_query
                )),
            )]
        )
    }
}
