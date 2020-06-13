use protocol::{
    listener::{Field, QueryListener},
    messages::Message,
    Command,
};
use smol::Task;
use sql_engine::{Handler, QueryEvent};
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
                                Ok(QueryEvent::SchemaCreated) => {
                                    match connection
                                        .send_command_complete(Message::CommandComplete(
                                            "CREATE SCHEMA".to_owned(),
                                        ))
                                        .await
                                    {
                                        Ok(()) => {}
                                        Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                    }
                                }
                                Ok(QueryEvent::SchemaDropped) => {
                                    match connection
                                        .send_command_complete(Message::CommandComplete(
                                            "DROP SCHEMA".to_owned(),
                                        ))
                                        .await
                                    {
                                        Ok(()) => {}
                                        Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                    }
                                }
                                Ok(QueryEvent::TableCreated) => {
                                    match connection
                                        .send_command_complete(Message::CommandComplete(
                                            "CREATE TABLE".to_owned(),
                                        ))
                                        .await
                                    {
                                        Ok(()) => {}
                                        Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                    }
                                }
                                Ok(QueryEvent::TableDropped) => {
                                    match connection
                                        .send_command_complete(Message::CommandComplete(
                                            "DROP TABLE".to_owned(),
                                        ))
                                        .await
                                    {
                                        Ok(()) => {}
                                        Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                    }
                                }
                                Ok(QueryEvent::RecordsInserted(len)) => {
                                    match connection
                                        .send_command_complete(Message::CommandComplete(format!(
                                            "INSERT 0 {}",
                                            len
                                        )))
                                        .await
                                    {
                                        Ok(()) => {}
                                        Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                    }
                                }
                                Ok(QueryEvent::RecordsSelected(projection)) => {
                                    let definition = projection.0;
                                    let description: Vec<Field> = definition
                                        .iter()
                                        .map(|name| Field::new(name.clone(), 21, 2))
                                        .collect();
                                    let records = projection.1;
                                    let len = records.len();
                                    match connection.send_row_description(description).await {
                                        Ok(()) => {}
                                        Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                    }
                                    for record in records {
                                        match connection.send_row_data(record).await {
                                            Ok(()) => {}
                                            Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                        }
                                    }
                                    match connection
                                        .send_command_complete(Message::CommandComplete(format!(
                                            "SELECT {}",
                                            len
                                        )))
                                        .await
                                    {
                                        Ok(()) => {}
                                        Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                    }
                                }
                                Ok(QueryEvent::RecordsUpdated(records_number)) => {
                                    match connection
                                        .send_command_complete(Message::CommandComplete(format!(
                                            "UPDATE {}",
                                            records_number
                                        )))
                                        .await
                                    {
                                        Ok(()) => {}
                                        Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                    }
                                }
                                Ok(QueryEvent::RecordsDeleted(records_number)) => {
                                    match connection
                                        .send_command_complete(Message::CommandComplete(format!(
                                            "DELETE {}",
                                            records_number
                                        )))
                                        .await
                                    {
                                        Ok(()) => {}
                                        Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                    }
                                }
                                Err(storage::frontend::Error::SchemaAlreadyExists(schema_name)) => {
                                    match connection
                                        .send_command_complete(Message::ErrorResponse(
                                            Some("ERROR".to_owned()),
                                            Some("42P06".to_owned()),
                                            Some(format!("schema \"{}\" already exists", schema_name)),
                                        ))
                                        .await
                                    {
                                        Ok(()) => {}
                                        Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                    }
                                }
                                Err(storage::frontend::Error::TableAlreadyExists(table_name)) => {
                                    match connection
                                        .send_command_complete(Message::ErrorResponse(
                                            Some("ERROR".to_owned()),
                                            Some("42P07".to_owned()),
                                            Some(format!("table \"{}\" already exists", table_name)),
                                        ))
                                        .await
                                    {
                                        Ok(()) => {}
                                        Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                    }
                                }
                                Err(storage::frontend::Error::NotSupportedOperation(raw_sql_query)) => {
                                    match connection
                                        .send_command_complete(Message::ErrorResponse(
                                            Some("ERROR".to_owned()),
                                            Some("42601".to_owned()),
                                            Some(format!(
                                                "Currently, Query '{}' can't be executed",
                                                raw_sql_query
                                            )),
                                        ))
                                        .await
                                    {
                                        Ok(()) => {}
                                        Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                    }
                                }
                                Err(e) => {
                                    match connection
                                        .send_command_complete(Message::ErrorResponse(
                                            Some("ERROR".to_owned()),
                                            Some("58000".to_owned()),
                                            Some(format!(
                                                "Unhandled error during executing query: '{}'\nThe error is: {:#?}",
                                                sql_query, e
                                            )),
                                        )).await
                                    {
                                        Ok(()) => {}
                                        Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                    }
                                }
                            },
                        }
                    }
                }).detach();
            }
        });
    }
}
