use protocol::{listener::Field, messages::Message, Command};
use sql_engine::{Handler, QueryEvent};
use std::{
    sync::atomic::{AtomicU8, Ordering},
    sync::{Arc, Mutex},
    thread,
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
        debug!("Starting server on {}", local_address);

        let listener =
            protocol::listener::QueryListener::bind(local_address).expect("open server connection");
        self.state.store(RUNNING, Ordering::SeqCst);

        let storage = Arc::new(Mutex::new(
            storage::frontend::FrontendStorage::default().unwrap(),
        ));

        while let Ok(Ok(mut connection)) = listener.accept() {
            let state = self.state.clone();
            let storage = storage.clone();
            thread::spawn(move || {
                let mut sql_handler = Handler::new(storage);

                debug!("ready to handle query");
                loop {
                    match connection.read_query() {
                        Err(_) => {
                            state.store(STOPPED, Ordering::SeqCst);
                            return;
                        }
                        Ok(Err(_)) => {
                            state.store(STOPPED, Ordering::SeqCst);
                            return;
                        }
                        Ok(Ok(Command::Terminate)) => {
                            state.store(STOPPED, Ordering::SeqCst);
                            return;
                        }
                        Ok(Ok(Command::Query(sql_query))) => match sql_handler
                            .execute(sql_query.as_str())
                            .expect("no system error")
                        {
                            Ok(QueryEvent::Terminate) => {
                                eprintln!("should terminate");
                                state.store(STOPPED, Ordering::SeqCst);
                                return;
                            }
                            Ok(QueryEvent::SchemaCreated) => {
                                match connection.send_command_complete(Message::CommandComplete(
                                    "CREATE SCHEMA".to_owned(),
                                )) {
                                    Ok(()) => {}
                                    Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                }
                            }
                            Ok(QueryEvent::SchemaDropped) => {
                                match connection.send_command_complete(Message::CommandComplete(
                                    "DROP SCHEMA".to_owned(),
                                )) {
                                    Ok(()) => {}
                                    Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                }
                            }
                            Ok(QueryEvent::TableCreated) => {
                                match connection.send_command_complete(Message::CommandComplete(
                                    "CREATE TABLE".to_owned(),
                                )) {
                                    Ok(()) => {}
                                    Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                }
                            }
                            Ok(QueryEvent::TableDropped) => {
                                match connection.send_command_complete(Message::CommandComplete(
                                    "DROP TABLE".to_owned(),
                                )) {
                                    Ok(()) => {}
                                    Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                }
                            }
                            Ok(QueryEvent::RecordsInserted(len)) => {
                                match connection.send_command_complete(Message::CommandComplete(
                                    format!("INSERT 0 {}", len),
                                )) {
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
                                match connection.send_row_description(description) {
                                    Ok(()) => {}
                                    Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                }
                                for record in records {
                                    match connection.send_row_data(record) {
                                        Ok(()) => {}
                                        Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                    }
                                }
                                match connection.send_command_complete(Message::CommandComplete(
                                    format!("SELECT {}", len),
                                )) {
                                    Ok(()) => {}
                                    Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                }
                            }
                            Ok(QueryEvent::RecordsUpdated(records_number)) => {
                                match connection.send_command_complete(Message::CommandComplete(
                                    format!("UPDATE {}", records_number),
                                )) {
                                    Ok(()) => {}
                                    Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                }
                            }
                            Ok(QueryEvent::RecordsDeleted(records_number)) => {
                                match connection.send_command_complete(Message::CommandComplete(
                                    format!("DELETE {}", records_number),
                                )) {
                                    Ok(()) => {}
                                    Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                }
                            }
                            Err(storage::frontend::Error::SchemaAlreadyExists(schema_name)) => {
                                match connection.send_command_complete(Message::ErrorResponse(
                                    Some("ERROR".to_owned()),
                                    Some("42P06".to_owned()),
                                    Some(format!("schema \"{}\" already exists", schema_name)),
                                )) {
                                    Ok(()) => {}
                                    Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                }
                            }
                            Err(storage::frontend::Error::TableAlreadyExists(table_name)) => {
                                match connection.send_command_complete(Message::ErrorResponse(
                                    Some("ERROR".to_owned()),
                                    Some("42P07".to_owned()),
                                    Some(format!("table \"{}\" already exists", table_name)),
                                )) {
                                    Ok(()) => {}
                                    Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                }
                            }
                            Err(storage::frontend::Error::NotSupportedOperation(raw_sql_query)) => {
                                match connection.send_command_complete(Message::ErrorResponse(
                                    Some("ERROR".to_owned()),
                                    Some("42601".to_owned()),
                                    Some(format!(
                                        "Currently, Query '{}' can't be executed",
                                        raw_sql_query
                                    )),
                                )) {
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
                                    ))
                                {
                                    Ok(()) => {}
                                    Err(error) => eprintln!("{:?}", error), // break Err(SystemError::io(error)),
                                }
                            }
                        },
                    }
                }
            });

            if self.state() == STOPPED {
                return;
            }
        }
    }
}
