#[macro_use]
extern crate log;
extern crate pretty_env_logger;

use std::io;
use std::io::{Read, Write};
use mio::net::{TcpListener, TcpStream};

use mio::{Events, Interest, Poll, Token};
use std::collections::HashMap;


use database::engine::{Engine, EngineEvent};

const PORT: usize = 7000;
const NETWORK_BUFFER_SIZE: usize = 256;
const SERVER: Token = Token(0);

fn main() -> io::Result<()> {
  pretty_env_logger::init();
  info!("Starting server on port {}", PORT);

  let mut counter: usize = 0;
  let mut sockets: HashMap<Token, TcpStream> = HashMap::new();
  let mut responses: HashMap<Token, Vec<Vec<u8>>> = HashMap::new();

  let mut poll = Poll::new().unwrap();
  let mut events = Events::with_capacity(128);

  let mut server = TcpListener::bind(format!("127.0.0.1:{}", PORT).parse().unwrap()).unwrap();
  let mut engine = Engine::default();
  info!("SQL engine has been created");

  poll.registry()
      .register(&mut server, SERVER, Interest::READABLE).unwrap();

  let mut buffer = [0 as u8; NETWORK_BUFFER_SIZE];
  info!("Network buffer of {} size created", NETWORK_BUFFER_SIZE);

  loop {
    poll.poll(&mut events, None).unwrap();

    for event in events.iter() {
      match event.token() {
        SERVER => {
          loop {
            match server.accept() {
              Ok((mut stream, address)) => {
                debug!("Connection {} is accepted", address);
                counter += 1;
                let token = Token(counter);

                // Register for readable events
                poll.registry()
                    .register(
                      &mut stream,
                      token,
                      Interest::READABLE | Interest::WRITABLE,
                    ).unwrap();

                sockets.insert(token, stream);
              },
              Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
              Err(e) => panic!("Unexpected error: {}", e)
            }
          }
        }
        token if event.is_readable() => {
          loop {
            let read = sockets.get_mut(&token).unwrap().read(&mut buffer);
            match read {
              Ok(0) => {
                sockets.remove(&token);
                break;
              }
              Ok(len) => {
                if let Some(stream) = sockets.get_mut(&token) {
                  trace!("{} bytes read from network connection", len);
                  let query = String::from_utf8(buffer[0..len].to_vec()).unwrap();
                  debug!("Received query {}", query);
                  let query_execution_result = engine.execute(query);
                  debug!("Query execution result");
                  debug!("{:?}", query_execution_result);
                  match query_execution_result {
                    Ok(engine_event) => match engine_event {
                      EngineEvent::TableCreated(table_name) => {
                        stream.write_all(vec![1 as u8].as_slice())?;
                        stream.write_all(format!("Table {} was created", table_name).as_bytes())?;
                      }
                      EngineEvent::RecordInserted
                      | EngineEvent::RecordsUpdated
                      | EngineEvent::RecordsDeleted => {
                        stream.write_all(vec![2 as u8].as_slice())?;
                        stream.write_all("done".as_bytes())?;
                      }
                      EngineEvent::RecordsSelected(records) => {
                        stream.write_all(vec![3 as u8].as_slice())?;
                        stream.write_all(vec![records.len() as u8].as_slice())?;
                        for record in records {
                          stream.write_all(record.as_slice())?;
                        }
                      }
                    }
                    Err(e) => {
                      stream.write_all(vec![u8::MAX].as_slice())?;
                      stream.write_all(format!("ERROR: {}", e).as_bytes())?;
                    }
                  }
                  stream.flush()?;
                }
              }
              Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
              Err(e) => panic!("Unexpected error: {}", e)
            }
          }
        }
        token if event.is_writable() => {
          match responses.remove(&token) {
            Some(response) =>
              for r in response {
                sockets.get_mut(&token).unwrap().write_all(r.as_slice()).unwrap()
              }
            None => {}
          }
        }
        token => panic!("Encountered unsupported TOKEN {:?}", token)
      }
    }
  }
}
