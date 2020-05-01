#[macro_use]
extern crate log;
extern crate pretty_env_logger;

use std::io;
use std::io::{Read, Write};
use std::net::TcpListener;

use database::engine::{Engine, EngineEvent};

const PORT: usize = 7000;
const NETWORK_BUFFER_SIZE: usize = 256;

fn main() -> io::Result<()> {
  pretty_env_logger::init();
  info!("Starting server on port {}", PORT);

  let server = TcpListener::bind(format!("127.0.0.1:{}", PORT)).unwrap();
  let mut engine = Engine::default();
  info!("SQL engine has been created");

  let mut buffer = [0 as u8; NETWORK_BUFFER_SIZE];
  info!("Network buffer of {} size created", NETWORK_BUFFER_SIZE);

  match server.accept() {
    Ok((mut stream, address)) => {
      debug!("Connection {} is accepted", address);
      while let Ok(len) = stream.read(&mut buffer) {
        trace!("{} bytes read from network connection", len);
        let query = String::from_utf8(buffer[0..len].to_vec()).unwrap();
        debug!("{}", query);
        debug!("Received from {} client", address);
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
      }
      stream.flush()?;
    }
    Err(e) => panic!(e)
  }

  Ok(())
}
