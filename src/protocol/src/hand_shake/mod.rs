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

mod state;

use crate::{ConnId, ConnSecretKey, Result};
use state::{MessageLen, ReadSetupMessage, SetupParsed, State};

#[derive(Debug, PartialEq)]
pub struct Process {
    state: Option<State>,
}

impl Process {
    pub fn start() -> Process {
        Process { state: None }
    }

    pub fn next_stage(&mut self, payload: Option<&[u8]>) -> Result<Status> {
        match self.state.take() {
            None => {
                self.state = Some(State::new());
                Ok(Status::Requesting(Request::Buffer(4)))
            }
            Some(state) => {
                if let Some(bytes) = payload {
                    let new_state = state.try_step(bytes)?;
                    let result = match new_state.clone() {
                        State::ParseSetup(ReadSetupMessage(len)) => Ok(Status::Requesting(Request::Buffer(len))),
                        State::MessageLen(MessageLen(len)) => Ok(Status::Requesting(Request::Buffer(len))),
                        State::SetupParsed(SetupParsed::Established(props)) => Ok(Status::Done(props)),
                        State::SetupParsed(SetupParsed::Secure) => Ok(Status::Requesting(Request::UpgradeToSsl)),
                        State::SetupParsed(SetupParsed::Cancel(conn_id, secret_key)) => {
                            Ok(Status::Cancel(conn_id, secret_key))
                        }
                    };
                    self.state = Some(new_state);
                    result
                } else {
                    self.state = Some(state.try_step(&[])?);
                    Ok(Status::Requesting(Request::Buffer(4)))
                }
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Status {
    Requesting(Request),
    Done(Vec<(String, String)>),
    Cancel(ConnId, ConnSecretKey),
}

#[derive(Debug, PartialEq)]
pub enum Request {
    Buffer(usize),
    UpgradeToSsl,
}

#[cfg(test)]
mod perform_hand_shake_loop {
    use super::*;
    use crate::{CANCEL_REQUEST_CODE, SSL_REQUEST_CODE, VERSION_3_CODE};

    #[test]
    fn init_hand_shake_process() -> Result<()> {
        let mut process = Process::start();
        assert_eq!(process.next_stage(None)?, Status::Requesting(Request::Buffer(4)));

        Ok(())
    }

    #[test]
    fn read_setup_message_length() -> Result<()> {
        let mut process = Process::start();

        process.next_stage(None)?;
        assert_eq!(
            process.next_stage(Some(&[0, 0, 0, 33]))?,
            Status::Requesting(Request::Buffer(29))
        );

        Ok(())
    }

    #[test]
    fn non_secure_connection_hand_shake() -> Result<()> {
        let mut process = Process::start();

        process.next_stage(None)?;
        process.next_stage(Some(&[0, 0, 0, 33]))?;

        let mut payload = vec![];
        payload.extend_from_slice(&Vec::from(VERSION_3_CODE));
        payload.extend_from_slice(b"key1\0");
        payload.extend_from_slice(b"value1\0");
        payload.extend_from_slice(b"key2\0");
        payload.extend_from_slice(b"value2\0");
        payload.extend_from_slice(&[0]);

        assert_eq!(
            process.next_stage(Some(&payload))?,
            Status::Done(vec![
                ("key1".to_owned(), "value1".to_owned()),
                ("key2".to_owned(), "value2".to_owned())
            ])
        );

        Ok(())
    }

    #[test]
    fn ssl_secure_connection_hand_shake() -> Result<()> {
        let mut process = Process::start();

        process.next_stage(None)?;
        process.next_stage(Some(&[0, 0, 0, 8]))?;

        assert_eq!(
            process.next_stage(Some(&Vec::from(SSL_REQUEST_CODE)))?,
            Status::Requesting(Request::UpgradeToSsl)
        );

        process.next_stage(None)?;
        process.next_stage(Some(&[0, 0, 0, 33]))?;

        let mut payload = vec![];
        payload.extend_from_slice(&Vec::from(VERSION_3_CODE));
        payload.extend_from_slice(b"key1\0");
        payload.extend_from_slice(b"value1\0");
        payload.extend_from_slice(b"key2\0");
        payload.extend_from_slice(b"value2\0");
        payload.extend_from_slice(&[0]);

        assert_eq!(
            process.next_stage(Some(&payload))?,
            Status::Done(vec![
                ("key1".to_owned(), "value1".to_owned()),
                ("key2".to_owned(), "value2".to_owned())
            ])
        );

        Ok(())
    }

    #[test]
    fn cancel_query_request() -> Result<()> {
        let conn_id: ConnId = 1;
        let secret_key: ConnSecretKey = 2;

        let mut process = Process::start();

        process.next_stage(None)?;
        process.next_stage(Some(&[0, 0, 0, 16]))?;

        let mut payload = vec![];
        payload.extend_from_slice(&Vec::from(CANCEL_REQUEST_CODE));
        payload.extend_from_slice(&conn_id.to_be_bytes());
        payload.extend_from_slice(&secret_key.to_be_bytes());

        assert_eq!(process.next_stage(Some(&payload))?, Status::Cancel(conn_id, secret_key));

        Ok(())
    }
}
