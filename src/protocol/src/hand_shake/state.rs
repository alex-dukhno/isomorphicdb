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

use crate::{
    messages::Cursor, Code, ConnId, ConnSecretKey, Error, ProtocolResult, CANCEL_REQUEST_CODE, SSL_REQUEST_CODE,
    VERSION_1_CODE, VERSION_2_CODE, VERSION_3_CODE,
};

trait ConnectionTransition<C> {
    fn transit(self, cursor: &mut Cursor) -> ProtocolResult<C>;
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct MessageLen(pub(crate) usize);

impl ConnectionTransition<ReadSetupMessage> for MessageLen {
    fn transit(self, cursor: &mut Cursor) -> ProtocolResult<ReadSetupMessage> {
        let len = cursor.read_i32()?;
        Ok(ReadSetupMessage((len - 4) as usize))
    }
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct ReadSetupMessage(pub(crate) usize);

impl ConnectionTransition<SetupParsed> for ReadSetupMessage {
    fn transit(self, cursor: &mut Cursor) -> ProtocolResult<SetupParsed> {
        let code = Code(cursor.read_i32()?);
        log::info!("Connection Code: {}", code);
        match code {
            VERSION_1_CODE => Err(Error::UnsupportedVersion),
            VERSION_2_CODE => Err(Error::UnsupportedVersion),
            VERSION_3_CODE => {
                let mut props = vec![];
                loop {
                    let key = cursor.read_cstr()?.to_owned();
                    if key == "" {
                        break;
                    }
                    let value = cursor.read_cstr()?.to_owned();
                    props.push((key, value));
                }
                Ok(SetupParsed::Established(props))
            }
            CANCEL_REQUEST_CODE => {
                let conn_id = cursor.read_i32()?;
                let secret_key = cursor.read_i32()?;
                Ok(SetupParsed::Cancel(conn_id, secret_key))
            }
            SSL_REQUEST_CODE => Ok(SetupParsed::Secure),
            _ => Err(Error::UnsupportedRequest),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum SetupParsed {
    Established(Vec<(String, String)>),
    Cancel(ConnId, ConnSecretKey),
    Secure,
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum State {
    MessageLen(MessageLen),
    ParseSetup(ReadSetupMessage),
    SetupParsed(SetupParsed),
}

impl State {
    pub(crate) fn new() -> State {
        State::MessageLen(MessageLen(4))
    }

    pub(crate) fn try_step(self, buf: &[u8]) -> ProtocolResult<State> {
        let mut buffer = Cursor::from(buf);
        match self {
            State::MessageLen(hand_shake) => Ok(State::ParseSetup(hand_shake.transit(&mut buffer)?)),
            State::ParseSetup(hand_shake) => Ok(State::SetupParsed(hand_shake.transit(&mut buffer)?)),
            State::SetupParsed(hand_shake) => match hand_shake {
                SetupParsed::Secure => Ok(State::MessageLen(MessageLen(4))),
                _ => Err(Error::VerificationFailed),
            },
        }
    }
}

#[cfg(test)]
mod connection_state_machine {
    use super::*;
    use crate::GSSENC_REQUEST_CODE;

    #[test]
    fn created_state() {
        let hand_shake = State::new();

        assert_eq!(hand_shake, State::MessageLen(MessageLen(4)));
    }

    #[test]
    fn read_setup_message_length() {
        let hand_shake = State::new();

        assert_eq!(
            hand_shake.try_step(&[0, 0, 0, 4]),
            Ok(State::ParseSetup(ReadSetupMessage(0)))
        );
    }

    #[test]
    fn non_recognizable_protocol_code() {
        let mut hand_shake = State::new();

        hand_shake = hand_shake.try_step(&[0, 0, 0, 25]).expect("proceed to the next step");

        assert_eq!(
            hand_shake.try_step(b"non_recognizable_code"),
            Err(Error::UnsupportedRequest)
        );
    }

    #[test]
    fn version_one_is_not_supported() {
        let mut hand_shake = State::new();

        hand_shake = hand_shake.try_step(&[0, 0, 0, 8]).expect("proceed to the next step");

        assert_eq!(
            hand_shake.try_step(&Vec::from(VERSION_1_CODE)),
            Err(Error::UnsupportedVersion)
        );
    }

    #[test]
    fn version_two_is_not_supported() {
        let mut hand_shake = State::new();

        hand_shake = hand_shake.try_step(&[0, 0, 0, 8]).expect("proceed to the next step");

        assert_eq!(
            hand_shake.try_step(&Vec::from(VERSION_2_CODE)),
            Err(Error::UnsupportedVersion)
        );
    }

    #[test]
    fn setup_version_three_with_client_params() {
        let mut hand_shake = State::new();

        hand_shake = hand_shake.try_step(&[0, 0, 0, 33]).expect("proceed to the next step");

        let mut payload = vec![];
        payload.extend_from_slice(&Vec::from(VERSION_3_CODE));
        payload.extend_from_slice(b"key1\0");
        payload.extend_from_slice(b"value1\0");
        payload.extend_from_slice(b"key2\0");
        payload.extend_from_slice(b"value2\0");
        payload.extend_from_slice(&[0]);

        assert_eq!(
            hand_shake.try_step(&payload),
            Ok(State::SetupParsed(SetupParsed::Established(vec![
                ("key1".to_owned(), "value1".to_owned()),
                ("key2".to_owned(), "value2".to_owned())
            ])))
        );
    }

    #[test]
    fn connection_established_with_ssl_request() {
        let mut hand_shake = State::new();

        hand_shake = hand_shake.try_step(&[0, 0, 0, 8]).expect("proceed to the next step");

        hand_shake = hand_shake
            .try_step(&Vec::from(SSL_REQUEST_CODE))
            .expect("proceed to the next step");
        assert_eq!(hand_shake, State::SetupParsed(SetupParsed::Secure));

        hand_shake = hand_shake.try_step(&[]).expect("proceed to the next step");

        hand_shake = hand_shake.try_step(&[0, 0, 0, 33]).expect("proceed to the next step");

        let mut payload = vec![];
        payload.extend_from_slice(&Vec::from(VERSION_3_CODE));
        payload.extend_from_slice(b"key1\0");
        payload.extend_from_slice(b"value1\0");
        payload.extend_from_slice(b"key2\0");
        payload.extend_from_slice(b"value2\0");
        payload.extend_from_slice(&[0]);

        assert_eq!(
            hand_shake.try_step(&payload),
            Ok(State::SetupParsed(SetupParsed::Established(vec![
                ("key1".to_owned(), "value1".to_owned()),
                ("key2".to_owned(), "value2".to_owned())
            ])))
        );
    }

    #[test]
    fn connection_established_with_gssenc_request() {
        let mut hand_shake = State::new();

        hand_shake = hand_shake.try_step(&[0, 0, 0, 8]).expect("proceed to the next step");

        assert_eq!(
            hand_shake.try_step(&Vec::from(GSSENC_REQUEST_CODE)),
            Err(Error::UnsupportedRequest)
        );
    }

    #[test]
    fn cancel_query_request() {
        let conn_id: ConnId = 1;
        let secret_key: ConnSecretKey = 2;
        let mut hand_shake = State::new();

        hand_shake = hand_shake.try_step(&[0, 0, 0, 33]).expect("proceed to the next step");

        let mut payload = vec![];
        payload.extend_from_slice(&Vec::from(CANCEL_REQUEST_CODE));
        payload.extend_from_slice(&conn_id.to_be_bytes());
        payload.extend_from_slice(&secret_key.to_be_bytes());

        assert_eq!(
            hand_shake.try_step(&payload),
            Ok(State::SetupParsed(SetupParsed::Cancel(conn_id, secret_key)))
        );
    }
}
