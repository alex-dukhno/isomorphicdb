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
    messages::Cursor, Code, ConnId, ConnSecretKey, Error, Result, CANCEL_REQUEST_CODE, SSL_REQUEST_CODE,
    VERSION_1_CODE, VERSION_2_CODE, VERSION_3_CODE,
};

trait ConnectionTransition<C> {
    fn transit(self, cursor: Cursor) -> Result<C>;
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct Created;

impl ConnectionTransition<MessageLen> for Created {
    fn transit(self, _cursor: Cursor) -> Result<MessageLen> {
        Ok(MessageLen(4))
    }
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct MessageLen(pub(crate) usize);

impl ConnectionTransition<ReadSetupMessage> for MessageLen {
    fn transit(self, mut cursor: Cursor) -> Result<ReadSetupMessage> {
        let len = cursor.read_i32()?;
        Ok(ReadSetupMessage((len - 4) as usize))
    }
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct ReadSetupMessage(pub usize);

impl ConnectionTransition<Intermediate> for ReadSetupMessage {
    fn transit(self, mut cursor: Cursor) -> Result<Intermediate> {
        let code = Code(cursor.read_i32()?);
        log::info!("Connection Code: {}", code);
        match code {
            Code(VERSION_1_CODE) => Err(Error::UnsupportedVersion),
            Code(VERSION_2_CODE) => Err(Error::UnsupportedVersion),
            Code(VERSION_3_CODE) => Ok(Intermediate(code, Some(cursor.into()))),
            Code(CANCEL_REQUEST_CODE) => Ok(Intermediate(code, Some(cursor.into()))),
            Code(SSL_REQUEST_CODE) => Ok(Intermediate(code, None)),
            _ => Err(Error::UnsupportedRequest),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct Intermediate(pub Code, pub Option<Vec<u8>>);

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct Done(pub Vec<(String, String)>);

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct Cancel(pub ConnId, pub ConnSecretKey);

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum InnerState {
    Created(Created),
    MessageLen(MessageLen),
    ParseSetup(ReadSetupMessage),
    Intermediate(Intermediate),
    Established(Done),
    Cancel(Cancel),
}

impl InnerState {
    pub(crate) fn new() -> InnerState {
        InnerState::Created(Created)
    }

    pub(crate) fn try_step(self, buf: Vec<u8>) -> Result<InnerState> {
        let buffer = Cursor::from(buf.as_slice());
        match self {
            InnerState::Created(hand_shake) => Ok(InnerState::MessageLen(hand_shake.transit(buffer)?)),
            InnerState::MessageLen(hand_shake) => Ok(InnerState::ParseSetup(hand_shake.transit(buffer)?)),
            InnerState::ParseSetup(hand_shake) => Ok(InnerState::Intermediate(hand_shake.transit(buffer)?)),
            InnerState::Intermediate(Intermediate(_, None)) => Ok(InnerState::MessageLen(MessageLen(4))),
            InnerState::Intermediate(Intermediate(version, Some(bytes))) => match version {
                Code(VERSION_3_CODE) => {
                    let mut cursor = Cursor::from(bytes.as_slice());
                    let mut props = vec![];
                    loop {
                        let key = cursor.read_cstr()?.to_owned();
                        if key == "" {
                            break;
                        }
                        let value = cursor.read_cstr()?.to_owned();
                        props.push((key, value));
                    }
                    Ok(InnerState::Established(Done(props)))
                }
                Code(CANCEL_REQUEST_CODE) => {
                    let mut cursor = Cursor::from(bytes.as_slice());
                    let conn_id = cursor.read_i32()?;
                    let secret_key = cursor.read_i32()?;
                    Ok(InnerState::Cancel(Cancel(conn_id, secret_key)))
                }
                _ => Err(Error::VerificationFailed),
            },
            _ => unimplemented!("There is no way to change hand shake state at this point"),
        }
    }
}

#[cfg(test)]
mod connection_state_machine {
    use super::*;
    use crate::GSSENC_REQUEST_CODE;

    #[test]
    fn created_state() {
        let hand_shake = InnerState::new();

        assert_eq!(hand_shake, InnerState::Created(Created));
        assert_eq!(hand_shake.try_step(vec![]), Ok(InnerState::MessageLen(MessageLen(4))));
    }

    #[test]
    fn read_setup_message_length() {
        let mut hand_shake = InnerState::new();
        hand_shake = hand_shake.try_step(vec![]).expect("connection state transition");

        assert_eq!(
            hand_shake.try_step(vec![0, 0, 0, 4]),
            Ok(InnerState::ParseSetup(ReadSetupMessage(0)))
        );
    }

    #[test]
    fn non_recognizable_protocol_code() {
        let mut hand_shake = InnerState::new();
        hand_shake = hand_shake.try_step(vec![]).expect("connection state transition");

        hand_shake = hand_shake
            .try_step(vec![0, 0, 0, 25])
            .expect("connection state transition");

        assert_eq!(
            hand_shake.try_step(b"non_recognizable_code".to_vec()),
            Err(Error::UnsupportedRequest)
        );
    }

    #[test]
    fn version_one_is_not_supported() {
        let mut hand_shake = InnerState::new();
        hand_shake = hand_shake.try_step(vec![]).expect("connection state transition");

        hand_shake = hand_shake
            .try_step(vec![0, 0, 0, 8])
            .expect("connection state transition");

        assert_eq!(
            hand_shake.try_step(VERSION_1_CODE.to_be_bytes().to_vec()),
            Err(Error::UnsupportedVersion)
        );
    }

    #[test]
    fn version_two_is_not_supported() {
        let mut hand_shake = InnerState::new();
        hand_shake = hand_shake.try_step(vec![]).expect("connection state transition");

        hand_shake = hand_shake
            .try_step(vec![0, 0, 0, 8])
            .expect("connection state transition");

        assert_eq!(
            hand_shake.try_step(VERSION_2_CODE.to_be_bytes().to_vec()),
            Err(Error::UnsupportedVersion)
        );
    }

    #[test]
    fn setup_version_three_connection() {
        let mut hand_shake = InnerState::new();
        hand_shake = hand_shake.try_step(vec![]).expect("connection state transition");

        assert_eq!(
            hand_shake.try_step(vec![0, 0, 0, 8]),
            Ok(InnerState::ParseSetup(ReadSetupMessage(4)))
        );
    }

    #[test]
    fn setup_version_three_with_client_params() {
        let mut hand_shake = InnerState::new();
        hand_shake = hand_shake.try_step(vec![]).expect("connection state transition");

        hand_shake = hand_shake
            .try_step(vec![0, 0, 0, 33])
            .expect("connection state transition");

        let mut payload = vec![];
        payload.extend_from_slice(&VERSION_3_CODE.to_be_bytes());
        payload.extend_from_slice(b"key1\0");
        payload.extend_from_slice(b"value1\0");
        payload.extend_from_slice(b"key2\0");
        payload.extend_from_slice(b"value2\0");
        payload.extend_from_slice(&[0]);

        hand_shake = hand_shake.try_step(payload).expect("resolved intermediate step");

        assert_eq!(
            hand_shake.try_step(vec![]),
            Ok(InnerState::Established(Done(vec![
                ("key1".to_owned(), "value1".to_owned()),
                ("key2".to_owned(), "value2".to_owned())
            ])))
        );
    }

    #[test]
    fn connection_established_with_ssl_request() {
        let mut hand_shake = InnerState::new();
        hand_shake = hand_shake.try_step(vec![]).expect("connection state transition");

        hand_shake = hand_shake
            .try_step(vec![0, 0, 0, 8])
            .expect("connection state transition");

        hand_shake = hand_shake
            .try_step(SSL_REQUEST_CODE.to_be_bytes().to_vec())
            .expect("ssl request handled");
        assert_eq!(
            hand_shake,
            InnerState::Intermediate(Intermediate(Code(SSL_REQUEST_CODE), None))
        );

        hand_shake = hand_shake.try_step(vec![]).expect("next step");

        hand_shake = hand_shake
            .try_step(vec![0, 0, 0, 33])
            .expect("connection state transition");

        let mut payload = vec![];
        payload.extend_from_slice(&VERSION_3_CODE.to_be_bytes());
        payload.extend_from_slice(b"key1\0");
        payload.extend_from_slice(b"value1\0");
        payload.extend_from_slice(b"key2\0");
        payload.extend_from_slice(b"value2\0");
        payload.extend_from_slice(&[0]);

        hand_shake = hand_shake.try_step(payload).expect("resolved intermediate step");

        assert_eq!(
            hand_shake.try_step(vec![]),
            Ok(InnerState::Established(Done(vec![
                ("key1".to_owned(), "value1".to_owned()),
                ("key2".to_owned(), "value2".to_owned())
            ])))
        );
    }

    #[test]
    fn connection_established_with_gssenc_request() {
        let mut hand_shake = InnerState::new();
        hand_shake = hand_shake.try_step(vec![]).expect("connection state transition");

        hand_shake = hand_shake
            .try_step(vec![0, 0, 0, 8])
            .expect("connection state transition");

        assert_eq!(
            hand_shake.try_step(GSSENC_REQUEST_CODE.to_be_bytes().to_vec()),
            Err(Error::UnsupportedRequest)
        );
    }

    #[test]
    fn cancel_query_request() -> Result<()> {
        let conn_id: ConnId = 1;
        let secret_key: ConnSecretKey = 2;
        let mut hand_shake = InnerState::new();
        hand_shake = hand_shake.try_step(vec![])?;

        hand_shake = hand_shake.try_step(vec![0, 0, 0, 33])?;

        let mut payload = vec![];
        payload.extend_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
        payload.extend_from_slice(&conn_id.to_be_bytes());
        payload.extend_from_slice(&secret_key.to_be_bytes());

        hand_shake = hand_shake.try_step(payload).expect("resolved intermediate step");

        assert_eq!(
            hand_shake.try_step(vec![])?,
            InnerState::Cancel(Cancel(conn_id, secret_key))
        );

        Ok(())
    }
}
