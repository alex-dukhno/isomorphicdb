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
    messages::Cursor, Code, ConnId, ConnSecretKey, Error, Result, CANCEL_REQUEST_CODE, GSSENC_REQUEST_CODE,
    SSL_REQUEST_CODE, VERSION_1_CODE, VERSION_2_CODE, VERSION_3_CODE,
};

trait ConnectionTransition<C> {
    fn transit(self, cursor: Cursor) -> Result<C>;
}

pub struct Factory;

impl Factory {
    pub fn new() -> HandShakeState {
        HandShakeState::Created(HandShake::new(Created))
    }
}

#[derive(Debug, PartialEq)]
pub struct HandShake<S> {
    pub state: S,
}

impl<S> HandShake<S> {
    fn new(state: S) -> HandShake<S> {
        HandShake { state }
    }
}

#[derive(Debug, PartialEq)]
pub struct Created;

impl ConnectionTransition<HandShake<MessageLen>> for HandShake<Created> {
    fn transit(self, _cursor: Cursor) -> Result<HandShake<MessageLen>> {
        Ok(HandShake::new(MessageLen(4)))
    }
}

#[derive(Debug, PartialEq)]
pub struct MessageLen(pub(crate) usize);

impl ConnectionTransition<HandShake<ReadSetupMessage>> for HandShake<MessageLen> {
    fn transit(self, mut cursor: Cursor) -> Result<HandShake<ReadSetupMessage>> {
        let len = cursor.read_i32()?;
        Ok(HandShake::new(ReadSetupMessage((len - 4) as usize)))
    }
}

#[derive(Debug, PartialEq)]
pub struct ReadSetupMessage(pub usize);

impl ConnectionTransition<HandShake<Intermediate>> for HandShake<ReadSetupMessage> {
    fn transit(self, mut cursor: Cursor) -> Result<HandShake<Intermediate>> {
        let code = Code(cursor.read_i32()?);
        log::info!("Connection Code: {}", code);
        match code {
            Code(VERSION_1_CODE) => Err(Error::UnsupportedVersion),
            Code(VERSION_2_CODE) => Err(Error::UnsupportedVersion),
            Code(VERSION_3_CODE) => Ok(HandShake::new(Intermediate(code, Some(cursor.into())))),
            Code(CANCEL_REQUEST_CODE) => Ok(HandShake::new(Intermediate(code, Some(cursor.into())))),
            Code(SSL_REQUEST_CODE) => Ok(HandShake::new(Intermediate(code, None))),
            Code(GSSENC_REQUEST_CODE) => Ok(HandShake::new(Intermediate(code, None))),
            _ => Err(Error::UnsupportedRequest),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Intermediate(pub Code, pub Option<Vec<u8>>);

#[derive(Debug, PartialEq)]
pub struct Done(pub Vec<(String, String)>);

#[derive(Debug, PartialEq)]
pub struct Cancel(pub ConnId, pub ConnSecretKey);

#[derive(Debug, PartialEq)]
pub enum HandShakeState {
    Created(HandShake<Created>),
    MessageLen(HandShake<MessageLen>),
    ParseSetup(HandShake<ReadSetupMessage>),
    Intermediate(HandShake<Intermediate>),
    Established(HandShake<Done>),
    Cancel(HandShake<Cancel>),
}

impl HandShakeState {
    pub fn try_step(self, buf: Vec<u8>) -> Result<HandShakeState> {
        let buffer = Cursor::from(buf.as_slice());
        match self {
            HandShakeState::Created(hand_shake) => Ok(HandShakeState::MessageLen(hand_shake.transit(buffer)?)),
            HandShakeState::MessageLen(hand_shake) => Ok(HandShakeState::ParseSetup(hand_shake.transit(buffer)?)),
            HandShakeState::ParseSetup(hand_shake) => Ok(HandShakeState::Intermediate(hand_shake.transit(buffer)?)),
            HandShakeState::Intermediate(HandShake {
                state: Intermediate(_, None),
            }) => Ok(HandShakeState::MessageLen(HandShake::new(MessageLen(4)))),
            HandShakeState::Intermediate(HandShake {
                state: Intermediate(version, Some(bytes)),
            }) => match version {
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
                    Ok(HandShakeState::Established(HandShake::new(Done(props))))
                }
                Code(CANCEL_REQUEST_CODE) => {
                    let mut cursor = Cursor::from(bytes.as_slice());
                    let conn_id = cursor.read_i32()?;
                    let secret_key = cursor.read_i32()?;
                    Ok(HandShakeState::Cancel(HandShake::new(Cancel(conn_id, secret_key))))
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

    #[test]
    fn created_state() {
        let hand_shake = Factory::new();

        assert_eq!(hand_shake, HandShakeState::Created(HandShake::new(Created)));
        assert_eq!(
            hand_shake.try_step(vec![]),
            Ok(HandShakeState::MessageLen(HandShake::new(MessageLen(4))))
        );
    }

    #[test]
    fn read_setup_message_length() {
        let mut hand_shake = Factory::new();
        hand_shake = hand_shake.try_step(vec![]).expect("connection state transition");

        assert_eq!(
            hand_shake.try_step(vec![0, 0, 0, 4]),
            Ok(HandShakeState::ParseSetup(HandShake::new(ReadSetupMessage(0))))
        );
    }

    #[test]
    fn non_recognizable_protocol_code() {
        let mut hand_shake = Factory::new();
        hand_shake = hand_shake.try_step(vec![]).expect("connection state transition");

        hand_shake = hand_shake
            .try_step(vec![0, 0, 0, 8])
            .expect("connection state transition");

        assert_eq!(
            hand_shake.try_step(b"non_recognizable_code".to_vec()),
            Err(Error::UnsupportedRequest)
        );
    }

    #[test]
    fn version_one_is_not_supported() {
        let mut hand_shake = Factory::new();
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
        let mut hand_shake = Factory::new();
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
        let mut hand_shake = Factory::new();
        hand_shake = hand_shake.try_step(vec![]).expect("connection state transition");

        assert_eq!(
            hand_shake.try_step(vec![0, 0, 0, 8]),
            Ok(HandShakeState::ParseSetup(HandShake::new(ReadSetupMessage(4))))
        );
    }

    #[test]
    fn setup_version_three_with_client_params() {
        let mut hand_shake = Factory::new();
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
            Ok(HandShakeState::Established(HandShake::new(Done(vec![
                ("key1".to_owned(), "value1".to_owned()),
                ("key2".to_owned(), "value2".to_owned())
            ]))))
        );
    }

    #[test]
    fn connection_established_with_ssl_request() {
        let mut hand_shake = Factory::new();
        hand_shake = hand_shake.try_step(vec![]).expect("connection state transition");

        hand_shake = hand_shake
            .try_step(vec![0, 0, 0, 8])
            .expect("connection state transition");

        hand_shake = hand_shake
            .try_step(SSL_REQUEST_CODE.to_be_bytes().to_vec())
            .expect("ssl request handled");
        assert_eq!(
            hand_shake,
            HandShakeState::Intermediate(HandShake::new(Intermediate(Code(SSL_REQUEST_CODE), None)))
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
            Ok(HandShakeState::Established(HandShake::new(Done(vec![
                ("key1".to_owned(), "value1".to_owned()),
                ("key2".to_owned(), "value2".to_owned())
            ]))))
        );
    }

    #[test]
    fn connection_established_with_gssenc_request() {
        let mut hand_shake = Factory::new();
        hand_shake = hand_shake.try_step(vec![]).expect("connection state transition");

        hand_shake = hand_shake
            .try_step(vec![0, 0, 0, 8])
            .expect("connection state transition");

        hand_shake = hand_shake
            .try_step(GSSENC_REQUEST_CODE.to_be_bytes().to_vec())
            .expect("gssenc request handled");

        assert_eq!(
            hand_shake,
            HandShakeState::Intermediate(HandShake::new(Intermediate(Code(GSSENC_REQUEST_CODE), None)))
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
            Ok(HandShakeState::Established(HandShake::new(Done(vec![
                ("key1".to_owned(), "value1".to_owned()),
                ("key2".to_owned(), "value2".to_owned())
            ]))))
        );
    }

    #[test]
    fn cancel_query_request() {
        let conn_id: ConnId = 1;
        let secret_key: ConnSecretKey = 2;
        let mut hand_shake = Factory::new();
        hand_shake = hand_shake.try_step(vec![]).expect("connection state transition");

        hand_shake = hand_shake
            .try_step(vec![0, 0, 0, 33])
            .expect("connection state transition");

        let mut payload = vec![];
        payload.extend_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
        payload.extend_from_slice(&conn_id.to_be_bytes());
        payload.extend_from_slice(&secret_key.to_be_bytes());

        hand_shake = hand_shake.try_step(payload).expect("resolved intermediate step");

        assert_eq!(
            hand_shake.try_step(vec![]),
            Ok(HandShakeState::Cancel(HandShake::new(Cancel(conn_id, secret_key))))
        );
    }
}
