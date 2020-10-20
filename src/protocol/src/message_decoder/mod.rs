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
use crate::{
    message_decoder::state::{Payload, Tag},
    messages::{Cursor, FrontendMessage},
    ProtocolResult,
};
use state::State;
use std::mem::MaybeUninit;

/// Represents a status of a `MessageDecoder` stage
#[derive(Debug, PartialEq)]
pub enum Status {
    /// `MessageDecoder` requests buffer with specified size
    Requesting(usize),
    /// `MessageDecoder` is in a process of decoding and decoded front message will be available
    /// after the next stage
    Decoding,
    /// `MessageDecoder` has decoded a message and returns it content
    Done(FrontendMessage),
}

/// Decodes messages from client
pub struct MessageDecoder {
    state: State,
    tag: u8,
}

impl Default for MessageDecoder {
    fn default() -> MessageDecoder {
        MessageDecoder::new()
    }
}

impl MessageDecoder {
    /// Creates new `MessageDecoder`
    pub fn new() -> MessageDecoder {
        MessageDecoder {
            state: State::new(),
            tag: 0,
        }
    }

    /// Proceed to the next stage of decoding received message
    pub fn next_stage(&mut self, payload: Option<&[u8]>) -> ProtocolResult<Status> {
        let payload = if let Some(payload) = payload { payload } else { &[] };
        let mut state = unsafe { MaybeUninit::zeroed().assume_init() };
        std::mem::swap(&mut state, &mut self.state);
        let (new_state, prev) = state.try_step(payload)?;
        self.state = new_state;
        match prev {
            State::Created(_) => Ok(Status::Requesting(1)),
            State::RequestingTag(_) => Ok(Status::Requesting(4)),
            State::Tag(Tag(tag)) => {
                self.tag = tag;
                Ok(Status::Requesting((Cursor::from(payload).read_i32()? - 4) as usize))
            }
            State::WaitingForPayload(_) => Ok(Status::Decoding),
            State::Payload(Payload(data)) => {
                let message = FrontendMessage::decode(self.tag, &data)?;
                Ok(Status::Done(message))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::QUERY;

    const QUERY_STRING: &str = "select * from t\0";
    const QUERY_BYTES: &[u8] = QUERY_STRING.as_bytes();
    const LEN: i32 = QUERY_STRING.len() as i32;

    #[test]
    fn request_message_tag() {
        let mut decoder = MessageDecoder::new();

        assert_eq!(decoder.next_stage(None), Ok(Status::Requesting(1)));
    }

    #[test]
    fn request_message_len() {
        let mut decoder = MessageDecoder::new();

        decoder.next_stage(None).expect("proceed to the next stage");
        assert_eq!(decoder.next_stage(Some(&[QUERY])), Ok(Status::Requesting(4)));
    }

    #[test]
    fn request_message_payload() {
        let mut decoder = MessageDecoder::new();

        decoder.next_stage(None).expect("proceed to the next stage");
        decoder.next_stage(Some(&[QUERY])).expect("proceed to the next stage");
        assert_eq!(
            decoder.next_stage(Some(&LEN.to_be_bytes())),
            Ok(Status::Requesting((LEN - 4) as usize))
        );
    }

    #[test]
    fn decoding_message() {
        let mut decoder = MessageDecoder::new();

        decoder.next_stage(None).expect("proceed to the next stage");
        decoder.next_stage(Some(&[QUERY])).expect("proceed to the next stage");
        decoder
            .next_stage(Some(&LEN.to_be_bytes()))
            .expect("proceed to the next stage");

        assert_eq!(decoder.next_stage(Some(QUERY_BYTES)), Ok(Status::Decoding));
    }

    #[test]
    fn request_next_message() {
        let mut decoder = MessageDecoder::new();

        decoder.next_stage(None).expect("proceed to the next stage");
        decoder.next_stage(Some(&[QUERY])).expect("proceed to the next stage");
        decoder
            .next_stage(Some(&LEN.to_be_bytes()))
            .expect("proceed to the next stage");

        decoder
            .next_stage(Some(QUERY_BYTES))
            .expect("proceed to the next stage");

        assert_eq!(
            decoder.next_stage(None),
            Ok(Status::Done(FrontendMessage::Query {
                sql: "select * from t".to_owned()
            }))
        );
    }

    #[test]
    fn full_cycle() {
        let mut decoder = MessageDecoder::new();

        decoder.next_stage(None).expect("proceed to the next stage");
        decoder.next_stage(Some(&[QUERY])).expect("proceed to the next stage");
        decoder
            .next_stage(Some(&LEN.to_be_bytes()))
            .expect("proceed to the next stage");

        decoder
            .next_stage(Some(QUERY_BYTES))
            .expect("proceed to the next stage");

        decoder.next_stage(None).expect("proceed to the next stage");

        assert_eq!(decoder.next_stage(None), Ok(Status::Requesting(1)));
    }
}
