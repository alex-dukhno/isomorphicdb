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

use crate::messages::Cursor;
use crate::Result;

trait Transform<C> {
    fn transform(self, buf: &mut Cursor) -> Result<C>;
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct Created;

impl Transform<RT> for Created {
    fn transform(self, _buf: &mut Cursor) -> Result<RT> {
        Ok(RT)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct RT;

impl Transform<Tag> for RT {
    fn transform(self, buf: &mut Cursor) -> Result<Tag> {
        Ok(Tag(buf.read_byte()?))
    }
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct Tag(pub(crate) u8);

impl Transform<W> for Tag {
    fn transform(self, _buf: &mut Cursor) -> Result<W> {
        Ok(W)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct W;

impl Transform<Payload> for W {
    fn transform(self, buf: &mut Cursor) -> Result<Payload> {
        Ok(Payload(Vec::from(&*buf)))
    }
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct Payload(pub(crate) Vec<u8>);

impl Transform<Created> for Payload {
    fn transform(self, _buf: &mut Cursor) -> Result<Created> {
        Ok(Created)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum State {
    Created(Created),
    RequestingTag(RT),
    Tag(Tag),
    WaitingForPayload(W),
    Payload(Payload),
}

impl State {
    pub(crate) fn new() -> State {
        State::Created(Created)
    }

    pub(crate) fn try_step(self, buf: &[u8]) -> Result<State> {
        let mut cursor = Cursor::from(buf);
        match self {
            State::Created(created) => Ok(State::RequestingTag(created.transform(&mut cursor)?)),
            State::RequestingTag(rt) => Ok(State::Tag(rt.transform(&mut cursor)?)),
            State::Tag(tag) => Ok(State::WaitingForPayload(tag.transform(&mut cursor)?)),
            State::WaitingForPayload(w) => Ok(State::Payload(w.transform(&mut cursor)?)),
            State::Payload(decoded) => Ok(State::Created(decoded.transform(&mut cursor)?)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::QUERY;

    const QUERY_STRING: &[u8] = "select * from t".as_bytes();

    #[test]
    fn created() {
        assert_eq!(State::new(), State::Created(Created));
    }

    #[test]
    fn requesting_tag() {
        let state = State::new();

        assert_eq!(state.try_step(&[]), Ok(State::RequestingTag(RT)))
    }

    #[test]
    fn parse_tag() {
        let mut state = State::new();

        state = state.try_step(&[]).expect("proceed to the next step");

        assert_eq!(state.try_step(&[QUERY]), Ok(State::Tag(Tag(QUERY))))
    }

    #[test]
    fn decoding_body() {
        let mut state = State::new();

        state = state.try_step(&[]).expect("proceed to the next step");
        state = state.try_step(&[QUERY]).expect("proceed to the next step");

        assert_eq!(state.try_step(&[]), Ok(State::WaitingForPayload(W)));
    }

    #[test]
    fn read_body() {
        let mut state = State::new();

        state = state.try_step(&[]).expect("proceed to the next step");
        state = state.try_step(&[QUERY]).expect("proceed to the next step");
        state = state.try_step(&[]).expect("proceed to the next step");

        assert_eq!(
            state.try_step(QUERY_STRING),
            Ok(State::Payload(Payload(QUERY_STRING.to_vec())))
        );
    }

    #[test]
    fn full_cycle() {
        let mut state = State::new();

        state = state.try_step(&[]).expect("proceed to the next step");
        state = state.try_step(&[QUERY]).expect("proceed to the next step");
        state = state.try_step(&[]).expect("proceed to the next step");
        state = state.try_step(QUERY_STRING).expect("proceed to the next step");

        assert_eq!(state.try_step(&[]), Ok(State::Created(Created)));
    }
}
