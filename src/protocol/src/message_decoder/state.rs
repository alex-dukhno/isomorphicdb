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

use crate::{messages::Cursor, ProtocolResult};

trait Transform<C> {
    fn transform(self, buf: &mut Cursor) -> ProtocolResult<C>;
}

#[derive(Debug, PartialEq)]
pub(crate) struct Created;

impl<'c> Transform<RequestingTag> for &'c Created {
    fn transform(self, _buf: &mut Cursor) -> ProtocolResult<RequestingTag> {
        Ok(RequestingTag)
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct RequestingTag;

impl<'rt> Transform<Tag> for &'rt RequestingTag {
    fn transform(self, buf: &mut Cursor) -> ProtocolResult<Tag> {
        Ok(Tag(buf.read_byte()?))
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct Tag(pub(crate) u8);

impl<'t> Transform<WaitingForPayload> for &'t Tag {
    fn transform(self, _buf: &mut Cursor) -> ProtocolResult<WaitingForPayload> {
        Ok(WaitingForPayload)
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct WaitingForPayload;

impl<'w> Transform<Payload> for &'w WaitingForPayload {
    fn transform(self, buf: &mut Cursor) -> ProtocolResult<Payload> {
        Ok(Payload(Vec::from(&*buf)))
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct Payload(pub(crate) Vec<u8>);

impl<'p> Transform<Created> for &'p Payload {
    fn transform(self, _buf: &mut Cursor) -> ProtocolResult<Created> {
        Ok(Created)
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum State {
    Created(Created),
    RequestingTag(RequestingTag),
    Tag(Tag),
    WaitingForPayload(WaitingForPayload),
    Payload(Payload),
}

impl State {
    pub(crate) fn new() -> State {
        State::Created(Created)
    }

    pub(crate) fn try_step(self, buf: &[u8]) -> ProtocolResult<(State, State)> {
        let mut cursor = Cursor::from(buf);
        match &self {
            State::Created(created) => Ok((State::RequestingTag(created.transform(&mut cursor)?), self)),
            State::RequestingTag(rt) => Ok((State::Tag(rt.transform(&mut cursor)?), self)),
            State::Tag(tag) => Ok((State::WaitingForPayload(tag.transform(&mut cursor)?), self)),
            State::WaitingForPayload(w) => Ok((State::Payload(w.transform(&mut cursor)?), self)),
            State::Payload(decoded) => Ok((State::Created(decoded.transform(&mut cursor)?), self)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::QUERY;

    const QUERY_STRING: &[u8] = b"select * from t";

    #[test]
    fn created() {
        assert_eq!(State::new(), State::Created(Created));
    }

    #[test]
    fn requesting_tag() {
        let state = State::new();

        assert_eq!(
            state.try_step(&[]),
            Ok((State::RequestingTag(RequestingTag), State::new()))
        );
    }

    #[test]
    fn parse_tag() {
        let state = State::new();

        let (state, _prev) = state.try_step(&[]).expect("proceed to the next step");

        assert_eq!(
            state.try_step(&[QUERY]),
            Ok((State::Tag(Tag(QUERY)), State::RequestingTag(RequestingTag)))
        );
    }

    #[test]
    fn decoding_body() {
        let state = State::new();

        let (state, _prev) = state.try_step(&[]).expect("proceed to the next step");
        let (state, _prev) = state.try_step(&[QUERY]).expect("proceed to the next step");

        assert_eq!(
            state.try_step(&[]),
            Ok((State::WaitingForPayload(WaitingForPayload), State::Tag(Tag(QUERY))))
        );
    }

    #[test]
    fn read_body() {
        let state = State::new();

        let (state, _prev) = state.try_step(&[]).expect("proceed to the next step");
        let (state, _prev) = state.try_step(&[QUERY]).expect("proceed to the next step");
        let (state, _prev) = state.try_step(&[]).expect("proceed to the next step");

        assert_eq!(
            state.try_step(QUERY_STRING),
            Ok((
                State::Payload(Payload(QUERY_STRING.to_vec())),
                State::WaitingForPayload(WaitingForPayload)
            ))
        );
    }

    #[test]
    fn full_cycle() {
        let state = State::new();

        let (state, _prev) = state.try_step(&[]).expect("proceed to the next step");
        let (state, _prev) = state.try_step(&[QUERY]).expect("proceed to the next step");
        let (state, _prev) = state.try_step(&[]).expect("proceed to the next step");
        let (state, _prev) = state.try_step(QUERY_STRING).expect("proceed to the next step");

        assert_eq!(
            state.try_step(&[]),
            Ok((State::Created(Created), State::Payload(Payload(QUERY_STRING.to_vec()))))
        );
    }
}
