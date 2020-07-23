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

use crate::{messages::Message, tests::async_io, Channel, Command, RequestReceiver, VERSION_3};
use async_mutex::Mutex as AsyncMutex;
use std::{io, sync::Arc};

#[cfg(test)]
mod read_query {
    use super::*;

    #[async_std::test]
    async fn read_termination_command() -> io::Result<()> {
        let test_case = super::async_io::TestCase::with_content(vec![&[88], &[0, 0, 0, 4]]).await;
        let channel = Arc::new(AsyncMutex::new(Channel::Plain(test_case)));
        let mut receiver = RequestReceiver::new((VERSION_3, vec![]), channel);

        let query = receiver.receive().await?;
        assert_eq!(query, Ok(Command::Terminate));

        Ok(())
    }

    #[async_std::test]
    async fn read_query_successfully() -> io::Result<()> {
        let test_case = super::async_io::TestCase::with_content(vec![&[81], &[0, 0, 0, 14], b"select 1;\0"]).await;
        let channel = Arc::new(AsyncMutex::new(Channel::Plain(test_case.clone())));
        let mut receiver = RequestReceiver::new((VERSION_3, vec![]), channel);

        let query = receiver.receive().await?;
        assert_eq!(query, Ok(Command::Query("select 1;".to_owned())));

        let actual_content = test_case.read_result().await;
        let mut expected_content = Vec::new();
        expected_content.extend_from_slice(Message::ReadyForQuery.as_vec().as_slice());
        assert_eq!(actual_content, expected_content);

        Ok(())
    }

    #[async_std::test]
    async fn unexpected_eof_when_read_type_code_of_query_request() {
        let test_case = super::async_io::TestCase::with_content(vec![]).await;
        let channel = Arc::new(AsyncMutex::new(Channel::Plain(test_case)));
        let mut connection = RequestReceiver::new((VERSION_3, vec![]), channel);

        let query = connection.receive().await;
        assert!(query.is_err());
    }

    #[async_std::test]
    async fn unexpected_eof_when_read_length_of_query() {
        let test_case = super::async_io::TestCase::with_content(vec![&[81]]).await;
        let channel = Arc::new(AsyncMutex::new(Channel::Plain(test_case)));
        let mut connection = RequestReceiver::new((VERSION_3, vec![]), channel);

        let query = connection.receive().await;
        assert!(query.is_err());
    }

    #[async_std::test]
    async fn unexpected_eof_when_query_string() {
        let test_case = super::async_io::TestCase::with_content(vec![&[81], &[0, 0, 0, 14], b"sel;\0"]).await;
        let channel = Arc::new(AsyncMutex::new(Channel::Plain(test_case)));
        let mut connection = RequestReceiver::new((VERSION_3, vec![]), channel);

        let query = connection.receive().await;
        assert!(query.is_err());
    }
}
