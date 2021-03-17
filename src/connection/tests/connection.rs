// Copyright 2020 - 2021 Alex Dukhno
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

use std::sync::Arc;

use async_mutex::Mutex as AsyncMutex;
use futures_lite::future::block_on;

use crate::connection::ConnSupervisor;
use crate::{
    connection::{
        network::{Stream, TestCase},
        Channel, RequestReceiver,
    },
    session::Command,
};

#[cfg(test)]
mod read_query {
    use super::*;

    #[test]
    fn read_termination_command() {
        block_on(async {
            let stream = Stream::from(TestCase::new(vec![&[88], &[0, 0, 0, 4]]));
            let channel = Arc::new(AsyncMutex::new(Channel::Plain(stream)));
            let conn_supervisor = ConnSupervisor::new(1, 2);
            let (conn_id, _) = conn_supervisor.alloc().unwrap();
            let mut receiver = RequestReceiver::new(conn_id, vec![], channel, conn_supervisor);

            let query = receiver.receive().await.expect("no io errors");
            assert_eq!(query, Ok(Command::Terminate));
        });
    }

    #[test]
    fn read_query_successfully() {
        block_on(async {
            let stream = Stream::from(TestCase::new(vec![&[81], &[0, 0, 0, 14], b"select 1;\0"]));
            let channel = Arc::new(AsyncMutex::new(Channel::Plain(stream)));
            let conn_supervisor = ConnSupervisor::new(1, 2);
            let (conn_id, _) = conn_supervisor.alloc().unwrap();
            let mut receiver = RequestReceiver::new(conn_id, vec![], channel, conn_supervisor);

            let query = receiver.receive().await.expect("no io errors");
            assert_eq!(
                query,
                Ok(Command::Query {
                    sql: "select 1;".to_owned()
                })
            );
        });
    }

    #[test]
    fn client_disconnected_immediately() {
        block_on(async {
            let stream = Stream::from(TestCase::new(vec![]));
            let channel = Arc::new(AsyncMutex::new(Channel::Plain(stream)));
            let conn_supervisor = ConnSupervisor::new(1, 2);
            let (conn_id, _) = conn_supervisor.alloc().unwrap();
            let mut receiver = RequestReceiver::new(conn_id, vec![], channel, conn_supervisor);

            let query = receiver.receive().await.expect("no io errors");
            assert_eq!(query, Ok(Command::Terminate));
        });
    }
}
