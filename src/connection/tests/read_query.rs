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

use crate::connection::{
    network::{Stream, TestCase},
    Channel, ConnSupervisor, Connection,
};
use async_mutex::Mutex as AsyncMutex;
use futures_lite::future::block_on;
use postgres::wire_protocol::CommandMessage;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

#[test]
fn read_termination_command() {
    block_on(async {
        let stream = Stream::from(TestCase::new(vec![&[88], &[0, 0, 0, 4]]));
        let channel = Arc::new(AsyncMutex::new(Channel::Plain(stream)));
        let conn_supervisor = ConnSupervisor::new(1, 2);
        let (conn_id, _) = conn_supervisor.alloc().unwrap();
        let mut connection = Connection::new(
            conn_id,
            vec![],
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1000),
            channel,
            conn_supervisor,
        );

        let query = connection.receive().await.expect("no io errors");
        assert_eq!(query, Ok(CommandMessage::Terminate));
    });
}

#[test]
fn read_query_successfully() {
    block_on(async {
        let stream = Stream::from(TestCase::new(vec![&[81], &[0, 0, 0, 14], b"select 1;\0"]));
        let channel = Arc::new(AsyncMutex::new(Channel::Plain(stream)));
        let conn_supervisor = ConnSupervisor::new(1, 2);
        let (conn_id, _) = conn_supervisor.alloc().unwrap();
        let mut connection = Connection::new(
            conn_id,
            vec![],
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1000),
            channel,
            conn_supervisor,
        );

        let query = connection.receive().await.expect("no io errors");
        assert_eq!(
            query,
            Ok(CommandMessage::Query {
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
        let mut connection = Connection::new(
            conn_id,
            vec![],
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1000),
            channel,
            conn_supervisor,
        );

        let query = connection.receive().await.expect("no io errors");
        assert_eq!(query, Ok(CommandMessage::Terminate));
    });
}
