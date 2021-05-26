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

#[cfg(test)]
mod jdbc_flow;
#[cfg(test)]
mod parse_bind_execute;
#[cfg(test)]
mod statement_description;

use super::*;
use postgre_sql::wire_protocol::{
    payload::{OutboundMessage, SMALLINT},
    WireError, WireResult,
};
use std::io;

pub struct MockConnection {
    inbound: Vec<InboundMessage>,
    outbound: Vec<OutboundMessage>,
}

impl MockConnection {
    fn new(inbound: Vec<InboundMessage>) -> MockConnection {
        MockConnection {
            inbound: inbound.into_iter().rev().collect(),
            outbound: vec![],
        }
    }
}

impl WireConnection for MockConnection {
    fn receive(&mut self) -> io::Result<WireResult> {
        match self.inbound.pop() {
            None => Ok(Err(WireError)),
            Some(inbound) => Ok(Ok(inbound)),
        }
    }

    fn send(&mut self, outbound: OutboundMessage) -> io::Result<()> {
        self.outbound.push(outbound);
        Ok(())
    }
}

#[test]
fn single_create_schema_request() {
    let mut connection = MockConnection::new(vec![InboundMessage::Query {
        sql: "create schema schema_name;".to_owned(),
    }]);

    let worker = Worker;

    worker.process(&mut connection, Database::new("IN_MEMORY"));

    assert_eq!(connection.outbound, vec![OutboundMessage::SchemaCreated, OutboundMessage::ReadyForQuery]);
}

#[test]
fn transaction_per_query() {
    let mut connection = MockConnection::new(vec![
        InboundMessage::Query {
            sql: "create schema schema_name;".to_owned(),
        },
        InboundMessage::Query {
            sql: "create table schema_name.table_name (col1 smallint);".to_owned(),
        },
    ]);

    let worker = Worker;

    worker.process(&mut connection, Database::new("IN_MEMORY"));

    assert_eq!(
        connection.outbound,
        vec![
            OutboundMessage::SchemaCreated,
            OutboundMessage::ReadyForQuery,
            OutboundMessage::TableCreated,
            OutboundMessage::ReadyForQuery
        ]
    );
}

#[test]
fn multiple_ddl_in_single_transaction() {
    let mut connection = MockConnection::new(vec![
        InboundMessage::Query { sql: "begin".to_owned() },
        InboundMessage::Query {
            sql: "create schema schema_name;".to_owned(),
        },
        InboundMessage::Query {
            sql: "create table schema_name.table_name (col1 smallint);".to_owned(),
        },
        InboundMessage::Query { sql: "commit".to_owned() },
    ]);

    let worker = Worker;

    worker.process(&mut connection, Database::new("IN_MEMORY"));

    assert_eq!(
        connection.outbound,
        vec![
            OutboundMessage::TransactionBegin,
            OutboundMessage::ReadyForQuery,
            OutboundMessage::SchemaCreated,
            OutboundMessage::ReadyForQuery,
            OutboundMessage::TableCreated,
            OutboundMessage::ReadyForQuery,
            OutboundMessage::TransactionCommit,
            OutboundMessage::ReadyForQuery,
        ]
    );
}

#[test]
fn prepare_and_execute_multiple_times_in_single_transaction() {
    let mut connection = MockConnection::new(vec![
        InboundMessage::Query { sql: "begin".to_owned() },
        InboundMessage::Query {
            sql: "create schema schema_name;".to_owned(),
        },
        InboundMessage::Query {
            sql: "create table schema_name.table_name (col1 smallint);".to_owned(),
        },
        InboundMessage::Query { sql: "commit".to_owned() },
        InboundMessage::Query { sql: "begin".to_owned() },
        InboundMessage::Query {
            sql: "prepare plan (smallint) as insert into schema_name.table_name values ($1)".to_owned(),
        },
        InboundMessage::Query {
            sql: "execute plan (1)".to_owned(),
        },
        InboundMessage::Query { sql: "commit".to_owned() },
        InboundMessage::Query { sql: "begin".to_owned() },
        InboundMessage::Query {
            sql: "execute plan (1)".to_owned(),
        },
        InboundMessage::Query {
            sql: "select * from schema_name.table_name".to_owned(),
        },
        InboundMessage::Query { sql: "commit".to_owned() },
    ]);

    let node_engine = Worker;

    node_engine.process(&mut connection, Database::new("IN_MEMORY"));

    assert_eq!(
        connection.outbound,
        vec![
            OutboundMessage::TransactionBegin,
            OutboundMessage::ReadyForQuery,
            OutboundMessage::SchemaCreated,
            OutboundMessage::ReadyForQuery,
            OutboundMessage::TableCreated,
            OutboundMessage::ReadyForQuery,
            OutboundMessage::TransactionCommit,
            OutboundMessage::ReadyForQuery,
            OutboundMessage::TransactionBegin,
            OutboundMessage::ReadyForQuery,
            OutboundMessage::StatementPrepared,
            OutboundMessage::ReadyForQuery,
            OutboundMessage::RecordsInserted(1),
            OutboundMessage::ReadyForQuery,
            OutboundMessage::TransactionCommit,
            OutboundMessage::ReadyForQuery,
            OutboundMessage::TransactionBegin,
            OutboundMessage::ReadyForQuery,
            OutboundMessage::RecordsInserted(1),
            OutboundMessage::ReadyForQuery,
            OutboundMessage::RowDescription(vec![("col1".to_owned(), SMALLINT)]),
            OutboundMessage::DataRow(vec!["1".to_owned()]),
            OutboundMessage::DataRow(vec!["1".to_owned()]),
            OutboundMessage::RecordsSelected(2),
            OutboundMessage::ReadyForQuery,
            OutboundMessage::TransactionCommit,
            OutboundMessage::ReadyForQuery,
        ]
    );
}
