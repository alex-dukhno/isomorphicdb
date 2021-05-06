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

use crate::query_executor::QueryExecutor;
use crate::{
    transaction_manager::{TransactionContext, TransactionManager},
    QueryPlanCache,
};
use postgre_sql::{
    query_ast::{Request, Transaction},
    query_parser::QueryParser,
    wire_protocol::{
        payload::{Inbound, Outbound},
        WireConnection,
    },
};
use storage::Database;

#[allow(dead_code)]
pub struct Worker;

impl Worker {
    #[allow(dead_code)]
    fn process<C: WireConnection>(&self, connection: &mut C, db_name: &str) {
        let mut query_plan_cache = QueryPlanCache::default();
        let query_parser = QueryParser;

        let database = Database::new(db_name);
        let query_engine = TransactionManager::new(database);

        let executor = QueryExecutor;
        let mut txn_state: Option<TransactionContext> = None;
        loop {
            let inbound_request = connection.receive();
            match inbound_request {
                Ok(Ok(inbound)) => match inbound {
                    Inbound::Query { sql } => match query_parser.parse(&sql) {
                        Ok(request) => match request {
                            Request::Transaction(transaction) => {
                                match transaction {
                                    Transaction::Begin => {
                                        debug_assert!(txn_state.is_none(), "transaction state should be implicit");
                                        txn_state = Some(query_engine.start_transaction());
                                        connection.send(Outbound::TransactionBegin).unwrap();
                                    }
                                    Transaction::Commit => {
                                        debug_assert!(txn_state.is_some(), "transaction state should be in progress");
                                        match txn_state {
                                            None => unimplemented!(),
                                            Some(txn) => {
                                                txn.commit();
                                                connection.send(Outbound::TransactionCommit).unwrap();
                                                txn_state = None;
                                            }
                                        }
                                    }
                                }
                                connection.send(Outbound::ReadyForQuery).unwrap();
                            }
                            Request::Config(_) => unimplemented!(),
                            Request::Statement(statement) => {
                                let (txn, finish_txn) = match txn_state.take() {
                                    None => (query_engine.start_transaction(), true),
                                    Some(txn) => (txn, false),
                                };
                                for outbound in executor.execute(statement, &txn, &mut query_plan_cache) {
                                    connection.send(outbound).unwrap();
                                }
                                if finish_txn {
                                    txn.commit();
                                } else {
                                    txn_state = Some(txn);
                                }
                            }
                        },
                        Err(_) => unimplemented!(),
                    },
                    other => unimplemented!("other inbound request {:?} is not handled", other),
                },
                _ => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use postgre_sql::wire_protocol::{
        payload::{Outbound, SMALLINT},
        WireError, WireResult,
    };

    use super::*;

    struct MockConnection {
        inbound: Vec<Inbound>,
        outbound: Vec<Outbound>,
    }

    impl MockConnection {
        fn new(inbound: Vec<Inbound>) -> MockConnection {
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

        fn send(&mut self, outbound: Outbound) -> io::Result<()> {
            self.outbound.push(outbound);
            Ok(())
        }
    }

    #[test]
    fn single_create_schema_request() {
        let mut connection = MockConnection::new(vec![Inbound::Query {
            sql: "create schema schema_name;".to_owned(),
        }]);

        let node_engine = Worker;

        node_engine.process(&mut connection, "IN_MEMORY");

        assert_eq!(
            connection.outbound,
            vec![Outbound::SchemaCreated, Outbound::ReadyForQuery]
        );
    }

    #[test]
    fn transaction_per_query() {
        let mut connection = MockConnection::new(vec![
            Inbound::Query {
                sql: "create schema schema_name;".to_owned(),
            },
            Inbound::Query {
                sql: "create table schema_name.table_name (col1 smallint);".to_owned(),
            },
        ]);

        let node_engine = Worker;

        node_engine.process(&mut connection, "IN_MEMORY");

        assert_eq!(
            connection.outbound,
            vec![
                Outbound::SchemaCreated,
                Outbound::ReadyForQuery,
                Outbound::TableCreated,
                Outbound::ReadyForQuery
            ]
        );
    }

    #[test]
    fn multiple_ddl_in_single_transaction() {
        let mut connection = MockConnection::new(vec![
            Inbound::Query {
                sql: "begin".to_owned(),
            },
            Inbound::Query {
                sql: "create schema schema_name;".to_owned(),
            },
            Inbound::Query {
                sql: "create table schema_name.table_name (col1 smallint);".to_owned(),
            },
            Inbound::Query {
                sql: "commit".to_owned(),
            },
        ]);

        let node_engine = Worker;

        node_engine.process(&mut connection, "IN_MEMORY");

        assert_eq!(
            connection.outbound,
            vec![
                Outbound::TransactionBegin,
                Outbound::ReadyForQuery,
                Outbound::SchemaCreated,
                Outbound::ReadyForQuery,
                Outbound::TableCreated,
                Outbound::ReadyForQuery,
                Outbound::TransactionCommit,
                Outbound::ReadyForQuery,
            ]
        );
    }

    #[test]
    fn prepare_and_execute_multiple_times_in_single_transaction() {
        let mut connection = MockConnection::new(vec![
            Inbound::Query {
                sql: "begin".to_owned(),
            },
            Inbound::Query {
                sql: "create schema schema_name;".to_owned(),
            },
            Inbound::Query {
                sql: "create table schema_name.table_name (col1 smallint);".to_owned(),
            },
            Inbound::Query {
                sql: "commit".to_owned(),
            },
            Inbound::Query {
                sql: "begin".to_owned(),
            },
            Inbound::Query {
                sql: "prepare plan (smallint) as insert into schema_name.table_name values ($1)".to_owned(),
            },
            Inbound::Query {
                sql: "execute plan (1)".to_owned(),
            },
            Inbound::Query {
                sql: "commit".to_owned(),
            },
            Inbound::Query {
                sql: "begin".to_owned(),
            },
            Inbound::Query {
                sql: "execute plan (1)".to_owned(),
            },
            Inbound::Query {
                sql: "select * from schema_name.table_name".to_owned(),
            },
            Inbound::Query {
                sql: "commit".to_owned(),
            },
        ]);

        let node_engine = Worker;

        node_engine.process(&mut connection, "IN_MEMORY");

        assert_eq!(
            connection.outbound,
            vec![
                Outbound::TransactionBegin,
                Outbound::ReadyForQuery,
                Outbound::SchemaCreated,
                Outbound::ReadyForQuery,
                Outbound::TableCreated,
                Outbound::ReadyForQuery,
                Outbound::TransactionCommit,
                Outbound::ReadyForQuery,
                Outbound::TransactionBegin,
                Outbound::ReadyForQuery,
                Outbound::StatementPrepared,
                Outbound::ReadyForQuery,
                Outbound::RecordsInserted(1),
                Outbound::ReadyForQuery,
                Outbound::TransactionCommit,
                Outbound::ReadyForQuery,
                Outbound::TransactionBegin,
                Outbound::ReadyForQuery,
                Outbound::RecordsInserted(1),
                Outbound::ReadyForQuery,
                Outbound::RowDescription(vec![("col1".to_owned(), SMALLINT)]),
                Outbound::DataRow(vec!["1".to_owned()]),
                Outbound::DataRow(vec!["1".to_owned()]),
                Outbound::RecordsSelected(2),
                Outbound::ReadyForQuery,
                Outbound::TransactionCommit,
                Outbound::ReadyForQuery,
            ]
        );
    }
}
