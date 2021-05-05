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

use crate::{transaction_manager::TransactionManager, txn_context::TransactionContext, QueryPlanCache};
use data_manipulation::QueryExecutionResult;
use data_repr::scalar::ScalarValue;
use postgre_sql::{
    query_ast::{Extended, Request, Statement, Transaction},
    query_parser::QueryParser,
    wire_protocol::{
        payload::{Inbound, Outbound},
        WireConnection,
    },
};
use storage::Database;
use types::{SqlType, SqlTypeFamily};

struct QueryExecutor;

impl QueryExecutor {
    fn execute(
        &self,
        statement: Statement,
        txn: &TransactionContext,
        query_plan_cache: &mut QueryPlanCache,
    ) -> Vec<Outbound> {
        let mut responses = vec![];
        match statement {
            Statement::Definition(definition) => match txn.apply_schema_change(definition) {
                Ok(success) => responses.push(success.into()),
                Err(failure) => responses.push(failure.into()),
            },
            Statement::Extended(extended) => match extended {
                Extended::Prepare {
                    query,
                    name,
                    param_types,
                } => {
                    let params: Vec<SqlTypeFamily> =
                        param_types.into_iter().map(|dt| SqlType::from(dt).family()).collect();
                    let typed_query = txn.process(query.clone(), params.clone()).unwrap();
                    let query_plan = txn.plan(typed_query);
                    query_plan_cache.allocate(name, query_plan, query, params);
                    responses.push(Outbound::StatementPrepared);
                }
                Extended::Execute { name, param_values } => {
                    match query_plan_cache.lookup(&name) {
                        None => unimplemented!(),
                        // TODO: workaround situate that QueryPlan is not clone ¯\_(ツ)_/¯
                        Some((query, params)) => {
                            let typed_query = txn.process(query.clone(), params.clone()).unwrap();
                            let query_plan = txn.plan(typed_query);
                            match query_plan.execute(param_values.into_iter().map(ScalarValue::from).collect()) {
                                Ok(QueryExecutionResult::Inserted(inserted)) => {
                                    responses.push(Outbound::RecordsInserted(inserted));
                                }
                                Ok(_) => {}
                                Err(_) => unimplemented!(),
                            }
                        }
                    }
                }
                Extended::Deallocate { name } => match query_plan_cache.deallocate(&name) {
                    None => {}
                    Some(_) => {}
                },
            },
            Statement::Query(query) => {
                let typed_query = txn.process(query, vec![]).unwrap();
                let query_plan = txn.plan(typed_query);
                match query_plan.execute(vec![]) {
                    Ok(QueryExecutionResult::Selected((desc, data))) => {
                        responses.push(Outbound::RowDescription(desc));
                        let selected = data.len();
                        for datum in data {
                            responses.push(Outbound::DataRow(datum.into_iter().map(|v| v.as_text()).collect()));
                        }
                        responses.push(Outbound::RecordsSelected(selected));
                    }
                    other => {
                        unimplemented!("branch {:?} is not implemented", other)
                    }
                }
            }
        }
        responses.push(Outbound::ReadyForQuery);
        responses
    }
}

#[allow(dead_code)]
pub struct Worker;

impl Worker {
    #[allow(dead_code)]
    fn process<C: WireConnection>(&self, connection: &mut C, db_name: &str) {
        let mut query_plan_cache = QueryPlanCache::default();
        let query_parser = QueryParser;

        let database = Database::new(db_name);
        let query_engine = TransactionManager::new(database);

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
                                let test = QueryExecutor;
                                for outbound in test.execute(statement, &txn, &mut query_plan_cache) {
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
