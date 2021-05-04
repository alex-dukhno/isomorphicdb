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

use crate::{query_engine::QueryEngine, session::Session};
use data_manipulation::QueryExecutionResult;
use data_repr::scalar::ScalarValue;
use postgre_sql::{
    query_ast::{Extended, Statement, Transaction},
    wire_protocol::{
        payload::{Inbound, Outbound},
        WireConnection,
    },
};
use storage::Database;
use types::{SqlType, SqlTypeFamily};

#[allow(dead_code)]
pub struct Worker;

impl Worker {
    #[allow(dead_code)]
    fn process<C: WireConnection>(&self, connection: &mut C, db_name: &str) {
        let mut session = Session::default();

        let database = Database::new(db_name);
        let query_engine = QueryEngine::new(database);

        let mut explicit_txn = false;
        let mut end_txn = true;
        loop {
            let txn = query_engine.start_transaction();
            match connection.receive() {
                Ok(Ok(inbound_request)) => match inbound_request {
                    Inbound::Query { sql } => {
                        let statements = match txn.parse(&sql) {
                            Ok(parsed) => parsed,
                            _ => unimplemented!(),
                        };
                        for statement in statements {
                            match statement {
                                Statement::Definition(ddl) => match txn.execute_ddl(ddl) {
                                    Ok(success) => {
                                        connection.send(success.into()).unwrap();
                                    }
                                    Err(error) => {
                                        connection.send(error.into()).unwrap();
                                    }
                                },
                                Statement::Transaction(txn_flow) => match txn_flow {
                                    Transaction::Begin => {
                                        end_txn = false;
                                        explicit_txn = true;
                                        connection.send(Outbound::TransactionBegin).unwrap();
                                    }
                                    Transaction::Commit => {
                                        end_txn = true;
                                    }
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
                                        session.cache(name, query_plan, query, params);
                                        connection.send(Outbound::StatementPrepared).unwrap();
                                    }
                                    Extended::Execute { name, param_values } => match session.find(&name) {
                                        None => unimplemented!(),
                                        // TODO: workaround situate that QueryPlan is not clone ¯\_(ツ)_/¯
                                        Some((query, params)) => {
                                            let typed_query = txn.process(query.clone(), params.clone()).unwrap();
                                            let query_plan = txn.plan(typed_query);
                                            match query_plan
                                                .execute(param_values.into_iter().map(ScalarValue::from).collect())
                                            {
                                                Ok(QueryExecutionResult::Inserted(inserted)) => {
                                                    connection.send(Outbound::RecordsInserted(inserted)).unwrap();
                                                }
                                                Ok(_) => {}
                                                Err(_) => unimplemented!(),
                                            }
                                        }
                                    },
                                    _ => unimplemented!(),
                                },
                                Statement::Query(query) => {
                                    let typed_query = txn.process(query, vec![]).unwrap();
                                    let query_plan = txn.plan(typed_query);
                                    match query_plan.execute(vec![]) {
                                        Ok(QueryExecutionResult::Selected((desc, data))) => {
                                            connection.send(Outbound::RowDescription(desc)).unwrap();
                                            let selected = data.len();
                                            for datum in data {
                                                connection
                                                    .send(Outbound::DataRow(
                                                        datum.into_iter().map(|v| v.as_text()).collect(),
                                                    ))
                                                    .unwrap();
                                            }
                                            connection.send(Outbound::RecordsSelected(selected)).unwrap();
                                        }
                                        other => unimplemented!("branch {:?} is not implemented", other),
                                    }
                                }
                                stmt => unimplemented!("statement {:?} could not processed", stmt),
                            }
                        }
                        if end_txn {
                            txn.commit();
                            if explicit_txn {
                                connection.send(Outbound::TransactionCommit).unwrap();
                                // reset the state
                                explicit_txn = false;
                                end_txn = true;
                            }
                        }
                        connection.send(Outbound::ReadyForQuery).unwrap();
                    }
                    _ => unimplemented!(),
                },
                _ => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use postgre_sql::wire_protocol::{
        payload::{Outbound, SMALLINT},
        WireError, WireResult,
    };
    use std::io;

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
