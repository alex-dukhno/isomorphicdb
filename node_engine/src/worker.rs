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

use crate::{
    query_executor::QueryExecutor,
    transaction_manager::{TransactionContext, TransactionManager},
    QueryPlanCache,
};
use data_repr::scalar::ScalarValue;
use postgre_sql::{
    query_ast::{Request, Statement, Transaction},
    query_parser::QueryParser,
    query_response::{QueryError, QueryEvent},
    wire_protocol::{
        payload::{InboundMessage, OutboundMessage, Value, BIGINT, BOOL, CHAR, INT, SMALLINT, VARCHAR},
        WireConnection,
    },
};
use storage::Database;
use types_old::SqlTypeFamilyOld;

pub struct Worker;

impl Worker {
    pub fn process<C: WireConnection>(&self, connection: &mut C, database: Database) {
        let mut query_plan_cache = QueryPlanCache::default();
        let query_parser = QueryParser;

        let transaction_manager = TransactionManager::new(database);

        let executor = QueryExecutor;
        let mut txn_state: Option<TransactionContext> = None;
        loop {
            let inbound_request = connection.receive();
            match inbound_request {
                Ok(Ok(inbound)) => match inbound {
                    InboundMessage::Query { sql } => match query_parser.parse(&sql) {
                        Ok(request) => match request {
                            Request::Transaction(transaction) => {
                                match transaction {
                                    Transaction::Begin => {
                                        debug_assert!(txn_state.is_none(), "transaction state should be implicit");
                                        txn_state = Some(transaction_manager.start_transaction());
                                        connection.send(OutboundMessage::TransactionBegin).unwrap();
                                    }
                                    Transaction::Commit => {
                                        debug_assert!(txn_state.is_some(), "transaction state should be in progress");
                                        match txn_state {
                                            None => unimplemented!(),
                                            Some(txn) => {
                                                txn.commit();
                                                connection.send(OutboundMessage::TransactionCommit).unwrap();
                                                txn_state = None;
                                            }
                                        }
                                    }
                                }
                                connection.send(OutboundMessage::ReadyForQuery).unwrap();
                            }
                            Request::Config(_) => {
                                connection.send(OutboundMessage::VariableSet).unwrap();
                                connection.send(OutboundMessage::ReadyForQuery).unwrap();
                            }
                            Request::Statement(statement) => {
                                let (txn, finish_txn) = match txn_state.take() {
                                    None => (transaction_manager.start_transaction(), true),
                                    Some(txn) => (txn, false),
                                };
                                for outbound in executor.execute_statement(statement, &txn, &mut query_plan_cache) {
                                    connection.send(outbound).unwrap();
                                }
                                if finish_txn {
                                    txn.commit();
                                } else {
                                    txn_state = Some(txn);
                                }
                            }
                        },
                        Err(parse_error) => {
                            let query_error: QueryError = parse_error.into();
                            connection.send(query_error.into()).unwrap();
                            connection.send(OutboundMessage::ReadyForQuery).unwrap();
                        }
                    },
                    InboundMessage::Parse {
                        statement_name,
                        sql,
                        param_types,
                    } => {
                        let (txn, finish_txn) = match txn_state.take() {
                            None => (transaction_manager.start_transaction(), true),
                            Some(txn) => (txn, false),
                        };
                        match query_plan_cache.find_described(&statement_name) {
                            Some((_, saved_sql, _)) if saved_sql == sql => {
                                connection.send(QueryEvent::ParseComplete.into()).unwrap();
                            }
                            _ => match query_parser.parse(&sql) {
                                Ok(request) => {
                                    connection.send(QueryEvent::ParseComplete.into()).unwrap();
                                    match request {
                                        Request::Statement(statement) => match statement {
                                            Statement::Query(query) => {
                                                query_plan_cache.save_parsed(statement_name, sql, query, param_types);
                                            }
                                            other => {
                                                connection.send(QueryError::syntax_error(format!("{:?}", other)).into()).unwrap();
                                            }
                                        },
                                        Request::Transaction(transaction) => match transaction {
                                            Transaction::Begin => {
                                                debug_assert!(txn_state.is_none(), "transaction state should be implicit");
                                                unimplemented!()
                                            }
                                            Transaction::Commit => {
                                                debug_assert!(txn_state.is_some(), "transaction state should be in progress");
                                                match txn_state {
                                                    None => unimplemented!(),
                                                    Some(txn) => {
                                                        txn.commit();
                                                        connection.send(OutboundMessage::TransactionCommit).unwrap();
                                                        txn_state = None;
                                                    }
                                                }
                                            }
                                        },
                                        other => unimplemented!("{:?} unimplemented", other),
                                    }
                                }
                                Err(parser_error) => {
                                    connection.send(QueryError::syntax_error(parser_error).into()).unwrap();
                                }
                            },
                        }
                        if finish_txn {
                            txn.commit();
                        } else {
                            txn_state = Some(txn);
                        }
                    }
                    InboundMessage::DescribeStatement { name } => {
                        let (txn, finish_txn) = match txn_state.take() {
                            None => (transaction_manager.start_transaction(), true),
                            Some(txn) => (txn, false),
                        };
                        match query_plan_cache.find_parsed(&name) {
                            None => connection.send(QueryError::prepared_statement_does_not_exist(name).into()).unwrap(),
                            Some((query, sql, _)) => {
                                let (untyped_query, param_types, responses) = executor.describe_statement(query.clone(), &txn);
                                for response in responses {
                                    connection.send(response).unwrap();
                                }
                                query_plan_cache.save_described(name, untyped_query, sql, param_types);
                            }
                        }
                        if finish_txn {
                            txn.commit();
                        } else {
                            txn_state = Some(txn);
                        }
                    }
                    InboundMessage::Bind {
                        portal_name,
                        statement_name,
                        query_param_formats,
                        query_params,
                        result_value_formats,
                    } => {
                        let (txn, finish_txn) = match txn_state.take() {
                            None => (transaction_manager.start_transaction(), true),
                            Some(txn) => (txn, false),
                        };
                        match query_plan_cache.find_described(&statement_name) {
                            Some((untyped_query, _, param_types)) => {
                                let (untyped_query, param_types) = (untyped_query.clone(), param_types.to_vec());

                                let mut arguments: Vec<ScalarValue> = vec![];
                                debug_assert!(
                                    query_params.len() == param_types.len() && query_params.len() == query_param_formats.len(),
                                    "encoded parameter values, their types_old and formats have to have same length"
                                );
                                for i in 0..query_params.len() {
                                    let raw_param = &query_params[i];
                                    let typ = param_types[i];
                                    let format = query_param_formats[i];
                                    match raw_param {
                                        None => arguments.push(ScalarValue::Null),
                                        Some(bytes) => {
                                            log::debug!("PG Type {:?}", typ);
                                            match decode(typ, format, &bytes) {
                                                Ok(param) => arguments.push(From::from(param)),
                                                Err(_) => unimplemented!(),
                                            }
                                        }
                                    }
                                }
                                let portal = crate::Portal {
                                    untyped_query,
                                    result_value_formats: result_value_formats.clone(),
                                    arguments,
                                    param_types: param_types.iter().map(From::from).collect::<Vec<SqlTypeFamilyOld>>(),
                                };
                                query_plan_cache.bind_portal(statement_name, portal_name, portal);
                                connection.send(OutboundMessage::BindComplete).unwrap();
                            }
                            None => connection
                                .send(QueryError::prepared_statement_does_not_exist(&statement_name).into())
                                .unwrap(),
                        }
                        if finish_txn {
                            txn.commit();
                        } else {
                            txn_state = Some(txn);
                        }
                    }
                    InboundMessage::DescribePortal { name } => {
                        let (txn, finish_txn) = match txn_state.take() {
                            None => (transaction_manager.start_transaction(), true),
                            Some(txn) => (txn, false),
                        };
                        match query_plan_cache.find_described(&name) {
                            None => connection.send(QueryError::prepared_statement_does_not_exist(&name).into()).unwrap(),
                            Some(_) => {
                                connection.send(OutboundMessage::StatementDescription(vec![])).unwrap();
                            }
                        }
                        if finish_txn {
                            txn.commit();
                        } else {
                            txn_state = Some(txn);
                        }
                    }
                    InboundMessage::Execute {
                        portal_name,
                        max_rows: _max_rows,
                    } => {
                        let (txn, finish_txn) = match txn_state.take() {
                            None => (transaction_manager.start_transaction(), true),
                            Some(txn) => (txn, false),
                        };
                        let responses = match query_plan_cache.find_portal(&portal_name) {
                            None => vec![QueryError::prepared_statement_does_not_exist(portal_name).into()],
                            Some(crate::Portal {
                                untyped_query,
                                result_value_formats: _result_value_formats,
                                arguments,
                                param_types,
                            }) => executor.execute_portal(untyped_query, param_types, arguments, &txn, &mut query_plan_cache),
                        };
                        for response in responses {
                            connection.send(response).unwrap();
                        }
                        if finish_txn {
                            txn.commit();
                        } else {
                            txn_state = Some(txn);
                        }
                    }
                    InboundMessage::Sync => {
                        connection.send(OutboundMessage::ReadyForQuery).unwrap();
                    }
                    InboundMessage::Terminate => break,
                    other => unimplemented!("other inbound request {:?} is not handled", other),
                },
                _ => break,
            }
        }
    }
}

pub fn decode(ty: u32, format: i16, raw: &[u8]) -> Result<Value, ()> {
    match format {
        1 => decode_binary(ty, raw),
        0 => decode_text(ty, raw),
        _ => unimplemented!(),
    }
}

fn decode_binary(ty: u32, raw: &[u8]) -> Result<Value, ()> {
    match ty {
        BOOL => {
            if raw.is_empty() {
                Err(())
            } else {
                Ok(Value::Bool(raw[0] != 0))
            }
        }
        CHAR | VARCHAR => std::str::from_utf8(raw).map(|s| Value::String(s.into())).map_err(|_cause| ()),
        SMALLINT => {
            if raw.len() < 4 {
                Err(())
            } else {
                Ok(Value::Int16(i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as i16))
            }
        }
        INT => {
            if raw.len() < 4 {
                Err(())
            } else {
                Ok(Value::Int32(i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]])))
            }
        }
        BIGINT => {
            if raw.len() < 8 {
                Err(())
            } else {
                Ok(Value::Int64(i64::from_be_bytes([
                    raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
                ])))
            }
        }
        _ => unimplemented!(),
    }
}

const BOOL_TRUE: &[&str] = &["t", "tr", "tru", "true", "y", "ye", "yes", "on", "1"];
const BOOL_FALSE: &[&str] = &["f", "fa", "fal", "fals", "false", "n", "no", "of", "off", "0"];

fn decode_text(ty: u32, raw: &[u8]) -> Result<Value, ()> {
    let s = match std::str::from_utf8(raw) {
        Ok(s) => s,
        Err(_cause) => return Err(()),
    };

    match ty {
        BOOL => {
            let v = s.trim().to_lowercase();
            if BOOL_TRUE.contains(&v.as_str()) {
                Ok(Value::Bool(true))
            } else if BOOL_FALSE.contains(&v.as_str()) {
                Ok(Value::Bool(false))
            } else {
                Err(())
            }
        }
        CHAR => Ok(Value::String(s.into())),
        VARCHAR => Ok(Value::String(s.into())),
        SMALLINT => s.trim().parse().map(Value::Int16).map_err(|_cause| ()),
        INT => s.trim().parse().map(Value::Int32).map_err(|_cause| ()),
        BIGINT => s.trim().parse().map(Value::Int64).map_err(|_cause| ()),
        _ => unimplemented!(),
    }
}

#[cfg(test)]
mod tests;
