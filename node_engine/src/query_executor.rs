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

use crate::transaction_manager::TransactionContext;
use crate::QueryPlanCache;
use data_manipulation::{QueryExecutionError, QueryExecutionResult, TypedQuery, UntypedQuery};
use data_repr::scalar::ScalarValue;
use definition::ColumnDef;
use postgre_sql::query_ast::{Extended, Query, Statement};
use postgre_sql::query_response::{QueryError, QueryEvent};
use postgre_sql::wire_protocol::payload::OutboundMessage;
use types::{SqlType, SqlTypeFamily};

pub struct QueryExecutor;

impl QueryExecutor {
    pub fn describe_statement(
        &self,
        query: Query,
        txn: &TransactionContext,
    ) -> (UntypedQuery, Vec<u32>, Vec<OutboundMessage>) {
        let mut responses = vec![];
        let (untyped_query, params) = match txn.analyze(query) {
            Ok(UntypedQuery::Insert(insert)) => {
                let param_types = txn.describe_insert(&insert);
                responses.push(OutboundMessage::StatementDescription(vec![]));
                responses.push(OutboundMessage::StatementParameters(param_types.to_vec()));
                (UntypedQuery::Insert(insert), param_types)
            }
            _ => unimplemented!(),
        };
        (untyped_query, params, responses)
        // match query_analyzer.analyze(statement.query().unwrap()) {
        //     Ok(UntypedQuery::Insert(insert)) => {
        //         let table_definition = catalog
        //             .table_definition(insert.full_table_name.clone())
        //             .unwrap()
        //             .unwrap();
        //         let param_types = table_definition
        //             .columns()
        //             .iter()
        //             .map(ColumnDef::sql_type)
        //             .map(|sql_type| (&sql_type).into())
        //             .collect::<Vec<u32>>();
        //         let x25: Vec<u8> = QueryEvent::StatementParameters(param_types.to_vec()).into();
        //         self.sender.lock().unwrap()
        //             .send(&x25)
        //             .expect("To Send Statement Parameters to Client");
        //         let x24: Vec<u8> = QueryEvent::StatementDescription(vec![]).into();
        //         self.sender.lock().unwrap()
        //             .send(&x24)
        //             .expect("To Send Statement Description to Client");
        //         statement.described(UntypedQuery::Insert(insert), param_types);
        //     }
        //     _ => {
        //         let x19: Vec<u8> = QueryError::prepared_statement_does_not_exist(name).into();
        //         self.sender.lock().unwrap()
        //             .send(&x19)
        //             .expect("To Send Error to Client");
        //     }
        // }
    }

    pub fn execute_portal(
        &self,
        untyped_query: UntypedQuery,
        params: Vec<SqlTypeFamily>,
        arguments: Vec<ScalarValue>,
        txn: &TransactionContext,
        query_plan_cache: &mut QueryPlanCache,
    ) -> Vec<OutboundMessage> {
        let mut responses = vec![];
        let typed_query = txn.process_untyped_query(untyped_query, params).unwrap();
        let query_plan = txn.plan(typed_query);
        match query_plan.execute(arguments) {
            Ok(QueryExecutionResult::Inserted(inserted)) => responses.push(OutboundMessage::RecordsInserted(inserted)),
            Ok(_) => {}
            Err(_) => unimplemented!(),
        }
        responses
    }

    pub fn execute_statement(
        &self,
        statement: Statement,
        txn: &TransactionContext,
        query_plan_cache: &mut QueryPlanCache,
    ) -> Vec<OutboundMessage> {
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
                    responses.push(OutboundMessage::StatementPrepared);
                }
                Extended::Execute { name, param_values } => {
                    match query_plan_cache.lookup(&name) {
                        None => responses.push(QueryError::prepared_statement_does_not_exist(&name).into()),
                        // TODO: workaround situation that QueryPlan is not cloneable ¯\_(ツ)_/¯
                        Some((query, params)) => {
                            let typed_query = txn.process(query.clone(), params.clone()).unwrap();
                            let query_plan = txn.plan(typed_query);
                            match query_plan.execute(param_values.into_iter().map(ScalarValue::from).collect()) {
                                Ok(QueryExecutionResult::Inserted(inserted)) => {
                                    responses.push(OutboundMessage::RecordsInserted(inserted))
                                }
                                Ok(_) => {}
                                Err(_) => unimplemented!(),
                            }
                        }
                    }
                }
                Extended::Deallocate { name } => match query_plan_cache.deallocate(&name) {
                    None => responses.push(QueryError::prepared_statement_does_not_exist(&name).into()),
                    Some(_) => responses.push(OutboundMessage::StatementDeallocated),
                },
            },
            Statement::Query(query) => {
                let query_events: Vec<OutboundMessage> = match txn
                    .process(query, vec![])
                    .map(|typed_query| txn.plan(typed_query))
                    .and_then(|plan| plan.execute(vec![]).map_err(QueryExecutionError::into))
                {
                    Ok(success) => {
                        let events: Vec<QueryEvent> = success.into();
                        events.into_iter().map(OutboundMessage::from).collect()
                    }
                    Err(failure) => {
                        vec![failure.into()]
                    }
                };
                responses.extend(query_events);
            }
        }
        responses.push(OutboundMessage::ReadyForQuery);
        responses
    }
}

#[cfg(test)]
mod tests;
