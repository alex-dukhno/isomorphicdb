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

use crate::{transaction_manager::TransactionContext, QueryPlanCache};
use data_repr::scalar::ScalarValue;
use postgre_sql::{
    query_ast::{Extended, Query, Statement},
    query_response::{QueryError, QueryEvent},
    wire_protocol::payload::OutboundMessage,
};
use query_plan::QueryExecutionResult;
use query_result::QueryExecutionError;
use types::{SqlType, SqlTypeFamily};
use untyped_queries::UntypedQuery;

pub struct QueryExecutor;

impl QueryExecutor {
    pub fn describe_statement(&self, query: Query, txn: &TransactionContext) -> (UntypedQuery, Vec<u32>, Vec<OutboundMessage>) {
        let mut responses = vec![];
        let (untyped_query, params) = match txn.analyze(query) {
            Ok(UntypedQuery::Insert(insert)) => {
                let param_types = txn.describe_insert(&insert);
                responses.push(OutboundMessage::StatementDescription(vec![]));
                responses.push(OutboundMessage::StatementParameters(param_types.to_vec()));
                (UntypedQuery::Insert(insert), param_types)
            }
            Ok(UntypedQuery::Update(update)) => {
                let param_types = txn.describe_update(&update);
                responses.push(OutboundMessage::StatementDescription(vec![]));
                responses.push(OutboundMessage::StatementParameters(param_types.to_vec()));
                (UntypedQuery::Update(update), param_types)
            }
            other => unimplemented!("{:?}", other),
        };
        (untyped_query, params, responses)
    }

    pub fn execute_portal(
        &self,
        untyped_query: UntypedQuery,
        params: Vec<SqlTypeFamily>,
        arguments: Vec<ScalarValue>,
        txn: &TransactionContext,
        _query_plan_cache: &mut QueryPlanCache,
    ) -> Vec<OutboundMessage> {
        let mut responses = vec![];
        let typed_query = txn.process_untyped_query(untyped_query, params).unwrap();
        let query_plan = txn.plan(typed_query);
        match query_plan.execute(arguments) {
            Ok(success) => {
                let events: Vec<QueryEvent> = success.into();
                let messages: Vec<OutboundMessage> = events.into_iter().map(QueryEvent::into).collect();
                responses.extend(messages);
            }
            Err(_) => unimplemented!(),
        }
        responses
    }

    pub fn execute_statement(&self, statement: Statement, txn: &TransactionContext, query_plan_cache: &mut QueryPlanCache) -> Vec<OutboundMessage> {
        let mut responses = vec![];
        match statement {
            Statement::Definition(definition) => match txn.apply_schema_change(definition) {
                Ok(success) => responses.push(success.into()),
                Err(failure) => responses.push(failure.into()),
            },
            Statement::Extended(extended) => match extended {
                Extended::Prepare { query, name, param_types } => {
                    let params: Vec<SqlTypeFamily> = param_types.into_iter().map(|dt| SqlType::from(dt).family()).collect();
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
                                Ok(QueryExecutionResult::Inserted(inserted)) => responses.push(OutboundMessage::RecordsInserted(inserted)),
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
