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
use data_manipulation::{QueryExecutionError, QueryExecutionResult};
use data_repr::scalar::ScalarValue;
use postgre_sql::query_ast::{Extended, Statement};
use postgre_sql::query_response::QueryEvent;
use postgre_sql::wire_protocol::payload::Outbound;
use types::{SqlType, SqlTypeFamily};

pub struct QueryExecutor;

impl QueryExecutor {
    pub fn execute(
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
                let query_events: Vec<Outbound> = match txn
                    .process(query, vec![])
                    .map(|typed_query| txn.plan(typed_query))
                    .and_then(|plan| plan.execute(vec![]).map_err(QueryExecutionError::into))
                {
                    Ok(success) => {
                        let events: Vec<QueryEvent> = success.into();
                        events.into_iter().map(Outbound::from).collect()
                    }
                    Err(failure) => {
                        vec![failure.into()]
                    }
                };
                responses.extend(query_events);
            }
        }
        responses.push(Outbound::ReadyForQuery);
        responses
    }
}

#[cfg(test)]
mod tests;
