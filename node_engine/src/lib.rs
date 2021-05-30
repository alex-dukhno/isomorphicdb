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

use data_repr::scalar::ScalarValue;
pub use engine::NodeEngine;
use postgre_sql::query_ast::Query;
use query_plan::QueryPlan;
use std::collections::HashMap;
use types::SqlTypeFamily;
use untyped_queries::UntypedQuery;

pub mod engine;
mod query_executor;
mod transaction_manager;
mod worker;

#[derive(Default)]
pub struct QueryPlanCache {
    plans: HashMap<String, (Query, Vec<SqlTypeFamily>)>,
    extended_query: HashMap<String, PreparedStatementState>,
    portal_per_statement: HashMap<String, Vec<String>>,
    all_portals: HashMap<String, Portal>,
}

impl QueryPlanCache {
    pub fn save_parsed(&mut self, name: String, sql: String, query: Query, param_types: Vec<u32>) {
        self.extended_query
            .insert(name, PreparedStatementState::Parsed { sql, query, param_types });
    }

    pub fn find_parsed(&mut self, name: &str) -> Option<(Query, String, Vec<u32>)> {
        match self.extended_query.remove(name) {
            None => None,
            Some(PreparedStatementState::Parsed { query, sql, param_types }) => Some((query, sql, param_types.to_vec())),
            Some(_) => None,
        }
    }

    pub fn save_described(&mut self, name: String, untyped_query: UntypedQuery, sql: String, param_types: Vec<u32>) {
        self.extended_query.insert(
            name,
            PreparedStatementState::Described {
                untyped_query,
                sql,
                param_types,
            },
        );
    }

    pub fn find_described(&mut self, name: &str) -> Option<(&UntypedQuery, &str, &Vec<u32>)> {
        match self.extended_query.get(name) {
            None => None,
            Some(PreparedStatementState::Described {
                untyped_query,
                sql,
                param_types,
            }) => Some((&untyped_query, &sql, &param_types)),
            Some(_) => None,
        }
    }

    pub fn bind_portal(&mut self, statement_name: String, portal_name: String, portal: Portal) {
        self.portal_per_statement
            .entry(statement_name)
            .or_insert_with(Vec::new)
            .push(portal_name.clone());
        self.all_portals.insert(portal_name, portal);
    }

    pub fn find_portal(&self, portal: &str) -> Option<Portal> {
        self.all_portals.get(portal).map(Portal::clone)
    }

    pub fn allocate(&mut self, name: String, _query_plan: QueryPlan, query_ast: Query, params: Vec<SqlTypeFamily>) {
        self.plans.insert(name, (query_ast, params));
    }

    pub fn lookup(&self, name: &str) -> Option<&(Query, Vec<SqlTypeFamily>)> {
        self.plans.get(name)
    }

    pub fn deallocate(&mut self, name: &str) -> Option<(Query, Vec<SqlTypeFamily>)> {
        self.plans.remove(name)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum PreparedStatementState {
    Parsed {
        sql: String,
        query: Query,
        param_types: Vec<u32>,
    },
    Described {
        sql: String,
        untyped_query: UntypedQuery,
        param_types: Vec<u32>,
    },
}

#[derive(Clone)]
pub struct Portal {
    pub untyped_query: UntypedQuery,
    pub result_value_formats: Vec<i16>,
    pub arguments: Vec<ScalarValue>,
    pub param_types: Vec<SqlTypeFamily>,
}
