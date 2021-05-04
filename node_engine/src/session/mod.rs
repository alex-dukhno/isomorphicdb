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

use crate::query_engine::{QueryEngine, TransactionContext};
use data_manipulation::QueryPlan;
use postgre_sql::query_ast::{Query, Statement};
use postgre_sql::query_response::{QueryError, QueryEvent};
use std::collections::HashMap;
use types::SqlTypeFamily;

#[derive(Default)]
pub struct Session {
    plans: HashMap<String, (Query, Vec<SqlTypeFamily>)>,
}

impl Session {
    pub fn cache(&mut self, name: String, _query_plan: QueryPlan, query_ast: Query, params: Vec<SqlTypeFamily>) {
        self.plans.insert(name, (query_ast, params));
    }

    pub fn find(&self, name: &str) -> Option<&(Query, Vec<SqlTypeFamily>)> {
        self.plans.get(name)
    }
}
