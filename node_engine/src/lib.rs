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

use data_manipulation::QueryPlan;
pub use node_engine_old::NodeEngineOld;
use postgre_sql::query_ast::Query;
use std::collections::HashMap;
use types::SqlTypeFamily;

mod node_engine_old;
mod query_engine_old;
mod session_old;
mod transaction_manager;
mod txn_context;
mod worker;

#[derive(Default)]
#[allow(dead_code)]
pub struct QueryPlanCache {
    plans: HashMap<String, (Query, Vec<SqlTypeFamily>)>,
}

#[allow(dead_code)]
impl QueryPlanCache {
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
