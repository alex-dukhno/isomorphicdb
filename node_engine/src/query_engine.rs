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

use crate::txn_context::TransactionContext;
use crate::QueryPlanCache;
use data_manipulation::QueryExecutionResult;
use data_repr::scalar::ScalarValue;
use postgre_sql::query_ast::{Extended, Statement, Transaction};
use postgre_sql::wire_protocol::payload::{Inbound, Outbound};
use storage::Database;
use types::SqlTypeFamily;

#[derive(Clone)]
pub struct QueryEngine {
    database: Database,
}

impl QueryEngine {
    pub fn new(database: Database) -> QueryEngine {
        QueryEngine { database }
    }

    pub fn start_transaction(&self) -> TransactionContext {
        TransactionContext::new(self.database.transaction())
    }
}
