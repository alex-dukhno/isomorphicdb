// Copyright 2020 Alex Dukhno
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

use crate::query::TableInserts;
use crate::dml::ExpressionEvaluation;
use kernel::SystemResult;
use protocol::results::{QueryErrorBuilder, QueryEvent, QueryResult};
use sql_types::ConstraintError;
use sqlparser::ast::{DataType, Expr, Ident, ObjectName, Query, SetExpr, UnaryOperator, Value};
use std::sync::{Arc, Mutex};
use storage::{backend::BackendStorage, frontend::FrontendStorage, ColumnDefinition, OperationOnTableError};

pub(crate) struct InsertCommand<'q, P: BackendStorage> {
    raw_sql_query: &'q str,
    table_inserts: TableInserts,
    storage: Arc<Mutex<FrontendStorage<P>>>,
}

impl<P: BackendStorage> InsertCommand<'_, P> {
    pub(crate) fn new(
        raw_sql_query: &'_ str,
        table_inserts: TableInserts,
        storage: Arc<Mutex<FrontendStorage<P>>>,
    ) -> InsertCommand<P> {
        InsertCommand {
            raw_sql_query,
            table_inserts,
            storage,
        }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<QueryResult> {
        todo!()
    }
}
