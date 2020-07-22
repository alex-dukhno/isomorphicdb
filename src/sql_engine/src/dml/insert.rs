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

use crate::query::{Datum, RelationOp, Row, ScalarOp, TableInserts};
use kernel::SystemResult;
use protocol::results::{QueryError, QueryErrorBuilder, QueryEvent, QueryResult};
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
        let table_id = &self.table_inserts.table_id;
        let columns_indices = self.table_inserts.column_indices.as_slice();

        // temporary: this only evaluates Values
        if let RelationOp::Constants(rows) = self.table_inserts.input.as_ref() {
            log::debug!("Input Data: {:#?}", rows);

            let rows = if !columns_indices.is_empty() {
                let mut ordered_rows = Vec::new();
                for row in rows {
                    let datums = row.unpack();
                    // build a temporary buffer to map the values back into.
                    // if this was a problem, QueryProcessor should catch it.
                    // TODO: The default value or NULL should be initialized for SQL types of all columns.
                    let mut ordered_datums = vec![Datum::from_null(); columns_indices.len()];

                    for (idx, datums) in columns_indices.iter().zip(datums.iter()) {
                        if let ScalarOp::Column(idx) = idx {
                            ordered_datums[*idx] = datums.clone();
                        } else {
                            panic!("INSERT: using unsupported input source");
                        }
                    }

                    ordered_rows.push(Row::pack(ordered_datums.as_slice()).to_bytes());
                }
                ordered_rows
            } else {
                rows.iter().map(|row| row.clone().to_bytes()).collect()
            };

            let len = rows.len();

            log::debug!("Row Data: {:#?}", rows);

            match self
                .storage
                .lock()
                .unwrap()
                .insert_into(table_id.schema_name(), table_id.name(), rows)
            {
                Ok(Ok(())) => Ok(Ok(QueryEvent::RecordsInserted(len))),
                _ => unreachable!(),
            }
        } else {
            Ok(Err(QueryErrorBuilder::new()
                .feature_not_supported(self.raw_sql_query.to_owned())
                .build()))
        }
    }
}
