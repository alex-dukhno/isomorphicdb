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

use std::sync::Arc;

use sqlparser::ast::{Query, SetExpr};

use data_manager::{DataManager, Row};
use kernel::SystemResult;
use protocol::{
    results::{QueryError, QueryEvent},
    Sender,
};
use representation::{Binary, Datum};
use sql_model::sql_types::ConstraintError;

use crate::query::expr::{ExprMetadata, ExpressionEvaluation};
use query_planner::plan::TableInserts;

pub(crate) struct InsertCommand<'ic> {
    raw_sql_query: &'ic str,
    table_inserts: TableInserts,
    storage: Arc<DataManager>,
    sender: Arc<dyn Sender>,
}

impl<'ic> InsertCommand<'ic> {
    pub(crate) fn new(
        raw_sql_query: &'ic str,
        table_inserts: TableInserts,
        storage: Arc<DataManager>,
        sender: Arc<dyn Sender>,
    ) -> InsertCommand<'ic> {
        InsertCommand {
            raw_sql_query,
            table_inserts,
            storage,
            sender,
        }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<()> {
        let Query { body, .. } = &*self.table_inserts.input;
        match &body {
            SetExpr::Values(values) => {
                let values = &values.0;

                let columns = if self.table_inserts.column_indices.is_empty() {
                    vec![]
                } else {
                    self.table_inserts
                        .column_indices
                        .clone()
                        .into_iter()
                        .map(|id| {
                            let sqlparser::ast::Ident { value, .. } = id;
                            value
                        })
                        .collect()
                };

                let table_definition = self.storage.table_columns(schema_id, table_id)?;
                let column_names = columns;
                let all_columns = table_definition.clone();

                let evaluation = ExpressionEvaluation::new(self.sender.clone(), table_definition);
                let mut rows = vec![];
                let mut has_error = false;
                for line in values {
                    let mut row = vec![];
                    for (idx, col) in line.iter().enumerate() {
                        let meta = ExprMetadata::new(&all_columns[idx], idx);
                        match evaluation.eval(col, Some(meta)) {
                            Ok(v) => {
                                if v.is_literal() {
                                    let datum = v.as_datum().unwrap();
                                    match all_columns[idx]
                                        .sql_type()
                                        .constraint()
                                        .validate(datum.to_string().as_str())
                                    {
                                        Ok(()) => row.push(v),
                                        Err(ConstraintError::OutOfRange) => {
                                            self.sender
                                                .send(Err(QueryError::out_of_range(
                                                    (&meta.column().sql_type()).into(),
                                                    meta.column().name(),
                                                    idx + 1,
                                                )))
                                                .expect("To Send Query Result to client");
                                            has_error = true;
                                        }
                                        Err(ConstraintError::TypeMismatch(value)) => {
                                            self.sender
                                                .send(Err(QueryError::type_mismatch(
                                                    &value,
                                                    (&meta.column().sql_type()).into(),
                                                    meta.column().name(),
                                                    idx + 1,
                                                )))
                                                .expect("To Send Query Result to client");
                                            has_error = true;
                                        }
                                        Err(ConstraintError::ValueTooLong(len)) => {
                                            self.sender
                                                .send(Err(QueryError::string_length_mismatch(
                                                    (&meta.column().sql_type()).into(),
                                                    len,
                                                    meta.column().name(),
                                                    idx + 1,
                                                )))
                                                .expect("To Send Query Result to client");
                                            has_error = true;
                                        }
                                    }
                                } else {
                                    self.sender
                                        .send(Err(QueryError::feature_not_supported(
                                            "Only expressions resulting in a literal are supported".to_string(),
                                        )))
                                        .expect("To Send Query Result to Client");
                                    return Ok(());
                                }
                            }
                            Err(_) => return Ok(()),
                        }
                    }
                    rows.push(row);
                }

                if has_error {
                    return Ok(());
                }

                let index_columns = if column_names.is_empty() {
                    let mut index_cols = vec![];
                    for (index, column_definition) in all_columns.iter().cloned().enumerate() {
                        index_cols.push((index, column_definition));
                    }

                    index_cols
                } else {
                    let mut index_cols = vec![];
                    let mut non_existing_cols = vec![];
                    for column_name in column_names {
                        let mut found = None;
                        for (index, column_definition) in all_columns.iter().enumerate() {
                            if column_definition.has_name(&column_name) {
                                found = Some((index, column_definition.clone()));
                                break;
                            }
                        }

                        match found {
                            Some(index_col) => index_cols.push(index_col),
                            None => non_existing_cols.push(column_name.clone()),
                        }
                    }

                    if !non_existing_cols.is_empty() {
                        self.sender
                            .send(Err(QueryError::column_does_not_exist(non_existing_cols)))
                            .expect("To Send Result to Client");
                        return Ok(());
                    }

                    index_cols
                };

                let mut to_write: Vec<Row> = vec![];
                for (_row_index, row) in rows.iter().enumerate() {
                    if row.len() > all_columns.len() {
                        self.sender
                            .send(Err(QueryError::too_many_insert_expressions()))
                            .expect("To Send Result to Client");
                        return Ok(());
                    }

                    let key = self.storage.next_key_id(schema_id, table_id).to_be_bytes().to_vec();

                    // TODO: The default value or NULL should be initialized for SQL types of all columns.
                    let mut record = vec![Datum::from_null(); all_columns.len()];
                    for (item, (index, _column_definition)) in row.iter().zip(index_columns.iter()) {
                        let datum = item.as_datum().unwrap();
                        record[*index] = datum;
                    }
                    to_write.push((Binary::with_data(key), Binary::pack(&record)));
                }

                match self.storage.write_into(schema_id, table_id, to_write) {
                    Err(error) => return Err(error),
                    Ok(size) => self
                        .sender
                        .send(Ok(QueryEvent::RecordsInserted(size)))
                        .expect("To Send Result to Client"),
                }

                Ok(())
            }
            _ => {
                self.sender
                    .send(Err(QueryError::feature_not_supported(self.raw_sql_query.to_owned())))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
        }
    }
}
