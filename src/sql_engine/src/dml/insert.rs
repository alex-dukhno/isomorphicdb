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

use crate::{
    catalog_manager::CatalogManager,
    query::{expr::ExpressionEvaluation, plan::TableInserts},
    ColumnDefinition,
};
use kernel::SystemResult;
use protocol::results::QueryError;
use protocol::{results::QueryEvent, Sender};
use representation::{Binary, Datum};
use sql_types::ConstraintError;
use sqlparser::ast::{DataType, Expr, Query, SetExpr, UnaryOperator, Value};
use std::{convert::TryFrom, str::FromStr, sync::Arc};
use storage::Row;
use protocol::messages::ColumnMetadata;
use crate::query::expr::ExprMetadata;

pub(crate) struct InsertCommand<'ic> {
    raw_sql_query: &'ic str,
    table_inserts: TableInserts,
    storage: Arc<CatalogManager>,
    session: Arc<dyn Sender>,
}

impl<'ic> InsertCommand<'ic> {
    pub(crate) fn new(
        raw_sql_query: &'ic str,
        table_inserts: TableInserts,
        storage: Arc<CatalogManager>,
        session: Arc<dyn Sender>,
    ) -> InsertCommand<'ic> {
        InsertCommand {
            raw_sql_query,
            table_inserts,
            storage,
            session,
        }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<()> {
        let table_name = self.table_inserts.table_id.name();
        let schema_name = self.table_inserts.table_id.schema_name();
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

                if !self.storage.schema_exists(schema_name) {
                    self.session
                        .send(Err(QueryError::schema_does_not_exist(schema_name.to_owned())))
                        .expect("To Send Result to Client");
                    return Ok(());
                }

                if !self.storage.table_exists(schema_name, table_name) {
                    self.session
                        .send(Err(QueryError::table_does_not_exist(
                            schema_name.to_owned() + "." + table_name,
                        )))
                        .expect("To Send Result to Client");
                    return Ok(());
                }

                let table_definition = self.storage.table_columns(&schema_name, &table_name)?;
                let column_names = columns;
                let all_columns = table_definition.clone();

                let evaluation = ExpressionEvaluation::new(self.session.clone(), table_definition);
                let mut rows = vec![];
                let mut has_error = false;
                for line in values{
                    let mut row = vec![];
                    for (idx, col) in line.iter().enumerate() {
                        let meta = ExprMetadata::new(&all_columns[idx], idx);
                        match evaluation.eval(col, Some(meta.clone())) {
                            Ok(v) => {
                                if v.is_literal() {
                                    let datum = v.as_datum().unwrap();
                                    match all_columns[idx].sql_type().constraint().validate(datum.to_string().as_str()){
                                        Ok(()) => row.push(v),
                                        Err(ConstraintError::OutOfRange) => {
                                            self.session.send(Err(QueryError::out_of_range(
                                                (&meta.column().sql_type()).into(),
                                                meta.column().name(),
                                                idx + 1,
                                            ))).expect("To Send Query Result to client");
                                            has_error = true;
                                        },
                                        Err(ConstraintError::TypeMismatch(value)) => {
                                            self.session.send(Err(QueryError::type_mismatch(
                                                &value,
                                                (&meta.column().sql_type()).into(),
                                                meta.column().name(),
                                                idx + 1,
                                            ))).expect("To Send Query Result to client");
                                            has_error = true;
                                        },
                                        Err(ConstraintError::ValueTooLong(len)) => {
                                            self.session.send(Err(QueryError::string_length_mismatch(
                                                (&meta.column().sql_type()).into(),
                                                len,
                                                meta.column().name(),
                                                idx + 1,
                                            ))).expect("To Send Query Result to client");
                                            has_error = true;
                                        },
                                    }
                                } else {
                                    self.session
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
                    return Ok(())
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
                            Some(index_col) => {
                                index_cols.push(index_col);
                            }
                            None => non_existing_cols.push(column_name.clone()),
                        }
                    }

                    if !non_existing_cols.is_empty() {
                        self.session
                            .send(Err(QueryError::column_does_not_exist(non_existing_cols)))
                            .expect("To Send Result to Client");
                        return Ok(());
                    }

                    index_cols
                };

                let mut to_write: Vec<Row> = vec![];
                if self.storage.table_exists(&schema_name, &table_name) {
                    let mut errors = Vec::new();

                    for (row_index, row) in rows.iter().enumerate() {
                        if row.len() > all_columns.len() {
                            // clear anything that could have been processed already.
                            to_write.clear();
                            self.session
                                .send(Err(QueryError::too_many_insert_expressions()))
                                .expect("To Send Result to Client");
                            return Ok(());
                        }

                        let key = self.storage.next_key_id().to_be_bytes().to_vec();

                        // TODO: The default value or NULL should be initialized for SQL types of all columns.
                        let mut record = vec![Datum::from_null(); all_columns.len()];
                        for (item, (index, column_definition)) in row.iter().zip(index_columns.iter()) {
                            let datum = item.as_datum().unwrap();
                            let v = datum.to_string();
                            match column_definition.sql_type().constraint().validate(v.as_str()) {
                                Ok(()) => {
                                    record[*index] = datum;
                                }
                                Err(e) => {
                                    errors.push((e, column_definition.clone()));
                                }
                            }
                        }

                        // if there was an error then exit the loop.
                        if !errors.is_empty() {
                            for (error, column_definition) in errors {
                                let error_to_send = match error {
                                    ConstraintError::OutOfRange => QueryError::out_of_range(
                                        (&column_definition.sql_type()).into(),
                                        column_definition.name(),
                                        row_index + 1,
                                    ),
                                    ConstraintError::TypeMismatch(value) => QueryError::type_mismatch(
                                        &value,
                                        (&column_definition.sql_type()).into(),
                                        column_definition.name(),
                                        row_index + 1,
                                    ),
                                    ConstraintError::ValueTooLong(len) => QueryError::string_length_mismatch(
                                        (&column_definition.sql_type()).into(),
                                        len,
                                        column_definition.name(),
                                        row_index + 1,
                                    ),
                                };
                                self.session
                                    .send(Err(error_to_send))
                                    .expect("To Send Query Result to Client");
                            }
                            return Ok(());
                        }
                        to_write.push((Binary::with_data(key), Binary::pack(&record)));
                    }
                }

                match self.storage.insert_into(&schema_name, &table_name, to_write) {
                    Err(error) => Err(error),
                    Ok(size) => {
                        self.session
                            .send(Ok(QueryEvent::RecordsInserted(size)))
                            .expect("To Send Result to Client");
                        Ok(())
                    }
                }
            }
            _ => {
                self.session
                    .send(Err(QueryError::feature_not_supported(self.raw_sql_query.to_owned())))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
        }
    }
}
