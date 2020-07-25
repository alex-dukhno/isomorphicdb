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

use crate::dml::ExpressionEvaluation;
use kernel::SystemResult;
use protocol::{
    results::{QueryErrorBuilder, QueryEvent},
    Sender,
};
use representation::{Binary, Datum};
use sql_types::ConstraintError;
use sqlparser::ast::{DataType, Expr, Ident, ObjectName, Query, SetExpr, UnaryOperator, Value};
use std::{
    convert::TryFrom,
    str::FromStr,
    sync::{Arc, Mutex},
};
use storage::{
    backend::{BackendStorage, Row},
    frontend::FrontendStorage,
    ColumnDefinition, OperationOnTableError,
};

pub(crate) struct InsertCommand<'ic, P: BackendStorage> {
    raw_sql_query: &'ic str,
    name: ObjectName,
    columns: Vec<Ident>,
    source: Box<Query>,
    storage: Arc<Mutex<FrontendStorage<P>>>,
    session: Arc<dyn Sender>,
}

impl<'ic, P: BackendStorage> InsertCommand<'ic, P> {
    pub(crate) fn new(
        raw_sql_query: &'ic str,
        name: ObjectName,
        columns: Vec<Ident>,
        source: Box<Query>,
        storage: Arc<Mutex<FrontendStorage<P>>>,
        session: Arc<dyn Sender>,
    ) -> InsertCommand<'ic, P> {
        InsertCommand {
            raw_sql_query,
            name,
            columns,
            source,
            storage,
            session,
        }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<()> {
        let table_name = self.name.0.pop().unwrap().to_string();
        let schema_name = self.name.0.pop().unwrap().to_string();
        let Query { body, .. } = &*self.source;
        match &body {
            SetExpr::Values(values) => {
                let values = &values.0;

                let columns = if self.columns.is_empty() {
                    vec![]
                } else {
                    self.columns
                        .clone()
                        .into_iter()
                        .map(|id| {
                            let sqlparser::ast::Ident { value, .. } = id;
                            value
                        })
                        .collect()
                };

                let evaluation = ExpressionEvaluation::new(self.session.clone());
                let mut rows = vec![];
                for line in values {
                    let mut row = vec![];
                    for col in line {
                        let v = match col {
                            Expr::Value(value) => value.clone(),
                            Expr::Cast { expr, data_type } => match (&**expr, data_type) {
                                (Expr::Value(Value::Boolean(v)), DataType::Boolean) => Value::Boolean(*v),
                                (Expr::Value(Value::SingleQuotedString(v)), DataType::Boolean) => {
                                    Value::Boolean(bool::from_str(v).unwrap())
                                }
                                _ => {
                                    self.session
                                        .send(Err(QueryErrorBuilder::new()
                                            .syntax_error(format!(
                                                "Cast from {:?} to {:?} is not currently supported",
                                                expr, data_type
                                            ))
                                            .build()))
                                        .expect("To Send Query Result to Client");
                                    return Ok(());
                                }
                            },
                            Expr::UnaryOp { op, expr } => match (op, &**expr) {
                                (UnaryOperator::Minus, Expr::Value(Value::Number(v))) => Value::Number(-v),
                                (op, expr) => {
                                    self.session
                                        .send(Err(QueryErrorBuilder::new()
                                            .syntax_error(op.to_string() + expr.to_string().as_str())
                                            .build()))
                                        .expect("To Send Query Result to Client");
                                    return Ok(());
                                }
                            },
                            expr @ Expr::BinaryOp { .. } => match evaluation.eval(expr) {
                                Ok(expr_result) => expr_result,
                                Err(()) => return Ok(()),
                            },
                            expr => {
                                self.session
                                    .send(Err(QueryErrorBuilder::new().syntax_error(expr.to_string()).build()))
                                    .expect("To Send Query Result to Client");
                                return Ok(());
                            }
                        };
                        row.push(v);
                    }
                    rows.push(row);
                }

                let len = rows.len();

                let column_names = columns;
                let all_columns = (self.storage.lock().unwrap()).table_columns(&schema_name, &table_name)?;
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
                            None => non_existing_cols.push(column_name),
                        }
                    }

                    if !non_existing_cols.is_empty() {
                        self.session
                            .send(Err(QueryErrorBuilder::new()
                                .column_does_not_exist(non_existing_cols)
                                .build()))
                            .expect("To Send Result to Client");
                        return Ok(());
                    }

                    index_cols
                };

                let mut to_write: Vec<Row> = vec![];
                if (self.storage.lock().unwrap()).table_exists(&schema_name, &table_name) {
                    let mut errors = Vec::new();

                    for (row_index, row) in rows.iter().enumerate() {
                        if row.len() > all_columns.len() {
                            // clear anything that could have been processed already.
                            to_write.clear();
                            self.session
                                .send(Err(QueryErrorBuilder::new().too_many_insert_expressions().build()))
                                .expect("To Send Result to Client");
                            return Ok(());
                        }

                        let key = (self.storage.lock().unwrap()).next_key_id().to_be_bytes().to_vec();

                        // TODO: The default value or NULL should be initialized for SQL types of all columns.
                        let mut record = vec![Datum::from_null(); all_columns.len()];
                        for (item, (index, column_definition)) in row.iter().zip(index_columns.iter()) {
                            let v = match item.clone() {
                                Value::Number(v) => v.to_string(),
                                Value::SingleQuotedString(v) => v.to_string(),
                                Value::Boolean(v) => v.to_string(),
                                _ => unimplemented!("other types not implemented"),
                            };
                            match column_definition.sql_type().constraint().validate(v.as_str()) {
                                Ok(()) => {
                                    record[*index] = Datum::try_from(item).unwrap();
                                }
                                Err(e) => {
                                    errors.push((e, column_definition.clone()));
                                }
                            }
                        }

                        // if there was an error then exit the loop.
                        if !errors.is_empty() {
                            // In SQL indexes start from 1, not 0.
                            let mut builder = QueryErrorBuilder::new();
                            let mut constraint_error_mapper =
                                |err: &ConstraintError, column_definition: &ColumnDefinition, row_index: usize| {
                                    match err {
                                        ConstraintError::OutOfRange => {
                                            builder.out_of_range(
                                                column_definition.sql_type().to_pg_types(),
                                                column_definition.name(),
                                                row_index,
                                            );
                                        }
                                        ConstraintError::TypeMismatch(value) => {
                                            builder.type_mismatch(
                                                value,
                                                column_definition.sql_type().to_pg_types(),
                                                column_definition.name(),
                                                row_index,
                                            );
                                        }
                                        ConstraintError::ValueTooLong(len) => {
                                            builder.string_length_mismatch(
                                                column_definition.sql_type().to_pg_types(),
                                                *len,
                                                column_definition.name(),
                                                row_index,
                                            );
                                        }
                                    }
                                };

                            errors.iter().for_each(|(err, column_definition)| {
                                constraint_error_mapper(err, column_definition, row_index + 1)
                            });
                            self.session
                                .send(Err(builder.build()))
                                .expect("To Send Query Result to Client");
                            return Ok(());
                        }

                        to_write.push((key, Binary::pack(&record).to_bytes()));
                    }
                }

                match (self.storage.lock().unwrap()).insert_into(&schema_name, &table_name, to_write)? {
                    Ok(_) => {
                        self.session
                            .send(Ok(QueryEvent::RecordsInserted(len)))
                            .expect("To Send Query Result to Client");
                        Ok(())
                    }
                    Err(OperationOnTableError::SchemaDoesNotExist) => {
                        self.session
                            .send(Err(QueryErrorBuilder::new().schema_does_not_exist(schema_name).build()))
                            .expect("To Send Query Result to Client");
                        Ok(())
                    }
                    Err(OperationOnTableError::TableDoesNotExist) => {
                        self.session
                            .send(Err(QueryErrorBuilder::new()
                                .table_does_not_exist(schema_name + "." + table_name.as_str())
                                .build()))
                            .expect("To Send Query Result to Client");
                        Ok(())
                    }
                    Err(OperationOnTableError::ColumnDoesNotExist(non_existing_columns)) => {
                        self.session
                            .send(Err(QueryErrorBuilder::new()
                                .column_does_not_exist(non_existing_columns)
                                .build()))
                            .expect("To Send Query Result to Client");
                        Ok(())
                    }
                    Err(OperationOnTableError::ConstraintViolations(constraint_errors, row_index)) => {
                        let mut builder = QueryErrorBuilder::new();
                        let mut constraint_error_mapper =
                            |err: &ConstraintError, column_definition: &ColumnDefinition, row_index: usize| match err {
                                ConstraintError::OutOfRange => {
                                    builder.out_of_range(
                                        column_definition.sql_type().to_pg_types(),
                                        column_definition.name(),
                                        row_index,
                                    );
                                }
                                ConstraintError::TypeMismatch(value) => {
                                    builder.type_mismatch(
                                        value,
                                        column_definition.sql_type().to_pg_types(),
                                        column_definition.name(),
                                        row_index,
                                    );
                                }
                                ConstraintError::ValueTooLong(len) => {
                                    builder.string_length_mismatch(
                                        column_definition.sql_type().to_pg_types(),
                                        *len,
                                        column_definition.name(),
                                        row_index,
                                    );
                                }
                            };

                        constraint_errors.iter().for_each(|(err, column_definition)| {
                            constraint_error_mapper(err, column_definition, row_index)
                        });
                        self.session
                            .send(Err(builder.build()))
                            .expect("To Send Query Result to Client");
                        Ok(())
                    }
                    Err(OperationOnTableError::InsertTooManyExpressions) => {
                        self.session
                            .send(Err(QueryErrorBuilder::new().too_many_insert_expressions().build()))
                            .expect("To Send Query Result to Client");
                        Ok(())
                    }
                }
            }
            _ => {
                self.session
                    .send(Err(QueryErrorBuilder::new()
                        .feature_not_supported(self.raw_sql_query.to_owned())
                        .build()))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
        }
    }
}
