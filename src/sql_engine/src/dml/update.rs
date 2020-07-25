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
use sql_types::ConstraintError;
use sqlparser::ast::{Assignment, Expr, Ident, ObjectName, UnaryOperator, Value};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use storage::backend::Row;
use storage::{backend, backend::BackendStorage, frontend::FrontendStorage, ColumnDefinition, OperationOnTableError};

pub(crate) struct UpdateCommand<'uc, P: BackendStorage> {
    raw_sql_query: &'uc str,
    name: ObjectName,
    assignments: Vec<Assignment>,
    storage: Arc<Mutex<FrontendStorage<P>>>,
    session: Arc<dyn Sender>,
}

impl<'uc, P: BackendStorage> UpdateCommand<'uc, P> {
    pub(crate) fn new(
        raw_sql_query: &'uc str,
        name: ObjectName,
        assignments: Vec<Assignment>,
        storage: Arc<Mutex<FrontendStorage<P>>>,
        session: Arc<dyn Sender>,
    ) -> UpdateCommand<'uc, P> {
        UpdateCommand {
            raw_sql_query,
            name,
            assignments,
            storage,
            session,
        }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<()> {
        let schema_name = self.name.0[0].to_string();
        let table_name = self.name.0[1].to_string();
        let mut to_update = vec![];

        for item in self.assignments.iter() {
            let Assignment { id, value } = &item;
            let Ident { value: column, .. } = id;

            let value = match value {
                Expr::Value(Value::Number(val)) => val.to_string(),
                Expr::Value(Value::SingleQuotedString(v)) => v.to_string(),
                Expr::UnaryOp { op, expr } => match (op, &**expr) {
                    (UnaryOperator::Minus, Expr::Value(Value::Number(v))) => "-".to_owned() + v.to_string().as_str(),
                    (op, expr) => {
                        self.session
                            .send(Err(QueryErrorBuilder::new()
                                .syntax_error(op.to_string() + expr.to_string().as_str())
                                .build()))
                            .expect("To Send Query Result to Client");
                        return Ok(());
                    }
                },
                expr @ Expr::BinaryOp { .. } => match ExpressionEvaluation::new(self.session.clone()).eval(expr) {
                    Ok(expr_result) => expr_result.value(),
                    Err(()) => return Ok(()),
                },
                expr => {
                    self.session
                        .send(Err(QueryErrorBuilder::new().syntax_error(expr.to_string()).build()))
                        .expect("To Send Query Result to Client");
                    return Ok(());
                }
            };

            to_update.push((column.to_owned(), value))
        }

        if !(self.storage.lock().unwrap()).schema_exists(&schema_name) {
            self.session
                .send(Err(QueryErrorBuilder::new().schema_does_not_exist(schema_name).build()))
                .expect("To Send Result to Client");
            return Ok(());
        }

        let all_columns = (self.storage.lock().unwrap()).table_columns(&schema_name, &table_name)?;
        let mut errors = Vec::new();
        let mut index_value_pairs = Vec::new();
        let mut non_existing_columns = BTreeSet::new();
        let mut column_exists = false;

        // only process the rows if the table and schema exist.
        if (self.storage.lock().unwrap()).table_exists(&schema_name, &table_name) {
            for (column_name, value) in to_update {
                for (index, column_definition) in all_columns.iter().enumerate() {
                    if column_definition.has_name(&column_name) {
                        match column_definition.sql_type().validate_and_serialize(value.as_str()) {
                            Ok(bytes) => {
                                index_value_pairs.push((index, bytes));
                            }
                            Err(e) => {
                                errors.push((e, column_definition.clone()));
                            }
                        }

                        column_exists = true;

                        break;
                    }
                }

                if !column_exists {
                    non_existing_columns.insert(column_name.clone());
                }
            }
        } else {
            self.session
                .send(Err(QueryErrorBuilder::new()
                    .table_does_not_exist(schema_name + "." + table_name.as_str())
                    .build()))
                .expect("To Send Result to Client");
            return Ok(());
        }

        if !non_existing_columns.is_empty() {
            self.session
                .send(Err(QueryErrorBuilder::new()
                    .column_does_not_exist(non_existing_columns.into_iter().collect())
                    .build()))
                .expect("To Send Result to Client");
            return Ok(());
        }
        if !errors.is_empty() {
            let row_index = 1;
            let mut builder = QueryErrorBuilder::new();
            let constraint_error_mapper = |(err, column_definition): &(ConstraintError, ColumnDefinition)| match err {
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

            errors.iter().for_each(constraint_error_mapper);
            self.session
                .send(Err(builder.build()))
                .expect("To Send Query Result to Client");
            return Ok(());
        }

        let to_update: Vec<Row> = (self.storage.lock().unwrap())
            .read_unchecked(&schema_name, &table_name)
            .expect("no system errors")
            .map(backend::Result::unwrap)
            .map(|(key, values)| {
                let mut values: Vec<&[u8]> = values.split(|b| *b == b'|').collect();
                for (index, updated_value) in &index_value_pairs {
                    values[*index] = updated_value;
                }

                (key, values.join(&b'|'))
            })
            .collect();

        match (self.storage.lock().unwrap()).update_all(&schema_name, &table_name, to_update)? {
            Ok(records_number) => {
                self.session
                    .send(Ok(QueryEvent::RecordsUpdated(records_number)))
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
                let constraint_error_mapper = |(err, column_definition): &(ConstraintError, ColumnDefinition)| match err
                {
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

                constraint_errors.iter().for_each(constraint_error_mapper);
                self.session
                    .send(Err(builder.build()))
                    .expect("To Send Query Result to Client");
                Ok(())
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
