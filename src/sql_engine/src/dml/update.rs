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
use protocol::results::{QueryErrorBuilder, QueryEvent, QueryResult};
use sql_types::ConstraintError;
use sqlparser::ast::{Assignment, Expr, Ident, ObjectName, UnaryOperator, Value};
use std::sync::{Arc, Mutex};
use storage::{backend::BackendStorage, frontend::FrontendStorage, ColumnDefinition, OperationOnTableError};

pub(crate) struct UpdateCommand<'q, P: BackendStorage> {
    raw_sql_query: &'q str,
    name: ObjectName,
    assignments: Vec<Assignment>,
    storage: Arc<Mutex<FrontendStorage<P>>>,
}

impl<P: BackendStorage> UpdateCommand<'_, P> {
    pub(crate) fn new(
        raw_sql_query: &'_ str,
        name: ObjectName,
        assignments: Vec<Assignment>,
        storage: Arc<Mutex<FrontendStorage<P>>>,
    ) -> UpdateCommand<P> {
        UpdateCommand {
            raw_sql_query,
            name,
            assignments,
            storage,
        }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<QueryResult> {
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
                        return Ok(Err(QueryErrorBuilder::new()
                            .syntax_error(op.to_string() + expr.to_string().as_str())
                            .build()))
                    }
                },
                expr @ Expr::BinaryOp { .. } => match ExpressionEvaluation::eval(expr) {
                    Ok(expr_result) => expr_result.value(),
                    Err(e) => return Ok(Err(e)),
                },
                expr => return Ok(Err(QueryErrorBuilder::new().syntax_error(expr.to_string()).build())),
            };

            to_update.push((column.to_owned(), value))
        }

        match (self.storage.lock().unwrap()).update_all(&schema_name, &table_name, to_update)? {
            Ok(records_number) => Ok(Ok(QueryEvent::RecordsUpdated(records_number))),
            Err(OperationOnTableError::SchemaDoesNotExist) => {
                Ok(Err(QueryErrorBuilder::new().schema_does_not_exist(schema_name).build()))
            }
            Err(OperationOnTableError::TableDoesNotExist) => Ok(Err(QueryErrorBuilder::new()
                .table_does_not_exist(schema_name + "." + table_name.as_str())
                .build())),
            Err(OperationOnTableError::ColumnDoesNotExist(non_existing_columns)) => Ok(Err(QueryErrorBuilder::new()
                .column_does_not_exist(non_existing_columns)
                .build())),
            Err(OperationOnTableError::ConstraintViolations(constraint_errors)) => {
                let mut builder = QueryErrorBuilder::new();
                let constraint_error_mapper = |(err, column_definition): &(ConstraintError, ColumnDefinition)| match err
                {
                    ConstraintError::OutOfRange => {
                        builder.out_of_range(column_definition.sql_type().to_pg_types());
                    }
                    ConstraintError::TypeMismatch(value) => {
                        builder.type_mismatch(value, column_definition.sql_type().to_pg_types());
                    }
                    ConstraintError::ValueTooLong(len) => {
                        builder.string_length_mismatch(column_definition.sql_type().to_pg_types(), *len);
                    }
                };

                constraint_errors.iter().for_each(constraint_error_mapper);
                Ok(Err(builder.build()))
            }
            _ => Ok(Err(QueryErrorBuilder::new()
                .feature_not_supported(self.raw_sql_query.to_owned())
                .build())),
        }
    }
}
