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

use kernel::SystemResult;
use protocol::results::{ConstraintViolation, QueryError, QueryEvent, QueryResult};
use sql_types::ConstraintError;
use sqlparser::ast::{Assignment, Expr, Ident, ObjectName, UnaryOperator, Value};
use std::sync::{Arc, Mutex};
use storage::{backend::BackendStorage, frontend::FrontendStorage, OperationOnTableError};

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

        let to_update: Vec<(String, String)> = self
            .assignments
            .iter()
            .map(|item| {
                let Assignment { id, value } = &item;
                let Ident { value: column, .. } = id;

                let value = match value {
                    Expr::Value(Value::Number(val)) => val.to_string(),
                    Expr::Value(Value::SingleQuotedString(v)) => v.to_string(),
                    Expr::UnaryOp { op, expr } => match (op, &**expr) {
                        (UnaryOperator::Minus, Expr::Value(Value::Number(v))) => {
                            "-".to_owned() + v.to_string().as_str()
                        }
                        (op, expr) => unimplemented!("{:?} {:?} is not currently supported", op, expr),
                    },
                    expr => unimplemented!("{:?} is not currently supported", expr),
                };

                (column.to_owned(), value)
            })
            .collect();

        match (self.storage.lock().unwrap()).update_all(&schema_name, &table_name, to_update)? {
            Ok(records_number) => Ok(Ok(QueryEvent::RecordsUpdated(records_number))),
            Err(OperationOnTableError::SchemaDoesNotExist) => Ok(Err(QueryError::schema_does_not_exist(schema_name))),
            Err(OperationOnTableError::TableDoesNotExist) => Ok(Err(QueryError::table_does_not_exist(
                schema_name + "." + table_name.as_str(),
            ))),
            Err(OperationOnTableError::ColumnDoesNotExist(non_existing_columns)) => {
                Ok(Err(QueryError::column_does_not_exist(non_existing_columns)))
            }
            Err(OperationOnTableError::ConstraintViolation(constraint_errors)) => {
                let mut violations = Vec::new();
                for (err, infos) in constraint_errors.into_iter() {
                    for info in infos {
                        for (_, sql_type) in info {
                            let violation = match err {
                                ConstraintError::OutOfRange => {
                                    ConstraintViolation::out_of_range(sql_type.to_pg_types())
                                }
                                ConstraintError::NotAnInt => ConstraintViolation::type_mismatch(sql_type.to_pg_types()),
                                ConstraintError::NotABool => ConstraintViolation::type_mismatch(sql_type.to_pg_types()),
                                ConstraintError::ValueTooLong => {
                                    if let Some(len) = sql_type.string_type_length() {
                                        ConstraintViolation::string_length_mismatch(sql_type.to_pg_types(), len)
                                    } else {
                                        // there error should only occur with string types
                                        unreachable!()
                                    }
                                }
                            };

                            violations.push(violation);
                        }
                    }
                }
                Ok(Err(QueryError::constraint_violations(violations)))
            }
            _ => Ok(Err(QueryError::not_supported_operation(self.raw_sql_query.to_owned()))),
        }
    }
}
