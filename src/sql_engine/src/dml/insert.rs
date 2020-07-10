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

use bigdecimal::BigDecimal;
use kernel::SystemResult;
use protocol::results::{ConstraintViolation, QueryError, QueryEvent, QueryResult};
use sql_types::ConstraintError;
use sqlparser::ast::{BinaryOperator, DataType, Expr, Ident, ObjectName, Query, SetExpr, UnaryOperator, Value};
use std::{
    ops::Deref,
    sync::{Arc, Mutex},
};
use storage::{backend::BackendStorage, frontend::FrontendStorage, ColumnDefinition, OperationOnTableError};

pub(crate) struct InsertCommand<'q, P: BackendStorage> {
    raw_sql_query: &'q str,
    name: ObjectName,
    columns: Vec<Ident>,
    source: Box<Query>,
    storage: Arc<Mutex<FrontendStorage<P>>>,
}

impl<P: BackendStorage> InsertCommand<'_, P> {
    pub(crate) fn new(
        raw_sql_query: &'_ str,
        name: ObjectName,
        columns: Vec<Ident>,
        source: Box<Query>,
        storage: Arc<Mutex<FrontendStorage<P>>>,
    ) -> InsertCommand<P> {
        InsertCommand {
            raw_sql_query,
            name,
            columns,
            source,
            storage,
        }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<QueryResult> {
        let table_name = self.name.0.pop().unwrap().to_string();
        let schema_name = self.name.0.pop().unwrap().to_string();
        let Query { body, .. } = &*self.source;
        if let SetExpr::Values(values) = &body {
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

            let mut rows = vec![];
            for line in values {
                let mut row = vec![];
                for col in line {
                    let v = match col {
                        Expr::Value(Value::Number(v)) => v.to_string(),
                        Expr::Value(Value::SingleQuotedString(v)) => v.to_string(),
                        Expr::Value(Value::Boolean(v)) => v.to_string(),
                        Expr::Cast { expr, data_type } => match (&**expr, data_type) {
                            (Expr::Value(Value::Boolean(v)), DataType::Boolean) => v.to_string(),
                            (Expr::Value(Value::SingleQuotedString(v)), DataType::Boolean) => v.to_string(),
                            _ => unimplemented!("Cast from {:?} to {:?} is not currently supported", expr, data_type),
                        },
                        Expr::UnaryOp { op, expr } => match (op, &**expr) {
                            (UnaryOperator::Minus, Expr::Value(Value::Number(v))) => {
                                "-".to_owned() + v.to_string().as_str()
                            }
                            (op, expr) => {
                                return Ok(Err(QueryError::syntax_error(
                                    op.to_string() + expr.to_string().as_str(),
                                )))
                            }
                        },
                        expr @ Expr::BinaryOp { .. } => match Self::eval(expr) {
                            Ok(expr_result) => expr_result.value(),
                            Err(e) => return Ok(Err(e)),
                        },
                        expr => return Ok(Err(QueryError::syntax_error(expr.to_string()))),
                    };
                    row.push(v);
                }
                rows.push(row);
            }

            let len = rows.len();
            match (self.storage.lock().unwrap()).insert_into(&schema_name, &table_name, columns, rows)? {
                Ok(_) => Ok(Ok(QueryEvent::RecordsInserted(len))),
                Err(OperationOnTableError::SchemaDoesNotExist) => {
                    Ok(Err(QueryError::schema_does_not_exist(schema_name)))
                }
                Err(OperationOnTableError::TableDoesNotExist) => Ok(Err(QueryError::table_does_not_exist(
                    schema_name + "." + table_name.as_str(),
                ))),
                Err(OperationOnTableError::ColumnDoesNotExist(non_existing_columns)) => {
                    Ok(Err(QueryError::column_does_not_exist(non_existing_columns)))
                }
                Err(OperationOnTableError::ConstraintViolations(constraint_errors)) => {
                    let constraint_error_mapper =
                        |(err, column_definition): &(ConstraintError, ColumnDefinition)| -> ConstraintViolation {
                            let sql_type = column_definition.sql_type();
                            match err {
                                ConstraintError::OutOfRange => {
                                    ConstraintViolation::out_of_range(sql_type.to_pg_types())
                                }
                                ConstraintError::TypeMismatch(value) => {
                                    ConstraintViolation::type_mismatch(value, sql_type.to_pg_types())
                                }
                                ConstraintError::ValueTooLong(len) => {
                                    ConstraintViolation::string_length_mismatch(sql_type.to_pg_types(), *len)
                                }
                            }
                        };

                    let violations = constraint_errors.iter().map(constraint_error_mapper).collect();

                    Ok(Err(QueryError::constraint_violations(violations)))
                }
                Err(e) => {
                    eprintln!("{:?}", e);
                    unimplemented!()
                }
            }
        } else {
            Ok(Err(QueryError::not_supported_operation(self.raw_sql_query.to_owned())))
        }
    }

    fn eval(expr: &Expr) -> Result<ExprResult, QueryError> {
        if let Expr::BinaryOp { op, left, right } = expr {
            let left = Self::eval(left.deref())?;
            let right = Self::eval(right.deref())?;
            match (left, right) {
                (ExprResult::Number(left), ExprResult::Number(right)) => match op {
                    BinaryOperator::Plus => Ok(ExprResult::Number(left + right)),
                    BinaryOperator::Minus => Ok(ExprResult::Number(left - right)),
                    BinaryOperator::Multiply => Ok(ExprResult::Number(left * right)),
                    BinaryOperator::Divide => Ok(ExprResult::Number(left / right)),
                    BinaryOperator::Modulus => Ok(ExprResult::Number(left % right)),
                    BinaryOperator::BitwiseAnd => {
                        let (left, _) = left.as_bigint_and_exponent();
                        let (right, _) = right.as_bigint_and_exponent();
                        Ok(ExprResult::Number(BigDecimal::from(left & &right)))
                    }
                    BinaryOperator::BitwiseOr => {
                        let (left, _) = left.as_bigint_and_exponent();
                        let (right, _) = right.as_bigint_and_exponent();
                        Ok(ExprResult::Number(BigDecimal::from(left | &right)))
                    }
                    operator => Err(QueryError::undefined_function(
                        operator.to_string(),
                        "NUMBER".to_owned(),
                        "NUMBER".to_owned(),
                    )),
                },
                (ExprResult::String(left), ExprResult::String(right)) => match op {
                    BinaryOperator::StringConcat => Ok(ExprResult::String(left + right.as_str())),
                    operator => Err(QueryError::undefined_function(
                        operator.to_string(),
                        "STRING".to_owned(),
                        "STRING".to_owned(),
                    )),
                },
                (ExprResult::Number(left), ExprResult::String(right)) => match op {
                    BinaryOperator::StringConcat => Ok(ExprResult::String(left.to_string() + right.as_str())),
                    operator => Err(QueryError::undefined_function(
                        operator.to_string(),
                        "NUMBER".to_owned(),
                        "STRING".to_owned(),
                    )),
                },
                (ExprResult::String(left), ExprResult::Number(right)) => match op {
                    BinaryOperator::StringConcat => Ok(ExprResult::String(left + right.to_string().as_str())),
                    operator => Err(QueryError::undefined_function(
                        operator.to_string(),
                        "STRING".to_owned(),
                        "NUMBER".to_owned(),
                    )),
                },
            }
        } else {
            match expr {
                Expr::Value(Value::Number(v)) => Ok(ExprResult::Number(v.clone())),
                Expr::Value(Value::SingleQuotedString(v)) => Ok(ExprResult::String(v.clone())),
                e => Err(QueryError::syntax_error(e.to_string())),
            }
        }
    }
}

#[derive(Debug)]
enum ExprResult {
    Number(BigDecimal),
    String(String),
}

impl ExprResult {
    fn value(self) -> String {
        match self {
            Self::Number(v) => v.to_string(),
            Self::String(v) => v,
        }
    }
}
