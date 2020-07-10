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
use protocol::results::{QueryErrorBuilder, QueryEvent, QueryResult};
use sql_types::{ConstraintError, SqlType};
use sqlparser::ast::{BinaryOperator, DataType, Expr, Ident, ObjectName, Query, SetExpr, UnaryOperator, Value};
use std::{
    ops::Deref,
    sync::{Arc, Mutex},
};
use storage::{backend::BackendStorage, frontend::FrontendStorage, OperationOnTableError};

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

            let rows: Vec<Vec<String>> = values
                .iter()
                .map(|v| {
                    v.iter()
                        .map(|v| match v {
                            Expr::Value(Value::Number(v)) => v.to_string(),
                            Expr::Value(Value::SingleQuotedString(v)) => v.to_string(),
                            Expr::Value(Value::Boolean(v)) => v.to_string(),
                            Expr::Cast { expr, data_type } => match (&**expr, data_type) {
                                (Expr::Value(Value::Boolean(v)), DataType::Boolean) => v.to_string(),
                                (Expr::Value(Value::SingleQuotedString(v)), DataType::Boolean) => v.to_string(),
                                _ => {
                                    unimplemented!("Cast from {:?} to {:?} is not currently supported", expr, data_type)
                                }
                            },
                            Expr::UnaryOp { op, expr } => match (op, &**expr) {
                                (UnaryOperator::Minus, Expr::Value(Value::Number(v))) => {
                                    "-".to_owned() + v.to_string().as_str()
                                }
                                (op, expr) => unimplemented!("{:?} {:?} is not currently supported", op, expr),
                            },
                            expr @ Expr::BinaryOp { .. } => Self::eval(expr).value(),
                            expr => unimplemented!("{:?} is not currently supported", expr),
                        })
                        .collect()
                })
                .collect();

            let len = rows.len();
            match (self.storage.lock().unwrap()).insert_into(&schema_name, &table_name, columns, rows)? {
                Ok(_) => Ok(Ok(QueryEvent::RecordsInserted(len))),
                Err(OperationOnTableError::SchemaDoesNotExist) => {
                    Ok(Err(QueryErrorBuilder::new().schema_does_not_exist(schema_name).build()))
                }
                Err(OperationOnTableError::TableDoesNotExist) => Ok(Err(QueryErrorBuilder::new()
                    .table_does_not_exist(schema_name + "." + table_name.as_str())
                    .build())),
                Err(OperationOnTableError::ColumnDoesNotExist(non_existing_columns)) => {
                    Ok(Err(QueryErrorBuilder::new()
                        .column_does_not_exist(non_existing_columns)
                        .build()))
                }
                Err(OperationOnTableError::ConstraintViolations(constraint_errors)) => {
                    let mut builder = QueryErrorBuilder::new();
                    let constraint_error_mapper = |(err, _, sql_type): &(ConstraintError, String, SqlType)| match err {
                        ConstraintError::OutOfRange => {
                            builder.out_of_range(sql_type.to_pg_types());
                        }
                        ConstraintError::TypeMismatch(value) => {
                            builder.type_mismatch(value, sql_type.to_pg_types());
                        }
                        ConstraintError::ValueTooLong(len) => {
                            builder.string_length_mismatch(sql_type.to_pg_types(), *len);
                        }
                    };

                    constraint_errors.iter().for_each(constraint_error_mapper);
                    Ok(Err(builder.build()))
                }
                Err(e) => {
                    eprintln!("{:?}", e);
                    unimplemented!()
                }
            }
        } else {
            Ok(Err(QueryErrorBuilder::new()
                .not_supported_operation(self.raw_sql_query.to_owned())
                .build()))
        }
    }

    fn eval(expr: &Expr) -> ExprResult {
        if let Expr::BinaryOp { op, left, right } = expr {
            let left: &Expr = left.deref();
            let right: &Expr = right.deref();
            match (left, right) {
                (Expr::Value(Value::Number(left)), Expr::Value(Value::Number(right))) => match op {
                    BinaryOperator::Plus => ExprResult::Number((left + right).to_string()),
                    BinaryOperator::Minus => ExprResult::Number((left - right).to_string()),
                    BinaryOperator::Multiply => ExprResult::Number((left * right).to_string()),
                    BinaryOperator::Divide => ExprResult::Number((left / right).to_string()),
                    BinaryOperator::Modulus => ExprResult::Number((left % right).to_string()),
                    BinaryOperator::BitwiseAnd => {
                        let (left, _) = left.as_bigint_and_exponent();
                        let (right, _) = right.as_bigint_and_exponent();
                        ExprResult::Number((left & right).to_string())
                    }
                    BinaryOperator::BitwiseOr => {
                        let (left, _) = left.as_bigint_and_exponent();
                        let (right, _) = right.as_bigint_and_exponent();
                        ExprResult::Number((left | right).to_string())
                    }
                    _ => unimplemented!(),
                },
                e => unimplemented!("{:?} not supported", e),
            }
        } else {
            unimplemented!("{:?} not supported", expr)
        }
    }
}

enum ExprResult {
    Number(String),
}

impl ExprResult {
    fn value(self) -> String {
        match self {
            Self::Number(v) => v,
        }
    }
}
