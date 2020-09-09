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

use data_manager::{DataManager, Row};
use kernel::SystemResult;
use protocol::{
    results::{QueryError, QueryEvent},
    Sender,
};
use representation::{Binary, Datum, EvalError};
use sql_model::sql_types::ConstraintError;

use bigdecimal::BigDecimal;
use query_planner::plan::TableInserts;
use sqlparser::ast::{BinaryOperator, DataType, Expr, UnaryOperator, Value};
use std::convert::TryFrom;
use std::ops::Deref;
use std::str::FromStr;

pub(crate) struct InsertCommand {
    table_inserts: TableInserts,
    data_manager: Arc<DataManager>,
    sender: Arc<dyn Sender>,
}

impl InsertCommand {
    pub(crate) fn new(
        table_inserts: TableInserts,
        data_manager: Arc<DataManager>,
        sender: Arc<dyn Sender>,
    ) -> InsertCommand {
        InsertCommand {
            table_inserts,
            data_manager,
            sender,
        }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<()> {
        let mut evaluation = InsertExpressionEvaluation::new(self.sender.clone());
        let mut rows = vec![];
        for line in &self.table_inserts.input {
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
                            self.sender
                                .send(Err(QueryError::syntax_error(format!(
                                    "Cast from {:?} to {:?} is not currently supported",
                                    expr, data_type
                                ))))
                                .expect("To Send Query Result to Client");
                            return Ok(());
                        }
                    },
                    Expr::UnaryOp { op, expr } => match (op, &**expr) {
                        (UnaryOperator::Minus, Expr::Value(Value::Number(v))) => Value::Number(-v),
                        (op, expr) => {
                            self.sender
                                .send(Err(QueryError::syntax_error(
                                    op.to_string() + expr.to_string().as_str(),
                                )))
                                .expect("To Send Query Result to Client");
                            return Ok(());
                        }
                    },
                    expr @ Expr::BinaryOp { .. } => match evaluation.eval(expr) {
                        Ok(expr_result) => expr_result,
                        Err(()) => return Ok(()),
                    },
                    expr => {
                        self.sender
                            .send(Err(QueryError::syntax_error(expr.to_string())))
                            .expect("To Send Query Result to Client");
                        return Ok(());
                    }
                };
                row.push(v);
            }
            rows.push(row);
        }

        let index_columns = &self.table_inserts.column_indices;

        let mut to_write: Vec<Row> = vec![];
        let mut row_index = 0;
        for row in rows.iter() {
            if row.len() > self.table_inserts.column_indices.len() {
                self.sender
                    .send(Err(QueryError::too_many_insert_expressions()))
                    .expect("To Send Result to Client");
                return Ok(());
            }

            let key = self
                .data_manager
                .next_key_id(&self.table_inserts.table_id)
                .to_be_bytes()
                .to_vec();

            // TODO: The default value or NULL should be initialized for SQL types of all columns.
            let mut record = vec![Datum::from_null(); self.table_inserts.column_indices.len()];
            let mut errors = vec![];
            for (item, (index, column_definition)) in row.iter().zip(index_columns.iter()) {
                match Datum::try_from(item) {
                    Ok(datum) => {
                        match column_definition
                            .sql_type()
                            .constraint()
                            .validate(datum.to_string().as_str())
                        {
                            Ok(()) => {
                                record[*index] = datum;
                            }
                            Err(e) => {
                                errors.push((e, column_definition.clone()));
                            }
                        }
                    }
                    Err(EvalError::OutOfRangeNumeric(_)) => {
                        errors.push((ConstraintError::OutOfRange, column_definition.clone()))
                    }
                    Err(_) => panic!(),
                }
            }
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
                            &column_definition.name(),
                            row_index + 1,
                        ),
                        ConstraintError::ValueTooLong(len) => QueryError::string_length_mismatch(
                            (&column_definition.sql_type()).into(),
                            len,
                            &column_definition.name(),
                            row_index + 1,
                        ),
                    };
                    self.sender
                        .send(Err(error_to_send))
                        .expect("To Send Query Result to Client");
                }
                return Ok(());
            }
            to_write.push((Binary::with_data(key), Binary::pack(&record)));
            row_index += 1;
        }

        match self.data_manager.write_into(&self.table_inserts.table_id, to_write) {
            Err(error) => return Err(error),
            Ok(size) => self
                .sender
                .send(Ok(QueryEvent::RecordsInserted(size)))
                .expect("To Send Result to Client"),
        }

        Ok(())
    }
}

pub(crate) struct InsertExpressionEvaluation {
    session: Arc<dyn Sender>,
}

impl InsertExpressionEvaluation {
    pub(crate) fn new(session: Arc<dyn Sender>) -> InsertExpressionEvaluation {
        InsertExpressionEvaluation { session }
    }

    pub(crate) fn eval(&mut self, expr: &Expr) -> Result<Value, ()> {
        match self.inner_eval(expr)? {
            ExprResult::Number(v) => Ok(Value::Number(v)),
            ExprResult::String(v) => Ok(Value::SingleQuotedString(v)),
        }
    }

    fn inner_eval(&mut self, expr: &Expr) -> Result<ExprResult, ()> {
        if let Expr::BinaryOp { op, left, right } = expr {
            let left = self.inner_eval(left.deref())?;
            let right = self.inner_eval(right.deref())?;
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
                    operator => {
                        self.session
                            .send(Err(QueryError::undefined_function(
                                operator.to_string(),
                                "NUMBER".to_owned(),
                                "NUMBER".to_owned(),
                            )))
                            .expect("To Send Query Result to Client");
                        Err(())
                    }
                },
                (ExprResult::String(left), ExprResult::String(right)) => match op {
                    BinaryOperator::StringConcat => Ok(ExprResult::String(left + right.as_str())),
                    operator => {
                        self.session
                            .send(Err(QueryError::undefined_function(
                                operator.to_string(),
                                "STRING".to_owned(),
                                "STRING".to_owned(),
                            )))
                            .expect("To Send Query Result to Client");
                        Err(())
                    }
                },
                (ExprResult::Number(left), ExprResult::String(right)) => match op {
                    BinaryOperator::StringConcat => Ok(ExprResult::String(left.to_string() + right.as_str())),
                    operator => {
                        self.session
                            .send(Err(QueryError::undefined_function(
                                operator.to_string(),
                                "NUMBER".to_owned(),
                                "STRING".to_owned(),
                            )))
                            .expect("To Send Query Result to Client");
                        Err(())
                    }
                },
                (ExprResult::String(left), ExprResult::Number(right)) => match op {
                    BinaryOperator::StringConcat => Ok(ExprResult::String(left + right.to_string().as_str())),
                    operator => {
                        self.session
                            .send(Err(QueryError::undefined_function(
                                operator.to_string(),
                                "STRING".to_owned(),
                                "NUMBER".to_owned(),
                            )))
                            .expect("To Send Query Result to Client");
                        Err(())
                    }
                },
            }
        } else {
            match expr {
                Expr::Value(Value::Number(v)) => Ok(ExprResult::Number(v.clone())),
                Expr::Value(Value::SingleQuotedString(v)) => Ok(ExprResult::String(v.clone())),
                e => {
                    self.session
                        .send(Err(QueryError::syntax_error(e.to_string())))
                        .expect("To Send Query Result to Client");
                    Err(())
                }
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum ExprResult {
    Number(BigDecimal),
    String(String),
}
