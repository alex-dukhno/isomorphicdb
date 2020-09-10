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
use kernel::{SystemError, SystemResult};
use protocol::{
    results::{QueryError, QueryEvent},
    Sender,
};
use representation::Binary;
use sql_model::sql_types::ConstraintError;

use ast::{
    scalar::{Operator, ScalarOp},
    values::ScalarValue,
    Datum, EvalError,
};
use bigdecimal::BigDecimal;
use query_planner::plan::TableInserts;
use std::{convert::TryFrom, ops::Deref};

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
                let v = match evaluation.eval(col) {
                    Ok(v) => v,
                    Err(()) => return Err(SystemError::runtime_check_failure(&"something going wrong ¯\\_(ツ)_/¯")),
                };
                row.push(v);
            }
            rows.push(row);
        }

        let index_columns = &self.table_inserts.column_indices;

        let mut to_write: Vec<Row> = vec![];
        for (row_index, row) in rows.iter().enumerate() {
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

    pub(crate) fn eval(&mut self, expr: &ScalarOp) -> Result<ScalarValue, ()> {
        self.inner_eval(expr)
    }

    fn inner_eval(&mut self, expr: &ScalarOp) -> Result<ScalarValue, ()> {
        match expr {
            ScalarOp::Binary(op, left, right) => {
                let left = self.inner_eval(left.deref())?;
                let right = self.inner_eval(right.deref())?;
                match (left, right) {
                    (ScalarValue::Number(left), ScalarValue::Number(right)) => match op {
                        Operator::Plus => Ok(ScalarValue::Number(left + right)),
                        Operator::Minus => Ok(ScalarValue::Number(left - right)),
                        Operator::Multiply => Ok(ScalarValue::Number(left * right)),
                        Operator::Divide => Ok(ScalarValue::Number(left / right)),
                        Operator::Modulus => Ok(ScalarValue::Number(left % right)),
                        Operator::BitwiseAnd => {
                            let (left, _) = left.as_bigint_and_exponent();
                            let (right, _) = right.as_bigint_and_exponent();
                            Ok(ScalarValue::Number(BigDecimal::from(left & &right)))
                        }
                        Operator::BitwiseOr => {
                            let (left, _) = left.as_bigint_and_exponent();
                            let (right, _) = right.as_bigint_and_exponent();
                            Ok(ScalarValue::Number(BigDecimal::from(left | &right)))
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
                    (ScalarValue::String(left), ScalarValue::String(right)) => match op {
                        Operator::StringConcat => Ok(ScalarValue::String(left + right.as_str())),
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
                    (ScalarValue::Number(left), ScalarValue::String(right)) => match op {
                        Operator::StringConcat => Ok(ScalarValue::String(left.to_string() + right.as_str())),
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
                    (ScalarValue::String(left), ScalarValue::Number(right)) => match op {
                        Operator::StringConcat => Ok(ScalarValue::String(left + right.to_string().as_str())),
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
                    (_left, _right) => Err(()),
                }
            }
            ScalarOp::Value(value) => Ok(value.clone()),
            e => {
                self.session
                    .send(Err(QueryError::syntax_error(format!("{:?}", e))))
                    .expect("To Send Query Result to Client");
                Err(())
            }
        }
    }
}
