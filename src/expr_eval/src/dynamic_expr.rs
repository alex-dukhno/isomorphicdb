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

use ast::{
    operations::{BinaryOp, ScalarOp},
    values::ScalarValue,
};
use bigdecimal::BigDecimal;
use protocol::{results::QueryError, Sender};
use repr::Datum;
use std::{
    collections::HashMap,
    convert::{From, TryInto},
    sync::Arc,
};

pub struct DynamicExpressionEvaluation {
    sender: Arc<dyn Sender>,
    columns: HashMap<String, usize>,
}

impl<'a> DynamicExpressionEvaluation {
    pub fn new(sender: Arc<dyn Sender>, columns: HashMap<String, usize>) -> DynamicExpressionEvaluation {
        Self { sender, columns }
    }

    pub fn eval<'b>(&self, row: &[Datum<'b>], eval: &ScalarOp) -> Result<ScalarOp, ()> {
        self.inner_eval(row, eval)
    }

    fn inner_eval<'b>(&self, row: &[Datum<'b>], eval: &ScalarOp) -> Result<ScalarOp, ()> {
        match eval {
            ScalarOp::Column(column_name) => {
                let datum: &Datum = &(row[self.columns[column_name]]);
                match datum.try_into() {
                    Ok(value) => Ok(ScalarOp::Value(value)),
                    Err(_) => Err(()),
                }
            }
            ScalarOp::Binary(op, lhs, rhs) => {
                let left = self.eval(row, lhs.as_ref())?;
                let right = self.eval(row, rhs.as_ref())?;
                self.eval_binary_literal_expr(op.clone(), left, right)
            }
            ScalarOp::Value(value) => Ok(ScalarOp::Value(value.clone())),
        }
    }

    fn eval_binary_literal_expr(&self, op: BinaryOp, left: ScalarOp, right: ScalarOp) -> Result<ScalarOp, ()> {
        match (left, right) {
            (ScalarOp::Value(ScalarValue::Number(left)), ScalarOp::Value(ScalarValue::Number(right))) => match op {
                BinaryOp::Add => Ok(ScalarOp::Value(ScalarValue::Number(left + right))),
                BinaryOp::Sub => Ok(ScalarOp::Value(ScalarValue::Number(left - right))),
                BinaryOp::Mul => Ok(ScalarOp::Value(ScalarValue::Number(left * right))),
                BinaryOp::Div => Ok(ScalarOp::Value(ScalarValue::Number(left / right))),
                BinaryOp::BitwiseAnd => {
                    let (left, left_exp) = left.as_bigint_and_exponent();
                    let (right, right_exp) = right.as_bigint_and_exponent();
                    if left_exp != 0 && right_exp != 0 {
                        self.sender
                            .send(Err(QueryError::undefined_function(op, "FLOAT", "FLOAT")))
                            .expect("To Send Result to Client");
                        Err(())
                    } else {
                        Ok(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(left & &right))))
                    }
                }
                BinaryOp::Mod => Ok(ScalarOp::Value(ScalarValue::Number(left % right))),
                BinaryOp::BitwiseOr => {
                    let (left, left_exp) = left.as_bigint_and_exponent();
                    let (right, right_exp) = right.as_bigint_and_exponent();
                    if left_exp != 0 && right_exp != 0 {
                        self.sender
                            .send(Err(QueryError::undefined_function(op, "FLOAT", "FLOAT")))
                            .expect("To Send Result to Client");
                        Err(())
                    } else {
                        Ok(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(left | &right))))
                    }
                }
                _ => {
                    self.sender
                        .send(Err(QueryError::undefined_function(op, "NUMBER", "NUMBER")))
                        .expect("To Send Query Result to Client");
                    Err(())
                }
            },
            (ScalarOp::Value(ScalarValue::String(left)), ScalarOp::Value(ScalarValue::String(right))) => match op {
                BinaryOp::Concat => Ok(ScalarOp::Value(ScalarValue::String(left + right.as_str()))),
                operator => {
                    self.sender
                        .send(Err(QueryError::undefined_function(operator, "STRING", "STRING")))
                        .expect("To Send Query Result to Client");
                    Err(())
                }
            },
            (ScalarOp::Value(ScalarValue::Number(left)), ScalarOp::Value(ScalarValue::String(right))) => match op {
                BinaryOp::Concat => Ok(ScalarOp::Value(ScalarValue::String(format!("{}{}", left, right)))),
                _ => {
                    self.sender
                        .send(Err(QueryError::undefined_function(op, "NUMBER", "STRING")))
                        .expect("To Send Query Result to Client");
                    Err(())
                }
            },
            (ScalarOp::Value(ScalarValue::String(left)), ScalarOp::Value(ScalarValue::Number(right))) => match op {
                BinaryOp::Concat => Ok(ScalarOp::Value(ScalarValue::String(format!("{}{}", left, right)))),
                _ => {
                    self.sender
                        .send(Err(QueryError::undefined_function(op, "STRING", "NUMBER")))
                        .expect("To Send Query Result to Client");
                    Err(())
                }
            },
            (left, right) => Ok(ScalarOp::Binary(op, Box::new(left), Box::new(right))),
        }
    }
}
