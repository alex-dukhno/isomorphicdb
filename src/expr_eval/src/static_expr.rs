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
    scalar::{Operator, ScalarOp},
    values::ScalarValue,
};
use bigdecimal::BigDecimal;
use protocol::{results::QueryError, Sender};
use std::sync::Arc;

pub struct StaticExpressionEvaluation {
    session: Arc<dyn Sender>,
}

impl StaticExpressionEvaluation {
    pub fn new(session: Arc<dyn Sender>) -> StaticExpressionEvaluation {
        StaticExpressionEvaluation { session }
    }

    pub fn eval(&self, expr: &ScalarOp) -> Result<ScalarOp, ()> {
        self.inner_eval(expr)
    }

    fn inner_eval(&self, expr: &ScalarOp) -> Result<ScalarOp, ()> {
        match expr {
            ScalarOp::Binary(op, left, right) => {
                let left = self.inner_eval(&*left)?;
                let right = self.inner_eval(&*right)?;
                match (left, right) {
                    (ScalarOp::Value(ScalarValue::Number(left)), ScalarOp::Value(ScalarValue::Number(right))) => {
                        match op {
                            Operator::Plus => Ok(ScalarOp::Value(ScalarValue::Number(left + right))),
                            Operator::Minus => Ok(ScalarOp::Value(ScalarValue::Number(left - right))),
                            Operator::Multiply => Ok(ScalarOp::Value(ScalarValue::Number(left * right))),
                            Operator::Divide => Ok(ScalarOp::Value(ScalarValue::Number(left / right))),
                            Operator::Modulus => Ok(ScalarOp::Value(ScalarValue::Number(left % right))),
                            Operator::BitwiseAnd => {
                                let (left, _) = left.as_bigint_and_exponent();
                                let (right, _) = right.as_bigint_and_exponent();
                                Ok(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(left & &right))))
                            }
                            Operator::BitwiseOr => {
                                let (left, _) = left.as_bigint_and_exponent();
                                let (right, _) = right.as_bigint_and_exponent();
                                Ok(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(left | &right))))
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
                        }
                    }
                    (ScalarOp::Value(ScalarValue::String(left)), ScalarOp::Value(ScalarValue::String(right))) => {
                        match op {
                            Operator::StringConcat => Ok(ScalarOp::Value(ScalarValue::String(left + right.as_str()))),
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
                        }
                    }
                    (ScalarOp::Value(ScalarValue::Number(left)), ScalarOp::Value(ScalarValue::String(right))) => {
                        match op {
                            Operator::StringConcat => {
                                Ok(ScalarOp::Value(ScalarValue::String(left.to_string() + right.as_str())))
                            }
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
                        }
                    }
                    (ScalarOp::Value(ScalarValue::String(left)), ScalarOp::Value(ScalarValue::Number(right))) => {
                        match op {
                            Operator::StringConcat => {
                                Ok(ScalarOp::Value(ScalarValue::String(left + right.to_string().as_str())))
                            }
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
                        }
                    }
                    (_left, _right) => Err(()),
                }
            }
            ScalarOp::Value(value) => Ok(ScalarOp::Value(value.clone())),
            ScalarOp::Column(col_name) => Ok(ScalarOp::Column(col_name.clone())),
        }
    }
}
