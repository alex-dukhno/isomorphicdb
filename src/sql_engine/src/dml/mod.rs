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
use protocol::{results::QueryError, Sender};
use sqlparser::ast::{BinaryOperator, Expr, Value};
use std::{ops::Deref, sync::Arc};

pub(crate) mod delete;
pub(crate) mod insert;
pub(crate) mod select;
pub(crate) mod update;

pub(crate) struct ExpressionEvaluation {
    session: Arc<dyn Sender>,
}

impl ExpressionEvaluation {
    pub(crate) fn new(session: Arc<dyn Sender>) -> ExpressionEvaluation {
        ExpressionEvaluation { session }
    }

    pub(crate) fn eval(&self, expr: &Expr) -> Result<Value, ()> {
        match self.inner_eval(expr)? {
            ExprResult::Number(v) => Ok(Value::Number(v)),
            ExprResult::String(v) => Ok(Value::SingleQuotedString(v)),
        }
    }

    fn inner_eval(&self, expr: &Expr) -> Result<ExprResult, ()> {
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
