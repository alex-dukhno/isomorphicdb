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
use protocol::results::{QueryError, QueryErrorBuilder};
use sqlparser::ast::{BinaryOperator, Expr, Value};
use std::ops::Deref;

pub(crate) mod delete;
pub(crate) mod insert;
pub(crate) mod select;
pub(crate) mod update;

pub(crate) struct ExpressionEvaluation;

impl ExpressionEvaluation {
    pub(crate) fn eval(expr: &Expr) -> Result<ExprResult, QueryError> {
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
                    operator => Err(QueryErrorBuilder::new()
                        .undefined_function(operator.to_string(), "NUMBER".to_owned(), "NUMBER".to_owned())
                        .build()),
                },
                (ExprResult::String(left), ExprResult::String(right)) => match op {
                    BinaryOperator::StringConcat => Ok(ExprResult::String(left + right.as_str())),
                    operator => Err(QueryErrorBuilder::new()
                        .undefined_function(operator.to_string(), "STRING".to_owned(), "STRING".to_owned())
                        .build()),
                },
                (ExprResult::Number(left), ExprResult::String(right)) => match op {
                    BinaryOperator::StringConcat => Ok(ExprResult::String(left.to_string() + right.as_str())),
                    operator => Err(QueryErrorBuilder::new()
                        .undefined_function(operator.to_string(), "NUMBER".to_owned(), "STRING".to_owned())
                        .build()),
                },
                (ExprResult::String(left), ExprResult::Number(right)) => match op {
                    BinaryOperator::StringConcat => Ok(ExprResult::String(left + right.to_string().as_str())),
                    operator => Err(QueryErrorBuilder::new()
                        .undefined_function(operator.to_string(), "STRING".to_owned(), "NUMBER".to_owned())
                        .build()),
                },
            }
        } else {
            match expr {
                Expr::Value(Value::Number(v)) => Ok(ExprResult::Number(v.clone())),
                Expr::Value(Value::SingleQuotedString(v)) => Ok(ExprResult::String(v.clone())),
                e => Err(QueryErrorBuilder::new().syntax_error(e.to_string()).build()),
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum ExprResult {
    Number(BigDecimal),
    String(String),
}

impl ExprResult {
    pub(crate) fn value(self) -> String {
        match self {
            Self::Number(v) => v.to_string(),
            Self::String(v) => v,
        }
    }
}
