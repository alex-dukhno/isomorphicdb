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

use std::convert::TryFrom;

use ast::{
    scalar::{Operator, ScalarOp},
    Datum, ScalarType,
};
use protocol::{results::QueryError, Sender};
use sql_model::sql_types::SqlType;
use std::collections::HashMap;

pub fn compatible_types_for_op(op: Operator, lhs_type: ScalarType, rhs_type: ScalarType) -> Option<ScalarType> {
    if lhs_type == rhs_type {
        if lhs_type.is_integer() {
            match op {
                Operator::Plus
                | Operator::Minus
                | Operator::Multiply
                | Operator::Divide
                | Operator::Modulus
                | Operator::BitwiseAnd
                | Operator::BitwiseOr => Some(lhs_type),
                Operator::StringConcat => Some(ScalarType::String),
            }
        } else if lhs_type.is_float() {
            match op {
                Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide => Some(lhs_type),
                _ => None,
            }
        } else if lhs_type.is_string() {
            match op {
                Operator::StringConcat => Some(ScalarType::String),
                _ => None,
            }
        } else {
            None
        }
    } else if (lhs_type.is_string() && rhs_type.is_integer()) || (lhs_type.is_integer() && rhs_type.is_string()) {
        match op {
            Operator::StringConcat => Some(ScalarType::String),
            _ => None,
        }
    } else {
        None
    }
}

pub struct DynamicExpressionEvaluation<'a> {
    session: &'a dyn Sender,
    columns: HashMap<String, (usize, SqlType)>,
}

impl<'a> DynamicExpressionEvaluation<'a> {
    pub fn new(session: &'a dyn Sender, columns: HashMap<String, (usize, SqlType)>) -> Self {
        Self { session, columns }
    }

    pub fn columns(&self) -> &HashMap<String, (usize, SqlType)> {
        &self.columns
    }

    pub fn eval<'b>(&self, row: &[Datum<'b>], eval: &ScalarOp, index: usize) -> Result<Datum<'b>, ()> {
        match eval {
            ScalarOp::Column(_column_name) => Ok(row[index].clone()),
            ScalarOp::Binary(op, lhs, rhs) => {
                let left = self.eval(row, lhs.as_ref(), index)?;
                let right = self.eval(row, rhs.as_ref(), index)?;
                Self::eval_binary_literal_expr(self.session, op.clone(), left, right)
            }
            ScalarOp::Value(value) => Datum::try_from(value).map_err(|_| ()),
        }
    }

    pub fn eval_binary_literal_expr<'b>(
        session: &dyn Sender,
        op: Operator,
        left: Datum<'b>,
        right: Datum<'b>,
    ) -> Result<Datum<'b>, ()> {
        if left.is_integer() && right.is_integer() {
            match op {
                Operator::Plus => Ok(left + right),
                Operator::Minus => Ok(left - right),
                Operator::Multiply => Ok(left * right),
                Operator::Divide => Ok(left / right),
                Operator::Modulus => Ok(left % right),
                Operator::BitwiseAnd => Ok(left & right),
                Operator::BitwiseOr => Ok(left | right),
                Operator::StringConcat => {
                    let kind = QueryError::undefined_function(op.to_string(), "NUMBER".to_owned(), "NUMBER".to_owned());
                    session.send(Err(kind)).expect("To Send Query Result to Client");
                    Err(())
                }
            }
        } else if left.is_float() && right.is_float() {
            match op {
                Operator::Plus => Ok(left + right),
                Operator::Minus => Ok(left - right),
                Operator::Multiply => Ok(left * right),
                Operator::Divide => Ok(left / right),
                Operator::StringConcat => {
                    let kind = QueryError::undefined_function(op.to_string(), "NUMBER".to_owned(), "NUMBER".to_owned());
                    session.send(Err(kind)).expect("To Send Query Result to Client");
                    Err(())
                }
                _ => panic!(),
            }
        } else if left.is_string() || right.is_string() {
            match op {
                Operator::StringConcat => {
                    let value = format!("{}{}", left.to_string(), right.to_string());
                    Ok(Datum::OwnedString(value))
                }
                _ => {
                    let kind = QueryError::undefined_function(op.to_string(), "STRING".to_owned(), "STRING".to_owned());
                    session.send(Err(kind)).expect("To Send Query Result to Client");
                    Err(())
                }
            }
        } else {
            session
                .send(Err(QueryError::syntax_error(format!(
                    "{} {} {}",
                    left.to_string(),
                    op.to_string(),
                    right.to_string()
                ))))
                .expect("To Send Query Result to Client");
            Err(())
        }
    }
}
