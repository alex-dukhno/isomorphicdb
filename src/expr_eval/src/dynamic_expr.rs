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
    operations::{BinaryOp, ScalarOp},
    Datum,
};
use protocol::{results::QueryError, Sender};
use std::{collections::HashMap, sync::Arc};

pub struct DynamicExpressionEvaluation {
    sender: Arc<dyn Sender>,
    columns: HashMap<String, usize>,
}

impl<'a> DynamicExpressionEvaluation {
    pub fn new(sender: Arc<dyn Sender>, columns: HashMap<String, usize>) -> DynamicExpressionEvaluation {
        Self { sender, columns }
    }

    pub fn eval<'b>(&self, row: &[Datum<'b>], eval: &ScalarOp) -> Result<Datum<'b>, ()> {
        self.inner_eval(row, eval)
    }

    fn inner_eval<'b>(&self, row: &[Datum<'b>], eval: &ScalarOp) -> Result<Datum<'b>, ()> {
        match eval {
            ScalarOp::Column(column_name) => Ok(row[self.columns[column_name]].clone()),
            ScalarOp::Binary(op, lhs, rhs) => {
                let left = self.eval(row, lhs.as_ref())?;
                let right = self.eval(row, rhs.as_ref())?;
                self.eval_binary_literal_expr(op.clone(), left, right)
            }
            ScalarOp::Value(value) => Datum::try_from(value).map_err(|_| ()),
        }
    }

    fn eval_binary_literal_expr<'b>(&self, op: BinaryOp, left: Datum<'b>, right: Datum<'b>) -> Result<Datum<'b>, ()> {
        if left.is_integer() && right.is_integer() {
            match op {
                BinaryOp::Add => Ok(left + right),
                BinaryOp::Sub => Ok(left - right),
                BinaryOp::Mul => Ok(left * right),
                BinaryOp::Div => Ok(left / right),
                BinaryOp::Mod => Ok(left % right),
                BinaryOp::BitwiseAnd => Ok(left & right),
                BinaryOp::BitwiseOr => Ok(left | right),
                _ => {
                    let kind =
                        QueryError::undefined_function(op.to_string(), "INTEGER".to_owned(), "INTEGER".to_owned());
                    self.sender.send(Err(kind)).expect("To Send Query Result to Client");
                    Err(())
                }
            }
        } else if left.is_float() && right.is_float() {
            match op {
                BinaryOp::Add => Ok(left + right),
                BinaryOp::Sub => Ok(left - right),
                BinaryOp::Mul => Ok(left * right),
                BinaryOp::Div => Ok(left / right),
                _ => {
                    let kind = QueryError::undefined_function(op.to_string(), "FLOAT".to_owned(), "FLOAT".to_owned());
                    self.sender.send(Err(kind)).expect("To Send Query Result to Client");
                    Err(())
                }
            }
        } else if left.is_string() || right.is_string() {
            match op {
                BinaryOp::Concat => {
                    let value = format!("{}{}", left.to_string(), right.to_string());
                    Ok(Datum::OwnedString(value))
                }
                _ => {
                    let kind = QueryError::undefined_function(op.to_string(), "STRING".to_owned(), "STRING".to_owned());
                    self.sender.send(Err(kind)).expect("To Send Query Result to Client");
                    Err(())
                }
            }
        } else {
            self.sender
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
