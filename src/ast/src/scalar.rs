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

use sqlparser::ast::{BinaryOperator, Expr};

use crate::{values::ScalarValue, Datum, OperationError, ScalarError, ScalarType};
use std::{
    convert::TryFrom,
    fmt::{self, Display, Formatter},
};

/// Operation performed on the table
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarOp {
    /// column access
    Column(usize, ScalarType),
    /// should be used instead of `Literal`
    Value(ScalarValue),
    /// literal value (owned) and expected type.
    Literal(Datum<'static>),
    Binary(Operator, Box<ScalarOp>, Box<ScalarOp>),
    /// binary operator
    OldBinary(BinaryOperator, Box<ScalarOp>, Box<ScalarOp>, ScalarType),
    Assignment {
        destination: usize,
        value: Box<ScalarOp>,
        ty: ScalarType,
    },
}

impl ScalarOp {
    pub fn transform(expr: &Expr) -> Result<Result<ScalarOp, OperationError>, ScalarError> {
        match expr {
            cast @ Expr::Cast { .. } => Ok(ScalarValue::transform(cast)?.map(ScalarOp::Value)),
            value @ Expr::Value(_) => Ok(ScalarValue::transform(value)?.map(ScalarOp::Value)),
            unary @ Expr::UnaryOp { .. } => Ok(ScalarValue::transform(unary)?.map(ScalarOp::Value)),
            Expr::BinaryOp { left, op, right } => match Operator::try_from(op) {
                Ok(operator) => {
                    let l = match ScalarOp::transform(left)? {
                        Ok(scalar_op) => scalar_op,
                        Err(error) => return Ok(Err(error)),
                    };
                    let r = match ScalarOp::transform(right)? {
                        Ok(scalar_op) => scalar_op,
                        Err(error) => return Ok(Err(error)),
                    };
                    Ok(Ok(ScalarOp::Binary(operator, Box::new(l), Box::new(r))))
                }
                Err(()) => Err(ScalarError::NotHandled(Expr::BinaryOp {
                    left: Box::new(*left.clone()),
                    op: op.clone(),
                    right: Box::new(*right.clone()),
                })),
            },
            _ => Err(ScalarError::NotHandled(expr.clone())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operator {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulus,
    BitwiseAnd,
    BitwiseOr,
    StringConcat,
}

impl Display for Operator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Operator::Plus => write!(f, "+"),
            Operator::Minus => write!(f, "-"),
            Operator::Multiply => write!(f, "*"),
            Operator::Divide => write!(f, "/"),
            Operator::Modulus => write!(f, "%"),
            Operator::BitwiseAnd => write!(f, "&"),
            Operator::BitwiseOr => write!(f, "|"),
            Operator::StringConcat => write!(f, "||"),
        }
    }
}

impl TryFrom<&BinaryOperator> for Operator {
    type Error = ();

    fn try_from(value: &BinaryOperator) -> Result<Self, Self::Error> {
        match &*value {
            BinaryOperator::Plus => Ok(Operator::Plus),
            BinaryOperator::Minus => Ok(Operator::Minus),
            BinaryOperator::Multiply => Ok(Operator::Multiply),
            BinaryOperator::Divide => Ok(Operator::Divide),
            BinaryOperator::Modulus => Ok(Operator::Modulus),
            BinaryOperator::StringConcat => Ok(Operator::StringConcat),
            BinaryOperator::BitwiseOr => Ok(Operator::BitwiseOr),
            BinaryOperator::BitwiseAnd => Ok(Operator::BitwiseAnd),
            BinaryOperator::Gt => Err(()),
            BinaryOperator::Lt => Err(()),
            BinaryOperator::GtEq => Err(()),
            BinaryOperator::LtEq => Err(()),
            BinaryOperator::Eq => Err(()),
            BinaryOperator::NotEq => Err(()),
            BinaryOperator::And => Err(()),
            BinaryOperator::Or => Err(()),
            BinaryOperator::Like => Err(()),
            BinaryOperator::NotLike => Err(()),
            BinaryOperator::BitwiseXor => Err(()),
        }
    }
}

impl ScalarOp {
    pub fn is_literal(&self) -> bool {
        match self {
            ScalarOp::Literal(_) => true,
            _ => false,
        }
    }

    pub fn as_datum(&self) -> Option<Datum<'static>> {
        match self {
            ScalarOp::Literal(datum) => Some(datum.clone()),
            _ => None,
        }
    }

    pub fn scalar_type(&self) -> ScalarType {
        match self {
            ScalarOp::Column(_, ty) => *ty,
            ScalarOp::Literal(datum) => datum.scalar_type().unwrap(),
            ScalarOp::Binary(_, _, _) => ScalarType::String,
            ScalarOp::OldBinary(_, _, _, ty) => *ty,
            ScalarOp::Assignment { ty, .. } => *ty,
            ScalarOp::Value(_) => ScalarType::String,
        }
    }
}
