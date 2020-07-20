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

use crate::query::repr::Datum;
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value};
use std::convert::TryFrom;

#[derive(Debug, Clone)]
pub enum EvalError {
    InvalidExpressionInStaticContext,
    UnsupportedDatum(String),
    OutOfRangeNumeric,
    UnsupportedOperation,
}

impl<'a> TryFrom<&Value> for Datum<'a> {
    type Error = EvalError;

    fn try_from(other: &Value) -> Result<Self, EvalError> {
        use Value::*;
        match other {
            Number(val) => {
                use crate::bigdecimal::ToPrimitive;
                if val.is_integer() {
                    if let Some(val) = val.to_i32() {
                        Ok(Datum::from_i32(val))
                    } else if let Some(val) = val.to_i64() {
                        Ok(Datum::from_i64(val))
                    } else {
                        Err(EvalError::OutOfRangeNumeric)
                    }
                } else {
                    if let Some(val) = val.to_f32() {
                        Ok(Datum::from_f32(val))
                    } else if let Some(val) = val.to_f64() {
                        Ok(Datum::from_f64(val))
                    } else {
                        Err(EvalError::OutOfRangeNumeric)
                    }
                }
            }
            SingleQuotedString(value) => Ok(Datum::from_string(value.clone())),
            NationalStringLiteral(value) => Err(EvalError::UnsupportedDatum("NationalStringLiteral".to_string())),
            HexStringLiteral(value) => match i64::from_str_radix(value.as_str(), 16) {
                Ok(val) => Ok(Datum::from_i64(val)),
                Err(_) => panic!("Failed to parse hex string"),
            },
            Boolean(val) => Ok(Datum::from_bool(*val)),
            Interval { .. } => Err(EvalError::UnsupportedDatum("Interval".to_string())),
            Null => Ok(Datum::from_null()),
        }
    }
}

// this must be improved later when we know what we are doing...
pub fn resolve_static_expr<'a>(expr: &'a Expr) -> Result<Datum<'a>, EvalError> {
    use Expr::*;
    match expr {
        BinaryOp { left, op, right } => {
            /*
                        let resolved_left = resolve_static_expr(left)?;
                        let resolved_right = resolve_static_expr(right)?;
                        resolve_binary_expr(*op, resolved_left, resolved_right)
            */
            Err(EvalError::UnsupportedOperation)
        }
        UnaryOp { op, expr } => {
            // let operand = resolve_static_expr(&expr)?;
            // resolve_unary_expr(*op, operand)
            Err(EvalError::UnsupportedOperation)
        }
        Nested(expr) => resolve_static_expr(&expr),
        Value(value) => Datum::try_from(value),
        _ => Err(EvalError::InvalidExpressionInStaticContext),
    }
}

// precondition: lhs and rhs must reduced to Expr::Value otherwise, the original expression will be returned.
pub fn resolve_binary_expr<'a>(_op: BinaryOperator, _lhs: Datum<'a>, rhs: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    Err(EvalError::UnsupportedOperation)
}

pub fn resolve_unary_expr<'a>(_op: UnaryOperator, _operand: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    Err(EvalError::UnsupportedOperation)
}
