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

use crate::{NotHandled, NotSupportedOperation, OperationError};
use bigdecimal::BigDecimal;
use sqlparser::ast::{DataType, Expr, UnaryOperator, Value};
use std::str::FromStr;

#[derive(PartialEq, Debug, Copy, Clone, Eq)]
pub struct Bool(pub bool);

impl FromStr for Bool {
    type Err = ParseBoolError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "t" | "true" | "on" | "yes" | "y" | "1" => Ok(Bool(true)),
            "f" | "false" | "off" | "no" | "n" | "0" => Ok(Bool(false)),
            _ => Err(ParseBoolError(s.to_string())),
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct ParseBoolError(String);

#[derive(Debug, PartialEq, Clone, Eq)]
pub enum ScalarValue {
    String(String),
    Number(BigDecimal),
    Bool(Bool),
    Null,
}

impl ScalarValue {
    pub fn transform(value: &Expr) -> Result<Result<ScalarValue, OperationError>, NotHandled> {
        match &*value {
            Expr::Value(Value::Null) => Ok(Ok(ScalarValue::Null)),
            Expr::Value(Value::Number(number)) => Ok(Ok(ScalarValue::Number(number.clone()))),
            Expr::Value(Value::SingleQuotedString(string)) => Ok(Ok(ScalarValue::String(string.clone()))),
            Expr::Value(Value::Boolean(bool)) => Ok(Ok(ScalarValue::Bool(Bool(*bool)))),
            Expr::Value(value) => Err(NotHandled(Expr::Value(value.clone()))),
            Expr::Cast { expr, data_type } => match (&**expr, data_type) {
                (Expr::Value(Value::SingleQuotedString(string)), DataType::Boolean) => {
                    match Bool::from_str(string.as_str()) {
                        Ok(Bool(boolean)) => Ok(Ok(ScalarValue::Bool(Bool(boolean)))),
                        Err(error) => Ok(Err(OperationError(
                            NotSupportedOperation::Cast(Value::SingleQuotedString(string.clone()), DataType::Boolean),
                            Some(error.0),
                        ))),
                    }
                }
                (Expr::Value(Value::Boolean(boolean)), DataType::Boolean) => Ok(Ok(ScalarValue::Bool(Bool(*boolean)))),
                (Expr::Value(value), data_type) => Ok(Err(OperationError(
                    NotSupportedOperation::Cast(value.clone(), data_type.clone()),
                    None,
                ))),
                _ => Err(NotHandled(Expr::Cast {
                    expr: Box::new(*expr.clone()),
                    data_type: data_type.clone(),
                })),
            },
            Expr::UnaryOp { op, expr } => match (op, &**expr) {
                (UnaryOperator::Minus, Expr::Value(Value::Number(number))) => Ok(Ok(ScalarValue::Number(-number))),
                (UnaryOperator::Plus, Expr::Value(Value::Number(number))) => {
                    Ok(Ok(ScalarValue::Number(number.clone())))
                }
                (UnaryOperator::Not, Expr::Value(Value::Number(_number))) => {
                    Ok(Err(OperationError(NotSupportedOperation::Not, None)))
                }
                _ => Err(NotHandled(Expr::UnaryOp {
                    op: op.clone(),
                    expr: expr.clone(),
                })),
            },
            expr => Err(NotHandled(expr.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    mod bool_parser {
        use super::*;

        #[test]
        fn true_values() {
            assert_eq!("t".parse(), Ok(Bool(true)));
            assert_eq!("TrUe".parse(), Ok(Bool(true)));
            assert_eq!("YeS".parse(), Ok(Bool(true)));
            assert_eq!("y".parse(), Ok(Bool(true)));
            assert_eq!("on".parse(), Ok(Bool(true)));
            assert_eq!("1".parse(), Ok(Bool(true)));
        }

        #[test]
        fn false_values() {
            assert_eq!("f".parse(), Ok(Bool(false)));
            assert_eq!("FalSe".parse(), Ok(Bool(false)));
            assert_eq!("nO".parse(), Ok(Bool(false)));
            assert_eq!("N".parse(), Ok(Bool(false)));
            assert_eq!("OfF".parse(), Ok(Bool(false)));
            assert_eq!("0".parse(), Ok(Bool(false)));
        }

        #[test]
        fn not_a_boolean_value() {
            assert_eq!(
                Bool::from_str("not a boolean"),
                Err(ParseBoolError("not a boolean".to_string()))
            )
        }
    }

    #[cfg(test)]
    mod scalar_value {
        use super::*;
        use sqlparser::ast::{DataType, UnaryOperator};

        #[test]
        fn from_number_value() {
            assert_eq!(
                ScalarValue::transform(&Expr::Value(Value::Number(BigDecimal::from(0u64)))),
                Ok(Ok(ScalarValue::Number(BigDecimal::from(0u64))))
            )
        }

        #[test]
        fn from_string_value() {
            assert_eq!(
                ScalarValue::transform(&Expr::Value(Value::SingleQuotedString("string".to_owned()))),
                Ok(Ok(ScalarValue::String("string".to_owned())))
            )
        }

        #[test]
        fn from_bool_value() {
            assert_eq!(
                ScalarValue::transform(&Expr::Value(Value::Boolean(true))),
                Ok(Ok(ScalarValue::Bool(Bool(true))))
            )
        }

        #[test]
        fn bool_cast_string() {
            assert_eq!(
                ScalarValue::transform(&Expr::Cast {
                    expr: Box::new(Expr::Value(Value::SingleQuotedString("true".to_string()))),
                    data_type: DataType::Boolean
                }),
                Ok(Ok(ScalarValue::Bool(Bool(true))))
            )
        }

        #[test]
        fn bool_cast_not_parsable_string() {
            assert_eq!(
                ScalarValue::transform(&Expr::Cast {
                    expr: Box::new(Expr::Value(Value::SingleQuotedString("not a boolean".to_string()))),
                    data_type: DataType::Boolean
                }),
                Ok(Err(OperationError(
                    NotSupportedOperation::Cast(
                        Value::SingleQuotedString("not a boolean".to_string()),
                        DataType::Boolean
                    ),
                    Some("not a boolean".to_string())
                )))
            );
        }

        #[test]
        fn bool_cast_value() {
            assert_eq!(
                ScalarValue::transform(&Expr::Cast {
                    expr: Box::new(Expr::Value(Value::Boolean(false))),
                    data_type: DataType::Boolean
                }),
                Ok(Ok(ScalarValue::Bool(Bool(false))))
            );
        }

        #[test]
        fn null_value() {
            assert_eq!(
                ScalarValue::transform(&Expr::Value(Value::Null)),
                Ok(Ok(ScalarValue::Null))
            )
        }

        #[test]
        fn unary_minus_with_number() {
            assert_eq!(
                ScalarValue::transform(&Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(Expr::Value(Value::Number(BigDecimal::from(100i64))))
                }),
                Ok(Ok(ScalarValue::Number(BigDecimal::from(-100i64))))
            )
        }

        #[test]
        fn unary_plus_with_number() {
            assert_eq!(
                ScalarValue::transform(&Expr::UnaryOp {
                    op: UnaryOperator::Plus,
                    expr: Box::new(Expr::Value(Value::Number(BigDecimal::from(1i64))))
                }),
                Ok(Ok(ScalarValue::Number(BigDecimal::from(1i64))))
            )
        }

        #[test]
        fn unary_not_with_number() {
            assert_eq!(
                ScalarValue::transform(&Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(Expr::Value(Value::Number(BigDecimal::from(0u64))))
                }),
                Ok(Err(OperationError(NotSupportedOperation::Not, None)))
            )
        }
    }
}
