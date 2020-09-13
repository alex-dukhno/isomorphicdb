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
use sql_model::sql_types::SqlType;
use sqlparser::ast::{DataType, Expr, UnaryOperator, Value};
use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

#[derive(PartialEq, Debug, Copy, Clone, Eq)]
pub struct Bool(pub bool);

impl FromStr for Bool {
    type Err = ParseBoolError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "t" | "true" | "on" | "yes" | "y" | "1" => Ok(Bool(true)),
            "f" | "false" | "off" | "no" | "n" | "0" => Ok(Bool(false)),
            _ => Err(ParseBoolError),
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct ParseBoolError;

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
                        Err(_error) => Ok(Err(OperationError(NotSupportedOperation::ExplicitCast(
                            Value::SingleQuotedString(string.clone()),
                            DataType::Boolean,
                        )))),
                    }
                }
                (Expr::Value(Value::Boolean(boolean)), DataType::Boolean) => Ok(Ok(ScalarValue::Bool(Bool(*boolean)))),
                (Expr::Value(value), data_type) => Ok(Err(OperationError(NotSupportedOperation::ExplicitCast(
                    value.clone(),
                    data_type.clone(),
                )))),
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
                    Ok(Err(OperationError(NotSupportedOperation::Not)))
                }
                _ => Err(NotHandled(Expr::UnaryOp {
                    op: op.clone(),
                    expr: expr.clone(),
                })),
            },
            expr => Err(NotHandled(expr.clone())),
        }
    }

    pub fn cast(&self, to_type: &SqlType) -> Result<ScalarValue, OperationError> {
        match (self, to_type) {
            (ScalarValue::Number(number), SqlType::Bool) => Ok(ScalarValue::Bool(Bool(number != &BigDecimal::from(0)))),
            (ScalarValue::Number(number), SqlType::Char(len))
            | (ScalarValue::Number(number), SqlType::VarChar(len)) => Ok(ScalarValue::String(
                number.to_string().chars().take(*len as usize).collect(),
            )),
            (ScalarValue::String(str), SqlType::Bool) => Bool::from_str(str)
                .map(ScalarValue::Bool)
                .map_err(|_err| OperationError(NotSupportedOperation::ImplicitCast(self.clone(), *to_type))),
            (ScalarValue::String(str), SqlType::SmallInt(_))
            | (ScalarValue::String(str), SqlType::Integer(_))
            | (ScalarValue::String(str), SqlType::BigInt(_))
            | (ScalarValue::String(str), SqlType::Real)
            | (ScalarValue::String(str), SqlType::DoublePrecision) => BigDecimal::from_str(str)
                .map(ScalarValue::Number)
                .map_err(|_err| OperationError(NotSupportedOperation::ImplicitCast(self.clone(), *to_type))),
            (ScalarValue::Bool(Bool(boolean)), SqlType::Char(len))
            | (ScalarValue::Bool(Bool(boolean)), SqlType::VarChar(len)) => Ok(ScalarValue::String(
                boolean.to_string().chars().take(*len as usize).collect(),
            )),
            (ScalarValue::Bool(Bool(boolean)), SqlType::SmallInt(_))
            | (ScalarValue::Bool(Bool(boolean)), SqlType::Integer(_))
            | (ScalarValue::Bool(Bool(boolean)), SqlType::BigInt(_))
            | (ScalarValue::Bool(Bool(boolean)), SqlType::Real)
            | (ScalarValue::Bool(Bool(boolean)), SqlType::DoublePrecision) => {
                if *boolean {
                    Ok(ScalarValue::Number(BigDecimal::from(1)))
                } else {
                    Ok(ScalarValue::Number(BigDecimal::from(0)))
                }
            }
            (ScalarValue::Null, _) => Ok(ScalarValue::Null),
            (ScalarValue::String(str), SqlType::Char(len)) | (ScalarValue::String(str), SqlType::VarChar(len)) => {
                Ok(ScalarValue::String(str.chars().take(*len as usize).collect()))
            }
            (ScalarValue::Number(number), SqlType::SmallInt(_))
            | (ScalarValue::Number(number), SqlType::Integer(_))
            | (ScalarValue::Number(number), SqlType::BigInt(_))
            | (ScalarValue::Number(number), SqlType::Real)
            | (ScalarValue::Number(number), SqlType::DoublePrecision) => Ok(ScalarValue::Number(number.clone())),
            (ScalarValue::Bool(Bool(boolean)), SqlType::Bool) => Ok(ScalarValue::Bool(Bool(*boolean))),
        }
    }
}

impl Display for ScalarValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::String(s) => write!(f, "{}", s),
            ScalarValue::Number(n) => write!(f, "{}", n),
            ScalarValue::Bool(Bool(b)) => write!(f, "{}", b),
            ScalarValue::Null => write!(f, "NULL"),
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
            assert_eq!(Bool::from_str("not a boolean"), Err(ParseBoolError))
        }
    }

    #[cfg(test)]
    mod ast_transformation {
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
                Ok(Err(OperationError(NotSupportedOperation::ExplicitCast(
                    Value::SingleQuotedString("not a boolean".to_string()),
                    DataType::Boolean
                ))))
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
                Ok(Err(OperationError(NotSupportedOperation::Not)))
            )
        }
    }

    #[cfg(test)]
    mod type_casting {
        use super::*;

        #[test]
        fn number_to_boolean() {
            assert_eq!(
                ScalarValue::Number(BigDecimal::from(1)).cast(&SqlType::Bool),
                Ok(ScalarValue::Bool(Bool(true)))
            );
        }

        #[test]
        fn string_to_boolean() {
            assert_eq!(
                ScalarValue::String("y".to_owned()).cast(&SqlType::Bool),
                Ok(ScalarValue::Bool(Bool(true)))
            );
        }

        #[test]
        fn not_supported_cast_string_to_boolean() {
            assert_eq!(
                ScalarValue::String("not boolean".to_owned()).cast(&SqlType::Bool),
                Err(OperationError(NotSupportedOperation::ImplicitCast(
                    ScalarValue::String("not boolean".to_owned()),
                    SqlType::Bool
                )))
            );
        }

        #[test]
        fn string_to_number() {
            assert_eq!(
                ScalarValue::String("123".to_owned()).cast(&SqlType::SmallInt(i16::min_value())),
                Ok(ScalarValue::Number(BigDecimal::from(123i32)))
            );
            assert_eq!(
                ScalarValue::String("123".to_owned()).cast(&SqlType::Integer(i32::min_value())),
                Ok(ScalarValue::Number(BigDecimal::from(123i32)))
            );
            assert_eq!(
                ScalarValue::String("123".to_owned()).cast(&SqlType::BigInt(i64::min_value())),
                Ok(ScalarValue::Number(BigDecimal::from(123i32)))
            );
            assert_eq!(
                ScalarValue::String("123".to_owned()).cast(&SqlType::Real),
                Ok(ScalarValue::Number(BigDecimal::from(123i32)))
            );
            assert_eq!(
                ScalarValue::String("123".to_owned()).cast(&SqlType::DoublePrecision),
                Ok(ScalarValue::Number(BigDecimal::from(123i32)))
            );
        }

        #[test]
        fn not_supported_string_to_number() {
            assert_eq!(
                ScalarValue::String("not a number".to_owned()).cast(&SqlType::Integer(i32::min_value())),
                Err(OperationError(NotSupportedOperation::ImplicitCast(
                    ScalarValue::String("not a number".to_owned()),
                    SqlType::Integer(i32::min_value())
                )))
            );
        }

        #[test]
        fn number_to_string() {
            assert_eq!(
                ScalarValue::Number(BigDecimal::from(123)).cast(&SqlType::Char(1)),
                Ok(ScalarValue::String("1".to_string()))
            );
            assert_eq!(
                ScalarValue::Number(BigDecimal::from(123)).cast(&SqlType::VarChar(5)),
                Ok(ScalarValue::String("123".to_string()))
            );
        }

        #[test]
        fn bool_to_string() {
            assert_eq!(
                ScalarValue::Bool(Bool(true)).cast(&SqlType::Char(1)),
                Ok(ScalarValue::String("t".to_string()))
            );
            assert_eq!(
                ScalarValue::Bool(Bool(true)).cast(&SqlType::VarChar(5)),
                Ok(ScalarValue::String("true".to_string()))
            );
        }

        #[test]
        fn bool_to_number() {
            assert_eq!(
                ScalarValue::Bool(Bool(true)).cast(&SqlType::SmallInt(i16::min_value())),
                Ok(ScalarValue::Number(BigDecimal::from(1)))
            );
            assert_eq!(
                ScalarValue::Bool(Bool(false)).cast(&SqlType::Integer(i32::min_value())),
                Ok(ScalarValue::Number(BigDecimal::from(0)))
            );
            assert_eq!(
                ScalarValue::Bool(Bool(true)).cast(&SqlType::BigInt(i64::min_value())),
                Ok(ScalarValue::Number(BigDecimal::from(1)))
            );
            assert_eq!(
                ScalarValue::Bool(Bool(false)).cast(&SqlType::Real),
                Ok(ScalarValue::Number(BigDecimal::from(0)))
            );
            assert_eq!(
                ScalarValue::Bool(Bool(true)).cast(&SqlType::DoublePrecision),
                Ok(ScalarValue::Number(BigDecimal::from(1)))
            );
        }

        #[test]
        fn null_is_always_null() {
            assert_eq!(
                ScalarValue::Null.cast(&SqlType::SmallInt(i16::min_value())),
                Ok(ScalarValue::Null)
            );
            assert_eq!(
                ScalarValue::Null.cast(&SqlType::Integer(i32::min_value())),
                Ok(ScalarValue::Null)
            );
            assert_eq!(
                ScalarValue::Null.cast(&SqlType::BigInt(i64::min_value())),
                Ok(ScalarValue::Null)
            );
            assert_eq!(ScalarValue::Null.cast(&SqlType::Real), Ok(ScalarValue::Null));
            assert_eq!(ScalarValue::Null.cast(&SqlType::DoublePrecision), Ok(ScalarValue::Null));
            assert_eq!(ScalarValue::Null.cast(&SqlType::Char(1)), Ok(ScalarValue::Null));
            assert_eq!(ScalarValue::Null.cast(&SqlType::VarChar(5)), Ok(ScalarValue::Null));
            assert_eq!(ScalarValue::Null.cast(&SqlType::Bool), Ok(ScalarValue::Null));
        }

        #[test]
        fn string_to_string() {
            assert_eq!(
                ScalarValue::String("123".to_owned()).cast(&SqlType::Char(1)),
                Ok(ScalarValue::String("1".to_string()))
            );
            assert_eq!(
                ScalarValue::String("123".to_owned()).cast(&SqlType::VarChar(4)),
                Ok(ScalarValue::String("123".to_string()))
            );
        }

        #[test]
        fn number_to_number() {
            assert_eq!(
                ScalarValue::Number(BigDecimal::from_str("123").unwrap()).cast(&SqlType::SmallInt(i16::min_value())),
                Ok(ScalarValue::Number(BigDecimal::from(123)))
            );
            assert_eq!(
                ScalarValue::Number(
                    BigDecimal::from_str(
                        "12345678901234567890123456789012345678901234567890123456789012345678901234567890"
                    )
                    .unwrap()
                )
                .cast(&SqlType::SmallInt(i16::min_value())),
                Ok(ScalarValue::Number(
                    BigDecimal::from_str(
                        "12345678901234567890123456789012345678901234567890123456789012345678901234567890"
                    )
                    .unwrap()
                ))
            );
            assert_eq!(
                ScalarValue::Number(BigDecimal::from_str("123").unwrap()).cast(&SqlType::Integer(i32::min_value())),
                Ok(ScalarValue::Number(BigDecimal::from(123)))
            );
            assert_eq!(
                ScalarValue::Number(
                    BigDecimal::from_str(
                        "12345678901234567890123456789012345678901234567890123456789012345678901234567890"
                    )
                    .unwrap()
                )
                .cast(&SqlType::Integer(i32::min_value())),
                Ok(ScalarValue::Number(
                    BigDecimal::from_str(
                        "12345678901234567890123456789012345678901234567890123456789012345678901234567890"
                    )
                    .unwrap()
                ))
            );
            assert_eq!(
                ScalarValue::Number(BigDecimal::from_str("123").unwrap()).cast(&SqlType::BigInt(i64::min_value())),
                Ok(ScalarValue::Number(BigDecimal::from(123)))
            );
            assert_eq!(
                ScalarValue::Number(
                    BigDecimal::from_str(
                        "12345678901234567890123456789012345678901234567890123456789012345678901234567890"
                    )
                    .unwrap()
                )
                .cast(&SqlType::BigInt(i64::min_value())),
                Ok(ScalarValue::Number(
                    BigDecimal::from_str(
                        "12345678901234567890123456789012345678901234567890123456789012345678901234567890"
                    )
                    .unwrap()
                ))
            );
            assert_eq!(
                ScalarValue::Number(BigDecimal::from_str("123").unwrap()).cast(&SqlType::Real),
                Ok(ScalarValue::Number(BigDecimal::from(123.0f32)))
            );
            assert_eq!(
                ScalarValue::Number(
                    BigDecimal::from_str(
                        "12345678901234567890123456789012345678901234567890123456789012345678901234567890"
                    )
                    .unwrap()
                )
                .cast(&SqlType::Real),
                Ok(ScalarValue::Number(
                    BigDecimal::from_str(
                        "12345678901234567890123456789012345678901234567890123456789012345678901234567890"
                    )
                    .unwrap()
                ))
            );
            assert_eq!(
                ScalarValue::Number(BigDecimal::from_str("123").unwrap()).cast(&SqlType::DoublePrecision),
                Ok(ScalarValue::Number(BigDecimal::from(123.0f64)))
            );
            assert_eq!(
                ScalarValue::Number(
                    BigDecimal::from_str(
                        "12345678901234567890123456789012345678901234567890123456789012345678901234567890\
                        12345678901234567890123456789012345678901234567890123456789012345678901234567890\
                        12345678901234567890123456789012345678901234567890123456789012345678901234567890\
                        12345678901234567890123456789012345678901234567890123456789012345678901234567890"
                    )
                    .unwrap()
                )
                .cast(&SqlType::DoublePrecision),
                Ok(ScalarValue::Number(
                    BigDecimal::from_str(
                        "12345678901234567890123456789012345678901234567890123456789012345678901234567890\
                        12345678901234567890123456789012345678901234567890123456789012345678901234567890\
                        12345678901234567890123456789012345678901234567890123456789012345678901234567890\
                        12345678901234567890123456789012345678901234567890123456789012345678901234567890"
                    )
                    .unwrap()
                ))
            );
        }

        #[test]
        fn bool_to_bool() {
            assert_eq!(
                ScalarValue::Bool(Bool(true)).cast(&SqlType::Bool),
                Ok(ScalarValue::Bool(Bool(true)))
            );
        }
    }
}
