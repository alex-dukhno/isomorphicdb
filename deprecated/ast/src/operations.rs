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

use sql_ast::{BinaryOperator, Expr};

use crate::{values::ScalarValue, NotHandled, OperationError};
use std::{
    convert::TryFrom,
    fmt::{self, Display, Formatter},
};

/// Operation performed on the table
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarOp {
    /// column access
    Column(String),
    /// literal value
    Value(ScalarValue),
    /// binary operator
    Binary(BinaryOp, Box<ScalarOp>, Box<ScalarOp>),
}

impl ScalarOp {
    pub fn transform(expr: &Expr) -> Result<Result<ScalarOp, OperationError>, NotHandled> {
        match expr {
            cast @ Expr::Cast { .. } => Ok(ScalarValue::transform(cast)?.map(ScalarOp::Value)),
            value @ Expr::Value(_) => Ok(ScalarValue::transform(value)?.map(ScalarOp::Value)),
            unary @ Expr::UnaryOp { .. } => Ok(ScalarValue::transform(unary)?.map(ScalarOp::Value)),
            Expr::BinaryOp { left, op, right } => match BinaryOp::try_from(op) {
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
                Err(()) => Err(NotHandled(Expr::BinaryOp {
                    left: Box::new(*left.clone()),
                    op: op.clone(),
                    right: Box::new(*right.clone()),
                })),
            },
            Expr::Nested(expr) => ScalarOp::transform(expr),
            Expr::Identifier(id) => Ok(Ok(ScalarOp::Column(id.value.to_lowercase()))),
            _ => Err(NotHandled(expr.clone())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    BitwiseAnd,
    BitwiseOr,
    Concat,
}

impl Display for BinaryOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BinaryOp::Add => write!(f, "+"),
            BinaryOp::Sub => write!(f, "-"),
            BinaryOp::Mul => write!(f, "*"),
            BinaryOp::Div => write!(f, "/"),
            BinaryOp::Mod => write!(f, "%"),
            BinaryOp::BitwiseAnd => write!(f, "&"),
            BinaryOp::BitwiseOr => write!(f, "|"),
            BinaryOp::Concat => write!(f, "||"),
        }
    }
}

impl TryFrom<&BinaryOperator> for BinaryOp {
    type Error = ();

    fn try_from(value: &BinaryOperator) -> Result<Self, Self::Error> {
        match &*value {
            BinaryOperator::Plus => Ok(BinaryOp::Add),
            BinaryOperator::Minus => Ok(BinaryOp::Sub),
            BinaryOperator::Multiply => Ok(BinaryOp::Mul),
            BinaryOperator::Divide => Ok(BinaryOp::Div),
            BinaryOperator::Modulus => Ok(BinaryOp::Mod),
            BinaryOperator::StringConcat => Ok(BinaryOp::Concat),
            BinaryOperator::BitwiseOr => Ok(BinaryOp::BitwiseOr),
            BinaryOperator::BitwiseAnd => Ok(BinaryOp::BitwiseAnd),
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
            BinaryOperator::PGBitwiseXor => Err(()),
            BinaryOperator::PGBitwiseShiftLeft => Err(()),
            BinaryOperator::PGBitwiseShiftRight => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigdecimal::BigDecimal;
    use sql_ast::{Ident, UnaryOperator, Value};

    #[cfg(test)]
    mod binary_operator {
        use super::*;

        #[test]
        fn not_supported() {
            assert_eq!(BinaryOp::try_from(&BinaryOperator::Gt), Err(()));
            assert_eq!(BinaryOp::try_from(&BinaryOperator::Lt), Err(()));
            assert_eq!(BinaryOp::try_from(&BinaryOperator::GtEq), Err(()));
            assert_eq!(BinaryOp::try_from(&BinaryOperator::LtEq), Err(()));
            assert_eq!(BinaryOp::try_from(&BinaryOperator::Eq), Err(()));
            assert_eq!(BinaryOp::try_from(&BinaryOperator::NotEq), Err(()));
            assert_eq!(BinaryOp::try_from(&BinaryOperator::And), Err(()));
            assert_eq!(BinaryOp::try_from(&BinaryOperator::Or), Err(()));
            assert_eq!(BinaryOp::try_from(&BinaryOperator::Like), Err(()));
            assert_eq!(BinaryOp::try_from(&BinaryOperator::NotLike), Err(()));
            assert_eq!(BinaryOp::try_from(&BinaryOperator::BitwiseXor), Err(()));
        }

        #[test]
        fn addition() {
            assert_eq!(BinaryOp::try_from(&BinaryOperator::Plus), Ok(BinaryOp::Add));
        }

        #[test]
        fn subtraction() {
            assert_eq!(BinaryOp::try_from(&BinaryOperator::Minus), Ok(BinaryOp::Sub));
        }

        #[test]
        fn multiplication() {
            assert_eq!(BinaryOp::try_from(&BinaryOperator::Multiply), Ok(BinaryOp::Mul));
        }

        #[test]
        fn division() {
            assert_eq!(BinaryOp::try_from(&BinaryOperator::Divide), Ok(BinaryOp::Div));
        }

        #[test]
        fn modulo() {
            assert_eq!(BinaryOp::try_from(&BinaryOperator::Modulus), Ok(BinaryOp::Mod));
        }

        #[test]
        fn concatenation() {
            assert_eq!(BinaryOp::try_from(&BinaryOperator::StringConcat), Ok(BinaryOp::Concat));
        }

        #[test]
        fn bitwise_or() {
            assert_eq!(BinaryOp::try_from(&BinaryOperator::BitwiseOr), Ok(BinaryOp::BitwiseOr));
        }

        #[test]
        fn bitwise_and() {
            assert_eq!(
                BinaryOp::try_from(&BinaryOperator::BitwiseAnd),
                Ok(BinaryOp::BitwiseAnd)
            );
        }

        #[test]
        fn display() {
            assert_eq!(BinaryOp::Add.to_string().as_str(), "+");
            assert_eq!(BinaryOp::Sub.to_string().as_str(), "-");
            assert_eq!(BinaryOp::Mul.to_string().as_str(), "*");
            assert_eq!(BinaryOp::Div.to_string().as_str(), "/");
            assert_eq!(BinaryOp::Mod.to_string().as_str(), "%");
            assert_eq!(BinaryOp::Concat.to_string().as_str(), "||");
            assert_eq!(BinaryOp::BitwiseOr.to_string().as_str(), "|");
            assert_eq!(BinaryOp::BitwiseAnd.to_string().as_str(), "&");
        }
    }

    #[cfg(test)]
    mod scalar_op {
        use super::*;

        #[test]
        fn unary_minus_with_number() {
            assert_eq!(
                ScalarOp::transform(&Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(Expr::Value(Value::Number(BigDecimal::from(100i64))))
                }),
                Ok(Ok(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(-100i64)))))
            )
        }

        #[test]
        fn identifier() {
            assert_eq!(
                ScalarOp::transform(&Expr::Identifier(Ident {
                    value: "column".to_owned(),
                    quote_style: None
                })),
                Ok(Ok(ScalarOp::Column("column".to_owned())))
            )
        }

        #[test]
        fn binary_operation_handled() {
            assert_eq!(
                ScalarOp::transform(&Expr::BinaryOp {
                    op: BinaryOperator::Plus,
                    left: Box::new(Expr::Value(Value::Number(BigDecimal::from(2i64)))),
                    right: Box::new(Expr::Value(Value::Number(BigDecimal::from(3i64))))
                }),
                Ok(Ok(ScalarOp::Binary(
                    BinaryOp::Add,
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(2i64)))),
                    Box::new(ScalarOp::Value(ScalarValue::Number(BigDecimal::from(3i64))))
                )))
            )
        }

        #[test]
        fn binary_operation_not_handled() {
            assert_eq!(
                ScalarOp::transform(&Expr::BinaryOp {
                    op: BinaryOperator::BitwiseXor,
                    left: Box::new(Expr::Value(Value::Number(BigDecimal::from(2i64)))),
                    right: Box::new(Expr::Value(Value::Number(BigDecimal::from(3i64))))
                }),
                Err(NotHandled(Expr::BinaryOp {
                    op: BinaryOperator::BitwiseXor,
                    left: Box::new(Expr::Value(Value::Number(BigDecimal::from(2i64)))),
                    right: Box::new(Expr::Value(Value::Number(BigDecimal::from(3i64))))
                }))
            )
        }
    }
}
