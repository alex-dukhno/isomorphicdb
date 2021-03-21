// Copyright 2020 - 2021 Alex Dukhno
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

use bigdecimal::{BigDecimal, ToPrimitive, Zero};
use data_manipulation_query_result::QueryExecutionError;
use query_ast::{BinaryOperator, UnaryOperator};
use regex::Regex;
use scalar::ScalarValue;
use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};
use types::{Bool, SqlTypeFamily};

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum BiArithmetic {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Exp,
}

impl BiArithmetic {
    fn eval(&self, left: BigDecimal, right: BigDecimal) -> BigDecimal {
        match self {
            BiArithmetic::Add => left + right,
            BiArithmetic::Sub => left - right,
            BiArithmetic::Mul => left * right,
            BiArithmetic::Div => left / right,
            BiArithmetic::Mod => left % right,
            BiArithmetic::Exp => {
                fn exp(x: &BigDecimal, n: &BigDecimal) -> BigDecimal {
                    if n < &BigDecimal::from(0) {
                        exp(&(1 / x), &-n)
                    } else if n == &BigDecimal::from(0) {
                        BigDecimal::from(1)
                    } else if n == &BigDecimal::from(1) {
                        x.clone()
                    } else if n % &BigDecimal::from(2) == BigDecimal::from(0) {
                        exp(&(x * x), &(n.clone() / 2))
                    } else {
                        x * exp(&(x * x), &((n - &BigDecimal::from(1)) / 2))
                    }
                }

                exp(&left, &right)
            }
        }
    }
}

impl Display for BiArithmetic {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BiArithmetic::Add => write!(f, "+"),
            BiArithmetic::Sub => write!(f, "-"),
            BiArithmetic::Mul => write!(f, "*"),
            BiArithmetic::Div => write!(f, "/"),
            BiArithmetic::Mod => write!(f, "%"),
            BiArithmetic::Exp => write!(f, "^"),
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum Comparison {
    NotEq,
    Eq,
    LtEq,
    GtEq,
    Lt,
    Gt,
}

impl Comparison {
    fn eval<E: PartialEq + PartialOrd>(&self, left_value: E, right_value: E) -> bool {
        match self {
            Comparison::NotEq => left_value != right_value,
            Comparison::Eq => left_value == right_value,
            Comparison::LtEq => left_value <= right_value,
            Comparison::GtEq => left_value >= right_value,
            Comparison::Lt => left_value < right_value,
            Comparison::Gt => left_value > right_value,
        }
    }
}

impl Display for Comparison {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Comparison::NotEq => write!(f, "<>"),
            Comparison::Eq => write!(f, "="),
            Comparison::LtEq => write!(f, "<="),
            Comparison::GtEq => write!(f, ">="),
            Comparison::Lt => write!(f, "<"),
            Comparison::Gt => write!(f, ">"),
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum Bitwise {
    ShiftRight,
    ShiftLeft,
    Xor,
    And,
    Or,
}

impl Bitwise {
    fn eval(&self, left: BigDecimal, right: BigDecimal) -> BigDecimal {
        match self {
            Bitwise::ShiftRight => BigDecimal::from(left.to_u64().unwrap() >> right.to_u64().unwrap()),
            Bitwise::ShiftLeft => BigDecimal::from(left.to_u64().unwrap() << right.to_u64().unwrap()),
            Bitwise::Xor => BigDecimal::from(left.to_u64().unwrap() ^ right.to_u64().unwrap()),
            Bitwise::And => BigDecimal::from(left.to_u64().unwrap() & right.to_u64().unwrap()),
            Bitwise::Or => BigDecimal::from(left.to_u64().unwrap() | right.to_u64().unwrap()),
        }
    }
}

impl Display for Bitwise {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Bitwise::ShiftRight => write!(f, ">>"),
            Bitwise::ShiftLeft => write!(f, "<<"),
            Bitwise::Xor => write!(f, "#"),
            Bitwise::And => write!(f, "&"),
            Bitwise::Or => write!(f, "|"),
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum BiLogical {
    Or,
    And,
}

impl BiLogical {
    fn eval(&self, left_value: bool, right_value: bool) -> bool {
        match self {
            BiLogical::Or => left_value || right_value,
            BiLogical::And => left_value && right_value,
        }
    }
}

impl Display for BiLogical {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BiLogical::Or => write!(f, "OR"),
            BiLogical::And => write!(f, "AND"),
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum Matching {
    Like,
    NotLike,
}

impl Matching {
    fn eval(&self, left: String, right: String) -> bool {
        let matches = Regex::new(left.replace("%", ".*").replace("_", ".+").as_str())
            .unwrap()
            .is_match(right.as_str());
        match self {
            Matching::Like => matches,
            Matching::NotLike => !matches,
        }
    }
}

impl Display for Matching {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Matching::Like => write!(f, "LIKE"),
            Matching::NotLike => write!(f, "NOT LIKE"),
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub struct Concat;

impl Concat {
    fn eval(&self, left: String, right: String) -> String {
        left + right.as_str()
    }
}

impl Display for Concat {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "||")
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum BiOperator {
    Arithmetic(BiArithmetic),
    Comparison(Comparison),
    Bitwise(Bitwise),
    Logical(BiLogical),
    Matching(Matching),
    StringOp(Concat),
}

impl BiOperator {
    pub fn eval(self, left: ScalarValue, right: ScalarValue) -> Result<ScalarValue, QueryExecutionError> {
        match self {
            BiOperator::Arithmetic(op) => match (left, right) {
                (ScalarValue::Num { value: left_value, .. }, ScalarValue::Num { value: right_value, .. }) => {
                    Ok(ScalarValue::Num {
                        value: op.eval(left_value, right_value),
                        type_family: SqlTypeFamily::BigInt,
                    })
                }
                (ScalarValue::Num { type_family, .. }, ScalarValue::String(value)) => {
                    Err(QueryExecutionError::invalid_text_representation(type_family, value))
                }
                (ScalarValue::Num { type_family, .. }, other) => Err(QueryExecutionError::undefined_bi_function(
                    self,
                    type_family,
                    other
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                )),
                (ScalarValue::String(value), ScalarValue::Num { type_family, .. }) => {
                    Err(QueryExecutionError::invalid_text_representation(type_family, value))
                }
                (other, ScalarValue::Num { type_family, .. }) => Err(QueryExecutionError::undefined_bi_function(
                    self,
                    other
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                    type_family,
                )),
                (other_left, other_right) => Err(QueryExecutionError::undefined_bi_function(
                    self,
                    other_left
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                    other_right
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                )),
            },
            BiOperator::Comparison(op) => match (left, right) {
                (ScalarValue::Bool(left_value), ScalarValue::Bool(right_value)) => {
                    Ok(ScalarValue::Bool(op.eval(left_value, right_value)))
                }
                (ScalarValue::String(left_value), ScalarValue::String(right_value)) => {
                    Ok(ScalarValue::Bool(op.eval(left_value, right_value)))
                }
                (ScalarValue::Num { value: left_value, .. }, ScalarValue::Num { value: right_value, .. }) => {
                    Ok(ScalarValue::Bool(op.eval(left_value, right_value)))
                }
                (other_left, other_right) => Err(QueryExecutionError::undefined_bi_function(
                    self,
                    other_left
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                    other_right
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                )),
            },
            BiOperator::Bitwise(op) => match (left, right) {
                (
                    ScalarValue::Num {
                        value: left_value,
                        type_family: SqlTypeFamily::SmallInt,
                    },
                    ScalarValue::Num {
                        value: right_value,
                        type_family: SqlTypeFamily::SmallInt,
                    },
                )
                | (
                    ScalarValue::Num {
                        value: left_value,
                        type_family: SqlTypeFamily::Integer,
                    },
                    ScalarValue::Num {
                        value: right_value,
                        type_family: SqlTypeFamily::Integer,
                    },
                )
                | (
                    ScalarValue::Num {
                        value: left_value,
                        type_family: SqlTypeFamily::BigInt,
                    },
                    ScalarValue::Num {
                        value: right_value,
                        type_family: SqlTypeFamily::BigInt,
                    },
                )
                | (
                    ScalarValue::Num {
                        value: left_value,
                        type_family: SqlTypeFamily::SmallInt,
                    },
                    ScalarValue::Num {
                        value: right_value,
                        type_family: SqlTypeFamily::Integer,
                    },
                )
                | (
                    ScalarValue::Num {
                        value: left_value,
                        type_family: SqlTypeFamily::SmallInt,
                    },
                    ScalarValue::Num {
                        value: right_value,
                        type_family: SqlTypeFamily::BigInt,
                    },
                )
                | (
                    ScalarValue::Num {
                        value: left_value,
                        type_family: SqlTypeFamily::Integer,
                    },
                    ScalarValue::Num {
                        value: right_value,
                        type_family: SqlTypeFamily::SmallInt,
                    },
                )
                | (
                    ScalarValue::Num {
                        value: left_value,
                        type_family: SqlTypeFamily::Integer,
                    },
                    ScalarValue::Num {
                        value: right_value,
                        type_family: SqlTypeFamily::BigInt,
                    },
                )
                | (
                    ScalarValue::Num {
                        value: left_value,
                        type_family: SqlTypeFamily::BigInt,
                    },
                    ScalarValue::Num {
                        value: right_value,
                        type_family: SqlTypeFamily::SmallInt,
                    },
                )
                | (
                    ScalarValue::Num {
                        value: left_value,
                        type_family: SqlTypeFamily::BigInt,
                    },
                    ScalarValue::Num {
                        value: right_value,
                        type_family: SqlTypeFamily::Integer,
                    },
                ) => Ok(ScalarValue::Num {
                    value: op.eval(left_value, right_value),
                    type_family: SqlTypeFamily::BigInt,
                }),
                (ScalarValue::Num { type_family, .. }, ScalarValue::String(value)) => {
                    Err(QueryExecutionError::invalid_text_representation(type_family, value))
                }
                (ScalarValue::Num { type_family, .. }, other) => Err(QueryExecutionError::undefined_bi_function(
                    self,
                    type_family,
                    other
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                )),
                (ScalarValue::String(value), ScalarValue::Num { type_family, .. }) => {
                    Err(QueryExecutionError::invalid_text_representation(type_family, value))
                }
                (other, ScalarValue::Num { type_family, .. }) => Err(QueryExecutionError::undefined_bi_function(
                    self,
                    other
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                    type_family,
                )),
                (other_left, other_right) => Err(QueryExecutionError::undefined_bi_function(
                    self,
                    other_left
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                    other_right
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                )),
            },
            BiOperator::Logical(op) => match (left, right) {
                (ScalarValue::Bool(left_value), ScalarValue::Bool(right_value)) => {
                    Ok(ScalarValue::Bool(op.eval(left_value, right_value)))
                }
                (other_left, other_right) => Err(QueryExecutionError::undefined_bi_function(
                    self,
                    other_left
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                    other_right
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                )),
            },
            BiOperator::Matching(op) => match (left, right) {
                (ScalarValue::String(left_value), ScalarValue::String(right_value)) => {
                    Ok(ScalarValue::Bool(op.eval(left_value, right_value)))
                }
                (other_left, other_right) => Err(QueryExecutionError::undefined_bi_function(
                    self,
                    other_left
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                    other_right
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                )),
            },
            BiOperator::StringOp(op) => match (left, right) {
                (ScalarValue::String(left_value), ScalarValue::String(right_value)) => {
                    Ok(ScalarValue::String(op.eval(left_value, right_value)))
                }
                (other_left, other_right) => Err(QueryExecutionError::undefined_bi_function(
                    self,
                    other_left
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                    other_right
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                )),
            },
        }
    }
}

impl From<BinaryOperator> for BiOperator {
    fn from(operator: BinaryOperator) -> Self {
        match operator {
            BinaryOperator::Plus => BiOperator::Arithmetic(BiArithmetic::Add),
            BinaryOperator::Minus => BiOperator::Arithmetic(BiArithmetic::Sub),
            BinaryOperator::Multiply => BiOperator::Arithmetic(BiArithmetic::Mul),
            BinaryOperator::Divide => BiOperator::Arithmetic(BiArithmetic::Div),
            BinaryOperator::Modulus => BiOperator::Arithmetic(BiArithmetic::Mod),
            BinaryOperator::Exp => BiOperator::Arithmetic(BiArithmetic::Exp),
            BinaryOperator::StringConcat => BiOperator::StringOp(Concat),
            BinaryOperator::Gt => BiOperator::Comparison(Comparison::Gt),
            BinaryOperator::Lt => BiOperator::Comparison(Comparison::Lt),
            BinaryOperator::GtEq => BiOperator::Comparison(Comparison::GtEq),
            BinaryOperator::LtEq => BiOperator::Comparison(Comparison::LtEq),
            BinaryOperator::Eq => BiOperator::Comparison(Comparison::Eq),
            BinaryOperator::NotEq => BiOperator::Comparison(Comparison::NotEq),
            BinaryOperator::And => BiOperator::Logical(BiLogical::And),
            BinaryOperator::Or => BiOperator::Logical(BiLogical::Or),
            BinaryOperator::Like => BiOperator::Matching(Matching::Like),
            BinaryOperator::NotLike => BiOperator::Matching(Matching::NotLike),
            BinaryOperator::BitwiseOr => BiOperator::Bitwise(Bitwise::Or),
            BinaryOperator::BitwiseAnd => BiOperator::Bitwise(Bitwise::And),
            BinaryOperator::BitwiseXor => BiOperator::Bitwise(Bitwise::Xor),
            BinaryOperator::BitwiseShiftLeft => BiOperator::Bitwise(Bitwise::ShiftLeft),
            BinaryOperator::BitwiseShiftRight => BiOperator::Bitwise(Bitwise::ShiftRight),
        }
    }
}

impl Display for BiOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BiOperator::Arithmetic(op) => write!(f, "{}", op),
            BiOperator::Comparison(op) => write!(f, "{}", op),
            BiOperator::Bitwise(op) => write!(f, "{}", op),
            BiOperator::Logical(op) => write!(f, "{}", op),
            BiOperator::Matching(op) => write!(f, "{}", op),
            BiOperator::StringOp(op) => write!(f, "{}", op),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum UnOperator {
    Arithmetic(UnArithmetic),
    LogicalNot,
    BitwiseNot,
    Cast(SqlTypeFamily),
}

impl UnOperator {
    pub fn eval(self, value: ScalarValue) -> Result<ScalarValue, QueryExecutionError> {
        match self {
            UnOperator::Arithmetic(operator) => match value {
                ScalarValue::Num { value, type_family } => operator.eval(value, type_family),
                other => Err(QueryExecutionError::undefined_function(
                    self,
                    other
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                )),
            },
            UnOperator::LogicalNot => match value {
                ScalarValue::Bool(value) => Ok(ScalarValue::Bool(!value)),
                other => Err(QueryExecutionError::datatype_mismatch(
                    self,
                    SqlTypeFamily::Bool,
                    other
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                )),
            },
            UnOperator::BitwiseNot => match value {
                ScalarValue::Num {
                    value,
                    type_family: SqlTypeFamily::SmallInt,
                } => Ok(ScalarValue::Num {
                    value: BigDecimal::from(!value.to_i16().unwrap()),
                    type_family: SqlTypeFamily::SmallInt,
                }),
                ScalarValue::Num {
                    value,
                    type_family: SqlTypeFamily::Integer,
                } => Ok(ScalarValue::Num {
                    value: BigDecimal::from(!value.to_i32().unwrap()),
                    type_family: SqlTypeFamily::Integer,
                }),
                ScalarValue::Num {
                    value,
                    type_family: SqlTypeFamily::BigInt,
                } => Ok(ScalarValue::Num {
                    value: BigDecimal::from(!value.to_i64().unwrap()),
                    type_family: SqlTypeFamily::BigInt,
                }),
                other => Err(QueryExecutionError::undefined_function(
                    self,
                    other
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                )),
            },
            UnOperator::Cast(type_family) => match value {
                ScalarValue::Null => Ok(ScalarValue::Null),
                ScalarValue::Bool(value) => match type_family {
                    SqlTypeFamily::Bool => Ok(ScalarValue::Bool(value)),
                    SqlTypeFamily::String => Ok(ScalarValue::String(value.to_string())),
                    other => Err(QueryExecutionError::cannot_coerce(SqlTypeFamily::Bool, other)),
                },
                ScalarValue::Num { value, .. } => match type_family {
                    SqlTypeFamily::Bool => Ok(ScalarValue::Bool(!value.is_zero())),
                    SqlTypeFamily::String => Ok(ScalarValue::String(value.to_string())),
                    other => Ok(ScalarValue::Num {
                        value,
                        type_family: other,
                    }),
                },
                ScalarValue::String(value) => match type_family {
                    SqlTypeFamily::String => Ok(ScalarValue::String(value)),
                    SqlTypeFamily::Bool => match Bool::from_str(value.as_str()) {
                        Ok(Bool(boolean)) => Ok(ScalarValue::Bool(boolean)),
                        Err(_) => Err(QueryExecutionError::invalid_text_representation(
                            SqlTypeFamily::Bool,
                            value,
                        )),
                    },
                    other => match BigDecimal::from_str(value.as_str()) {
                        Ok(value) => Ok(ScalarValue::Num {
                            value,
                            type_family: other,
                        }),
                        Err(_) => Err(QueryExecutionError::invalid_text_representation(other, value)),
                    },
                },
            },
        }
    }
}

impl From<UnaryOperator> for UnOperator {
    fn from(operator: UnaryOperator) -> UnOperator {
        match operator {
            UnaryOperator::Minus => UnOperator::Arithmetic(UnArithmetic::Neg),
            UnaryOperator::Plus => UnOperator::Arithmetic(UnArithmetic::Pos),
            UnaryOperator::Not => UnOperator::LogicalNot,
            UnaryOperator::BitwiseNot => UnOperator::BitwiseNot,
            UnaryOperator::SquareRoot => UnOperator::Arithmetic(UnArithmetic::SquareRoot),
            UnaryOperator::CubeRoot => UnOperator::Arithmetic(UnArithmetic::CubeRoot),
            UnaryOperator::PostfixFactorial => UnOperator::Arithmetic(UnArithmetic::Factorial),
            UnaryOperator::PrefixFactorial => UnOperator::Arithmetic(UnArithmetic::Factorial),
            UnaryOperator::Abs => UnOperator::Arithmetic(UnArithmetic::Abs),
        }
    }
}

impl Display for UnOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            UnOperator::Arithmetic(op) => write!(f, "{}", op),
            UnOperator::LogicalNot => write!(f, "NOT"),
            UnOperator::BitwiseNot => write!(f, "~"),
            UnOperator::Cast(type_family) => write!(f, "::{}", type_family),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum UnArithmetic {
    Neg,
    Pos,
    SquareRoot,
    CubeRoot,
    Factorial,
    Abs,
}

impl UnArithmetic {
    fn eval(&self, value: BigDecimal, type_family: SqlTypeFamily) -> Result<ScalarValue, QueryExecutionError> {
        match self {
            UnArithmetic::Neg => Ok(ScalarValue::Num {
                value: -value,
                type_family,
            }),
            UnArithmetic::Pos => Ok(ScalarValue::Num { value, type_family }),
            UnArithmetic::SquareRoot => Ok(ScalarValue::Num {
                value: value
                    .sqrt()
                    .ok_or(QueryExecutionError::InvalidArgumentForPowerFunction)?,
                type_family: SqlTypeFamily::Double,
            }),
            UnArithmetic::CubeRoot => Ok(ScalarValue::Num {
                value: value.cbrt(),
                type_family: SqlTypeFamily::Double,
            }),
            UnArithmetic::Factorial => {
                if vec![SqlTypeFamily::SmallInt, SqlTypeFamily::Integer, SqlTypeFamily::BigInt].contains(&type_family) {
                    let mut result = BigDecimal::from(1);
                    let mut n = BigDecimal::from(1);
                    while n <= value {
                        result *= n.clone();
                        n += BigDecimal::from(1);
                    }
                    Ok(ScalarValue::Num {
                        value: result,
                        type_family: SqlTypeFamily::BigInt,
                    })
                } else {
                    Err(QueryExecutionError::undefined_function(self, type_family))
                }
            }
            UnArithmetic::Abs => Ok(ScalarValue::Num {
                value: value.abs(),
                type_family,
            }),
        }
    }
}

impl Display for UnArithmetic {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            UnArithmetic::Neg => write!(f, "-"),
            UnArithmetic::Pos => write!(f, "+"),
            UnArithmetic::SquareRoot => write!(f, "|/"),
            UnArithmetic::CubeRoot => write!(f, "||/"),
            UnArithmetic::Factorial => write!(f, "!"),
            UnArithmetic::Abs => write!(f, "@"),
        }
    }
}

#[cfg(test)]
mod tests;
