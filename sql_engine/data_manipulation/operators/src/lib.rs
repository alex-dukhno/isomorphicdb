// Copyright 2020 - present Alex Dukhno
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

use bigdecimal::{BigDecimal, ToPrimitive};
use data_manipulation_query_result::QueryExecutionError;
use data_manipulation_typed_values::TypedValue;
use regex::Regex;
use std::fmt::{self, Display, Formatter};
use types::SqlTypeFamily;

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
    pub fn eval(self, left: TypedValue, right: TypedValue) -> Result<TypedValue, QueryExecutionError> {
        match self {
            BiOperator::Arithmetic(op) => match (left, right) {
                (TypedValue::Num { value: left_value, .. }, TypedValue::Num { value: right_value, .. }) => {
                    Ok(TypedValue::Num {
                        value: op.eval(left_value, right_value),
                        type_family: SqlTypeFamily::BigInt,
                    })
                }
                (TypedValue::Num { type_family, .. }, TypedValue::String(value)) => {
                    Err(QueryExecutionError::invalid_text_representation(type_family, value))
                }
                (TypedValue::Num { type_family, .. }, other) => Err(QueryExecutionError::undefined_bi_function(
                    self,
                    type_family,
                    other
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                )),
                (TypedValue::String(value), TypedValue::Num { type_family, .. }) => {
                    Err(QueryExecutionError::invalid_text_representation(type_family, value))
                }
                (other, TypedValue::Num { type_family, .. }) => Err(QueryExecutionError::undefined_bi_function(
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
                (TypedValue::Bool(left_value), TypedValue::Bool(right_value)) => {
                    Ok(TypedValue::Bool(op.eval(left_value, right_value)))
                }
                (TypedValue::String(left_value), TypedValue::String(right_value)) => {
                    Ok(TypedValue::Bool(op.eval(left_value, right_value)))
                }
                (TypedValue::Num { value: left_value, .. }, TypedValue::Num { value: right_value, .. }) => {
                    Ok(TypedValue::Bool(op.eval(left_value, right_value)))
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
                    TypedValue::Num {
                        value: left_value,
                        type_family: SqlTypeFamily::SmallInt,
                    },
                    TypedValue::Num {
                        value: right_value,
                        type_family: SqlTypeFamily::SmallInt,
                    },
                )
                | (
                    TypedValue::Num {
                        value: left_value,
                        type_family: SqlTypeFamily::Integer,
                    },
                    TypedValue::Num {
                        value: right_value,
                        type_family: SqlTypeFamily::Integer,
                    },
                )
                | (
                    TypedValue::Num {
                        value: left_value,
                        type_family: SqlTypeFamily::BigInt,
                    },
                    TypedValue::Num {
                        value: right_value,
                        type_family: SqlTypeFamily::BigInt,
                    },
                )
                | (
                    TypedValue::Num {
                        value: left_value,
                        type_family: SqlTypeFamily::SmallInt,
                    },
                    TypedValue::Num {
                        value: right_value,
                        type_family: SqlTypeFamily::Integer,
                    },
                )
                | (
                    TypedValue::Num {
                        value: left_value,
                        type_family: SqlTypeFamily::SmallInt,
                    },
                    TypedValue::Num {
                        value: right_value,
                        type_family: SqlTypeFamily::BigInt,
                    },
                )
                | (
                    TypedValue::Num {
                        value: left_value,
                        type_family: SqlTypeFamily::Integer,
                    },
                    TypedValue::Num {
                        value: right_value,
                        type_family: SqlTypeFamily::SmallInt,
                    },
                )
                | (
                    TypedValue::Num {
                        value: left_value,
                        type_family: SqlTypeFamily::Integer,
                    },
                    TypedValue::Num {
                        value: right_value,
                        type_family: SqlTypeFamily::BigInt,
                    },
                )
                | (
                    TypedValue::Num {
                        value: left_value,
                        type_family: SqlTypeFamily::BigInt,
                    },
                    TypedValue::Num {
                        value: right_value,
                        type_family: SqlTypeFamily::SmallInt,
                    },
                )
                | (
                    TypedValue::Num {
                        value: left_value,
                        type_family: SqlTypeFamily::BigInt,
                    },
                    TypedValue::Num {
                        value: right_value,
                        type_family: SqlTypeFamily::Integer,
                    },
                ) => Ok(TypedValue::Num {
                    value: op.eval(left_value, right_value),
                    type_family: SqlTypeFamily::BigInt,
                }),
                (TypedValue::Num { type_family, .. }, TypedValue::String(value)) => {
                    Err(QueryExecutionError::invalid_text_representation(type_family, value))
                }
                (TypedValue::Num { type_family, .. }, other) => Err(QueryExecutionError::undefined_bi_function(
                    self,
                    type_family,
                    other
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                )),
                (TypedValue::String(value), TypedValue::Num { type_family, .. }) => {
                    Err(QueryExecutionError::invalid_text_representation(type_family, value))
                }
                (other, TypedValue::Num { type_family, .. }) => Err(QueryExecutionError::undefined_bi_function(
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
                (TypedValue::Bool(left_value), TypedValue::Bool(right_value)) => {
                    Ok(TypedValue::Bool(op.eval(left_value, right_value)))
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
                (TypedValue::String(left_value), TypedValue::String(right_value)) => {
                    Ok(TypedValue::Bool(op.eval(left_value, right_value)))
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
                (TypedValue::String(left_value), TypedValue::String(right_value)) => {
                    Ok(TypedValue::String(op.eval(left_value, right_value)))
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
}

impl UnOperator {
    pub fn eval(self, value: TypedValue) -> Result<TypedValue, QueryExecutionError> {
        match self {
            UnOperator::Arithmetic(operator) => match value {
                TypedValue::Num { value, type_family } => operator.eval(value, type_family),
                other => Err(QueryExecutionError::undefined_function(
                    self,
                    other
                        .type_family()
                        .map(|ty| ty.to_string())
                        .unwrap_or_else(|| "unknown".to_owned()),
                )),
            },
            UnOperator::LogicalNot => match value {
                TypedValue::Bool(value) => Ok(TypedValue::Bool(!value)),
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
                TypedValue::Num {
                    value,
                    type_family: SqlTypeFamily::SmallInt,
                } => Ok(TypedValue::Num {
                    value: BigDecimal::from(!value.to_i16().unwrap()),
                    type_family: SqlTypeFamily::SmallInt,
                }),
                TypedValue::Num {
                    value,
                    type_family: SqlTypeFamily::Integer,
                } => Ok(TypedValue::Num {
                    value: BigDecimal::from(!value.to_i32().unwrap()),
                    type_family: SqlTypeFamily::Integer,
                }),
                TypedValue::Num {
                    value,
                    type_family: SqlTypeFamily::BigInt,
                } => Ok(TypedValue::Num {
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
        }
    }
}

impl Display for UnOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            UnOperator::Arithmetic(op) => write!(f, "{}", op),
            UnOperator::LogicalNot => write!(f, "NOT"),
            UnOperator::BitwiseNot => write!(f, "~"),
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
    fn eval(&self, value: BigDecimal, type_family: SqlTypeFamily) -> Result<TypedValue, QueryExecutionError> {
        match self {
            UnArithmetic::Neg => Ok(TypedValue::Num {
                value: -value,
                type_family,
            }),
            UnArithmetic::Pos => Ok(TypedValue::Num { value, type_family }),
            UnArithmetic::SquareRoot => Ok(TypedValue::Num {
                value: value
                    .sqrt()
                    .ok_or(QueryExecutionError::InvalidArgumentForPowerFunction)?,
                type_family: SqlTypeFamily::Double,
            }),
            UnArithmetic::CubeRoot => Ok(TypedValue::Num {
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
                    Ok(TypedValue::Num {
                        value: result,
                        type_family: SqlTypeFamily::BigInt,
                    })
                } else {
                    Err(QueryExecutionError::undefined_function(self, type_family))
                }
            }
            UnArithmetic::Abs => Ok(TypedValue::Num {
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
