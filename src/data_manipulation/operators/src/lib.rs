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

use std::fmt::{self, Display, Formatter};
use data_manipulation_typed_values::TypedValue;
use types::SqlTypeFamily;
use data_manipulation_query_result::QueryExecutionError;
use bigdecimal::ToPrimitive;
use bigdecimal::BigDecimal;

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum BiArithmetic {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Exp,
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

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum Bitwise {
    ShiftRight,
    ShiftLeft,
    Xor,
    And,
    Or,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum BiLogical {
    Or,
    And,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum PatternMatching {
    Like,
    NotLike,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum StringOp {
    Concat,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum BiOperator {
    Arithmetic(BiArithmetic),
    Comparison(Comparison),
    Bitwise(Bitwise),
    Logical(BiLogical),
    PatternMatching(PatternMatching),
    StringOp(StringOp),
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
                    other.type_family().map(|ty| ty.to_string()).unwrap_or_else(|| "unknown".to_owned()),
                )),
            },
            UnOperator::LogicalNot => match value {
                TypedValue::Bool(value) => Ok(TypedValue::Bool(!value)),
                other => Err(QueryExecutionError::datatype_mismatch(
                    self,
                    SqlTypeFamily::Bool,
                    other.type_family().map(|ty| ty.to_string()).unwrap_or_else(|| "unknown".to_owned()),
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
                    other.type_family().map(|ty| ty.to_string()).unwrap_or_else(|| "unknown".to_owned()),
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
            UnArithmetic::Neg => Ok(TypedValue::Num { value: -value, type_family }),
            UnArithmetic::Pos => Ok(TypedValue::Num { value, type_family }),
            UnArithmetic::SquareRoot => Ok(TypedValue::Num { value: value.sqrt().ok_or(QueryExecutionError::InvalidArgumentForPowerFunction)?, type_family: SqlTypeFamily::Double }),
            UnArithmetic::CubeRoot => Ok(TypedValue::Num { value: value.cbrt(), type_family: SqlTypeFamily::Double }),
            UnArithmetic::Factorial => {
                if vec![SqlTypeFamily::SmallInt, SqlTypeFamily::Integer, SqlTypeFamily::BigInt].contains(&type_family) {
                    let mut result = BigDecimal::from(1);
                    let mut n = BigDecimal::from(1);
                    while n <= value {
                        result *= n.clone();
                        n += BigDecimal::from(1);
                    }
                    Ok(TypedValue::Num { value: result, type_family: SqlTypeFamily::BigInt })
                } else {
                    Err(QueryExecutionError::undefined_function(
                        self,
                        type_family,
                    ))
                }
            }
            UnArithmetic::Abs => Ok(TypedValue::Num { value: value.abs(), type_family }),
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
