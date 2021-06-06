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

use query_ast::BinaryOperator;
use std::{
    cmp::Ordering,
    fmt::{self, Display, Formatter},
};
use types::{FloatNumFamily, IntNumFamily, SqlTypeFamily, TemporalFamily};

#[derive(Debug, PartialEq)]
pub struct UndefinedBiOperatorError {
    pub op: BiOperator,
    pub left: SqlTypeFamily,
    pub right: SqlTypeFamily,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum BiArithmetic {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Exp,
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
    pub fn infer_return_type(&self, left: SqlTypeFamily, right: SqlTypeFamily) -> Result<SqlTypeFamily, UndefinedBiOperatorError> {
        match self {
            BiOperator::Arithmetic(BiArithmetic::Add) => match (left, right) {
                (SqlTypeFamily::Float(_), SqlTypeFamily::Int(_)) => Ok(SqlTypeFamily::Float(FloatNumFamily::Double)),
                (SqlTypeFamily::Int(_), SqlTypeFamily::Float(_)) => Ok(SqlTypeFamily::Float(FloatNumFamily::Double)),
                (SqlTypeFamily::Float(_), SqlTypeFamily::Float(_)) => Ok(SqlTypeFamily::Float(FloatNumFamily::Double)),
                (SqlTypeFamily::Float(_), SqlTypeFamily::Numeric) => Ok(SqlTypeFamily::Float(FloatNumFamily::Double)),
                (SqlTypeFamily::Numeric, SqlTypeFamily::Float(_)) => Ok(SqlTypeFamily::Float(FloatNumFamily::Double)),
                (SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Temporal(TemporalFamily::Interval))
                | (SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Temporal(TemporalFamily::Time)) => {
                    Ok(SqlTypeFamily::Temporal(TemporalFamily::Time))
                }
                (SqlTypeFamily::Int(IntNumFamily::SmallInt), SqlTypeFamily::Temporal(TemporalFamily::Date))
                | (SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Int(IntNumFamily::SmallInt))
                | (SqlTypeFamily::Int(IntNumFamily::Integer), SqlTypeFamily::Temporal(TemporalFamily::Date))
                | (SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Int(IntNumFamily::Integer)) => {
                    Ok(SqlTypeFamily::Temporal(TemporalFamily::Date))
                }
                (SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Temporal(TemporalFamily::Interval))
                | (SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Temporal(TemporalFamily::Date))
                | (SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Temporal(TemporalFamily::Timestamp))
                | (SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::Temporal(TemporalFamily::Time))
                | (SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Temporal(TemporalFamily::Date))
                | (SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Temporal(TemporalFamily::Time))
                | (SqlTypeFamily::Temporal(TemporalFamily::Timestamp), SqlTypeFamily::Temporal(TemporalFamily::Interval))
                | (SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Temporal(TemporalFamily::Timestamp)) => {
                    Ok(SqlTypeFamily::Temporal(TemporalFamily::Timestamp))
                }
                (SqlTypeFamily::Temporal(TemporalFamily::Time), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ))
                | (SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Temporal(TemporalFamily::Time))
                | (SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Temporal(TemporalFamily::Interval))
                | (SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ))
                | (SqlTypeFamily::Unknown, SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ))
                | (SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ), SqlTypeFamily::Unknown) => {
                    Ok(SqlTypeFamily::Temporal(TemporalFamily::TimestampTZ))
                }
                (SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Temporal(TemporalFamily::Interval))
                | (SqlTypeFamily::Unknown, SqlTypeFamily::Temporal(TemporalFamily::Interval))
                | (SqlTypeFamily::Temporal(TemporalFamily::Interval), SqlTypeFamily::Unknown) => {
                    Ok(SqlTypeFamily::Temporal(TemporalFamily::Interval))
                }
                (SqlTypeFamily::Unknown, SqlTypeFamily::Unknown) => Err(UndefinedBiOperatorError { op: *self, left, right }),
                (SqlTypeFamily::Unknown, SqlTypeFamily::Temporal(TemporalFamily::Date)) => Err(UndefinedBiOperatorError { op: *self, left, right }),
                (SqlTypeFamily::Temporal(TemporalFamily::Date), SqlTypeFamily::Unknown) => Err(UndefinedBiOperatorError { op: *self, left, right }),
                (SqlTypeFamily::Unknown, SqlTypeFamily::String(_)) => Err(UndefinedBiOperatorError { op: *self, left, right }),
                (SqlTypeFamily::String(_), SqlTypeFamily::Unknown) => Err(UndefinedBiOperatorError { op: *self, left, right }),
                (SqlTypeFamily::Unknown, SqlTypeFamily::Bool) => Err(UndefinedBiOperatorError { op: *self, left, right }),
                (SqlTypeFamily::Bool, SqlTypeFamily::Unknown) => Err(UndefinedBiOperatorError { op: *self, left, right }),
                (SqlTypeFamily::String(_), SqlTypeFamily::String(_)) => Err(UndefinedBiOperatorError { op: *self, left, right }),
                (SqlTypeFamily::String(_), SqlTypeFamily::Temporal(_)) => Err(UndefinedBiOperatorError { op: *self, left, right }),
                (SqlTypeFamily::Temporal(_), SqlTypeFamily::String(_)) => Err(UndefinedBiOperatorError { op: *self, left, right }),
                (SqlTypeFamily::String(_), SqlTypeFamily::Int(_)) => Err(UndefinedBiOperatorError { op: *self, left, right }),
                (SqlTypeFamily::Int(_), SqlTypeFamily::String(_)) => Err(UndefinedBiOperatorError { op: *self, left, right }),
                (SqlTypeFamily::String(_), SqlTypeFamily::Float(_)) => Err(UndefinedBiOperatorError { op: *self, left, right }),
                (SqlTypeFamily::Float(_), SqlTypeFamily::String(_)) => Err(UndefinedBiOperatorError { op: *self, left, right }),
                (SqlTypeFamily::String(_), SqlTypeFamily::Numeric) => Err(UndefinedBiOperatorError { op: *self, left, right }),
                (SqlTypeFamily::Numeric, SqlTypeFamily::String(_)) => Err(UndefinedBiOperatorError { op: *self, left, right }),
                (SqlTypeFamily::String(_), SqlTypeFamily::Bool) => Err(UndefinedBiOperatorError { op: *self, left, right }),
                (SqlTypeFamily::Bool, SqlTypeFamily::String(_)) => Err(UndefinedBiOperatorError { op: *self, left, right }),
                (SqlTypeFamily::Temporal(_), SqlTypeFamily::Temporal(_)) => Err(UndefinedBiOperatorError { op: *self, left, right }),
                (left, right) => match left.partial_cmp(&right) {
                    None => Err(UndefinedBiOperatorError { op: *self, left, right }),
                    Some(Ordering::Greater) => Ok(left),
                    Some(Ordering::Equal) => Ok(left),
                    Some(Ordering::Less) => Ok(right),
                },
            },
            _ => unimplemented!(),
        }
    }
}

impl From<BinaryOperator> for BiOperator {
    fn from(operator: BinaryOperator) -> BiOperator {
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

#[cfg(test)]
mod tests;
