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

use types::SqlTypeFamily;

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum Arithmetic {
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
pub enum Logical {
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
pub enum Operation {
    Arithmetic(Arithmetic),
    Comparison(Comparison),
    Bitwise(Bitwise),
    Logical(Logical),
    PatternMatching(PatternMatching),
    StringOp(StringOp),
}

impl Operation {
    pub fn resulted_types(&self) -> Vec<SqlTypeFamily> {
        match self {
            Operation::Arithmetic(_) => vec![SqlTypeFamily::Integer, SqlTypeFamily::Real],
            Operation::Comparison(_) => vec![SqlTypeFamily::Bool],
            Operation::Bitwise(_) => vec![SqlTypeFamily::Integer],
            Operation::Logical(_) => vec![SqlTypeFamily::Bool],
            Operation::PatternMatching(_) => vec![SqlTypeFamily::Bool],
            Operation::StringOp(_) => vec![SqlTypeFamily::Bool],
        }
    }

    pub fn supported_type_family(&self, left: Option<SqlTypeFamily>, right: Option<SqlTypeFamily>) -> bool {
        match self {
            Operation::Arithmetic(_) => {
                left == Some(SqlTypeFamily::Integer) && right == Some(SqlTypeFamily::Integer)
                    || left == Some(SqlTypeFamily::Real) && right == Some(SqlTypeFamily::Integer)
                    || left == Some(SqlTypeFamily::Integer) && right == Some(SqlTypeFamily::Real)
                    || left == Some(SqlTypeFamily::Real) && right == Some(SqlTypeFamily::Real)
            }
            Operation::Comparison(_) => left.is_some() && left == right,
            Operation::Bitwise(_) => left == Some(SqlTypeFamily::Integer) && right == Some(SqlTypeFamily::Integer),
            Operation::Logical(_) => left == Some(SqlTypeFamily::Bool) && right == Some(SqlTypeFamily::Bool),
            Operation::PatternMatching(_) => {
                left == Some(SqlTypeFamily::String) && right == Some(SqlTypeFamily::String)
            }
            Operation::StringOp(_) => left == Some(SqlTypeFamily::String) && right == Some(SqlTypeFamily::String),
        }
    }
}

#[cfg(test)]
mod tests;
