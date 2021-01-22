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

use types::{SqlFamilyType};

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
    pub fn resulted_types(&self) -> Vec<SqlFamilyType> {
        match self {
            Operation::Arithmetic(_) => vec![SqlFamilyType::Integer, SqlFamilyType::Float],
            Operation::Comparison(_) => vec![SqlFamilyType::Bool],
            Operation::Bitwise(_) => vec![SqlFamilyType::Integer],
            Operation::Logical(_) => vec![SqlFamilyType::Bool],
            Operation::PatternMatching(_) => vec![SqlFamilyType::Bool],
            Operation::StringOp(_) => vec![SqlFamilyType::Bool],
        }
    }

    pub fn supported_type_family(&self, left: Option<SqlFamilyType>, right: Option<SqlFamilyType>) -> bool {
        match self {
            Operation::Arithmetic(_) => {
                left == Some(SqlFamilyType::Integer) && right == Some(SqlFamilyType::Integer)
                    || left == Some(SqlFamilyType::Float) && right == Some(SqlFamilyType::Integer)
                    || left == Some(SqlFamilyType::Integer) && right == Some(SqlFamilyType::Float)
                    || left == Some(SqlFamilyType::Float) && right == Some(SqlFamilyType::Float)
            }
            Operation::Comparison(_) => left.is_some() && left == right,
            Operation::Bitwise(_) => left == Some(SqlFamilyType::Integer) && right == Some(SqlFamilyType::Integer),
            Operation::Logical(_) => left == Some(SqlFamilyType::Bool) && right == Some(SqlFamilyType::Bool),
            Operation::PatternMatching(_) => {
                left == Some(SqlFamilyType::String) && right == Some(SqlFamilyType::String)
            }
            Operation::StringOp(_) => left == Some(SqlFamilyType::String) && right == Some(SqlFamilyType::String),
        }
    }
}

#[cfg(test)]
mod tests;
