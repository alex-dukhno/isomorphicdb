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

use bigdecimal::BigDecimal;
use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};
use types::{GeneralType, SqlType};

#[derive(Debug, PartialEq)]
pub enum Arithmetic {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Exp,
}

#[derive(Debug, PartialEq)]
pub enum Comparison {
    NotEq,
    Eq,
    LtEq,
    GtEq,
    Lt,
    Gt,
}

#[derive(Debug, PartialEq)]
pub enum Bitwise {
    ShiftRight,
    ShiftLeft,
    Xor,
    And,
    Or,
}

#[derive(Debug, PartialEq)]
pub enum Logical {
    Or,
    And,
}

#[derive(Debug, PartialEq)]
pub enum PatternMatching {
    Like,
    NotLike,
}

#[derive(Debug, PartialEq)]
pub enum StringOp {
    Concat,
}

#[derive(Debug, PartialEq)]
pub enum Operation {
    Arithmetic(Arithmetic),
    Comparison(Comparison),
    Bitwise(Bitwise),
    Logical(Logical),
    PatternMatching(PatternMatching),
    StringOp(StringOp),
}

impl Operation {
    pub fn acceptable_operand_types(&self) -> Vec<(GeneralType, GeneralType)> {
        match self {
            Operation::Logical(_) => vec![(GeneralType::Bool, GeneralType::Bool)],
            Operation::Comparison(_) => vec![
                (GeneralType::Bool, GeneralType::Bool),
                (GeneralType::Number, GeneralType::Number),
                (GeneralType::String, GeneralType::String),
            ],
            Operation::Arithmetic(_) | Operation::Bitwise(_) => vec![(GeneralType::Number, GeneralType::Number)],
            Operation::StringOp(_) | Operation::PatternMatching(_) => vec![(GeneralType::String, GeneralType::String)],
        }
    }

    pub fn result_type(&self) -> GeneralType {
        match self {
            Operation::StringOp(_) => GeneralType::String,
            Operation::Arithmetic(_) | Operation::Bitwise(_) => GeneralType::Number,
            Operation::Comparison(_) | Operation::Logical(_) | Operation::PatternMatching(_) => GeneralType::Bool,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Operator {
    Const(ScalarValue),
    Param(usize),
    Column { sql_type: SqlType, index: usize },
}

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

impl Display for ScalarValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::String(s) => write!(f, "{}", s),
            ScalarValue::Number(n) => write!(f, "{}", n),
            ScalarValue::Bool(Bool(true)) => write!(f, "t"),
            ScalarValue::Bool(Bool(false)) => write!(f, "f"),
            ScalarValue::Null => write!(f, "NULL"),
        }
    }
}

#[cfg(test)]
mod tests;
