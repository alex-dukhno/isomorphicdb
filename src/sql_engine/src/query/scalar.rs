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

///! Module for representing scalar level operations. Implementation of
///! theses operators will be defined in a sperate module.
use super::ColumnType;
use representation::{Binary, Datum};
// use crate::query::relation::RelationType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Plus,
    Minus,
    Multiply,
    Division,
    Modulo,

    BitAnd,
    BitOr,
    BitXOR,

    Equal,
    Greater,
    Less,
    GreaterEqual,
    LessEqual,
    NotEqual,
    PlusEqual,
    MinusEqual,

    MultiplyEqual,
    DivisionEqual,
    ModuleEqual,
    BitAndEqual,
    XOREqual,
    OREqual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Plus,
    Minus,
    BitNot,
}

/// Operation performed on the table
/// influenced by Materialized's ScalarExpr
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarOp {
    /// column access
    Column(usize),
    /// literal value (owned) and expected type.
    Literal(Datum<'static>),
    /// binary operator
    Binary(BinaryOp, Box<ScalarOp>, Box<ScalarOp>),
    /// unary operator
    Unary(UnaryOp, Box<ScalarOp>),
}

impl ScalarOp {
    pub fn is_literal(&self) -> bool {
        match self {
            ScalarOp::Literal(_) => true,
            _ => false,
        }
    }

    pub fn as_datum(&self) -> Option<Datum<'static>> {
        match self {
            ScalarOp::Literal(datum) => Some(datum.clone()),
            _ => None,
        }
    }
}
