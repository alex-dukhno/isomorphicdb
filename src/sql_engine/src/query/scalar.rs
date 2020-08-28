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
use sqlparser::ast::{BinaryOperator, UnaryOperator};
// use crate::query::relation::RelationType;

/// Operation performed on the table
/// influenced by Materialized's ScalarExpr
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarOp {
    /// column access
    Column(usize),
    /// literal value (owned) and expected type.
    Literal(Datum<'static>),
    /// binary operator
    Binary(BinaryOperator, Box<ScalarOp>, Box<ScalarOp>),
    /// unary operator
    Unary(UnaryOperator, Box<ScalarOp>),
    Assignment {
        destination: usize,
        value: Box<ScalarOp>,
    }
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
