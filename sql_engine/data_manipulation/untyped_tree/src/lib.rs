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

use bigdecimal::BigDecimal;
use data_manipulation_operators::{BiOperator, UnOperator};
use std::{
    fmt,
    fmt::{Display, Formatter},
};
use types::{Bool, SqlType, SqlTypeFamily};

#[derive(Debug, PartialEq, Clone)]
pub enum UntypedItem {
    Const(UntypedValue),
    Param(usize),
    Column { name: String, sql_type: SqlType, index: usize },
}

#[derive(Debug, PartialEq, Clone, Eq)]
pub enum UntypedValue {
    Literal(String),
    Int(i32),
    BigInt(i64),
    Number(BigDecimal),
    Bool(Bool),
    Null,
}

impl UntypedValue {
    pub fn kind(&self) -> Option<SqlTypeFamily> {
        match self {
            UntypedValue::Int(_) => Some(SqlTypeFamily::Integer),
            UntypedValue::BigInt(_) => Some(SqlTypeFamily::BigInt),
            UntypedValue::Number(_) => Some(SqlTypeFamily::Real),
            UntypedValue::Bool(_) => Some(SqlTypeFamily::Bool),
            UntypedValue::Literal(_) => None,
            UntypedValue::Null => None,
        }
    }
}

impl Display for UntypedValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            UntypedValue::Int(n) => write!(f, "{}", n),
            UntypedValue::BigInt(n) => write!(f, "{}", n),
            UntypedValue::Number(n) => write!(f, "{}", n),
            UntypedValue::Bool(Bool(true)) => write!(f, "t"),
            UntypedValue::Bool(Bool(false)) => write!(f, "f"),
            UntypedValue::Literal(value) => write!(f, "{}", value),
            UntypedValue::Null => write!(f, "NULL"),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum UntypedTree {
    BiOp {
        left: Box<UntypedTree>,
        op: BiOperator,
        right: Box<UntypedTree>,
    },
    UnOp {
        op: UnOperator,
        item: Box<UntypedTree>,
    },
    Item(UntypedItem),
}
