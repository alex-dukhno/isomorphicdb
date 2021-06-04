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
use operators::{BiOperator, UnOperator};
use std::fmt::{self, Display, Formatter};
use types::{IntNumFamily, SqlTypeFamily};

#[derive(Debug, PartialEq, Clone)]
pub enum TypedTree {
    BiOp {
        type_family: SqlTypeFamily,
        left: Box<TypedTree>,
        op: BiOperator,
        right: Box<TypedTree>,
    },
    UnOp {
        op: UnOperator,
        item: Box<TypedTree>,
    },
    Item(TypedItem),
}

impl TypedTree {
    pub fn type_family(&self) -> SqlTypeFamily {
        match self {
            TypedTree::BiOp { type_family, .. } => *type_family,
            TypedTree::UnOp {
                op: UnOperator::Cast(type_family),
                ..
            } => *type_family,
            TypedTree::UnOp { item, .. } => item.type_family(),
            TypedTree::Item(item) => item.type_family(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum TypedItem {
    Const(TypedValue),
    Param { index: usize, type_family: SqlTypeFamily },
    Null(SqlTypeFamily),
    Column { name: String, sql_type: SqlTypeFamily, index: usize },
}

impl TypedItem {
    fn type_family(&self) -> SqlTypeFamily {
        match self {
            TypedItem::Const(TypedValue::Int(_)) => SqlTypeFamily::Int(IntNumFamily::Integer),
            TypedItem::Const(TypedValue::BigInt(_)) => SqlTypeFamily::Int(IntNumFamily::BigInt),
            TypedItem::Const(TypedValue::Numeric(_)) => SqlTypeFamily::Numeric,
            TypedItem::Const(TypedValue::Bool(_)) => SqlTypeFamily::Bool,
            TypedItem::Const(TypedValue::StringLiteral(_)) => SqlTypeFamily::Unknown,
            TypedItem::Param { type_family, .. } => *type_family,
            TypedItem::Null(type_family) => *type_family,
            TypedItem::Column { sql_type, .. } => *sql_type,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum TypedValue {
    Int(i32),
    BigInt(i64),
    Numeric(BigDecimal),
    StringLiteral(String),
    Bool(bool),
}

impl Display for TypedValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TypedValue::Int(value) => write!(f, "{}", value),
            TypedValue::BigInt(value) => write!(f, "{}", value),
            TypedValue::Numeric(value) => write!(f, "{}", value),
            TypedValue::StringLiteral(value) => write!(f, "{}", value),
            TypedValue::Bool(value) => write!(f, "{}", value),
        }
    }
}
