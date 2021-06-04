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
use std::{
    fmt,
    fmt::{Display, Formatter},
    str::FromStr,
};
use typed_tree::{TypedItem, TypedValue};
use types::SqlTypeFamily;

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

#[derive(Debug, PartialEq, Clone)]
pub enum UntypedItem {
    Const(UntypedValue),
    Param(usize),
    Column { name: String, sql_type: SqlTypeFamily, index: usize },
}

impl UntypedItem {
    pub fn infer_type(self) -> TypedItem {
        match self {
            UntypedItem::Const(UntypedValue::Int(value)) => TypedItem::Const(TypedValue::Int(value)),
            UntypedItem::Const(UntypedValue::BigInt(value)) => TypedItem::Const(TypedValue::BigInt(value)),
            UntypedItem::Const(UntypedValue::Number(value)) => TypedItem::Const(TypedValue::Numeric(value)),
            UntypedItem::Const(UntypedValue::Literal(value)) => TypedItem::Const(TypedValue::StringLiteral(value)),
            UntypedItem::Const(UntypedValue::Null) => TypedItem::Null(SqlTypeFamily::Unknown),
            UntypedItem::Param(index) => TypedItem::Param {
                index,
                type_family: SqlTypeFamily::Unknown,
            },
            UntypedItem::Column { name, sql_type, index } => TypedItem::Column { name, sql_type, index },
        }
    }
}

#[derive(Debug, PartialEq, Clone, Eq)]
pub enum UntypedValue {
    Literal(String),
    Int(i32),
    BigInt(i64),
    Number(BigDecimal),
    Null,
}

impl Display for UntypedValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            UntypedValue::Int(n) => write!(f, "{}", n),
            UntypedValue::BigInt(n) => write!(f, "{}", n),
            UntypedValue::Number(n) => write!(f, "{}", n),
            UntypedValue::Literal(value) => write!(f, "{}", value),
            UntypedValue::Null => write!(f, "NULL"),
        }
    }
}

#[derive(PartialEq, Debug, Copy, Clone, Eq)]
pub struct Bool(pub bool);

impl FromStr for Bool {
    type Err = ParseBoolError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let val = s.to_lowercase();
        match val.as_str() {
            "t" | "true" | "on" | "yes" | "y" | "1" => Ok(Bool(true)),
            "f" | "false" | "off" | "no" | "n" | "0" => Ok(Bool(false)),
            _ => Err(ParseBoolError(val)),
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct ParseBoolError(String);

impl Display for ParseBoolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "error to parse {:?} into boolean", self.0)
    }
}
