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
use types_old::{SqlTypeFamilyOld, SqlTypeOld};

#[derive(Debug, PartialEq, Clone)]
pub enum UntypedItemOld {
    Const(UntypedValueOld),
    Param(usize),
    Column { name: String, sql_type: SqlTypeOld, index: usize },
}

#[derive(Debug, PartialEq, Clone, Eq)]
pub enum UntypedValueOld {
    Literal(String),
    Int(i32),
    BigInt(i64),
    Number(BigDecimal),
    Null,
}

impl UntypedValueOld {
    pub fn kind(&self) -> Option<SqlTypeFamilyOld> {
        match self {
            UntypedValueOld::Int(_) => Some(SqlTypeFamilyOld::Integer),
            UntypedValueOld::BigInt(_) => Some(SqlTypeFamilyOld::BigInt),
            UntypedValueOld::Number(_) => Some(SqlTypeFamilyOld::Real),
            UntypedValueOld::Literal(_) => None,
            UntypedValueOld::Null => None,
        }
    }
}

impl Display for UntypedValueOld {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            UntypedValueOld::Int(n) => write!(f, "{}", n),
            UntypedValueOld::BigInt(n) => write!(f, "{}", n),
            UntypedValueOld::Number(n) => write!(f, "{}", n),
            UntypedValueOld::Literal(value) => write!(f, "{}", value),
            UntypedValueOld::Null => write!(f, "NULL"),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum UntypedTreeOld {
    BiOp {
        left: Box<UntypedTreeOld>,
        op: BiOperator,
        right: Box<UntypedTreeOld>,
    },
    UnOp {
        op: UnOperator,
        item: Box<UntypedTreeOld>,
    },
    Item(UntypedItemOld),
}
