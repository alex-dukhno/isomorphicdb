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

use data_manipulation_operators::Operation;
use types::SqlTypeFamily;

#[derive(Debug, PartialEq, Clone)]
pub enum StaticTypedTree {
    Item(StaticTypedItem),
    Operation {
        type_family: Option<SqlTypeFamily>,
        left: Box<StaticTypedTree>,
        op: Operation,
        right: Box<StaticTypedTree>,
    },
}

impl StaticTypedTree {
    pub fn type_family(&self) -> Option<SqlTypeFamily> {
        match self {
            StaticTypedTree::Item(item) => item.type_family(),
            StaticTypedTree::Operation { type_family, .. } => *type_family,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum StaticTypedItem {
    Const(TypedValue),
    Param {
        index: usize,
        type_family: Option<SqlTypeFamily>,
    },
    Null(Option<SqlTypeFamily>),
}

impl StaticTypedItem {
    fn type_family(&self) -> Option<SqlTypeFamily> {
        match self {
            StaticTypedItem::Const(typed_value) => typed_value.type_family(),
            StaticTypedItem::Param { type_family, .. } => *type_family,
            StaticTypedItem::Null(type_family) => *type_family,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum TypedValue {
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Real(f32),
    String(String),
    Double(f64),
    Bool(bool),
}

impl TypedValue {
    fn type_family(&self) -> Option<SqlTypeFamily> {
        match self {
            TypedValue::SmallInt(_) => Some(SqlTypeFamily::SmallInt),
            TypedValue::Integer(_) => Some(SqlTypeFamily::Integer),
            TypedValue::BigInt(_) => Some(SqlTypeFamily::BigInt),
            TypedValue::Real(_) => Some(SqlTypeFamily::Real),
            TypedValue::Double(_) => Some(SqlTypeFamily::Double),
            TypedValue::String(_) => Some(SqlTypeFamily::String),
            TypedValue::Bool(_) => Some(SqlTypeFamily::Bool),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum DynamicTypedTree {
    Operation {
        left: Box<DynamicTypedTree>,
        op: Operation,
        right: Box<DynamicTypedTree>,
    },
    Item(DynamicTypedItem),
}

#[derive(Debug, PartialEq, Clone)]
pub enum DynamicTypedItem {
    Const(TypedValue),
    Column(String),
}
