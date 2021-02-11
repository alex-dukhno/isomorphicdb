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

use data_manipulation_operators::{BiOperator, UnOperator};
use data_manipulation_query_result::QueryExecutionError;
use data_manipulation_typed_values::TypedValue;
use types::SqlTypeFamily;

#[derive(Debug, PartialEq, Clone)]
pub enum StaticTypedTree {
    Item(StaticTypedItem),
    BiOp {
        type_family: SqlTypeFamily,
        left: Box<StaticTypedTree>,
        op: BiOperator,
        right: Box<StaticTypedTree>,
    },
    UnOp {
        op: UnOperator,
        item: Box<StaticTypedTree>,
    },
}

impl StaticTypedTree {
    pub fn type_family(&self) -> Option<SqlTypeFamily> {
        match self {
            StaticTypedTree::Item(item) => item.type_family(),
            StaticTypedTree::BiOp { type_family, .. } => Some(*type_family),
            StaticTypedTree::UnOp { item, .. } => item.type_family(),
        }
    }

    pub fn eval(self) -> Result<TypedValue, QueryExecutionError> {
        match self {
            StaticTypedTree::Item(StaticTypedItem::Const(value)) => Ok(value),
            StaticTypedTree::Item(StaticTypedItem::Null(_)) => unimplemented!(),
            StaticTypedTree::Item(StaticTypedItem::Param { .. }) => unimplemented!(),
            StaticTypedTree::UnOp { op, item } => op.eval(item.eval()?),
            StaticTypedTree::BiOp { left, op, right, .. } => op.eval(left.eval()?, right.eval()?),
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
pub enum DynamicTypedTree {
    BiOp {
        type_family: SqlTypeFamily,
        left: Box<DynamicTypedTree>,
        op: BiOperator,
        right: Box<DynamicTypedTree>,
    },
    UnOp {
        op: UnOperator,
        item: Box<DynamicTypedTree>,
    },
    Item(DynamicTypedItem),
}

impl DynamicTypedTree {
    pub fn type_family(&self) -> Option<SqlTypeFamily> {
        match self {
            DynamicTypedTree::Item(item) => item.type_family(),
            DynamicTypedTree::BiOp { type_family, .. } => Some(*type_family),
            DynamicTypedTree::UnOp { item, .. } => item.type_family(),
        }
    }

    pub fn eval(&self) -> Result<TypedValue, QueryExecutionError> {
        match self {
            DynamicTypedTree::Item(DynamicTypedItem::Const(value)) => Ok(value.clone()),
            DynamicTypedTree::Item(DynamicTypedItem::Column(_)) => unimplemented!(),
            DynamicTypedTree::UnOp { op, item } => op.eval(item.eval()?),
            DynamicTypedTree::BiOp { left, op, right, .. } => op.eval(left.eval()?, right.eval()?),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum DynamicTypedItem {
    Const(TypedValue),
    Column(String),
}

impl DynamicTypedItem {
    fn type_family(&self) -> Option<SqlTypeFamily> {
        match self {
            DynamicTypedItem::Const(typed_value) => typed_value.type_family(),
            DynamicTypedItem::Column(_typed_value) => None,
        }
    }
}

#[cfg(test)]
mod tests;
