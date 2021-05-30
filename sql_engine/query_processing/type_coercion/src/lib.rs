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

use checked_tree::{CheckedItem, CheckedTree, CheckedValue};
use exec_tree::{ExecutableItem, ExecutableTree, ExecutableValue};
use query_response::QueryError;
use typed_tree::TypedTreeOld;

#[derive(Debug, PartialEq)]
pub struct TypeCoercionError;

impl From<TypeCoercionError> for QueryError {
    fn from(_error: TypeCoercionError) -> QueryError {
        unimplemented!()
    }
}

pub struct TypeCoercion;

impl TypeCoercion {
    pub fn coerce_type(&self, tree: CheckedTree) -> Result<ExecutableTree, TypeCoercionError> {
        match tree {
            CheckedTree::Item(CheckedItem::Const(CheckedValue::Numeric(value))) => {
                Ok(ExecutableTree::Item(ExecutableItem::Const(ExecutableValue::Numeric(value))))
            }
            CheckedTree::Item(CheckedItem::Const(CheckedValue::Int(value))) => {
                Ok(ExecutableTree::Item(ExecutableItem::Const(ExecutableValue::Int(value))))
            }
            CheckedTree::Item(CheckedItem::Const(CheckedValue::BigInt(value))) => {
                Ok(ExecutableTree::Item(ExecutableItem::Const(ExecutableValue::BigInt(value))))
            }
            CheckedTree::Item(CheckedItem::Const(CheckedValue::StringLiteral(value))) => {
                Ok(ExecutableTree::Item(ExecutableItem::Const(ExecutableValue::StringLiteral(value))))
            }
            CheckedTree::Item(CheckedItem::Const(CheckedValue::Null)) => Ok(ExecutableTree::Item(ExecutableItem::Const(ExecutableValue::Null))),
            CheckedTree::UnOp { op, item } => Ok(ExecutableTree::UnOp {
                op,
                item: Box::new(self.coerce_type(*item)?),
            }),
            CheckedTree::BiOp { left, op, right } => Ok(ExecutableTree::BiOp {
                op,
                left: Box::new(self.coerce_type(*left)?),
                right: Box::new(self.coerce_type(*right)?),
            }),
        }
    }
}

pub struct TypeCoercionOld;

impl TypeCoercionOld {
    pub fn coerce(&self, tree: TypedTreeOld) -> TypedTreeOld {
        tree
    }
}

#[cfg(test)]
mod tests;
