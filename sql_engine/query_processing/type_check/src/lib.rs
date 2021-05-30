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
use checked_tree::{CheckedItem, CheckedTree, CheckedValue};
use query_response::QueryError;
use typed_tree::TypedTreeOld;
use typed_tree::{TypedItem, TypedTree, TypedValue};

pub struct TypeCheckerOld;

impl TypeCheckerOld {
    pub fn type_check(&self, tree: TypedTreeOld) -> TypedTreeOld {
        tree
    }
}

#[derive(Debug, PartialEq)]
pub struct TypeCheckError;

impl From<TypeCheckError> for QueryError {
    fn from(_error: TypeCheckError) -> QueryError {
        unimplemented!()
    }
}

pub struct TypeChecker;

impl TypeChecker {
    pub fn type_check(&self, tree: TypedTree) -> Result<CheckedTree, TypeCheckError> {
        match tree {
            TypedTree::Item(TypedItem::Const(TypedValue::Numeric(value))) => Ok(CheckedTree::Item(CheckedItem::Const(CheckedValue::Numeric(value)))),
            TypedTree::Item(TypedItem::Const(TypedValue::Int(value))) => Ok(CheckedTree::Item(CheckedItem::Const(CheckedValue::Int(value)))),
            TypedTree::Item(TypedItem::Const(TypedValue::BigInt(value))) => Ok(CheckedTree::Item(CheckedItem::Const(CheckedValue::BigInt(value)))),
            TypedTree::Item(TypedItem::Const(TypedValue::StringLiteral(value))) => {
                Ok(CheckedTree::Item(CheckedItem::Const(CheckedValue::StringLiteral(value))))
            }
            TypedTree::Item(TypedItem::Const(TypedValue::Null)) => Ok(CheckedTree::Item(CheckedItem::Const(CheckedValue::Null))),
            TypedTree::UnOp { op, item } => Ok(CheckedTree::UnOp {
                op,
                item: Box::new(self.type_check(*item)?),
            }),
            TypedTree::BiOp { left, op, right } => Ok(CheckedTree::BiOp {
                op,
                left: Box::new(self.type_check(*left)?),
                right: Box::new(self.type_check(*right)?),
            }),
        }
    }
}

#[cfg(test)]
mod tests;
