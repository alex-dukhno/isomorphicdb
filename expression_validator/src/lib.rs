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

use operators::UnOperator;
use typed_tree::TypedTree;
use types::SqlTypeFamily;
use untyped_tree::UntypedTree;

#[derive(Debug, PartialEq)]
pub enum ExpressionError {
    DatatypeMismatch { expected: SqlTypeFamily, actual: SqlTypeFamily },
}

pub struct ExpressionValidator;

impl ExpressionValidator {
    pub fn validate(&self, untyped_tree: UntypedTree, end_type: SqlTypeFamily) -> Result<TypedTree, ExpressionError> {
        let typed_tree = self.inner_process(untyped_tree, end_type);
        let expression_type = typed_tree.type_family();
        match expression_type.partial_cmp(&end_type) {
            None => Err(ExpressionError::DatatypeMismatch {
                expected: end_type,
                actual: expression_type,
            }),
            Some(_) => {
                let coerced = if typed_tree.type_family() != end_type {
                    TypedTree::UnOp {
                        op: UnOperator::Cast(end_type),
                        item: Box::new(typed_tree),
                    }
                } else {
                    typed_tree
                };
                Ok(coerced)
            }
        }
    }

    fn inner_process(&self, untyped_tree: UntypedTree, end_type: SqlTypeFamily) -> TypedTree {
        match untyped_tree {
            UntypedTree::BiOp { op, left, right } => {
                let typed_left = self.inner_process(*left, end_type);
                let left_coerced = self.type_coerce(typed_left);
                let typed_right = self.inner_process(*right, end_type);
                let right_coerced = self.type_coerce(typed_right);
                let return_type = op.infer_return_type(left_coerced.type_family(), right_coerced.type_family());
                TypedTree::BiOp {
                    op,
                    left: Box::new(left_coerced),
                    right: Box::new(right_coerced),
                    type_family: return_type,
                }
            }
            UntypedTree::UnOp { op, item } => {
                let end_type = match op {
                    UnOperator::Cast(type_family) => type_family,
                    _ => end_type,
                };
                let typed_item = self.inner_process(*item, end_type);
                let type_coerced = self.type_coerce(typed_item);
                TypedTree::UnOp {
                    op,
                    item: Box::new(type_coerced),
                }
            }
            UntypedTree::Item(item) => TypedTree::Item(item.infer_type()),
        }
    }

    fn type_coerce(&self, typed_tree: TypedTree) -> TypedTree {
        typed_tree
    }
}

#[cfg(test)]
mod tests;
