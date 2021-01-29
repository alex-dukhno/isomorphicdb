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

use super::*;
use data_manipulation_operators::{Arithmetic, Operation};
use data_manipulation_typed_tree::{StaticTypedItem, StaticTypedTree, TypedValue};
use types::SqlTypeFamily;

#[test]
fn add_same_types() {
    let type_inference = TypeInference::default();
    let untyped_tree = StaticUntypedTree::Operation {
        left: Box::new(untyped_number(BigDecimal::from(1))),
        op: Operation::Arithmetic(Arithmetic::Add),
        right: Box::new(untyped_number(BigDecimal::from(2))),
    };

    assert_eq!(
        type_inference.infer_static(untyped_tree),
        StaticTypedTree::Operation {
            type_family: Some(SqlTypeFamily::SmallInt),
            left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(1)))),
            op: Operation::Arithmetic(Arithmetic::Add),
            right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(2))))
        }
    );
}

#[test]
fn add_different_types() {
    let type_inference = TypeInference::default();
    let untyped_tree = StaticUntypedTree::Operation {
        left: Box::new(untyped_number(BigDecimal::from(i64::MAX - i32::MAX as i64))),
        op: Operation::Arithmetic(Arithmetic::Add),
        right: Box::new(untyped_number(BigDecimal::from(2))),
    };

    assert_eq!(
        type_inference.infer_static(untyped_tree),
        StaticTypedTree::Operation {
            type_family: Some(SqlTypeFamily::BigInt),
            left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::BigInt(
                i64::MAX - i32::MAX as i64
            )))),
            op: Operation::Arithmetic(Arithmetic::Add),
            right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(2))))
        }
    )
}
