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
use bigdecimal::{BigDecimal, FromPrimitive};
use data_manipulation_typed_tree::{StaticTypedItem, StaticTypedTree, TypedValue};

#[test]
fn smallint() {
    let type_inference = TypeInference::default();
    let untyped_tree = untyped_number(BigDecimal::from(0));

    assert_eq!(
        type_inference.infer_static(untyped_tree),
        StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::SmallInt(0)))
    );
}

#[test]
fn integer() {
    let type_inference = TypeInference::default();
    let untyped_tree = untyped_number(BigDecimal::from(i32::MAX - i16::MAX as i32));

    assert_eq!(
        type_inference.infer_static(untyped_tree),
        StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Integer((i32::MAX - i16::MAX as i32) as u32)))
    );
}

#[test]
fn bigint() {
    let type_inference = TypeInference::default();
    let tree = untyped_number(BigDecimal::from(i64::MAX - i32::MAX as i64));

    assert_eq!(
        type_inference.infer_static(tree),
        StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::BigInt((i64::MAX - i32::MAX as i64) as u64)))
    );
}

#[test]
fn real() {
    let type_inference = TypeInference::default();
    let tree = untyped_number(BigDecimal::from_f32(3.8).unwrap());

    assert_eq!(
        type_inference.infer_static(tree),
        StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Real(3.8)))
    );
}

#[test]
fn string() {
    let type_inference = TypeInference::default();
    let tree = untyped_string("str".to_owned());

    assert_eq!(
        type_inference.infer_static(tree),
        StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String("str".to_owned())))
    );
}
