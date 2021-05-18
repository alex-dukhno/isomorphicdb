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

use super::*;
use data_manipulation_operators::{BiArithmetic, BiOperator, UnArithmetic, UnOperator};

#[test]
fn negate_number() {
    let type_inference = TypeInference::default();
    let untyped_tree = UntypedTree::UnOp {
        op: UnOperator::Arithmetic(UnArithmetic::Neg),
        item: Box::new(untyped_number(BigDecimal::from(2))),
    };

    assert_eq!(
        type_inference.infer_type(untyped_tree, &[]),
        TypedTree::UnOp {
            op: UnOperator::Arithmetic(UnArithmetic::Neg),
            item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(2),
                type_family: SqlTypeFamily::SmallInt
            })))
        }
    );
}

#[test]
fn add_same_types() {
    let type_inference = TypeInference::default();
    let untyped_tree = UntypedTree::BiOp {
        left: Box::new(untyped_number(BigDecimal::from(1))),
        op: BiOperator::Arithmetic(BiArithmetic::Add),
        right: Box::new(untyped_number(BigDecimal::from(2))),
    };

    assert_eq!(
        type_inference.infer_type(untyped_tree, &[]),
        TypedTree::BiOp {
            type_family: SqlTypeFamily::SmallInt,
            left: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(1),
                type_family: SqlTypeFamily::SmallInt
            }))),
            op: BiOperator::Arithmetic(BiArithmetic::Add),
            right: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(2),
                type_family: SqlTypeFamily::SmallInt
            })))
        }
    );
}

#[test]
fn add_different_types() {
    let type_inference = TypeInference::default();
    let untyped_tree = UntypedTree::BiOp {
        left: Box::new(untyped_number(BigDecimal::from(i64::MAX - i32::MAX as i64))),
        op: BiOperator::Arithmetic(BiArithmetic::Add),
        right: Box::new(untyped_number(BigDecimal::from(2))),
    };

    assert_eq!(
        type_inference.infer_type(untyped_tree, &[]),
        TypedTree::BiOp {
            type_family: SqlTypeFamily::BigInt,
            left: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Num {
                value: BigDecimal::from((i64::MAX - i32::MAX as i64) as u64),
                type_family: SqlTypeFamily::BigInt
            }))),
            op: BiOperator::Arithmetic(BiArithmetic::Add),
            right: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(2),
                type_family: SqlTypeFamily::SmallInt
            })))
        }
    )
}
