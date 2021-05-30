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
use bigdecimal::BigDecimal;
use operators::{BiArithmetic, BiOperator, UnArithmetic, UnOperator};

#[test]
fn negate_number() {
    let type_coercion = TypeCoercion;
    let checked_tree = CheckedTree::UnOp {
        op: UnOperator::Arithmetic(UnArithmetic::Neg),
        item: Box::new(checked_number(BigDecimal::from(9223372036854775808u64))),
    };

    assert_eq!(
        type_coercion.coerce_type(checked_tree),
        Ok(ExecutableTree::UnOp {
            op: UnOperator::Arithmetic(UnArithmetic::Neg),
            item: Box::new(ExecutableTree::Item(ExecutableItem::Const(ExecutableValue::Numeric(BigDecimal::from(
                9223372036854775808u64
            ))))),
        })
    );
}

#[test]
fn addition() {
    let type_coercion = TypeCoercion;
    let checked_tree = CheckedTree::BiOp {
        op: BiOperator::Arithmetic(BiArithmetic::Add),
        left: Box::new(checked_number(BigDecimal::from(9223372036854775808u64))),
        right: Box::new(checked_number(BigDecimal::from(9223372036854775808u64))),
    };

    assert_eq!(
        type_coercion.coerce_type(checked_tree),
        Ok(ExecutableTree::BiOp {
            op: BiOperator::Arithmetic(BiArithmetic::Add),
            left: Box::new(ExecutableTree::Item(ExecutableItem::Const(ExecutableValue::Numeric(BigDecimal::from(
                9223372036854775808u64
            ))))),
            right: Box::new(ExecutableTree::Item(ExecutableItem::Const(ExecutableValue::Numeric(BigDecimal::from(
                9223372036854775808u64
            ))))),
        })
    );
}
