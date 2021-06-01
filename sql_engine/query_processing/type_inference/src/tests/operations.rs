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
use operators::{BiArithmetic, BiOperator, UnArithmetic, UnOperator};
use query_ast::{BinaryOperator, UnaryOperator};

#[test]
fn negate_number() {
    let type_inference = TypeInference;
    let untyped_tree = Expr::UnaryOp {
        op: UnaryOperator::Minus,
        expr: Box::new(untyped_number("9223372036854775808")),
    };

    assert_eq!(
        type_inference.infer_type(untyped_tree),
        Ok(TypedTree::UnOp {
            op: UnOperator::Arithmetic(UnArithmetic::Neg),
            item: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Numeric(BigDecimal::from(
                9223372036854775808u64
            ))))),
        })
    );
}

#[test]
fn addition() {
    let type_inference = TypeInference;
    let untyped_tree = Expr::BinaryOp {
        op: BinaryOperator::Plus,
        left: Box::new(untyped_number("9223372036854775808")),
        right: Box::new(untyped_number("9223372036854775808")),
    };

    assert_eq!(
        type_inference.infer_type(untyped_tree),
        Ok(TypedTree::BiOp {
            op: BiOperator::Arithmetic(BiArithmetic::Add),
            left: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Numeric(BigDecimal::from(
                9223372036854775808u64
            ))))),
            right: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Numeric(BigDecimal::from(
                9223372036854775808u64
            ))))),
        })
    );
}
