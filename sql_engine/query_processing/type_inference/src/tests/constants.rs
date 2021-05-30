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

#[test]
fn integer() {
    let type_inference = TypeInference;
    let untyped_tree = untyped_int(0);

    assert_eq!(
        type_inference.infer_type(untyped_tree),
        Ok(TypedTree::Item(TypedItem::Const(TypedValue::Int(0))))
    );
}

#[test]
fn bigint() {
    let type_inference = TypeInference;

    assert_eq!(
        type_inference.infer_type(untyped_number("2147483648")),
        Ok(TypedTree::Item(TypedItem::Const(TypedValue::BigInt(2147483648))))
    );
    assert_eq!(
        type_inference.infer_type(untyped_number("-2147483649")),
        Ok(TypedTree::Item(TypedItem::Const(TypedValue::BigInt(-2147483649))))
    );
    assert_eq!(
        type_inference.infer_type(untyped_number("9223372036854775807")),
        Ok(TypedTree::Item(TypedItem::Const(TypedValue::BigInt(9223372036854775807))))
    );
    assert_eq!(
        type_inference.infer_type(untyped_number("-9223372036854775808")),
        Ok(TypedTree::Item(TypedItem::Const(TypedValue::BigInt(-9223372036854775808))))
    );
}

#[test]
fn numeric() {
    let type_inference = TypeInference;

    assert_eq!(
        type_inference.infer_type(untyped_number("-9223372036854775809")),
        Ok(TypedTree::Item(TypedItem::Const(TypedValue::Numeric(
            BigDecimal::from_str("-9223372036854775809").unwrap()
        ))))
    );
    assert_eq!(
        type_inference.infer_type(untyped_number("9223372036854775808")),
        Ok(TypedTree::Item(TypedItem::Const(TypedValue::Numeric(
            BigDecimal::from_str("9223372036854775808").unwrap()
        ))))
    );
    assert_eq!(
        type_inference.infer_type(untyped_number("92233.72036854775808")),
        Ok(TypedTree::Item(TypedItem::Const(TypedValue::Numeric(
            BigDecimal::from_str("92233.72036854775808").unwrap()
        ))))
    );
}

#[test]
fn string_literal() {
    let type_inference = TypeInference;

    assert_eq!(
        type_inference.infer_type(untyped_string("abc")),
        Ok(TypedTree::Item(TypedItem::Const(TypedValue::StringLiteral("abc".to_owned()))))
    );
}

#[test]
fn null_literal() {
    let type_inference = TypeInference;

    assert_eq!(
        type_inference.infer_type(untyped_null()),
        Ok(TypedTree::Item(TypedItem::Const(TypedValue::Null)))
    );
}
