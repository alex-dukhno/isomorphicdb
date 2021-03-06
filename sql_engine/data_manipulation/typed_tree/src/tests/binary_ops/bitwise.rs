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
use data_manipulation_operators::Bitwise;

#[rstest::rstest(
    operator,
    left,
    right,
    result,
    case::shift_right(BiOperator::Bitwise(Bitwise::ShiftRight), 4, 1, 2),
    case::shift_left(BiOperator::Bitwise(Bitwise::ShiftLeft), 4, 1, 8),
    case::and(BiOperator::Bitwise(Bitwise::And), 5, 4, 4),
    case::or(BiOperator::Bitwise(Bitwise::Or), 5, 4, 5),
    case::xor(BiOperator::Bitwise(Bitwise::Xor), 5, 4, 1)
)]
fn integer_and_integer(operator: BiOperator, left: u32, right: u32, result: u32) {
    assert_eq!(
        StaticTypedTree::BiOp {
            type_family: SqlTypeFamily::SmallInt,
            left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(left),
                type_family: SqlTypeFamily::SmallInt
            }))),
            op: operator,
            right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(right),
                type_family: SqlTypeFamily::SmallInt
            }))),
        }
        .eval(),
        Ok(ScalarValue::Num {
            value: BigDecimal::from(result),
            type_family: SqlTypeFamily::BigInt
        })
    );
}

#[rstest::rstest(
    operator,
    case::shift_right(BiOperator::Bitwise(Bitwise::ShiftRight)),
    case::shift_left(BiOperator::Bitwise(Bitwise::ShiftLeft)),
    case::and(BiOperator::Bitwise(Bitwise::And)),
    case::or(BiOperator::Bitwise(Bitwise::Or)),
    case::xor(BiOperator::Bitwise(Bitwise::Xor))
)]
fn integer_and_float(operator: BiOperator) {
    assert_eq!(
        StaticTypedTree::BiOp {
            type_family: SqlTypeFamily::Real,
            left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(0),
                type_family: SqlTypeFamily::SmallInt
            }))),
            op: operator,
            right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(0),
                type_family: SqlTypeFamily::Real
            }))),
        }
        .eval(),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamily::SmallInt,
            SqlTypeFamily::Real
        ))
    );
}

#[rstest::rstest(
    operator,
    case::shift_right(BiOperator::Bitwise(Bitwise::ShiftRight)),
    case::shift_left(BiOperator::Bitwise(Bitwise::ShiftLeft)),
    case::and(BiOperator::Bitwise(Bitwise::And)),
    case::or(BiOperator::Bitwise(Bitwise::Or)),
    case::xor(BiOperator::Bitwise(Bitwise::Xor))
)]
fn integer_and_boolean(operator: BiOperator) {
    assert_eq!(
        StaticTypedTree::BiOp {
            type_family: SqlTypeFamily::Integer,
            left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            }))),
            op: operator,
            right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true)))),
        }
        .eval(),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamily::Integer,
            SqlTypeFamily::Bool
        ))
    );

    assert_eq!(
        StaticTypedTree::BiOp {
            type_family: SqlTypeFamily::Integer,
            left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true)))),
            op: operator,
            right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            }))),
        }
        .eval(),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamily::Bool,
            SqlTypeFamily::Integer
        ))
    );
}

#[rstest::rstest(
    operator,
    case::shift_right(BiOperator::Bitwise(Bitwise::ShiftRight)),
    case::shift_left(BiOperator::Bitwise(Bitwise::ShiftLeft)),
    case::and(BiOperator::Bitwise(Bitwise::And)),
    case::or(BiOperator::Bitwise(Bitwise::Or)),
    case::xor(BiOperator::Bitwise(Bitwise::Xor))
)]
fn integer_and_string(operator: BiOperator) {
    assert_eq!(
        StaticTypedTree::BiOp {
            type_family: SqlTypeFamily::Integer,
            left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            }))),
            op: operator,
            right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(
                "abc".to_owned()
            )))),
        }
        .eval(),
        Err(QueryExecutionError::invalid_text_representation(
            SqlTypeFamily::Integer,
            &"abc"
        ))
    );

    assert_eq!(
        StaticTypedTree::BiOp {
            type_family: SqlTypeFamily::Integer,
            left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(
                "abc".to_owned()
            )))),
            op: operator,
            right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            }))),
        }
        .eval(),
        Err(QueryExecutionError::invalid_text_representation(
            SqlTypeFamily::Integer,
            &"abc"
        ))
    );
}

#[rstest::rstest(
    operator,
    case::shift_right(BiOperator::Bitwise(Bitwise::ShiftRight)),
    case::shift_left(BiOperator::Bitwise(Bitwise::ShiftLeft)),
    case::and(BiOperator::Bitwise(Bitwise::And)),
    case::or(BiOperator::Bitwise(Bitwise::Or)),
    case::xor(BiOperator::Bitwise(Bitwise::Xor))
)]
fn others(operator: BiOperator) {
    assert_eq!(
        StaticTypedTree::BiOp {
            type_family: SqlTypeFamily::Integer,
            left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(
                "abc".to_owned()
            )))),
            op: operator,
            right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true)))),
        }
        .eval(),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamily::String,
            SqlTypeFamily::Bool
        ))
    );

    assert_eq!(
        StaticTypedTree::BiOp {
            type_family: SqlTypeFamily::Integer,
            left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true)))),
            op: operator,
            right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(
                "abc".to_owned()
            )))),
        }
        .eval(),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamily::Bool,
            SqlTypeFamily::String
        ))
    );

    assert_eq!(
        StaticTypedTree::BiOp {
            type_family: SqlTypeFamily::Real,
            left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(0),
                type_family: SqlTypeFamily::Real
            }))),
            op: operator,
            right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(0),
                type_family: SqlTypeFamily::Real
            }))),
        }
        .eval(),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamily::Real,
            SqlTypeFamily::Real
        ))
    );
}
