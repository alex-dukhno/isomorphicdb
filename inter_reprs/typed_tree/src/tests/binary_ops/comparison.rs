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
use operators::Comparison;

#[rstest::rstest(
    operator,
    left,
    right,
    result,
    case::eq(BiOperator::Comparison(Comparison::Eq), 10, 10, true),
    case::not_eq(BiOperator::Comparison(Comparison::NotEq), 10, 10, false),
    case::lt(BiOperator::Comparison(Comparison::Lt), 10, 11, true),
    case::gt(BiOperator::Comparison(Comparison::Gt), 10, 11, false),
    case::lt_eq(BiOperator::Comparison(Comparison::LtEq), 10, 10, true),
    case::gt_eq(BiOperator::Comparison(Comparison::GtEq), 10, 10, true)
)]
fn number_and_number(operator: BiOperator, left: u32, right: u32, result: bool) {
    assert_eq!(
        TypedTree::BiOp {
            type_family: SqlTypeFamily::SmallInt,
            left: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(left),
                type_family: SqlTypeFamily::SmallInt
            }))),
            op: operator,
            right: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(right),
                type_family: SqlTypeFamily::SmallInt
            }))),
        }
        .eval(&[], &[]),
        Ok(ScalarValue::Bool(result))
    );
}

#[rstest::rstest(
    operator,
    left,
    right,
    result,
    case::eq(BiOperator::Comparison(Comparison::Eq), "10", "10", true),
    case::not_eq(BiOperator::Comparison(Comparison::NotEq), "10", "10", false),
    case::lt(BiOperator::Comparison(Comparison::Lt), "10", "11", true),
    case::gt(BiOperator::Comparison(Comparison::Gt), "10", "11", false),
    case::lt_eq(BiOperator::Comparison(Comparison::LtEq), "10", "10", true),
    case::gt_eq(BiOperator::Comparison(Comparison::GtEq), "10", "10", true)
)]
fn string_and_string(operator: BiOperator, left: &str, right: &str, result: bool) {
    assert_eq!(
        TypedTree::BiOp {
            type_family: SqlTypeFamily::SmallInt,
            left: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::String(left.to_owned())))),
            op: operator,
            right: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::String(right.to_owned())))),
        }
        .eval(&[], &[]),
        Ok(ScalarValue::Bool(result))
    );
}

#[rstest::rstest(
    operator,
    left,
    right,
    result,
    case::eq(BiOperator::Comparison(Comparison::Eq), true, true, true),
    case::not_eq(BiOperator::Comparison(Comparison::NotEq), true, true, false),
    case::lt(BiOperator::Comparison(Comparison::Lt), false, true, true),
    case::gt(BiOperator::Comparison(Comparison::Gt), false, true, false),
    case::lt_eq(BiOperator::Comparison(Comparison::LtEq), true, true, true),
    case::gt_eq(BiOperator::Comparison(Comparison::GtEq), true, true, true)
)]
fn boolean_and_boolean(operator: BiOperator, left: bool, right: bool, result: bool) {
    assert_eq!(
        TypedTree::BiOp {
            type_family: SqlTypeFamily::SmallInt,
            left: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Bool(left)))),
            op: operator,
            right: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Bool(right)))),
        }
        .eval(&[], &[]),
        Ok(ScalarValue::Bool(result))
    );
}

#[rstest::rstest(
    operator,
    case::eq(BiOperator::Comparison(Comparison::Eq)),
    case::not_eq(BiOperator::Comparison(Comparison::NotEq)),
    case::lt(BiOperator::Comparison(Comparison::Lt)),
    case::gt(BiOperator::Comparison(Comparison::Gt)),
    case::lt_eq(BiOperator::Comparison(Comparison::LtEq)),
    case::gt_eq(BiOperator::Comparison(Comparison::GtEq))
)]
fn others(operator: BiOperator) {
    assert_eq!(
        TypedTree::BiOp {
            type_family: SqlTypeFamily::Bool,
            left: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::String("abc".to_owned())))),
            op: operator,
            right: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            }))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamily::String,
            SqlTypeFamily::Integer
        ))
    );

    assert_eq!(
        TypedTree::BiOp {
            type_family: SqlTypeFamily::Bool,
            left: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            }))),
            op: operator,
            right: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::String("abc".to_owned())))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamily::Integer,
            SqlTypeFamily::String
        ))
    );
    assert_eq!(
        TypedTree::BiOp {
            type_family: SqlTypeFamily::Bool,
            left: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::String("abc".to_owned())))),
            op: operator,
            right: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Bool(true)))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamily::String,
            SqlTypeFamily::Bool
        ))
    );

    assert_eq!(
        TypedTree::BiOp {
            type_family: SqlTypeFamily::Bool,
            left: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Bool(true)))),
            op: operator,
            right: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::String("abc".to_owned())))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamily::Bool,
            SqlTypeFamily::String
        ))
    );
    assert_eq!(
        TypedTree::BiOp {
            type_family: SqlTypeFamily::Bool,
            left: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Bool(true)))),
            op: operator,
            right: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            }))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamily::Bool,
            SqlTypeFamily::Integer
        ))
    );

    assert_eq!(
        TypedTree::BiOp {
            type_family: SqlTypeFamily::Integer,
            left: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            }))),
            op: operator,
            right: Box::new(TypedTree::Item(TypedItem::Const(TypedValue::Bool(true)))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamily::Integer,
            SqlTypeFamily::Bool
        ))
    );
}
