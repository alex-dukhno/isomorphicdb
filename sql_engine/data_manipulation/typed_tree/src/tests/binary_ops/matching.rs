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
use data_manipulation_operators::Matching;

#[rstest::rstest(
    operator,
    left,
    right,
    result,
    case::like(BiOperator::Matching(Matching::Like), "1%", "123", true),
    case::not_like(BiOperator::Matching(Matching::NotLike), "1%", "234", true)
)]
fn string_and_string(operator: BiOperator, left: &str, right: &str, result: bool) {
    assert_eq!(
        StaticTypedTree::BiOp {
            type_family: SqlTypeFamily::String,
            left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(
                left.to_owned()
            )))),
            op: operator,
            right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(
                right.to_owned()
            )))),
        }
        .eval(),
        Ok(TypedValue::Bool(result))
    );
}

#[rstest::rstest(
    operator,
    case::like(BiOperator::Matching(Matching::Like)),
    case::not_like(BiOperator::Matching(Matching::NotLike))
)]
fn string_and_boolean(operator: BiOperator) {
    assert_eq!(
        StaticTypedTree::BiOp {
            type_family: SqlTypeFamily::Bool,
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
            type_family: SqlTypeFamily::Bool,
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
}

#[rstest::rstest(
    operator,
    case::like(BiOperator::Matching(Matching::Like)),
    case::not_like(BiOperator::Matching(Matching::NotLike))
)]
fn string_and_number(operator: BiOperator) {
    assert_eq!(
        StaticTypedTree::BiOp {
            type_family: SqlTypeFamily::String,
            left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(0),
                type_family: SqlTypeFamily::Integer
            }))),
            op: operator,
            right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(
                "abc".to_owned()
            )))),
        }
        .eval(),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamily::Integer,
            SqlTypeFamily::String
        ))
    );

    assert_eq!(
        StaticTypedTree::BiOp {
            type_family: SqlTypeFamily::String,
            left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(
                "abc".to_owned()
            )))),
            op: operator,
            right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(0),
                type_family: SqlTypeFamily::Integer
            }))),
        }
        .eval(),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamily::String,
            SqlTypeFamily::Integer
        ))
    );
}

#[rstest::rstest(
    operator,
    case::like(BiOperator::Matching(Matching::Like)),
    case::not_like(BiOperator::Matching(Matching::NotLike))
)]
fn others(operator: BiOperator) {
    assert_eq!(
        StaticTypedTree::BiOp {
            type_family: SqlTypeFamily::String,
            left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true)))),
            op: operator,
            right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(false)))),
        }
        .eval(),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamily::Bool,
            SqlTypeFamily::Bool
        ))
    );
    assert_eq!(
        StaticTypedTree::BiOp {
            type_family: SqlTypeFamily::String,
            left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            }))),
            op: operator,
            right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(false)))),
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
            type_family: SqlTypeFamily::String,
            left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true)))),
            op: operator,
            right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            })))
        }
        .eval(),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamily::Bool,
            SqlTypeFamily::Integer
        ))
    );

    assert_eq!(
        StaticTypedTree::BiOp {
            type_family: SqlTypeFamily::String,
            left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            }))),
            op: operator,
            right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            }))),
        }
        .eval(),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamily::Integer,
            SqlTypeFamily::Integer
        ))
    );
}
