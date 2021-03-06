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
use data_manipulation_operators::BiLogical;

#[rstest::rstest(
    operator,
    left,
    right,
    result,
    case::and(BiOperator::Logical(BiLogical::And), true, false, false),
    case::or(BiOperator::Logical(BiLogical::Or), true, false, true)
)]
fn boolean_and_boolean(operator: BiOperator, left: bool, right: bool, result: bool) {
    assert_eq!(
        StaticTypedTree::BiOp {
            type_family: SqlTypeFamily::SmallInt,
            left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(left)))),
            op: operator,
            right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(right)))),
        }
        .eval(),
        Ok(ScalarValue::Bool(result))
    );
}

#[rstest::rstest(
    operator,
    case::and(BiOperator::Logical(BiLogical::And)),
    case::or(BiOperator::Logical(BiLogical::Or))
)]
fn number_and_boolean(operator: BiOperator) {
    assert_eq!(
        StaticTypedTree::BiOp {
            type_family: SqlTypeFamily::Bool,
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
            type_family: SqlTypeFamily::Bool,
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
    case::and(BiOperator::Logical(BiLogical::And)),
    case::or(BiOperator::Logical(BiLogical::Or))
)]
fn boolean_and_string(operator: BiOperator) {
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
}

#[rstest::rstest(
    operator,
    case::and(BiOperator::Logical(BiLogical::And)),
    case::or(BiOperator::Logical(BiLogical::Or))
)]
fn others(operator: BiOperator) {
    assert_eq!(
        StaticTypedTree::BiOp {
            type_family: SqlTypeFamily::Bool,
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
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamily::String,
            SqlTypeFamily::Integer
        ))
    );

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
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamily::Integer,
            SqlTypeFamily::String
        ))
    );
}
