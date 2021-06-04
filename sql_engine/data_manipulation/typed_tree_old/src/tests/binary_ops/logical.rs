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
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamilyOld::SmallInt,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(left)))),
            op: operator,
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(right)))),
        }
        .eval(&[], &[]),
        Ok(ScalarValue::Bool(result))
    );
}

#[rstest::rstest(operator, case::and(BiOperator::Logical(BiLogical::And)), case::or(BiOperator::Logical(BiLogical::Or)))]
fn number_and_boolean(operator: BiOperator) {
    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamilyOld::Bool,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamilyOld::Integer
            }))),
            op: operator,
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(true)))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamilyOld::Integer,
            SqlTypeFamilyOld::Bool
        ))
    );

    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamilyOld::Bool,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(true)))),
            op: operator,
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamilyOld::Integer
            }))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamilyOld::Bool,
            SqlTypeFamilyOld::Integer
        ))
    );
}

#[rstest::rstest(operator, case::and(BiOperator::Logical(BiLogical::And)), case::or(BiOperator::Logical(BiLogical::Or)))]
fn boolean_and_string(operator: BiOperator) {
    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamilyOld::Integer,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(true)))),
            op: operator,
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("abc".to_owned())))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamilyOld::Bool,
            SqlTypeFamilyOld::String
        ))
    );

    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamilyOld::Bool,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("abc".to_owned())))),
            op: operator,
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(true)))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamilyOld::String,
            SqlTypeFamilyOld::Bool
        ))
    );
}

#[rstest::rstest(operator, case::and(BiOperator::Logical(BiLogical::And)), case::or(BiOperator::Logical(BiLogical::Or)))]
fn others(operator: BiOperator) {
    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamilyOld::Bool,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("abc".to_owned())))),
            op: operator,
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamilyOld::Integer
            }))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamilyOld::String,
            SqlTypeFamilyOld::Integer
        ))
    );

    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamilyOld::Integer,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamilyOld::Integer
            }))),
            op: operator,
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("abc".to_owned())))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamilyOld::Integer,
            SqlTypeFamilyOld::String
        ))
    );
}
