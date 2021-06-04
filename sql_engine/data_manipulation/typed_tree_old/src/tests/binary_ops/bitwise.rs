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
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamilyOld::SmallInt,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(left),
                type_family: SqlTypeFamilyOld::SmallInt
            }))),
            op: operator,
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(right),
                type_family: SqlTypeFamilyOld::SmallInt
            }))),
        }
        .eval(&[], &[]),
        Ok(ScalarValue::Num {
            value: BigDecimal::from(result),
            type_family: SqlTypeFamilyOld::BigInt
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
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamilyOld::Real,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(0),
                type_family: SqlTypeFamilyOld::SmallInt
            }))),
            op: operator,
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(0),
                type_family: SqlTypeFamilyOld::Real
            }))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamilyOld::SmallInt,
            SqlTypeFamilyOld::Real
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
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamilyOld::Integer,
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
            type_family: SqlTypeFamilyOld::Integer,
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
        Err(QueryExecutionError::invalid_text_representation(SqlTypeFamilyOld::Integer, &"abc"))
    );

    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamilyOld::Integer,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("abc".to_owned())))),
            op: operator,
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamilyOld::Integer
            }))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::invalid_text_representation(SqlTypeFamilyOld::Integer, &"abc"))
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
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamilyOld::Integer,
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
            type_family: SqlTypeFamilyOld::Real,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(0),
                type_family: SqlTypeFamilyOld::Real
            }))),
            op: operator,
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(0),
                type_family: SqlTypeFamilyOld::Real
            }))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamilyOld::Real,
            SqlTypeFamilyOld::Real
        ))
    );
}
