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
use operators_old::Concat;

#[test]
fn string_and_string() {
    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamily::String,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("1".to_owned())))),
            op: BiOperator::StringOp(Concat),
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("2".to_owned())))),
        }
        .eval(&[], &[]),
        Ok(ScalarValue::String("12".to_owned()))
    );
}

#[test]
fn string_and_boolean() {
    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamily::Bool,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("abc".to_owned())))),
            op: BiOperator::StringOp(Concat),
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(true)))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            BiOperator::StringOp(Concat),
            SqlTypeFamily::String,
            SqlTypeFamily::Bool
        ))
    );

    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamily::Bool,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(true)))),
            op: BiOperator::StringOp(Concat),
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("abc".to_owned())))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            BiOperator::StringOp(Concat),
            SqlTypeFamily::Bool,
            SqlTypeFamily::String
        ))
    );
}

#[test]
fn string_and_number() {
    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamily::String,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(0),
                type_family: SqlTypeFamily::Integer
            }))),
            op: BiOperator::StringOp(Concat),
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("abc".to_owned())))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            BiOperator::StringOp(Concat),
            SqlTypeFamily::Integer,
            SqlTypeFamily::String
        ))
    );

    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamily::String,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("abc".to_owned())))),
            op: BiOperator::StringOp(Concat),
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(0),
                type_family: SqlTypeFamily::Integer
            }))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            BiOperator::StringOp(Concat),
            SqlTypeFamily::String,
            SqlTypeFamily::Integer
        ))
    );
}

#[test]
fn others() {
    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamily::String,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(true)))),
            op: BiOperator::StringOp(Concat),
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(false)))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            BiOperator::StringOp(Concat),
            SqlTypeFamily::Bool,
            SqlTypeFamily::Bool
        ))
    );
    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamily::String,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            }))),
            op: BiOperator::StringOp(Concat),
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(false)))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            BiOperator::StringOp(Concat),
            SqlTypeFamily::Integer,
            SqlTypeFamily::Bool
        ))
    );
    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamily::String,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(true)))),
            op: BiOperator::StringOp(Concat),
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            })))
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            BiOperator::StringOp(Concat),
            SqlTypeFamily::Bool,
            SqlTypeFamily::Integer
        ))
    );

    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamily::String,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            }))),
            op: BiOperator::StringOp(Concat),
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            }))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            BiOperator::StringOp(Concat),
            SqlTypeFamily::Integer,
            SqlTypeFamily::Integer
        ))
    );
}
