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
use data_manipulation_operators::Concat;

#[test]
fn string_and_string() {
    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamilyOld::String,
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
            type_family: SqlTypeFamilyOld::Bool,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("abc".to_owned())))),
            op: BiOperator::StringOp(Concat),
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(true)))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            BiOperator::StringOp(Concat),
            SqlTypeFamilyOld::String,
            SqlTypeFamilyOld::Bool
        ))
    );

    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamilyOld::Bool,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(true)))),
            op: BiOperator::StringOp(Concat),
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("abc".to_owned())))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            BiOperator::StringOp(Concat),
            SqlTypeFamilyOld::Bool,
            SqlTypeFamilyOld::String
        ))
    );
}

#[test]
fn string_and_number() {
    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamilyOld::String,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(0),
                type_family: SqlTypeFamilyOld::Integer
            }))),
            op: BiOperator::StringOp(Concat),
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("abc".to_owned())))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            BiOperator::StringOp(Concat),
            SqlTypeFamilyOld::Integer,
            SqlTypeFamilyOld::String
        ))
    );

    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamilyOld::String,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("abc".to_owned())))),
            op: BiOperator::StringOp(Concat),
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(0),
                type_family: SqlTypeFamilyOld::Integer
            }))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            BiOperator::StringOp(Concat),
            SqlTypeFamilyOld::String,
            SqlTypeFamilyOld::Integer
        ))
    );
}

#[test]
fn others() {
    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamilyOld::String,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(true)))),
            op: BiOperator::StringOp(Concat),
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(false)))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            BiOperator::StringOp(Concat),
            SqlTypeFamilyOld::Bool,
            SqlTypeFamilyOld::Bool
        ))
    );
    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamilyOld::String,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamilyOld::Integer
            }))),
            op: BiOperator::StringOp(Concat),
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(false)))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            BiOperator::StringOp(Concat),
            SqlTypeFamilyOld::Integer,
            SqlTypeFamilyOld::Bool
        ))
    );
    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamilyOld::String,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(true)))),
            op: BiOperator::StringOp(Concat),
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamilyOld::Integer
            })))
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            BiOperator::StringOp(Concat),
            SqlTypeFamilyOld::Bool,
            SqlTypeFamilyOld::Integer
        ))
    );

    assert_eq!(
        TypedTreeOld::BiOp {
            type_family: SqlTypeFamilyOld::String,
            left: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamilyOld::Integer
            }))),
            op: BiOperator::StringOp(Concat),
            right: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamilyOld::Integer
            }))),
        }
        .eval(&[], &[]),
        Err(QueryExecutionError::undefined_bi_function(
            BiOperator::StringOp(Concat),
            SqlTypeFamilyOld::Integer,
            SqlTypeFamilyOld::Integer
        ))
    );
}
