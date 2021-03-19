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
use data_manipulation_operators::BiArithmetic;

#[rstest::rstest(
    operator,
    left,
    right,
    result,
    case::add(BiOperator::Arithmetic(BiArithmetic::Add), 10, 10, 20),
    case::sub(BiOperator::Arithmetic(BiArithmetic::Sub), 20, 10, 10),
    case::mul(BiOperator::Arithmetic(BiArithmetic::Mul), 5, 4, 20),
    case::div(BiOperator::Arithmetic(BiArithmetic::Div), 20, 4, 5),
    case::div(BiOperator::Arithmetic(BiArithmetic::Mod), 5, 2, 1),
    case::div(BiOperator::Arithmetic(BiArithmetic::Exp), 5, 2, 25)
)]
fn number_and_number(operator: BiOperator, left: u32, right: u32, result: u32) {
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
        .eval(&[]),
        Ok(ScalarValue::Num {
            value: BigDecimal::from(result),
            type_family: SqlTypeFamily::BigInt
        })
    );
}

#[rstest::rstest(
    operator,
    case::add(BiOperator::Arithmetic(BiArithmetic::Add)),
    case::sub(BiOperator::Arithmetic(BiArithmetic::Sub)),
    case::mul(BiOperator::Arithmetic(BiArithmetic::Mul)),
    case::div(BiOperator::Arithmetic(BiArithmetic::Div)),
    case::div(BiOperator::Arithmetic(BiArithmetic::Mod)),
    case::div(BiOperator::Arithmetic(BiArithmetic::Exp))
)]
fn number_and_boolean(operator: BiOperator) {
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
        .eval(&[]),
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
        .eval(&[]),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamily::Bool,
            SqlTypeFamily::Integer
        ))
    );
}

#[rstest::rstest(
    operator,
    case::add(BiOperator::Arithmetic(BiArithmetic::Add)),
    case::sub(BiOperator::Arithmetic(BiArithmetic::Sub)),
    case::mul(BiOperator::Arithmetic(BiArithmetic::Mul)),
    case::div(BiOperator::Arithmetic(BiArithmetic::Div)),
    case::div(BiOperator::Arithmetic(BiArithmetic::Mod)),
    case::div(BiOperator::Arithmetic(BiArithmetic::Exp))
)]
fn number_and_string(operator: BiOperator) {
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
        .eval(&[]),
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
        .eval(&[]),
        Err(QueryExecutionError::invalid_text_representation(
            SqlTypeFamily::Integer,
            &"abc"
        ))
    );
}

#[rstest::rstest(
    operator,
    case::add(BiOperator::Arithmetic(BiArithmetic::Add)),
    case::sub(BiOperator::Arithmetic(BiArithmetic::Sub)),
    case::mul(BiOperator::Arithmetic(BiArithmetic::Mul)),
    case::div(BiOperator::Arithmetic(BiArithmetic::Div)),
    case::div(BiOperator::Arithmetic(BiArithmetic::Mod)),
    case::div(BiOperator::Arithmetic(BiArithmetic::Exp))
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
        .eval(&[]),
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
        .eval(&[]),
        Err(QueryExecutionError::undefined_bi_function(
            operator,
            SqlTypeFamily::Bool,
            SqlTypeFamily::String
        ))
    );
}
