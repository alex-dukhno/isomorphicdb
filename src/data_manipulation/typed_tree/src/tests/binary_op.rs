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

#[cfg(test)]
mod addition {
    use super::*;

    #[test]
    fn small_int_and_small_int_without_overflow() {
        assert_eq!(
            StaticTypedTree::BiOp {
                type_family: SqlTypeFamily::SmallInt,
                left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(10),
                    type_family: SqlTypeFamily::SmallInt
                }))),
                op: BiOperator::Arithmetic(BiArithmetic::Add),
                right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(10),
                    type_family: SqlTypeFamily::SmallInt
                }))),
            }
            .eval(),
            Ok(TypedValue::Num {
                value: BigDecimal::from(20),
                type_family: SqlTypeFamily::BigInt
            })
        );
    }

    #[test]
    fn small_int_and_small_int_with_overflow() {
        assert_eq!(
            StaticTypedTree::BiOp {
                type_family: SqlTypeFamily::SmallInt,
                left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(32000),
                    type_family: SqlTypeFamily::SmallInt
                }))),
                op: BiOperator::Arithmetic(BiArithmetic::Add),
                right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(32000),
                    type_family: SqlTypeFamily::SmallInt
                }))),
            }
            .eval(),
            Ok(TypedValue::Num {
                value: BigDecimal::from(64000),
                type_family: SqlTypeFamily::BigInt
            })
        );
    }

    #[test]
    fn integer_and_integer() {
        assert_eq!(
            StaticTypedTree::BiOp {
                type_family: SqlTypeFamily::Integer,
                left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(32767),
                    type_family: SqlTypeFamily::Integer
                }))),
                op: BiOperator::Arithmetic(BiArithmetic::Add),
                right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(32767),
                    type_family: SqlTypeFamily::Integer
                }))),
            }
            .eval(),
            Ok(TypedValue::Num {
                value: BigDecimal::from(65534),
                type_family: SqlTypeFamily::BigInt
            })
        );
    }

    #[test]
    fn number_and_boolean() {
        assert_eq!(
            StaticTypedTree::BiOp {
                type_family: SqlTypeFamily::Integer,
                left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(32767),
                    type_family: SqlTypeFamily::Integer
                }))),
                op: BiOperator::Arithmetic(BiArithmetic::Add),
                right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true)))),
            }
            .eval(),
            Err(QueryExecutionError::undefined_bi_function(
                BiOperator::Arithmetic(BiArithmetic::Add),
                SqlTypeFamily::Integer,
                SqlTypeFamily::Bool
            ))
        );

        assert_eq!(
            StaticTypedTree::BiOp {
                type_family: SqlTypeFamily::Integer,
                left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true)))),
                op: BiOperator::Arithmetic(BiArithmetic::Add),
                right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(32767),
                    type_family: SqlTypeFamily::Integer
                }))),
            }
            .eval(),
            Err(QueryExecutionError::undefined_bi_function(
                BiOperator::Arithmetic(BiArithmetic::Add),
                SqlTypeFamily::Bool,
                SqlTypeFamily::Integer
            ))
        );
    }

    #[test]
    fn number_and_string() {
        assert_eq!(
            StaticTypedTree::BiOp {
                type_family: SqlTypeFamily::Integer,
                left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(32767),
                    type_family: SqlTypeFamily::Integer
                }))),
                op: BiOperator::Arithmetic(BiArithmetic::Add),
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
                op: BiOperator::Arithmetic(BiArithmetic::Add),
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

    #[test]
    fn others() {
        assert_eq!(
            StaticTypedTree::BiOp {
                type_family: SqlTypeFamily::Integer,
                left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(
                    "abc".to_owned()
                )))),
                op: BiOperator::Arithmetic(BiArithmetic::Add),
                right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true)))),
            }
            .eval(),
            Err(QueryExecutionError::undefined_bi_function(
                BiOperator::Arithmetic(BiArithmetic::Add),
                SqlTypeFamily::String,
                SqlTypeFamily::Bool
            ))
        );

        assert_eq!(
            StaticTypedTree::BiOp {
                type_family: SqlTypeFamily::Integer,
                left: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true)))),
                op: BiOperator::Arithmetic(BiArithmetic::Add),
                right: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(
                    "abc".to_owned()
                )))),
            }
            .eval(),
            Err(QueryExecutionError::undefined_bi_function(
                BiOperator::Arithmetic(BiArithmetic::Add),
                SqlTypeFamily::Bool,
                SqlTypeFamily::String
            ))
        );
    }
}
