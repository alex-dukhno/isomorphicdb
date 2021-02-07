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
mod unary_minus {
    use super::*;

    #[test]
    fn with_numbers() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperation::Arithmetic(UnArithmetic::Neg),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(32767),
                    type_family: SqlTypeFamily::Integer
                }))),
            }
            .eval(),
            Ok(TypedValue::Num {
                value: BigDecimal::from(-32767),
                type_family: SqlTypeFamily::Integer
            })
        );

        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperation::Arithmetic(UnArithmetic::Neg),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(32768),
                    type_family: SqlTypeFamily::Integer
                }))),
            }
            .eval(),
            Ok(TypedValue::Num {
                value: BigDecimal::from(-32768),
                type_family: SqlTypeFamily::Integer
            })
        );
    }

    #[test]
    fn doubly_applied() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperation::Arithmetic(UnArithmetic::Neg),
                item: Box::new(StaticTypedTree::UnOp {
                    op: UnOperation::Arithmetic(UnArithmetic::Neg),
                    item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                        value: BigDecimal::from(32767),
                        type_family: SqlTypeFamily::Integer
                    }))),
                })
            }
            .eval(),
            Ok(TypedValue::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            })
        );

        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperation::Arithmetic(UnArithmetic::Neg),
                item: Box::new(StaticTypedTree::UnOp {
                    op: UnOperation::Arithmetic(UnArithmetic::Neg),
                    item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                        value: BigDecimal::from(32768),
                        type_family: SqlTypeFamily::Integer
                    }))),
                })
            }
            .eval(),
            Ok(TypedValue::Num {
                value: BigDecimal::from(32768),
                type_family: SqlTypeFamily::Integer
            })
        );
    }

    #[test]
    fn with_string() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperation::Arithmetic(UnArithmetic::Neg),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(
                    "str".to_owned()
                ))))
            }
            .eval(),
            Err(QueryExecutionError::undefined_function(
                UnOperation::Arithmetic(UnArithmetic::Neg),
                SqlTypeFamily::String
            ))
        );
    }

    #[test]
    fn with_boolean() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperation::Arithmetic(UnArithmetic::Neg),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true))))
            }
            .eval(),
            Err(QueryExecutionError::undefined_function(
                UnOperation::Arithmetic(UnArithmetic::Neg),
                SqlTypeFamily::Bool
            ))
        );
    }
}

#[cfg(test)]
mod unary_plus {
    use super::*;

    #[test]
    fn with_numbers() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperation::Arithmetic(UnArithmetic::Pos),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(32767),
                    type_family: SqlTypeFamily::Integer
                }))),
            }
            .eval(),
            Ok(TypedValue::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            })
        );

        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperation::Arithmetic(UnArithmetic::Pos),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(32768),
                    type_family: SqlTypeFamily::Integer
                }))),
            }
            .eval(),
            Ok(TypedValue::Num {
                value: BigDecimal::from(32768),
                type_family: SqlTypeFamily::Integer
            })
        );
    }

    #[test]
    fn doubly_applied() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperation::Arithmetic(UnArithmetic::Pos),
                item: Box::new(StaticTypedTree::UnOp {
                    op: UnOperation::Arithmetic(UnArithmetic::Pos),
                    item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                        value: BigDecimal::from(32767),
                        type_family: SqlTypeFamily::Integer
                    }))),
                })
            }
            .eval(),
            Ok(TypedValue::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            })
        );

        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperation::Arithmetic(UnArithmetic::Pos),
                item: Box::new(StaticTypedTree::UnOp {
                    op: UnOperation::Arithmetic(UnArithmetic::Pos),
                    item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                        value: BigDecimal::from(32768),
                        type_family: SqlTypeFamily::Integer
                    }))),
                })
            }
            .eval(),
            Ok(TypedValue::Num {
                value: BigDecimal::from(32768),
                type_family: SqlTypeFamily::Integer
            })
        );
    }

    #[test]
    fn with_string() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperation::Arithmetic(UnArithmetic::Pos),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(
                    "str".to_owned()
                ))))
            }
            .eval(),
            Err(QueryExecutionError::undefined_function(
                UnOperation::Arithmetic(UnArithmetic::Pos),
                SqlTypeFamily::String
            ))
        );
    }

    #[test]
    fn with_boolean() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperation::Arithmetic(UnArithmetic::Pos),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true))))
            }
            .eval(),
            Err(QueryExecutionError::undefined_function(
                UnOperation::Arithmetic(UnArithmetic::Pos),
                SqlTypeFamily::Bool
            ))
        );
    }
}

#[cfg(test)]
mod unary_not {
    use super::*;

    #[test]
    fn with_bool() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperation::Logical(UnLogical::Not),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true)))),
            }
            .eval(),
            Ok(TypedValue::Bool(false))
        );
    }

    #[test]
    fn with_number() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperation::Logical(UnLogical::Not),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(0),
                    type_family: SqlTypeFamily::SmallInt
                }))),
            }
            .eval(),
            Err(QueryExecutionError::datatype_mismatch(
                UnOperation::Logical(UnLogical::Not),
                SqlTypeFamily::Bool,
                SqlTypeFamily::SmallInt
            ))
        );
    }

    #[test]
    fn with_string() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperation::Logical(UnLogical::Not),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(
                    "str".to_owned()
                )))),
            }
            .eval(),
            Err(QueryExecutionError::datatype_mismatch(
                UnOperation::Logical(UnLogical::Not),
                SqlTypeFamily::Bool,
                SqlTypeFamily::String
            ))
        );
    }
}
