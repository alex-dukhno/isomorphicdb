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
use data_manipulation_operators::UnArithmetic;

#[cfg(test)]
mod unary_minus {
    use super::*;

    #[test]
    fn with_numbers() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Neg),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(32767),
                    type_family: SqlTypeFamily::Integer
                }))),
            }
            .eval(&[]),
            Ok(ScalarValue::Num {
                value: BigDecimal::from(-32767),
                type_family: SqlTypeFamily::Integer
            })
        );

        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Neg),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(32768),
                    type_family: SqlTypeFamily::Integer
                }))),
            }
            .eval(&[]),
            Ok(ScalarValue::Num {
                value: BigDecimal::from(-32768),
                type_family: SqlTypeFamily::Integer
            })
        );
    }

    #[test]
    fn doubly_applied() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Neg),
                item: Box::new(StaticTypedTree::UnOp {
                    op: UnOperator::Arithmetic(UnArithmetic::Neg),
                    item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                        value: BigDecimal::from(32767),
                        type_family: SqlTypeFamily::Integer
                    }))),
                })
            }
            .eval(&[]),
            Ok(ScalarValue::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            })
        );

        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Neg),
                item: Box::new(StaticTypedTree::UnOp {
                    op: UnOperator::Arithmetic(UnArithmetic::Neg),
                    item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                        value: BigDecimal::from(32768),
                        type_family: SqlTypeFamily::Integer
                    }))),
                })
            }
            .eval(&[]),
            Ok(ScalarValue::Num {
                value: BigDecimal::from(32768),
                type_family: SqlTypeFamily::Integer
            })
        );
    }

    #[test]
    fn with_string() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Neg),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(
                    "str".to_owned()
                ))))
            }
            .eval(&[]),
            Err(QueryExecutionError::undefined_function(
                UnOperator::Arithmetic(UnArithmetic::Neg),
                SqlTypeFamily::String
            ))
        );
    }

    #[test]
    fn with_boolean() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Neg),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true))))
            }
            .eval(&[]),
            Err(QueryExecutionError::undefined_function(
                UnOperator::Arithmetic(UnArithmetic::Neg),
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
                op: UnOperator::Arithmetic(UnArithmetic::Pos),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(32767),
                    type_family: SqlTypeFamily::Integer
                }))),
            }
            .eval(&[]),
            Ok(ScalarValue::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamily::Integer
            })
        );

        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Pos),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(32768),
                    type_family: SqlTypeFamily::Integer
                }))),
            }
            .eval(&[]),
            Ok(ScalarValue::Num {
                value: BigDecimal::from(32768),
                type_family: SqlTypeFamily::Integer
            })
        );
    }

    #[test]
    fn with_string() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Pos),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(
                    "str".to_owned()
                ))))
            }
            .eval(&[]),
            Err(QueryExecutionError::undefined_function(
                UnOperator::Arithmetic(UnArithmetic::Pos),
                SqlTypeFamily::String
            ))
        );
    }

    #[test]
    fn with_boolean() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Pos),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true))))
            }
            .eval(&[]),
            Err(QueryExecutionError::undefined_function(
                UnOperator::Arithmetic(UnArithmetic::Pos),
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
                op: UnOperator::LogicalNot,
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true)))),
            }
            .eval(&[]),
            Ok(ScalarValue::Bool(false))
        );
    }

    #[test]
    fn with_number() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::LogicalNot,
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(0),
                    type_family: SqlTypeFamily::SmallInt
                }))),
            }
            .eval(&[]),
            Err(QueryExecutionError::datatype_mismatch(
                UnOperator::LogicalNot,
                SqlTypeFamily::Bool,
                SqlTypeFamily::SmallInt
            ))
        );
    }

    #[test]
    fn with_string() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::LogicalNot,
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(
                    "str".to_owned()
                )))),
            }
            .eval(&[]),
            Err(QueryExecutionError::datatype_mismatch(
                UnOperator::LogicalNot,
                SqlTypeFamily::Bool,
                SqlTypeFamily::String
            ))
        );
    }
}

#[cfg(test)]
mod unary_bitwise_not {
    use super::*;

    #[test]
    fn with_integers() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::BitwiseNot,
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(1),
                    type_family: SqlTypeFamily::SmallInt
                }))),
            }
            .eval(&[]),
            Ok(ScalarValue::Num {
                value: BigDecimal::from(!1),
                type_family: SqlTypeFamily::SmallInt
            })
        );

        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::BitwiseNot,
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(i32::MAX - i16::MAX as i32),
                    type_family: SqlTypeFamily::Integer
                }))),
            }
            .eval(&[]),
            Ok(ScalarValue::Num {
                value: BigDecimal::from(!(i32::MAX - i16::MAX as i32)),
                type_family: SqlTypeFamily::Integer
            })
        );

        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::BitwiseNot,
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(i64::MAX - i32::MAX as i64),
                    type_family: SqlTypeFamily::BigInt
                }))),
            }
            .eval(&[]),
            Ok(ScalarValue::Num {
                value: BigDecimal::from(!(i64::MAX - i32::MAX as i64)),
                type_family: SqlTypeFamily::BigInt
            })
        );
    }

    #[test]
    fn with_float_numbers() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::BitwiseNot,
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(1),
                    type_family: SqlTypeFamily::Real
                }))),
            }
            .eval(&[]),
            Err(QueryExecutionError::undefined_function(
                UnOperator::BitwiseNot,
                SqlTypeFamily::Real
            ))
        );

        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::BitwiseNot,
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(i32::MAX - i16::MAX as i32),
                    type_family: SqlTypeFamily::Double
                }))),
            }
            .eval(&[]),
            Err(QueryExecutionError::undefined_function(
                UnOperator::BitwiseNot,
                SqlTypeFamily::Double
            ))
        );
    }

    #[test]
    fn with_string() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::BitwiseNot,
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(
                    "str".to_owned()
                )))),
            }
            .eval(&[]),
            Err(QueryExecutionError::undefined_function(
                UnOperator::BitwiseNot,
                SqlTypeFamily::String
            ))
        );
    }

    #[test]
    fn with_boolean() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::BitwiseNot,
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true))))
            }
            .eval(&[]),
            Err(QueryExecutionError::undefined_function(
                UnOperator::BitwiseNot,
                SqlTypeFamily::Bool
            ))
        );
    }
}

#[cfg(test)]
mod square_root {
    use super::*;

    #[test]
    fn with_numbers() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::SquareRoot),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(32768),
                    type_family: SqlTypeFamily::Integer
                }))),
            }
            .eval(&[]),
            Ok(ScalarValue::Num {
                value: BigDecimal::from(32768).sqrt().unwrap(),
                type_family: SqlTypeFamily::Double
            })
        );
    }

    #[test]
    fn with_negative_numbers() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::SquareRoot),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(-32768),
                    type_family: SqlTypeFamily::Integer
                }))),
            }
            .eval(&[]),
            Err(QueryExecutionError::InvalidArgumentForPowerFunction)
        );
    }

    #[test]
    fn with_string() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::SquareRoot),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(
                    "str".to_owned()
                ))))
            }
            .eval(&[]),
            Err(QueryExecutionError::undefined_function(
                UnOperator::Arithmetic(UnArithmetic::SquareRoot),
                SqlTypeFamily::String
            ))
        );
    }

    #[test]
    fn with_boolean() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::SquareRoot),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true))))
            }
            .eval(&[]),
            Err(QueryExecutionError::undefined_function(
                UnOperator::Arithmetic(UnArithmetic::SquareRoot),
                SqlTypeFamily::Bool
            ))
        );
    }
}

#[cfg(test)]
mod cube_root {
    use super::*;

    #[test]
    fn with_numbers() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::CubeRoot),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(32768),
                    type_family: SqlTypeFamily::Integer
                }))),
            }
            .eval(&[]),
            Ok(ScalarValue::Num {
                value: BigDecimal::from(32768).cbrt(),
                type_family: SqlTypeFamily::Double
            })
        );
    }

    #[test]
    fn with_string() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::CubeRoot),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(
                    "str".to_owned()
                ))))
            }
            .eval(&[]),
            Err(QueryExecutionError::undefined_function(
                UnOperator::Arithmetic(UnArithmetic::CubeRoot),
                SqlTypeFamily::String
            ))
        );
    }

    #[test]
    fn with_boolean() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::CubeRoot),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true))))
            }
            .eval(&[]),
            Err(QueryExecutionError::undefined_function(
                UnOperator::Arithmetic(UnArithmetic::CubeRoot),
                SqlTypeFamily::Bool
            ))
        );
    }
}

#[cfg(test)]
mod factorial {
    use super::*;

    #[test]
    fn with_numbers() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Factorial),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(3),
                    type_family: SqlTypeFamily::Integer
                }))),
            }
            .eval(&[]),
            Ok(ScalarValue::Num {
                value: BigDecimal::from(6),
                type_family: SqlTypeFamily::BigInt
            })
        );
    }

    #[test]
    fn with_negative_numbers() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Factorial),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(-3),
                    type_family: SqlTypeFamily::Integer
                }))),
            }
            .eval(&[]),
            Ok(ScalarValue::Num {
                value: BigDecimal::from(1),
                type_family: SqlTypeFamily::BigInt
            })
        );
    }

    #[test]
    fn with_float_numbers() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Factorial),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(1),
                    type_family: SqlTypeFamily::Real
                }))),
            }
            .eval(&[]),
            Err(QueryExecutionError::undefined_function(
                UnOperator::Arithmetic(UnArithmetic::Factorial),
                SqlTypeFamily::Real
            ))
        );

        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Factorial),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(i32::MAX - i16::MAX as i32),
                    type_family: SqlTypeFamily::Double
                }))),
            }
            .eval(&[]),
            Err(QueryExecutionError::undefined_function(
                UnOperator::Arithmetic(UnArithmetic::Factorial),
                SqlTypeFamily::Double
            ))
        );
    }

    #[test]
    fn with_string() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Factorial),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(
                    "str".to_owned()
                ))))
            }
            .eval(&[]),
            Err(QueryExecutionError::undefined_function(
                UnOperator::Arithmetic(UnArithmetic::Factorial),
                SqlTypeFamily::String
            ))
        );
    }

    #[test]
    fn with_boolean() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Factorial),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true))))
            }
            .eval(&[]),
            Err(QueryExecutionError::undefined_function(
                UnOperator::Arithmetic(UnArithmetic::Factorial),
                SqlTypeFamily::Bool
            ))
        );
    }
}

#[cfg(test)]
mod absolute_value {
    use super::*;

    #[test]
    fn with_positive_numbers() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Abs),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(3),
                    type_family: SqlTypeFamily::Integer
                }))),
            }
            .eval(&[]),
            Ok(ScalarValue::Num {
                value: BigDecimal::from(3),
                type_family: SqlTypeFamily::Integer
            })
        );
    }

    #[test]
    fn with_negative_numbers() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Abs),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Num {
                    value: BigDecimal::from(-3),
                    type_family: SqlTypeFamily::Integer
                }))),
            }
            .eval(&[]),
            Ok(ScalarValue::Num {
                value: BigDecimal::from(3),
                type_family: SqlTypeFamily::Integer
            })
        );
    }

    #[test]
    fn with_string() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Abs),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::String(
                    "str".to_owned()
                ))))
            }
            .eval(&[]),
            Err(QueryExecutionError::undefined_function(
                UnOperator::Arithmetic(UnArithmetic::Abs),
                SqlTypeFamily::String
            ))
        );
    }

    #[test]
    fn with_boolean() {
        assert_eq!(
            StaticTypedTree::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Abs),
                item: Box::new(StaticTypedTree::Item(StaticTypedItem::Const(TypedValue::Bool(true))))
            }
            .eval(&[]),
            Err(QueryExecutionError::undefined_function(
                UnOperator::Arithmetic(UnArithmetic::Abs),
                SqlTypeFamily::Bool
            ))
        );
    }
}
