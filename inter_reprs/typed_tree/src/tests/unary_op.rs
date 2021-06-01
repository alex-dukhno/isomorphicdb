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
use operators_old::UnArithmetic;

#[cfg(test)]
mod unary_minus {
    use super::*;

    #[test]
    fn with_numbers() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Neg),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                    value: BigDecimal::from(32767),
                    type_family: SqlTypeFamilyOld::Integer
                }))),
            }
            .eval(&[], &[]),
            Ok(ScalarValueOld::Num {
                value: BigDecimal::from(-32767),
                type_family: SqlTypeFamilyOld::Integer
            })
        );

        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Neg),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                    value: BigDecimal::from(32768),
                    type_family: SqlTypeFamilyOld::Integer
                }))),
            }
            .eval(&[], &[]),
            Ok(ScalarValueOld::Num {
                value: BigDecimal::from(-32768),
                type_family: SqlTypeFamilyOld::Integer
            })
        );
    }

    #[test]
    fn doubly_applied() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Neg),
                item: Box::new(TypedTreeOld::UnOp {
                    op: UnOperator::Arithmetic(UnArithmetic::Neg),
                    item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                        value: BigDecimal::from(32767),
                        type_family: SqlTypeFamilyOld::Integer
                    }))),
                })
            }
            .eval(&[], &[]),
            Ok(ScalarValueOld::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamilyOld::Integer
            })
        );

        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Neg),
                item: Box::new(TypedTreeOld::UnOp {
                    op: UnOperator::Arithmetic(UnArithmetic::Neg),
                    item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                        value: BigDecimal::from(32768),
                        type_family: SqlTypeFamilyOld::Integer
                    }))),
                })
            }
            .eval(&[], &[]),
            Ok(ScalarValueOld::Num {
                value: BigDecimal::from(32768),
                type_family: SqlTypeFamilyOld::Integer
            })
        );
    }

    #[test]
    fn with_string() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Neg),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("str".to_owned()))))
            }
            .eval(&[], &[]),
            Err(QueryExecutionError::undefined_unary_function(
                UnOperator::Arithmetic(UnArithmetic::Neg),
                SqlTypeFamilyOld::String
            ))
        );
    }

    #[test]
    fn with_boolean() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Neg),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(true))))
            }
            .eval(&[], &[]),
            Err(QueryExecutionError::undefined_unary_function(
                UnOperator::Arithmetic(UnArithmetic::Neg),
                SqlTypeFamilyOld::Bool
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
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Pos),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                    value: BigDecimal::from(32767),
                    type_family: SqlTypeFamilyOld::Integer
                }))),
            }
            .eval(&[], &[]),
            Ok(ScalarValueOld::Num {
                value: BigDecimal::from(32767),
                type_family: SqlTypeFamilyOld::Integer
            })
        );

        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Pos),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                    value: BigDecimal::from(32768),
                    type_family: SqlTypeFamilyOld::Integer
                }))),
            }
            .eval(&[], &[]),
            Ok(ScalarValueOld::Num {
                value: BigDecimal::from(32768),
                type_family: SqlTypeFamilyOld::Integer
            })
        );
    }

    #[test]
    fn with_string() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Pos),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("str".to_owned()))))
            }
            .eval(&[], &[]),
            Err(QueryExecutionError::undefined_unary_function(
                UnOperator::Arithmetic(UnArithmetic::Pos),
                SqlTypeFamilyOld::String
            ))
        );
    }

    #[test]
    fn with_boolean() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Pos),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(true))))
            }
            .eval(&[], &[]),
            Err(QueryExecutionError::undefined_unary_function(
                UnOperator::Arithmetic(UnArithmetic::Pos),
                SqlTypeFamilyOld::Bool
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
            TypedTreeOld::UnOp {
                op: UnOperator::LogicalNot,
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(true)))),
            }
            .eval(&[], &[]),
            Ok(ScalarValueOld::Bool(false))
        );
    }

    #[test]
    fn with_number() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::LogicalNot,
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                    value: BigDecimal::from(0),
                    type_family: SqlTypeFamilyOld::SmallInt
                }))),
            }
            .eval(&[], &[]),
            Err(QueryExecutionError::datatype_mismatch(
                UnOperator::LogicalNot,
                SqlTypeFamilyOld::Bool,
                SqlTypeFamilyOld::SmallInt
            ))
        );
    }

    #[test]
    fn with_string() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::LogicalNot,
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("str".to_owned())))),
            }
            .eval(&[], &[]),
            Err(QueryExecutionError::datatype_mismatch(
                UnOperator::LogicalNot,
                SqlTypeFamilyOld::Bool,
                SqlTypeFamilyOld::String
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
            TypedTreeOld::UnOp {
                op: UnOperator::BitwiseNot,
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                    value: BigDecimal::from(1),
                    type_family: SqlTypeFamilyOld::SmallInt
                }))),
            }
            .eval(&[], &[]),
            Ok(ScalarValueOld::Num {
                value: BigDecimal::from(!1),
                type_family: SqlTypeFamilyOld::SmallInt
            })
        );

        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::BitwiseNot,
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                    value: BigDecimal::from(i32::MAX - i16::MAX as i32),
                    type_family: SqlTypeFamilyOld::Integer
                }))),
            }
            .eval(&[], &[]),
            Ok(ScalarValueOld::Num {
                value: BigDecimal::from(!(i32::MAX - i16::MAX as i32)),
                type_family: SqlTypeFamilyOld::Integer
            })
        );

        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::BitwiseNot,
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                    value: BigDecimal::from(i64::MAX - i32::MAX as i64),
                    type_family: SqlTypeFamilyOld::BigInt
                }))),
            }
            .eval(&[], &[]),
            Ok(ScalarValueOld::Num {
                value: BigDecimal::from(!(i64::MAX - i32::MAX as i64)),
                type_family: SqlTypeFamilyOld::BigInt
            })
        );
    }

    #[test]
    fn with_float_numbers() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::BitwiseNot,
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                    value: BigDecimal::from(1),
                    type_family: SqlTypeFamilyOld::Real
                }))),
            }
            .eval(&[], &[]),
            Err(QueryExecutionError::undefined_unary_function(
                UnOperator::BitwiseNot,
                SqlTypeFamilyOld::Real
            ))
        );

        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::BitwiseNot,
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                    value: BigDecimal::from(i32::MAX - i16::MAX as i32),
                    type_family: SqlTypeFamilyOld::Double
                }))),
            }
            .eval(&[], &[]),
            Err(QueryExecutionError::undefined_unary_function(
                UnOperator::BitwiseNot,
                SqlTypeFamilyOld::Double
            ))
        );
    }

    #[test]
    fn with_string() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::BitwiseNot,
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("str".to_owned())))),
            }
            .eval(&[], &[]),
            Err(QueryExecutionError::undefined_unary_function(
                UnOperator::BitwiseNot,
                SqlTypeFamilyOld::String
            ))
        );
    }

    #[test]
    fn with_boolean() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::BitwiseNot,
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(true))))
            }
            .eval(&[], &[]),
            Err(QueryExecutionError::undefined_unary_function(
                UnOperator::BitwiseNot,
                SqlTypeFamilyOld::Bool
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
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::SquareRoot),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                    value: BigDecimal::from(32768),
                    type_family: SqlTypeFamilyOld::Integer
                }))),
            }
            .eval(&[], &[]),
            Ok(ScalarValueOld::Num {
                value: BigDecimal::from(32768).sqrt().unwrap(),
                type_family: SqlTypeFamilyOld::Double
            })
        );
    }

    #[test]
    fn with_negative_numbers() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::SquareRoot),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                    value: BigDecimal::from(-32768),
                    type_family: SqlTypeFamilyOld::Integer
                }))),
            }
            .eval(&[], &[]),
            Err(QueryExecutionError::InvalidArgumentForPowerFunction)
        );
    }

    #[test]
    fn with_string() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::SquareRoot),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("str".to_owned()))))
            }
            .eval(&[], &[]),
            Err(QueryExecutionError::undefined_unary_function(
                UnOperator::Arithmetic(UnArithmetic::SquareRoot),
                SqlTypeFamilyOld::String
            ))
        );
    }

    #[test]
    fn with_boolean() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::SquareRoot),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(true))))
            }
            .eval(&[], &[]),
            Err(QueryExecutionError::undefined_unary_function(
                UnOperator::Arithmetic(UnArithmetic::SquareRoot),
                SqlTypeFamilyOld::Bool
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
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::CubeRoot),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                    value: BigDecimal::from(32768),
                    type_family: SqlTypeFamilyOld::Integer
                }))),
            }
            .eval(&[], &[]),
            Ok(ScalarValueOld::Num {
                value: BigDecimal::from(32768).cbrt(),
                type_family: SqlTypeFamilyOld::Double
            })
        );
    }

    #[test]
    fn with_string() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::CubeRoot),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("str".to_owned()))))
            }
            .eval(&[], &[]),
            Err(QueryExecutionError::undefined_unary_function(
                UnOperator::Arithmetic(UnArithmetic::CubeRoot),
                SqlTypeFamilyOld::String
            ))
        );
    }

    #[test]
    fn with_boolean() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::CubeRoot),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(true))))
            }
            .eval(&[], &[]),
            Err(QueryExecutionError::undefined_unary_function(
                UnOperator::Arithmetic(UnArithmetic::CubeRoot),
                SqlTypeFamilyOld::Bool
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
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Factorial),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                    value: BigDecimal::from(3),
                    type_family: SqlTypeFamilyOld::Integer
                }))),
            }
            .eval(&[], &[]),
            Ok(ScalarValueOld::Num {
                value: BigDecimal::from(6),
                type_family: SqlTypeFamilyOld::BigInt
            })
        );
    }

    #[test]
    fn with_negative_numbers() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Factorial),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                    value: BigDecimal::from(-3),
                    type_family: SqlTypeFamilyOld::Integer
                }))),
            }
            .eval(&[], &[]),
            Ok(ScalarValueOld::Num {
                value: BigDecimal::from(1),
                type_family: SqlTypeFamilyOld::BigInt
            })
        );
    }

    #[test]
    fn with_float_numbers() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Factorial),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                    value: BigDecimal::from(1),
                    type_family: SqlTypeFamilyOld::Real
                }))),
            }
            .eval(&[], &[]),
            Err(QueryExecutionError::undefined_unary_function(
                UnOperator::Arithmetic(UnArithmetic::Factorial),
                SqlTypeFamilyOld::Real
            ))
        );

        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Factorial),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                    value: BigDecimal::from(i32::MAX - i16::MAX as i32),
                    type_family: SqlTypeFamilyOld::Double
                }))),
            }
            .eval(&[], &[]),
            Err(QueryExecutionError::undefined_unary_function(
                UnOperator::Arithmetic(UnArithmetic::Factorial),
                SqlTypeFamilyOld::Double
            ))
        );
    }

    #[test]
    fn with_string() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Factorial),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("str".to_owned()))))
            }
            .eval(&[], &[]),
            Err(QueryExecutionError::undefined_unary_function(
                UnOperator::Arithmetic(UnArithmetic::Factorial),
                SqlTypeFamilyOld::String
            ))
        );
    }

    #[test]
    fn with_boolean() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Factorial),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(true))))
            }
            .eval(&[], &[]),
            Err(QueryExecutionError::undefined_unary_function(
                UnOperator::Arithmetic(UnArithmetic::Factorial),
                SqlTypeFamilyOld::Bool
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
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Abs),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                    value: BigDecimal::from(3),
                    type_family: SqlTypeFamilyOld::Integer
                }))),
            }
            .eval(&[], &[]),
            Ok(ScalarValueOld::Num {
                value: BigDecimal::from(3),
                type_family: SqlTypeFamilyOld::Integer
            })
        );
    }

    #[test]
    fn with_negative_numbers() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Abs),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Num {
                    value: BigDecimal::from(-3),
                    type_family: SqlTypeFamilyOld::Integer
                }))),
            }
            .eval(&[], &[]),
            Ok(ScalarValueOld::Num {
                value: BigDecimal::from(3),
                type_family: SqlTypeFamilyOld::Integer
            })
        );
    }

    #[test]
    fn with_string() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Abs),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::String("str".to_owned()))))
            }
            .eval(&[], &[]),
            Err(QueryExecutionError::undefined_unary_function(
                UnOperator::Arithmetic(UnArithmetic::Abs),
                SqlTypeFamilyOld::String
            ))
        );
    }

    #[test]
    fn with_boolean() {
        assert_eq!(
            TypedTreeOld::UnOp {
                op: UnOperator::Arithmetic(UnArithmetic::Abs),
                item: Box::new(TypedTreeOld::Item(TypedItemOld::Const(TypedValueOld::Bool(true))))
            }
            .eval(&[], &[]),
            Err(QueryExecutionError::undefined_unary_function(
                UnOperator::Arithmetic(UnArithmetic::Abs),
                SqlTypeFamilyOld::Bool
            ))
        );
    }
}
