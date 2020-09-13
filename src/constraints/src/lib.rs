// Copyright 2020 Alex Dukhno
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

use ast::{
    values::{Bool, ScalarValue},
    Datum,
};
use bigdecimal::{BigDecimal, ToPrimitive};
use num_bigint::BigInt;
use sql_model::sql_types::SqlType;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConstraintError {
    OutOfRange,
    TypeMismatch(String),
    ValueTooLong(u64),
}

pub trait Constraint {
    fn validate(&self, in_value: ScalarValue) -> Result<Datum, ConstraintError>;
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum TypeConstraint {
    Bool,
    Char(u64),
    VarChar(u64),
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Real,
    DoublePrecision,
}

impl From<&SqlType> for TypeConstraint {
    fn from(sql_type: &SqlType) -> TypeConstraint {
        match sql_type {
            SqlType::Bool => TypeConstraint::Bool,
            SqlType::Char(len) => TypeConstraint::Char(*len),
            SqlType::VarChar(len) => TypeConstraint::VarChar(*len),
            SqlType::SmallInt(min) => TypeConstraint::SmallInt(*min),
            SqlType::Integer(min) => TypeConstraint::Integer(*min),
            SqlType::BigInt(min) => TypeConstraint::BigInt(*min),
            SqlType::Real => TypeConstraint::Real,
            SqlType::DoublePrecision => TypeConstraint::DoublePrecision,
        }
    }
}

impl Constraint for TypeConstraint {
    fn validate(&self, in_value: ScalarValue) -> Result<Datum, ConstraintError> {
        match self {
            TypeConstraint::SmallInt(min) => match &in_value {
                ScalarValue::Number(value) => {
                    let (int, exp) = value.as_bigint_and_exponent();
                    if exp != 0 {
                        Err(ConstraintError::TypeMismatch(in_value.to_string()))
                    } else if BigInt::from(*min) <= int && int <= BigInt::from(i16::max_value()) {
                        Ok(Datum::Int16(int.to_i16().unwrap()))
                    } else {
                        Err(ConstraintError::OutOfRange)
                    }
                }
                _ => Err(ConstraintError::TypeMismatch(in_value.to_string())),
            },
            TypeConstraint::Integer(min) => match &in_value {
                ScalarValue::Number(value) => {
                    let (int, exp) = value.as_bigint_and_exponent();
                    if exp != 0 {
                        Err(ConstraintError::TypeMismatch(in_value.to_string()))
                    } else if BigInt::from(*min) <= int && int <= BigInt::from(i32::max_value()) {
                        Ok(Datum::Int32(int.to_i32().unwrap()))
                    } else {
                        Err(ConstraintError::OutOfRange)
                    }
                }
                _ => Err(ConstraintError::TypeMismatch(in_value.to_string())),
            },
            TypeConstraint::BigInt(min) => match &in_value {
                ScalarValue::Number(value) => {
                    let (int, exp) = value.as_bigint_and_exponent();
                    if exp != 0 {
                        Err(ConstraintError::TypeMismatch(in_value.to_string()))
                    } else if BigInt::from(*min) <= int && int <= BigInt::from(i64::max_value()) {
                        Ok(Datum::Int64(int.to_i64().unwrap()))
                    } else {
                        Err(ConstraintError::OutOfRange)
                    }
                }
                _ => Err(ConstraintError::TypeMismatch(in_value.to_string())),
            },
            TypeConstraint::Char(len) => match &in_value {
                ScalarValue::String(in_value) => {
                    let trimmed = in_value.trim_end();
                    if trimmed.len() > *len as usize {
                        Err(ConstraintError::ValueTooLong(*len))
                    } else {
                        Ok(Datum::OwnedString(trimmed.to_owned()))
                    }
                }
                _ => Err(ConstraintError::TypeMismatch(in_value.to_string())),
            },
            TypeConstraint::VarChar(len) => match &in_value {
                ScalarValue::String(in_value) => {
                    let trimmed = in_value.trim_end();
                    if trimmed.len() > *len as usize {
                        Err(ConstraintError::ValueTooLong(*len))
                    } else {
                        Ok(Datum::OwnedString(trimmed.to_owned()))
                    }
                }
                _ => Err(ConstraintError::TypeMismatch(in_value.to_string())),
            },
            TypeConstraint::Bool => match &in_value {
                ScalarValue::Bool(Bool(boolean)) => Ok(Datum::from_bool(*boolean)),
                _ => Err(ConstraintError::TypeMismatch(in_value.to_string())),
            },
            TypeConstraint::Real => match &in_value {
                ScalarValue::Number(value) => {
                    if BigDecimal::from(f32::MIN) <= *value && *value <= BigDecimal::from(f32::MAX) {
                        Ok(Datum::from_f32(value.to_f32().unwrap()))
                    } else {
                        Err(ConstraintError::OutOfRange)
                    }
                }
                _ => Err(ConstraintError::TypeMismatch(in_value.to_string())),
            },
            TypeConstraint::DoublePrecision => match &in_value {
                ScalarValue::Number(value) => {
                    if BigDecimal::from(f64::MIN) <= *value && *value <= BigDecimal::from(f64::MAX) {
                        Ok(Datum::from_f64(value.to_f64().unwrap()))
                    } else {
                        Err(ConstraintError::OutOfRange)
                    }
                }
                _ => Err(ConstraintError::TypeMismatch(in_value.to_string())),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::values::Bool;
    use bigdecimal::BigDecimal;
    use std::str::FromStr;

    #[cfg(test)]
    mod ints {
        use super::*;

        #[cfg(test)]
        mod small {
            use super::*;

            #[cfg(test)]
            mod validation {
                use super::*;

                #[rstest::fixture]
                fn constraint() -> TypeConstraint {
                    TypeConstraint::SmallInt(i16::min_value())
                }

                #[rstest::rstest]
                fn in_range(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(BigDecimal::from_str("1").unwrap())),
                        Ok(Datum::Int16(1))
                    );
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(BigDecimal::from_str("32767").unwrap())),
                        Ok(Datum::Int16(i16::max_value()))
                    );
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(BigDecimal::from_str("-32768").unwrap())),
                        Ok(Datum::Int16(i16::min_value()))
                    );
                }

                #[rstest::rstest]
                fn greater_than_max(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(BigDecimal::from_str("32769").unwrap())),
                        Err(ConstraintError::OutOfRange)
                    )
                }

                #[rstest::rstest]
                fn less_than_min(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(BigDecimal::from_str("-32769").unwrap())),
                        Err(ConstraintError::OutOfRange)
                    )
                }

                #[rstest::rstest]
                fn a_float_number(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(BigDecimal::from_str("-3276.9").unwrap())),
                        Err(ConstraintError::TypeMismatch("-3276.9".to_owned()))
                    )
                }

                #[rstest::rstest]
                fn a_string(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::String("str".to_owned())),
                        Err(ConstraintError::TypeMismatch("str".to_owned()))
                    )
                }

                #[test]
                fn min_bound() {
                    let constraint = TypeConstraint::SmallInt(0);

                    assert_eq!(
                        constraint.validate(ScalarValue::Number(BigDecimal::from_str("-1").unwrap())),
                        Err(ConstraintError::OutOfRange)
                    )
                }
            }
        }

        #[cfg(test)]
        mod integer {
            use super::*;

            #[cfg(test)]
            mod validation {
                use super::*;

                #[rstest::fixture]
                fn constraint() -> TypeConstraint {
                    TypeConstraint::Integer(i32::min_value())
                }

                #[rstest::rstest]
                fn in_range(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(BigDecimal::from_str("1").unwrap())),
                        Ok(Datum::Int32(1))
                    );
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(BigDecimal::from_str("-2147483648").unwrap())),
                        Ok(Datum::Int32(i32::min_value()))
                    );
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(BigDecimal::from_str("2147483647").unwrap())),
                        Ok(Datum::Int32(i32::max_value()))
                    );
                }

                #[rstest::rstest]
                fn greater_than_max(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(BigDecimal::from_str("2147483649").unwrap())),
                        Err(ConstraintError::OutOfRange)
                    )
                }

                #[rstest::rstest]
                fn less_than_min(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(BigDecimal::from_str("-2147483649").unwrap())),
                        Err(ConstraintError::OutOfRange)
                    )
                }

                #[rstest::rstest]
                fn a_float_number(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(BigDecimal::from_str("-214748.3649").unwrap())),
                        Err(ConstraintError::TypeMismatch("-214748.3649".to_owned()))
                    )
                }

                #[rstest::rstest]
                fn a_string(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::String("str".to_string())),
                        Err(ConstraintError::TypeMismatch("str".to_owned()))
                    )
                }

                #[test]
                fn min_bound() {
                    let constraint = TypeConstraint::Integer(0);

                    assert_eq!(
                        constraint.validate(ScalarValue::Number(BigDecimal::from_str("-1").unwrap())),
                        Err(ConstraintError::OutOfRange)
                    )
                }
            }
        }

        #[cfg(test)]
        mod big_int {
            use super::*;

            #[cfg(test)]
            mod validation {
                use super::*;

                #[rstest::fixture]
                fn constraint() -> TypeConstraint {
                    TypeConstraint::BigInt(i64::min_value())
                }

                #[rstest::rstest]
                fn in_range(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(BigDecimal::from_str("1").unwrap())),
                        Ok(Datum::Int64(1))
                    );
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(
                            BigDecimal::from_str("-9223372036854775808").unwrap()
                        )),
                        Ok(Datum::Int64(i64::min_value()))
                    );
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(
                            BigDecimal::from_str("9223372036854775807").unwrap()
                        )),
                        Ok(Datum::Int64(i64::max_value()))
                    );
                }

                #[rstest::rstest]
                fn greater_than_max(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(
                            BigDecimal::from_str("9223372036854775809").unwrap()
                        )),
                        Err(ConstraintError::OutOfRange)
                    )
                }

                #[rstest::rstest]
                fn less_than_min(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(
                            BigDecimal::from_str("-9223372036854775809").unwrap()
                        )),
                        Err(ConstraintError::OutOfRange)
                    )
                }

                #[rstest::rstest]
                fn a_float_number(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(BigDecimal::from_str("-3276.9").unwrap())),
                        Err(ConstraintError::TypeMismatch("-3276.9".to_owned()))
                    )
                }

                #[rstest::rstest]
                fn a_string(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::String("str".to_owned())),
                        Err(ConstraintError::TypeMismatch("str".to_owned()))
                    )
                }

                #[test]
                fn min_bound() {
                    let constraint = TypeConstraint::BigInt(0);

                    assert_eq!(
                        constraint.validate(ScalarValue::Number(BigDecimal::from_str("-1").unwrap())),
                        Err(ConstraintError::OutOfRange)
                    )
                }
            }
        }
    }

    #[cfg(test)]
    mod strings {
        use super::*;

        #[cfg(test)]
        mod chars {
            use super::*;

            #[cfg(test)]
            mod validation {
                use super::*;

                #[rstest::fixture]
                fn constraint() -> TypeConstraint {
                    TypeConstraint::Char(10)
                }

                #[rstest::rstest]
                fn in_length(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::String("1".to_owned())),
                        Ok(Datum::OwnedString("1".to_owned()))
                    )
                }

                #[rstest::rstest]
                fn too_long(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::String("1".repeat(20))),
                        Err(ConstraintError::ValueTooLong(10))
                    )
                }
            }
        }

        #[cfg(test)]
        mod var_chars {
            use super::*;

            #[cfg(test)]
            mod validation {
                use super::*;

                #[rstest::fixture]
                fn constraint() -> TypeConstraint {
                    TypeConstraint::VarChar(10)
                }

                #[rstest::rstest]
                fn in_length(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::String("1".to_owned())),
                        Ok(Datum::OwnedString("1".to_owned()))
                    )
                }

                #[rstest::rstest]
                fn too_long(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::String("1".repeat(20))),
                        Err(ConstraintError::ValueTooLong(10))
                    )
                }
            }
        }
    }

    #[cfg(test)]
    mod bool {
        use super::*;

        #[cfg(test)]
        mod validation {
            use super::*;

            #[rstest::fixture]
            fn constraint() -> TypeConstraint {
                TypeConstraint::Bool
            }

            #[rstest::rstest]
            fn is_ok_true(constraint: TypeConstraint) {
                assert_eq!(
                    constraint.validate(ScalarValue::Bool(Bool(true))),
                    Ok(Datum::from_bool(true))
                );
            }

            #[rstest::rstest]
            fn is_ok_false(constraint: TypeConstraint) {
                assert_eq!(
                    constraint.validate(ScalarValue::Bool(Bool(false))),
                    Ok(Datum::from_bool(false))
                );
            }

            #[rstest::rstest]
            fn is_non_bool(constraint: TypeConstraint) {
                assert_eq!(
                    constraint.validate(ScalarValue::String("oops".to_owned())),
                    Err(ConstraintError::TypeMismatch("oops".to_owned()))
                )
            }
        }
    }

    #[cfg(test)]
    mod floats {
        use super::*;

        #[cfg(test)]
        mod real {
            use super::*;

            #[cfg(test)]
            mod validation {
                use super::*;

                #[rstest::fixture]
                fn constraint() -> TypeConstraint {
                    TypeConstraint::Real
                }

                #[rstest::rstest]
                fn in_range(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(BigDecimal::from_str("1").unwrap())),
                        Ok(Datum::from_f32(1.0))
                    );
                    //TODO investigate floating point conversion
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(BigDecimal::from(
                            f32::MAX - 50000000000000000000000000000000.0
                        ))),
                        Ok(Datum::from_f32(f32::MAX - 50000000000000000000000000000000.0))
                    );
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(BigDecimal::from(
                            f32::MIN + 50000000000000000000000000000000.0
                        ))),
                        Ok(Datum::from_f32(f32::MIN + 50000000000000000000000000000000.0))
                    );
                }

                #[rstest::rstest]
                fn greater_than_max(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(
                            BigDecimal::from_str(&(f32::MAX.to_string() + "1")).unwrap()
                        )),
                        Err(ConstraintError::OutOfRange)
                    )
                }

                #[rstest::rstest]
                fn less_than_min(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(
                            BigDecimal::from_str(&(f32::MIN.to_string() + "1")).unwrap()
                        )),
                        Err(ConstraintError::OutOfRange)
                    )
                }

                #[rstest::rstest]
                fn a_string(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::String("str".to_owned())),
                        Err(ConstraintError::TypeMismatch("str".to_owned()))
                    )
                }
            }
        }

        #[cfg(test)]
        mod double_precision {
            use super::*;

            #[cfg(test)]
            mod validation {
                use super::*;

                #[rstest::fixture]
                fn constraint() -> TypeConstraint {
                    TypeConstraint::DoublePrecision
                }

                #[rstest::rstest]
                fn in_range(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(BigDecimal::from_str("1").unwrap())),
                        Ok(Datum::from_f64(1.0))
                    );
                    //TODO investigate floating point conversion
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(
                            BigDecimal::from_str(&f64::MAX.to_string()).unwrap()
                        )),
                        Ok(Datum::from_f64(f64::MAX))
                    );
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(
                            BigDecimal::from_str(&f64::MIN.to_string()).unwrap()
                        )),
                        Ok(Datum::from_f64(f64::MIN))
                    );
                }

                #[rstest::rstest]
                fn greater_than_max(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(
                            BigDecimal::from_str(&(f64::MAX.to_string() + "1")).unwrap()
                        )),
                        Err(ConstraintError::OutOfRange)
                    )
                }

                #[rstest::rstest]
                fn less_than_min(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::Number(
                            BigDecimal::from_str(&(f64::MIN.to_string() + "1")).unwrap()
                        )),
                        Err(ConstraintError::OutOfRange)
                    )
                }

                #[rstest::rstest]
                fn a_string(constraint: TypeConstraint) {
                    assert_eq!(
                        constraint.validate(ScalarValue::String("str".to_owned())),
                        Err(ConstraintError::TypeMismatch("str".to_owned()))
                    )
                }
            }
        }
    }
}
