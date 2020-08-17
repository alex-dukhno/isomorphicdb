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

use protocol::sql_types::PostgreSqlType;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::string::ToString;

#[derive(PartialEq, Eq, Debug, Copy, Clone, Serialize, Deserialize, Hash, Ord, PartialOrd)]
pub enum SqlType {
    Bool,
    Char(u64),
    VarChar(u64),
    // Decimal,
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    // Real,
    // DoublePrecision,
    // Time,
    // TimeWithTimeZone,
    // Timestamp,
    // TimestampWithTimeZone,
    // Date,
    // Interval,
}

impl ToString for SqlType {
    fn to_string(&self) -> String {
        let string: &'static str = self.into();
        string.to_owned()
    }
}

impl SqlType {
    pub fn validate_and_serialize(&self, value: &str) -> Result<Vec<u8>, ConstraintError> {
        self.constraint().validate(value).map(|()| self.serializer().ser(value))
    }

    pub fn constraint(&self) -> Box<dyn Constraint> {
        match *self {
            Self::Char(length) => Box::new(CharSqlTypeConstraint { length }),
            Self::VarChar(length) => Box::new(VarCharSqlTypeConstraint { length }),
            Self::SmallInt(min) => Box::new(SmallIntTypeConstraint { min }),
            Self::Integer(min) => Box::new(IntegerSqlTypeConstraint { min }),
            Self::BigInt(min) => Box::new(BigIntTypeConstraint { min }),
            Self::Bool => Box::new(BoolSqlTypeConstraint),
            // sql_type => unimplemented!("Type constraint for {:?} is not currently implemented", sql_type),
        }
    }

    pub fn serializer(&self) -> Box<dyn Serializer> {
        match *self {
            Self::Char(_length) => Box::new(CharSqlTypeSerializer),
            Self::VarChar(_length) => Box::new(VarCharSqlTypeSerializer),
            Self::SmallInt(_min) => Box::new(SmallIntTypeSerializer),
            Self::Integer(_min) => Box::new(IntegerSqlTypeSerializer),
            Self::BigInt(_min) => Box::new(BigIntTypeSerializer),
            Self::Bool => Box::new(BoolSqlTypeSerializer),
            // sql_type => unimplemented!("Type Serializer for {:?} is not currently implemented", sql_type),
        }
    }
}

impl Into<&'static str> for &SqlType {
    fn into(self) -> &'static str {
        match self {
            SqlType::Bool => "bool",
            SqlType::Char(_) => "char",
            SqlType::VarChar(_) => "varchar",
            SqlType::SmallInt(_) => "smallint",
            SqlType::Integer(_) => "integer",
            SqlType::BigInt(_) => "bigint",
        }
    }
}

impl Into<PostgreSqlType> for &SqlType {
    fn into(self) -> PostgreSqlType {
        match self {
            SqlType::Bool => PostgreSqlType::Bool,
            SqlType::Char(_) => PostgreSqlType::Char,
            SqlType::VarChar(_) => PostgreSqlType::VarChar,
            // SqlType::Decimal => PostgreSqlType::Decimal,
            SqlType::SmallInt(_) => PostgreSqlType::SmallInt,
            SqlType::Integer(_) => PostgreSqlType::Integer,
            SqlType::BigInt(_) => PostgreSqlType::BigInt,
            // SqlType::Real => PostgreSqlType::Real,
            // SqlType::DoublePrecision => PostgreSqlType::DoublePrecision,
            // SqlType::Time => PostgreSqlType::Time,
            // SqlType::TimeWithTimeZone => PostgreSqlType::TimeWithTimeZone,
            // SqlType::Timestamp => PostgreSqlType::Timestamp,
            // SqlType::TimestampWithTimeZone => PostgreSqlType::TimestampWithTimeZone,
            // SqlType::Date => PostgreSqlType::Date,
            // SqlType::Interval => PostgreSqlType::Interval,
        }
    }
}

pub trait Constraint {
    fn validate(&self, in_value: &str) -> Result<(), ConstraintError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConstraintError {
    OutOfRange,
    TypeMismatch(String),
    ValueTooLong(u64),
}

pub trait Serializer {
    fn ser(&self, in_value: &str) -> Vec<u8>;

    fn des(&self, out_value: &[u8]) -> String;
}

struct SmallIntTypeConstraint {
    min: i16,
}

impl Constraint for SmallIntTypeConstraint {
    fn validate(&self, in_value: &str) -> Result<(), ConstraintError> {
        match lexical::parse::<i16, _>(in_value) {
            Ok(value) => {
                if self.min <= value {
                    Ok(())
                } else {
                    Err(ConstraintError::OutOfRange)
                }
            }
            Err(e) if e.code == lexical::ErrorCode::InvalidDigit => {
                Err(ConstraintError::TypeMismatch(in_value.to_owned()))
            }
            Err(_) => Err(ConstraintError::OutOfRange),
        }
    }
}

struct SmallIntTypeSerializer;

impl Serializer for SmallIntTypeSerializer {
    #[allow(clippy::match_wild_err_arm)]
    fn ser(&self, in_value: &str) -> Vec<u8> {
        match lexical::parse::<i16, _>(in_value) {
            Ok(parsed) => parsed.to_be_bytes().to_vec(),
            Err(_) => unreachable!(),
        }
    }

    fn des(&self, out_value: &[u8]) -> String {
        i16::from_be_bytes(out_value[0..2].try_into().unwrap()).to_string()
    }
}

struct IntegerSqlTypeConstraint {
    min: i32,
}

impl Constraint for IntegerSqlTypeConstraint {
    fn validate(&self, in_value: &str) -> Result<(), ConstraintError> {
        match lexical::parse::<i32, _>(in_value) {
            Ok(value) => {
                if self.min <= value {
                    Ok(())
                } else {
                    Err(ConstraintError::OutOfRange)
                }
            }
            Err(e) if e.code == lexical::ErrorCode::InvalidDigit => {
                Err(ConstraintError::TypeMismatch(in_value.to_owned()))
            }
            Err(_) => Err(ConstraintError::OutOfRange),
        }
    }
}

struct IntegerSqlTypeSerializer;

impl Serializer for IntegerSqlTypeSerializer {
    #[allow(clippy::match_wild_err_arm)]
    fn ser(&self, in_value: &str) -> Vec<u8> {
        match lexical::parse::<i32, _>(in_value) {
            Ok(parsed) => parsed.to_be_bytes().to_vec(),
            Err(_) => unreachable!(),
        }
    }

    fn des(&self, out_value: &[u8]) -> String {
        i32::from_be_bytes(out_value[0..4].try_into().unwrap()).to_string()
    }
}

struct BigIntTypeConstraint {
    min: i64,
}

impl Constraint for BigIntTypeConstraint {
    fn validate(&self, in_value: &str) -> Result<(), ConstraintError> {
        match lexical::parse::<i64, _>(in_value) {
            Ok(value) => {
                if self.min <= value {
                    Ok(())
                } else {
                    Err(ConstraintError::OutOfRange)
                }
            }
            Err(e) if e.code == lexical::ErrorCode::InvalidDigit => {
                Err(ConstraintError::TypeMismatch(in_value.to_owned()))
            }
            Err(_) => Err(ConstraintError::OutOfRange),
        }
    }
}

struct BigIntTypeSerializer;

impl Serializer for BigIntTypeSerializer {
    #[allow(clippy::match_wild_err_arm)]
    fn ser(&self, in_value: &str) -> Vec<u8> {
        match lexical::parse::<i64, _>(in_value) {
            Ok(parsed) => parsed.to_be_bytes().to_vec(),
            Err(_) => unreachable!(),
        }
    }

    fn des(&self, out_value: &[u8]) -> String {
        i64::from_be_bytes(out_value[0..8].try_into().unwrap()).to_string()
    }
}

struct CharSqlTypeConstraint {
    length: u64,
}

impl Constraint for CharSqlTypeConstraint {
    fn validate(&self, in_value: &str) -> Result<(), ConstraintError> {
        let trimmed = in_value.trim_end();
        if trimmed.len() > self.length as usize {
            Err(ConstraintError::ValueTooLong(self.length))
        } else {
            Ok(())
        }
    }
}

struct CharSqlTypeSerializer;

impl Serializer for CharSqlTypeSerializer {
    fn ser(&self, in_value: &str) -> Vec<u8> {
        in_value.trim_end().as_bytes().to_vec()
    }

    fn des(&self, out_value: &[u8]) -> String {
        String::from_utf8(out_value.to_vec()).unwrap()
    }
}

struct VarCharSqlTypeConstraint {
    length: u64,
}

impl Constraint for VarCharSqlTypeConstraint {
    fn validate(&self, in_value: &str) -> Result<(), ConstraintError> {
        let trimmed = in_value.trim_end();
        if trimmed.len() > self.length as usize {
            Err(ConstraintError::ValueTooLong(self.length))
        } else {
            Ok(())
        }
    }
}

struct VarCharSqlTypeSerializer;

impl Serializer for VarCharSqlTypeSerializer {
    fn ser(&self, in_value: &str) -> Vec<u8> {
        in_value.trim_end().as_bytes().to_vec()
    }

    fn des(&self, out_value: &[u8]) -> String {
        String::from_utf8(out_value.to_vec()).unwrap()
    }
}

struct BoolSqlTypeConstraint;

impl Constraint for BoolSqlTypeConstraint {
    fn validate(&self, in_value: &str) -> Result<(), ConstraintError> {
        let normalized_value = in_value.to_lowercase();
        match normalized_value.as_str() {
            "true" | "false" | "t" | "f" => Ok(()),
            "yes" | "no" | "y" | "n" => Ok(()),
            "on" | "off" => Ok(()),
            "1" | "0" => Ok(()),
            _ => Err(ConstraintError::TypeMismatch(in_value.to_owned())),
        }
    }
}

struct BoolSqlTypeSerializer;

impl Serializer for BoolSqlTypeSerializer {
    fn ser(&self, in_value: &str) -> Vec<u8> {
        let normalized_value = in_value.to_lowercase();
        match normalized_value.as_str() {
            "true" | "t" | "yes" | "y" | "on" | "1" => vec![1u8],
            _ => vec![0u8],
        }
    }

    fn des(&self, out_value: &[u8]) -> String {
        // The datatype output function for type boolean always emits either
        // t or f, as shown in Example 8.2.
        // See https://www.postgresql.org/docs/12/datatype-boolean.html#DATATYPE-BOOLEAN-EXAMPLE
        match out_value {
            [0u8] => "f".to_string(),
            [1u8] => "t".to_string(),
            other => panic!("Expected byte 0 or 1, but got {:?}", other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    mod to_postgresql_type_conversion {
        use super::*;
        use protocol::sql_types::PostgreSqlType;

        #[test]
        fn boolean() {
            let pg_type: PostgreSqlType = (&SqlType::Bool).into();
            assert_eq!(pg_type, PostgreSqlType::Bool);
        }

        #[test]
        fn small_int() {
            let pg_type: PostgreSqlType = (&SqlType::SmallInt(i16::min_value())).into();
            assert_eq!(pg_type, PostgreSqlType::SmallInt);
        }

        #[test]
        fn integer() {
            let pg_type: PostgreSqlType = (&SqlType::Integer(i32::min_value())).into();
            assert_eq!(pg_type, PostgreSqlType::Integer);
        }

        #[test]
        fn big_int() {
            let pg_type: PostgreSqlType = (&SqlType::BigInt(i64::min_value())).into();
            assert_eq!(pg_type, PostgreSqlType::BigInt);
        }

        #[test]
        fn char() {
            let pg_type: PostgreSqlType = (&SqlType::Char(0)).into();
            assert_eq!(pg_type, PostgreSqlType::Char);
        }

        #[test]
        fn var_char() {
            let pg_type: PostgreSqlType = (&SqlType::VarChar(0)).into();
            assert_eq!(pg_type, PostgreSqlType::VarChar);
        }

        // #[test]
        // fn decimal() {
        //     let pg_type: PostgreSqlType = (&SqlType::Decimal).into();
        //     assert_eq!(pg_type, PostgreSqlType::Decimal);
        // }
        //
        // #[test]
        // fn real() {
        //     let pg_type: PostgreSqlType = (&SqlType::Real).into();
        //     assert_eq!(pg_type, PostgreSqlType::Real);
        // }
        //
        // #[test]
        // fn double_precision() {
        //     let pg_type: PostgreSqlType = (&SqlType::DoublePrecision).into();
        //     assert_eq!(pg_type, PostgreSqlType::DoublePrecision);
        // }
        //
        // #[test]
        // fn time() {
        //     let pg_type: PostgreSqlType = (&SqlType::Time).into();
        //     assert_eq!(pg_type, PostgreSqlType::Time);
        // }
        //
        // #[test]
        // fn time_with_time_zone() {
        //     let pg_type: PostgreSqlType = (&SqlType::TimeWithTimeZone).into();
        //     assert_eq!(pg_type, PostgreSqlType::TimeWithTimeZone);
        // }
        //
        // #[test]
        // fn timestamp() {
        //     let pg_type: PostgreSqlType = (&SqlType::Timestamp).into();
        //     assert_eq!(pg_type, PostgreSqlType::Timestamp);
        // }
        //
        // #[test]
        // fn timestamp_with_timezone() {
        //     let pg_type: PostgreSqlType = (&SqlType::TimestampWithTimeZone).into();
        //     assert_eq!(pg_type, PostgreSqlType::TimestampWithTimeZone);
        // }
        //
        // #[test]
        // fn date() {
        //     let pg_type: PostgreSqlType = (&SqlType::Date).into();
        //     assert_eq!(pg_type, PostgreSqlType::Date);
        // }
        //
        // #[test]
        // fn interval() {
        //     let pg_type: PostgreSqlType = (&SqlType::Interval).into();
        //     assert_eq!(pg_type, PostgreSqlType::Interval);
        // }
    }

    #[cfg(test)]
    mod ints {
        use super::*;

        #[cfg(test)]
        mod small {
            use super::*;

            #[cfg(test)]
            mod serialization {
                use super::*;

                #[rstest::fixture]
                fn serializer() -> Box<dyn Serializer> {
                    SqlType::SmallInt(i16::min_value()).serializer()
                }

                #[rstest::rstest]
                fn serialize(serializer: Box<dyn Serializer>) {
                    assert_eq!(serializer.ser("1"), vec![0, 1])
                }

                #[rstest::rstest]
                fn deserialize(serializer: Box<dyn Serializer>) {
                    assert_eq!(serializer.des(&[0, 1]), "1".to_owned())
                }
            }

            #[cfg(test)]
            mod validation {
                use super::*;

                #[rstest::fixture]
                fn constraint() -> Box<dyn Constraint> {
                    SqlType::SmallInt(i16::min_value()).constraint()
                }

                #[rstest::rstest]
                fn in_range(constraint: Box<dyn Constraint>) {
                    assert_eq!(constraint.validate("1"), Ok(()));
                    assert_eq!(constraint.validate("32767"), Ok(()));
                    assert_eq!(constraint.validate("-32768"), Ok(()));
                }

                #[rstest::rstest]
                fn greater_than_max(constraint: Box<dyn Constraint>) {
                    assert_eq!(constraint.validate("32769"), Err(ConstraintError::OutOfRange))
                }

                #[rstest::rstest]
                fn less_than_min(constraint: Box<dyn Constraint>) {
                    assert_eq!(constraint.validate("-32769"), Err(ConstraintError::OutOfRange))
                }

                #[rstest::rstest]
                fn a_float_number(constraint: Box<dyn Constraint>) {
                    assert_eq!(
                        constraint.validate("-3276.9"),
                        Err(ConstraintError::TypeMismatch("-3276.9".to_owned()))
                    )
                }

                #[rstest::rstest]
                fn a_string(constraint: Box<dyn Constraint>) {
                    assert_eq!(
                        constraint.validate("str"),
                        Err(ConstraintError::TypeMismatch("str".to_owned()))
                    )
                }

                #[test]
                fn min_bound() {
                    let constraint = SqlType::SmallInt(0).constraint();

                    assert_eq!(constraint.validate("-1"), Err(ConstraintError::OutOfRange))
                }
            }
        }

        #[cfg(test)]
        mod integer {
            use super::*;

            #[cfg(test)]
            mod serialization {
                use super::*;

                #[rstest::fixture]
                fn serializer() -> Box<dyn Serializer> {
                    SqlType::Integer(i32::min_value()).serializer()
                }

                #[rstest::rstest]
                fn serialize(serializer: Box<dyn Serializer>) {
                    assert_eq!(serializer.ser("1"), vec![0, 0, 0, 1])
                }

                #[rstest::rstest]
                fn deserialize(serializer: Box<dyn Serializer>) {
                    assert_eq!(serializer.des(&[0, 0, 0, 1]), "1".to_owned())
                }
            }

            #[cfg(test)]
            mod validation {
                use super::*;

                #[rstest::fixture]
                fn constraint() -> Box<dyn Constraint> {
                    SqlType::Integer(i32::min_value()).constraint()
                }

                #[rstest::rstest]
                fn in_range(constraint: Box<dyn Constraint>) {
                    assert_eq!(constraint.validate("1"), Ok(()));
                    assert_eq!(constraint.validate("-2147483648"), Ok(()));
                    assert_eq!(constraint.validate("2147483647"), Ok(()));
                }

                #[rstest::rstest]
                fn greater_than_max(constraint: Box<dyn Constraint>) {
                    assert_eq!(constraint.validate("2147483649"), Err(ConstraintError::OutOfRange))
                }

                #[rstest::rstest]
                fn less_than_min(constraint: Box<dyn Constraint>) {
                    assert_eq!(constraint.validate("-2147483649"), Err(ConstraintError::OutOfRange))
                }

                #[rstest::rstest]
                fn a_float_number(constraint: Box<dyn Constraint>) {
                    assert_eq!(
                        constraint.validate("-214748.3649"),
                        Err(ConstraintError::TypeMismatch("-214748.3649".to_owned()))
                    )
                }

                #[rstest::rstest]
                fn a_string(constraint: Box<dyn Constraint>) {
                    assert_eq!(
                        constraint.validate("str"),
                        Err(ConstraintError::TypeMismatch("str".to_owned()))
                    )
                }

                #[test]
                fn min_bound() {
                    let constraint = SqlType::Integer(0).constraint();

                    assert_eq!(constraint.validate("-1"), Err(ConstraintError::OutOfRange))
                }
            }
        }

        #[cfg(test)]
        mod big_int {
            use super::*;

            #[cfg(test)]
            mod serialization {
                use super::*;

                #[rstest::fixture]
                fn serializer() -> Box<dyn Serializer> {
                    SqlType::BigInt(i64::min_value()).serializer()
                }

                #[rstest::rstest]
                fn serialize(serializer: Box<dyn Serializer>) {
                    assert_eq!(serializer.ser("1"), vec![0, 0, 0, 0, 0, 0, 0, 1])
                }

                #[rstest::rstest]
                fn deserialize(serializer: Box<dyn Serializer>) {
                    assert_eq!(serializer.des(&[0, 0, 0, 0, 0, 0, 0, 1]), "1".to_owned())
                }
            }

            #[cfg(test)]
            mod validation {
                use super::*;

                #[rstest::fixture]
                fn constraint() -> Box<dyn Constraint> {
                    SqlType::BigInt(i64::min_value()).constraint()
                }

                #[rstest::rstest]
                fn in_range(constraint: Box<dyn Constraint>) {
                    assert_eq!(constraint.validate("1"), Ok(()));
                    assert_eq!(constraint.validate("-9223372036854775808"), Ok(()));
                    assert_eq!(constraint.validate("9223372036854775807"), Ok(()));
                }

                #[rstest::rstest]
                fn greater_than_max(constraint: Box<dyn Constraint>) {
                    assert_eq!(
                        constraint.validate("9223372036854775809"),
                        Err(ConstraintError::OutOfRange)
                    )
                }

                #[rstest::rstest]
                fn less_than_min(constraint: Box<dyn Constraint>) {
                    assert_eq!(
                        constraint.validate("-9223372036854775809"),
                        Err(ConstraintError::OutOfRange)
                    )
                }

                #[rstest::rstest]
                fn a_float_number(constraint: Box<dyn Constraint>) {
                    assert_eq!(
                        constraint.validate("-3276.9"),
                        Err(ConstraintError::TypeMismatch("-3276.9".to_owned()))
                    )
                }

                #[rstest::rstest]
                fn a_string(constraint: Box<dyn Constraint>) {
                    assert_eq!(
                        constraint.validate("str"),
                        Err(ConstraintError::TypeMismatch("str".to_owned()))
                    )
                }

                #[test]
                fn min_bound() {
                    let constraint = SqlType::BigInt(0).constraint();

                    assert_eq!(constraint.validate("-1"), Err(ConstraintError::OutOfRange))
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
            mod serialization {
                use super::*;

                #[rstest::fixture]
                fn serializer() -> Box<dyn Serializer> {
                    SqlType::Char(10).serializer()
                }

                #[rstest::rstest]
                fn serialize(serializer: Box<dyn Serializer>) {
                    assert_eq!(serializer.ser("str"), vec![115, 116, 114])
                }

                #[rstest::rstest]
                fn deserialize(serializer: Box<dyn Serializer>) {
                    assert_eq!(serializer.des(&[115, 116, 114]), "str".to_owned())
                }
            }

            #[cfg(test)]
            mod validation {
                use super::*;

                #[rstest::fixture]
                fn constraint() -> Box<dyn Constraint> {
                    SqlType::Char(10).constraint()
                }

                #[rstest::rstest]
                fn in_length(constraint: Box<dyn Constraint>) {
                    assert_eq!(constraint.validate("1"), Ok(()))
                }

                #[rstest::rstest]
                fn too_long(constraint: Box<dyn Constraint>) {
                    assert_eq!(
                        constraint.validate("1".repeat(20).as_str()),
                        Err(ConstraintError::ValueTooLong(10))
                    )
                }
            }
        }

        #[cfg(test)]
        mod var_chars {
            use super::*;

            #[cfg(test)]
            mod serialization {
                use super::*;

                #[rstest::fixture]
                fn serializer() -> Box<dyn Serializer> {
                    SqlType::VarChar(10).serializer()
                }

                #[rstest::rstest]
                fn serialize(serializer: Box<dyn Serializer>) {
                    assert_eq!(serializer.ser("str"), vec![115, 116, 114])
                }

                #[rstest::rstest]
                fn deserialize(serializer: Box<dyn Serializer>) {
                    assert_eq!(serializer.des(&[115, 116, 114]), "str".to_owned())
                }
            }

            #[cfg(test)]
            mod validation {
                use super::*;

                #[rstest::fixture]
                fn constraint() -> Box<dyn Constraint> {
                    SqlType::VarChar(10).constraint()
                }

                #[rstest::rstest]
                fn in_length(constraint: Box<dyn Constraint>) {
                    assert_eq!(constraint.validate("1"), Ok(()))
                }

                #[rstest::rstest]
                fn too_long(constraint: Box<dyn Constraint>) {
                    assert_eq!(
                        constraint.validate("1".repeat(20).as_str()),
                        Err(ConstraintError::ValueTooLong(10))
                    )
                }
            }
        }
    }

    mod bool {
        use super::*;
        #[cfg(test)]
        mod serialization {
            use super::*;

            #[rstest::fixture]
            fn serializer() -> Box<dyn Serializer> {
                SqlType::Bool.serializer()
            }

            #[rstest::rstest]
            fn serialize(serializer: Box<dyn Serializer>) {
                assert_eq!(serializer.ser("TRUE"), vec![1]);
                assert_eq!(serializer.ser("true"), vec![1]);
                assert_eq!(serializer.ser("t"), vec![1]);
                assert_eq!(serializer.ser("yes"), vec![1]);
                assert_eq!(serializer.ser("y"), vec![1]);
                assert_eq!(serializer.ser("on"), vec![1]);
                assert_eq!(serializer.ser("1"), vec![1]);
                assert_eq!(serializer.ser("YES"), vec![1]);

                assert_eq!(serializer.ser("FALSE"), vec![0]);
                assert_eq!(serializer.ser("false"), vec![0]);
                assert_eq!(serializer.ser("f"), vec![0]);
                assert_eq!(serializer.ser("no"), vec![0]);
                assert_eq!(serializer.ser("n"), vec![0]);
                assert_eq!(serializer.ser("off"), vec![0]);
                assert_eq!(serializer.ser("0"), vec![0]);
                assert_eq!(serializer.ser("NO"), vec![0]);
            }

            #[rstest::rstest]
            fn deserialize(serializer: Box<dyn Serializer>) {
                assert_eq!(serializer.des(&[0]), "f".to_owned());
                assert_eq!(serializer.des(&[1]), "t".to_owned());
            }
        }

        #[cfg(test)]
        mod validation {
            use super::*;

            #[rstest::fixture]
            fn constraint() -> Box<dyn Constraint> {
                SqlType::Bool.constraint()
            }

            #[rstest::rstest]
            fn is_ok_true(constraint: Box<dyn Constraint>) {
                assert_eq!(constraint.validate("TRUE"), Ok(()));
                assert_eq!(constraint.validate("true"), Ok(()));
                assert_eq!(constraint.validate("t"), Ok(()));
                assert_eq!(constraint.validate("yes"), Ok(()));
                assert_eq!(constraint.validate("y"), Ok(()));
                assert_eq!(constraint.validate("on"), Ok(()));
                assert_eq!(constraint.validate("1"), Ok(()));
                assert_eq!(constraint.validate("YES"), Ok(()));
            }

            #[rstest::rstest]
            fn is_ok_false(constraint: Box<dyn Constraint>) {
                assert_eq!(constraint.validate("FALSE"), Ok(()));
                assert_eq!(constraint.validate("false"), Ok(()));
                assert_eq!(constraint.validate("f"), Ok(()));
                assert_eq!(constraint.validate("no"), Ok(()));
                assert_eq!(constraint.validate("n"), Ok(()));
                assert_eq!(constraint.validate("off"), Ok(()));
                assert_eq!(constraint.validate("0"), Ok(()));
                assert_eq!(constraint.validate("NO"), Ok(()));
            }

            #[rstest::rstest]
            fn is_non_bool(constraint: Box<dyn Constraint>) {
                assert_eq!(
                    constraint.validate("oops"),
                    Err(ConstraintError::TypeMismatch("oops".to_owned()))
                )
            }
        }
    }
}
