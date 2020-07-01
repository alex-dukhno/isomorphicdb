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

use serde::{Deserialize, Serialize};
use std::convert::TryInto;

#[derive(PartialEq, Debug, Copy, Clone, Serialize, Deserialize)]
pub enum SqlType {
    Bool,
    Char(u64),
    VarChar(u64),
    Decimal,
    SmallInt,
    Integer,
    BigInt,
    Real,
    DoublePrecision,
    Time,
    TimeWithTimeZone,
    Timestamp,
    TimestampWithTimeZone,
    Date,
    Interval,
}

impl SqlType {
    pub fn constraint(&self) -> Box<dyn Constraint> {
        match *self {
            SqlType::Char(length) => Box::new(CharSqlTypeConstraint { length }),
            SqlType::VarChar(length) => Box::new(VarCharSqlTypeConstraint { length }),
            SqlType::SmallInt => Box::new(SmallIntTypeConstraint),
            SqlType::Integer => Box::new(IntegerSqlTypeConstraint),
            SqlType::BigInt => Box::new(BigIntTypeConstraint),
            sql_type => unimplemented!("Type constraint for {:?} is not currently implemented", sql_type),
        }
    }

    pub fn serializer(&self) -> Box<dyn Serializer> {
        match *self {
            SqlType::Char(_length) => Box::new(CharSqlTypeSerializer),
            SqlType::VarChar(_length) => Box::new(VarCharSqlTypeSerializer),
            SqlType::SmallInt => Box::new(SmallIntTypeSerializer),
            SqlType::Integer => Box::new(IntegerSqlTypeSerializer),
            SqlType::BigInt => Box::new(BigIntTypeSerializer),
            sql_type => unimplemented!("Type Serializer for {:?} is not currently implemented", sql_type),
        }
    }
}

pub trait Constraint {
    fn validate(&self, in_value: &str) -> Result<(), ConstraintError>;
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum ConstraintError {
    OutOfRange,
    NotAnInt,
    ValueTooLong,
}

pub trait Serializer {
    fn ser(&self, in_value: &str) -> Vec<u8>;

    fn des(&self, out_value: &[u8]) -> String;
}

struct SmallIntTypeConstraint;

impl Constraint for SmallIntTypeConstraint {
    fn validate(&self, in_value: &str) -> Result<(), ConstraintError> {
        match lexical::parse::<i16, _>(in_value) {
            Ok(_) => Ok(()),
            Err(e) if e.code == lexical::ErrorCode::InvalidDigit => Err(ConstraintError::NotAnInt),
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
            Err(_) => unimplemented!(),
        }
    }

    fn des(&self, out_value: &[u8]) -> String {
        i16::from_be_bytes(out_value[0..2].try_into().unwrap()).to_string()
    }
}

struct IntegerSqlTypeConstraint;

impl Constraint for IntegerSqlTypeConstraint {
    fn validate(&self, in_value: &str) -> Result<(), ConstraintError> {
        match lexical::parse::<i32, _>(in_value) {
            Ok(_) => Ok(()),
            Err(e) if e.code == lexical::ErrorCode::InvalidDigit => Err(ConstraintError::NotAnInt),
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
            Err(_) => unimplemented!(),
        }
    }

    fn des(&self, out_value: &[u8]) -> String {
        i32::from_be_bytes(out_value[0..4].try_into().unwrap()).to_string()
    }
}

struct BigIntTypeConstraint;

impl Constraint for BigIntTypeConstraint {
    fn validate(&self, in_value: &str) -> Result<(), ConstraintError> {
        match lexical::parse::<i64, _>(in_value) {
            Ok(_) => Ok(()),
            Err(e) if e.code == lexical::ErrorCode::InvalidDigit => Err(ConstraintError::NotAnInt),
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
            Err(_) => unimplemented!(),
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
            Err(ConstraintError::ValueTooLong)
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
            Err(ConstraintError::ValueTooLong)
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

#[cfg(test)]
mod tests {
    use super::*;

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
                    SqlType::SmallInt.serializer()
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
                    SqlType::SmallInt.constraint()
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
                    assert_eq!(constraint.validate("-3276.9"), Err(ConstraintError::NotAnInt))
                }

                #[rstest::rstest]
                fn a_string(constraint: Box<dyn Constraint>) {
                    assert_eq!(constraint.validate("str"), Err(ConstraintError::NotAnInt))
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
                    SqlType::Integer.serializer()
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
                    SqlType::Integer.constraint()
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
                    assert_eq!(constraint.validate("-214748.3649"), Err(ConstraintError::NotAnInt))
                }

                #[rstest::rstest]
                fn a_string(constraint: Box<dyn Constraint>) {
                    assert_eq!(constraint.validate("str"), Err(ConstraintError::NotAnInt))
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
                    SqlType::BigInt.serializer()
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
                    SqlType::BigInt.constraint()
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
                    assert_eq!(constraint.validate("-3276.9"), Err(ConstraintError::NotAnInt))
                }

                #[rstest::rstest]
                fn a_string(constraint: Box<dyn Constraint>) {
                    assert_eq!(constraint.validate("str"), Err(ConstraintError::NotAnInt))
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
                        Err(ConstraintError::ValueTooLong)
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
                        Err(ConstraintError::ValueTooLong)
                    )
                }
            }
        }
    }
}
