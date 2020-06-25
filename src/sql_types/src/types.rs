use std::convert::TryInto;

#[derive(Debug, PartialEq)]
pub enum ConstraintError {
    OutOfRange,
    NotAnInt,
    ValueTooLong,
}

pub trait SQLType {
    fn constraint(&self) -> Box<dyn Constraint>;

    fn serializer(&self) -> Box<dyn Serializer>;
}

pub trait Constraint {
    fn validate(&self, in_value: &str) -> Result<(), ConstraintError>;
}

pub trait Serializer {
    fn ser(&self, in_value: &str) -> Vec<u8>;

    fn des(&self, out_value: &[u8]) -> String;
}

pub(crate) struct SmallIntSqlType;

impl SQLType for SmallIntSqlType {
    fn constraint(&self) -> Box<dyn Constraint> {
        Box::new(SmallIntTypeConstraint)
    }

    fn serializer(&self) -> Box<dyn Serializer> {
        Box::new(SmallIntTypeSerializer)
    }
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

pub(crate) struct IntegerSqlType;

impl SQLType for IntegerSqlType {
    fn constraint(&self) -> Box<dyn Constraint> {
        Box::new(IntegerSqlTypeConstraint)
    }

    fn serializer(&self) -> Box<dyn Serializer> {
        Box::new(IntegerSqlTypeSerializer)
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

pub(crate) struct BigIntSqlType;

impl SQLType for BigIntSqlType {
    fn constraint(&self) -> Box<dyn Constraint> {
        Box::new(BigIntTypeConstraint)
    }

    fn serializer(&self) -> Box<dyn Serializer> {
        Box::new(BigIntTypeSerializer)
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

pub(crate) struct CharSqlType {
    length: u64,
}

impl CharSqlType {
    pub(crate) fn new(length: u64) -> CharSqlType {
        CharSqlType { length }
    }
}

impl SQLType for CharSqlType {
    fn constraint(&self) -> Box<dyn Constraint> {
        Box::new(CharSqlTypeConstraint { length: self.length })
    }

    fn serializer(&self) -> Box<dyn Serializer> {
        Box::new(CharSqlTypeSerializer)
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

pub(crate) struct VarCharSqlType {
    length: u64,
}

impl VarCharSqlType {
    pub(crate) fn new(length: u64) -> VarCharSqlType {
        VarCharSqlType { length }
    }
}

impl SQLType for VarCharSqlType {
    fn constraint(&self) -> Box<dyn Constraint> {
        Box::new(VarCharSqlTypeConstraint { length: self.length })
    }

    fn serializer(&self) -> Box<dyn Serializer> {
        Box::new(VarCharSqlTypeSerializer)
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
                    SmallIntSqlType.serializer()
                }

                #[rstest::rstest]
                fn serialize(serializer: Box<dyn Serializer>) {
                    assert_eq!(serializer.ser("1"), vec![0, 1])
                }

                #[rstest::rstest]
                fn deserialize(serializer: Box<dyn Serializer>) {
                    assert_eq!(serializer.des(&vec![0, 1]), "1".to_owned())
                }
            }

            #[cfg(test)]
            mod validation {
                use super::*;

                #[rstest::fixture]
                fn constraint() -> Box<dyn Constraint> {
                    SmallIntSqlType.constraint()
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
                    IntegerSqlType.serializer()
                }

                #[rstest::rstest]
                fn serialize(serializer: Box<dyn Serializer>) {
                    assert_eq!(serializer.ser("1"), vec![0, 0, 0, 1])
                }

                #[rstest::rstest]
                fn deserialize(serializer: Box<dyn Serializer>) {
                    assert_eq!(serializer.des(&vec![0, 0, 0, 1]), "1".to_owned())
                }
            }

            #[cfg(test)]
            mod validation {
                use super::*;

                #[rstest::fixture]
                fn constraint() -> Box<dyn Constraint> {
                    IntegerSqlType.constraint()
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
                    BigIntSqlType.serializer()
                }

                #[rstest::rstest]
                fn serialize(serializer: Box<dyn Serializer>) {
                    assert_eq!(serializer.ser("1"), vec![0, 0, 0, 0, 0, 0, 0, 1])
                }

                #[rstest::rstest]
                fn deserialize(serializer: Box<dyn Serializer>) {
                    assert_eq!(serializer.des(&vec![0, 0, 0, 0, 0, 0, 0, 1]), "1".to_owned())
                }
            }

            #[cfg(test)]
            mod validation {
                use super::*;

                #[rstest::fixture]
                fn constraint() -> Box<dyn Constraint> {
                    BigIntSqlType.constraint()
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
                    CharSqlType::new(10).serializer()
                }

                #[rstest::rstest]
                fn serialize(serializer: Box<dyn Serializer>) {
                    assert_eq!(serializer.ser("str"), vec![115, 116, 114])
                }

                #[rstest::rstest]
                fn deserialize(serializer: Box<dyn Serializer>) {
                    assert_eq!(serializer.des(&vec![115, 116, 114]), "str".to_owned())
                }
            }

            #[cfg(test)]
            mod validation {
                use super::*;

                #[rstest::fixture]
                fn constraint() -> Box<dyn Constraint> {
                    CharSqlType::new(10).constraint()
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
                    VarCharSqlType::new(10).serializer()
                }

                #[rstest::rstest]
                fn serialize(serializer: Box<dyn Serializer>) {
                    assert_eq!(serializer.ser("str"), vec![115, 116, 114])
                }

                #[rstest::rstest]
                fn deserialize(serializer: Box<dyn Serializer>) {
                    assert_eq!(serializer.des(&vec![115, 116, 114]), "str".to_owned())
                }
            }

            #[cfg(test)]
            mod validation {
                use super::*;

                #[rstest::fixture]
                fn constraint() -> Box<dyn Constraint> {
                    VarCharSqlType::new(10).constraint()
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
