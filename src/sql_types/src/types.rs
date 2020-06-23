use std::convert::TryInto;

#[derive(Debug, PartialEq)]
pub enum ConstraintError {
    OutOfRange,
    NotAnInt,
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

    fn des(&self, out_value: &Vec<u8>) -> String;
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
    fn ser(&self, in_value: &str) -> Vec<u8> {
        match lexical::parse::<i16, _>(in_value) {
            Ok(parsed) => parsed.to_be_bytes().to_vec(),
            Err(_e) => unimplemented!(),
        }
    }

    fn des(&self, out_value: &Vec<u8>) -> String {
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
    fn ser(&self, in_value: &str) -> Vec<u8> {
        match lexical::parse::<i32, _>(in_value) {
            Ok(parsed) => parsed.to_be_bytes().to_vec(),
            Err(_e) => unimplemented!(),
        }
    }

    fn des(&self, out_value: &Vec<u8>) -> String {
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
    fn ser(&self, in_value: &str) -> Vec<u8> {
        match lexical::parse::<i64, _>(in_value) {
            Ok(parsed) => parsed.to_be_bytes().to_vec(),
            Err(_e) => unimplemented!(),
        }
    }

    fn des(&self, out_value: &Vec<u8>) -> String {
        i64::from_be_bytes(out_value[0..8].try_into().unwrap()).to_string()
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
                    assert_eq!(constraint.validate("1"), Ok(()))
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
                    assert_eq!(constraint.validate("1"), Ok(()))
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
                    assert_eq!(constraint.validate("-3276.9"), Err(ConstraintError::NotAnInt))
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
                    assert_eq!(constraint.validate("1"), Ok(()))
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
}
