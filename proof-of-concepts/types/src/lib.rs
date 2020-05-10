use std::convert::TryFrom;

use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use sqlparser::ast::Value;

type AstTypeValue = Value;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Hash)]
pub enum Type {
    Int(BigInt),
    Decimal(BigDecimal),
    VarChar(String),
}

#[derive(Debug, PartialEq)]
pub enum TypeError {
    Unsupported(String),
}

impl TryFrom<AstTypeValue> for Type {
    type Error = TypeError;

    fn try_from(value: AstTypeValue) -> Result<Self, Self::Error> {
        match value {
            Value::Number(src) => {
                let (value, scale) = src.as_bigint_and_exponent();
                if scale == 0 {
                    Ok(Type::Int(value))
                } else {
                    Ok(Type::Decimal(src))
                }
            }
            Value::SingleQuotedString(value) => Ok(Type::VarChar(value)),
            ast_type => Err(TypeError::Unsupported(format!("{:?}", ast_type))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int(value: i32) -> AstTypeValue {
        Value::Number(BigDecimal::from(value))
    }

    fn decimal(value: i32, scale: i64) -> AstTypeValue {
        Value::Number(BigDecimal::new(BigInt::from(value), scale))
    }

    fn string(value: &'static str) -> AstTypeValue {
        Value::SingleQuotedString(value.to_owned())
    }

    #[cfg(test)]
    mod integer_value {
        use super::*;

        #[test]
        fn from_ast() {
            assert_eq!(Type::try_from(int(100)), Ok(Type::Int(BigInt::from(100))))
        }
    }

    #[cfg(test)]
    mod decimal_value {
        use super::*;

        #[test]
        fn decimal_value() {
            assert_eq!(
                Type::try_from(decimal(1000, 1)),
                Ok(Type::Decimal(BigDecimal::new(BigInt::from(1000), 1,)))
            )
        }
    }

    #[cfg(test)]
    mod var_char_value {
        use super::*;

        #[test]
        fn from_ast() {
            assert_eq!(
                Type::try_from(string("string value")),
                Ok(Type::VarChar("string value".to_owned()))
            )
        }
    }
}
