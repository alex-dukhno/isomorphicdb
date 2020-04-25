use std::convert::TryFrom;

use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use sqlparser::ast::Value;

type AstTypeValue = Value;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
pub struct Int {
  value: BigInt
}

impl Int {
  pub fn new(value: BigInt) -> Self {
    Self { value }
  }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Decimal {
  value: BigDecimal
}

impl Decimal {
  pub fn new(value: BigDecimal) -> Self {
    Self { value }
  }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct VarChar {
  value: String
}

impl VarChar {
  pub fn new(value: String) -> Self {
    Self { value }
  }
}

impl From<&str> for VarChar {
  fn from(s: &str) -> Self {
    Self::new(s.to_owned())
  }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Type {
  Int(Int),
  Decimal(Decimal),
  VarChar(VarChar),
}

#[derive(Debug, PartialEq)]
pub enum TypeError {
  Unsupported(String)
}

impl TryFrom<AstTypeValue> for Type {
  type Error = TypeError;

  fn try_from(value: AstTypeValue) -> Result<Self, Self::Error> {
    match value {
      Value::Number(src) => {
        let (value, scale) = src.as_bigint_and_exponent();
        if scale == 0 {
          Ok(Type::Int(Int::new(value)))
        } else {
          Ok(Type::Decimal(Decimal::new(src)))
        }
      }
      Value::SingleQuotedString(value) => Ok(Type::VarChar(VarChar::new(value))),
      ast_type => Err(TypeError::Unsupported(format!("{:?}", ast_type)))
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
      assert_eq!(
        Type::try_from(int(100)),
        Ok(Type::Int(Int::new(BigInt::from(100))))
      )
    }

    #[test]
    fn integer_value_serialize() {
      assert_eq!(
        bincode::serialize(&Int::new(BigInt::from(100))).unwrap(),
        bincode::serialize(&BigInt::from(100)).unwrap()
      )
    }

    #[test]
    fn integer_value_deserialize() {
      assert_eq!(
        Int::new(BigInt::from(100)),
        bincode::deserialize(bincode::serialize(&BigInt::from(100)).unwrap().as_slice()).unwrap()
      )
    }
  }

  #[cfg(test)]
  mod decimal_value {
    use super::*;

    #[test]
    fn decimal_value() {
      assert_eq!(
        Type::try_from(decimal(1000, 1)),
        Ok(Type::Decimal(Decimal::new(BigDecimal::new(BigInt::from(1000), 1))))
      )
    }

    #[test]
    fn decimal_value_serialize() {
      assert_eq!(
        bincode::serialize(&Decimal::new(BigDecimal::new(BigInt::from(1000), 1))).unwrap(),
        bincode::serialize(&BigDecimal::new(BigInt::from(1000), 1)).unwrap()
      )
    }

    // TODO: bincode does not support deserialize_any enable after
    //       https://github.com/akubera/bigdecimal-rs/pull/51 is merged or use fork
    #[ignore]
    #[test]
    fn decimal_value_serialize_deserialize() {
      assert_eq!(
        Decimal::new(BigDecimal::new(BigInt::from(1000), 1)),
        bincode::deserialize(bincode::serialize(&BigDecimal::new(BigInt::from(1000), 1)).unwrap().as_slice()).unwrap()
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
        Ok(Type::VarChar(VarChar::from("string value")))
      )
    }

    #[test]
    fn characters_serialize() {
      assert_eq!(
        bincode::serialize(&VarChar::from("str")).unwrap(),
        bincode::serialize("str".as_bytes()).unwrap()
      )
    }

    #[test]
    fn characters_deserialize() {
      assert_eq!(
        VarChar::from("str"),
        bincode::deserialize(bincode::serialize("str".as_bytes()).unwrap().as_slice()).unwrap()
      )
    }
  }
}
