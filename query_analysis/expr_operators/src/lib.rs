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

use bigdecimal::BigDecimal;
use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};
use types::{GeneralType, SqlType};

#[derive(Debug, PartialEq)]
pub enum Arithmetic {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Exp,
}

#[derive(Debug, PartialEq)]
pub enum Comparison {
    NotEq,
    Eq,
    LtEq,
    GtEq,
    Lt,
    Gt,
}

#[derive(Debug, PartialEq)]
pub enum Bitwise {
    ShiftRight,
    ShiftLeft,
    Xor,
    And,
    Or,
}

#[derive(Debug, PartialEq)]
pub enum Logical {
    Or,
    And,
}

#[derive(Debug, PartialEq)]
pub enum PatternMatching {
    Like,
    NotLike,
}

#[derive(Debug, PartialEq)]
pub enum StringOp {
    Concat,
}

#[derive(Debug, PartialEq)]
pub enum Operation {
    Arithmetic(Arithmetic),
    Comparison(Comparison),
    Bitwise(Bitwise),
    Logical(Logical),
    PatternMatching(PatternMatching),
    StringOp(StringOp),
}

impl Operation {
    pub fn acceptable_operand_types(&self) -> Vec<(GeneralType, GeneralType)> {
        match self {
            Operation::Logical(_) => vec![(GeneralType::Bool, GeneralType::Bool)],
            Operation::Comparison(_) => vec![
                (GeneralType::Bool, GeneralType::Bool),
                (GeneralType::Number, GeneralType::Number),
                (GeneralType::String, GeneralType::String),
            ],
            Operation::Arithmetic(_) | Operation::Bitwise(_) => vec![(GeneralType::Number, GeneralType::Number)],
            Operation::StringOp(_) | Operation::PatternMatching(_) => vec![(GeneralType::String, GeneralType::String)],
        }
    }

    pub fn result_type(&self) -> GeneralType {
        match self {
            Operation::StringOp(_) => GeneralType::String,
            Operation::Arithmetic(_) | Operation::Bitwise(_) => GeneralType::Number,
            Operation::Comparison(_) | Operation::Logical(_) | Operation::PatternMatching(_) => GeneralType::Bool,
        }
    }

    pub fn validate_operands(&self, left: SqlType, right: SqlType) -> Result<(), OperationError> {
        Ok(())
    }
}

pub struct OperationError;

#[derive(Debug, PartialEq)]
pub enum InsertOperator {
    Const(ScalarValue),
    Param(usize),
}

#[derive(Debug, PartialEq)]
pub enum Operator {
    Const(ScalarValue),
    Param(usize),
    Column { sql_type: SqlType, index: usize },
}

#[derive(PartialEq, Debug, Copy, Clone, Eq)]
pub struct Bool(pub bool);

impl FromStr for Bool {
    type Err = ParseBoolError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let val = s.to_lowercase();
        match val.as_str() {
            "t" | "true" | "on" | "yes" | "y" | "1" => Ok(Bool(true)),
            "f" | "false" | "off" | "no" | "n" | "0" => Ok(Bool(false)),
            _ => Err(ParseBoolError(val)),
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct ParseBoolError(String);

impl Display for ParseBoolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "error to parse {:?} into boolean", self.0)
    }
}

#[derive(Debug, PartialEq)]
pub enum ImplicitCastError {
    StringDataRightTruncation(SqlType),                              // Error code: 22001
    DatatypeMismatch { column_type: SqlType, source_type: SqlType }, // Error code: 42804
    InvalidInputSyntaxForType { sql_type: SqlType, value: String },  // Error code: 22P02
}

impl ImplicitCastError {
    pub fn string_data_right_truncation(sql_type: SqlType) -> ImplicitCastError {
        ImplicitCastError::StringDataRightTruncation(sql_type)
    }

    pub fn datatype_mismatch(column_type: SqlType, source_type: SqlType) -> ImplicitCastError {
        ImplicitCastError::DatatypeMismatch {
            column_type,
            source_type,
        }
    }

    pub fn invalid_input_syntax_for_type<V: ToString>(sql_type: SqlType, value: V) -> ImplicitCastError {
        ImplicitCastError::InvalidInputSyntaxForType {
            sql_type,
            value: value.to_string(),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Eq)]
pub enum ScalarValue {
    String(String),
    Number(BigDecimal),
    Bool(Bool),
    Null,
}

impl ScalarValue {
    pub fn implicit_cast_to(&self, target_type: SqlType) -> Result<ScalarValue, ImplicitCastError> {
        match self {
            ScalarValue::Bool(boolean) => match target_type {
                SqlType::Bool => Ok(ScalarValue::Bool(*boolean)),
                SqlType::Char(len) | SqlType::VarChar(len) => {
                    let r = boolean.0.to_string();
                    if r.len() as u64 > len {
                        Err(ImplicitCastError::string_data_right_truncation(target_type))
                    } else {
                        Ok(ScalarValue::String(r))
                    }
                }
                SqlType::SmallInt | SqlType::Integer | SqlType::BigInt | SqlType::Real | SqlType::DoublePrecision => {
                    Err(ImplicitCastError::datatype_mismatch(target_type, SqlType::Bool))
                }
            },
            ScalarValue::String(string) => match target_type {
                SqlType::Bool => match Bool::from_str(&string) {
                    Ok(bool) => Ok(ScalarValue::Bool(bool)),
                    Err(parse_error) => {
                        log::debug!("Could not cast {:?} to bool due to {:?}", string, parse_error);
                        Err(ImplicitCastError::invalid_input_syntax_for_type(target_type, string))
                    }
                },
                SqlType::Char(_) | SqlType::VarChar(_) => Ok(ScalarValue::String(string.clone())),
                SqlType::SmallInt | SqlType::Integer | SqlType::BigInt | SqlType::Real | SqlType::DoublePrecision => {
                    match BigDecimal::from_str(&string) {
                        Ok(num) => Ok(ScalarValue::Number(num)),
                        Err(parse_error) => {
                            log::debug!("Could not cast {:?} to bool due to {:?}", string, parse_error);
                            Err(ImplicitCastError::invalid_input_syntax_for_type(target_type, string))
                        }
                    }
                }
            },
            ScalarValue::Number(num) => match target_type {
                SqlType::Bool => {
                    if num.is_integer() {
                        if &BigDecimal::from(i32::MIN) <= num && num <= &BigDecimal::from(i32::MAX) {
                            Err(ImplicitCastError::datatype_mismatch(target_type, SqlType::Integer))
                        } else if &BigDecimal::from(i64::MIN) <= num && num <= &BigDecimal::from(i64::MAX) {
                            Err(ImplicitCastError::datatype_mismatch(target_type, SqlType::BigInt))
                        } else {
                            unimplemented!("NUMERIC types are not implemented")
                        }
                    } else {
                        if &BigDecimal::from_str(&f32::MIN.to_string()).unwrap() <= num
                            && num <= &BigDecimal::from_str(&f32::MAX.to_string()).unwrap()
                        {
                            Err(ImplicitCastError::datatype_mismatch(target_type, SqlType::Real))
                        } else if &BigDecimal::from_str(&f64::MIN.to_string()).unwrap() <= num
                            && num <= &BigDecimal::from_str(&f64::MAX.to_string()).unwrap()
                        {
                            Err(ImplicitCastError::datatype_mismatch(
                                target_type,
                                SqlType::DoublePrecision,
                            ))
                        } else {
                            unimplemented!("NUMERIC types are not implemented")
                        }
                    }
                }
                SqlType::Char(len) | SqlType::VarChar(len) => {
                    let r = num.to_string();
                    if r.len() as u64 <= len {
                        Ok(ScalarValue::String(r))
                    } else {
                        Err(ImplicitCastError::string_data_right_truncation(target_type))
                    }
                }
                SqlType::SmallInt | SqlType::Integer | SqlType::BigInt | SqlType::Real | SqlType::DoublePrecision => {
                    Ok(ScalarValue::Number(num.clone()))
                }
            },
            ScalarValue::Null => Ok(ScalarValue::Null),
        }
    }

    // when user does `<value>::<sql_type>` or `cast <value> as <sql_type>` operations
    // pub fn strict_cast_to(self, target_type: &SqlType) -> Result<ScalarValue, CastError> {
    //     Err(CastError)
    // }
}

// TODO it makes `ScalarValue` implement `ToString`
//      find a better abstraction to return
//      text representation of computed value
impl Display for ScalarValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::String(s) => write!(f, "{}", s),
            ScalarValue::Number(n) => write!(f, "{}", n),
            ScalarValue::Bool(Bool(true)) => write!(f, "t"),
            ScalarValue::Bool(Bool(false)) => write!(f, "f"),
            ScalarValue::Null => write!(f, "NULL"),
        }
    }
}

#[cfg(test)]
mod tests;
