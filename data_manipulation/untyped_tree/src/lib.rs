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
use data_manipulation_operators::{BiOperation, UnOperation};
use std::{
    fmt,
    fmt::{Display, Formatter},
    str::FromStr,
};
use types::{SqlType, SqlTypeFamily};

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

#[derive(Debug, PartialEq)]
pub enum StaticUntypedItem {
    Const(UntypedValue),
    Param(usize),
}

#[derive(Debug, PartialEq)]
pub enum DynamicUntypedItem {
    Const(UntypedValue),
    Param(usize),
    Column {
        name: String,
        sql_type: SqlType,
        index: usize,
    },
}

#[derive(Debug, PartialEq, Clone, Eq)]
pub enum UntypedValue {
    String(String),
    Number(BigDecimal),
    Bool(Bool),
    Null,
}

impl UntypedValue {
    pub fn kind(&self) -> Option<SqlTypeFamily> {
        match self {
            UntypedValue::String(_) => Some(SqlTypeFamily::String),
            UntypedValue::Number(num) if num.is_integer() => Some(SqlTypeFamily::Integer),
            UntypedValue::Number(_) => Some(SqlTypeFamily::Real),
            UntypedValue::Bool(_) => Some(SqlTypeFamily::Bool),
            UntypedValue::Null => None,
        }
    }

    pub fn implicit_cast_to(&self, target_type: SqlType) -> Result<UntypedValue, ImplicitCastError> {
        match self {
            UntypedValue::Bool(boolean) => match target_type {
                SqlType::Bool => Ok(UntypedValue::Bool(*boolean)),
                SqlType::Str { len, .. } => {
                    let r = boolean.0.to_string();
                    if r.len() as u64 > len {
                        Err(ImplicitCastError::string_data_right_truncation(target_type))
                    } else {
                        Ok(UntypedValue::String(r))
                    }
                }
                SqlType::Num(_) => Err(ImplicitCastError::datatype_mismatch(target_type, SqlType::bool())),
            },
            UntypedValue::String(string) => match target_type {
                SqlType::Bool => match Bool::from_str(&string) {
                    Ok(bool) => Ok(UntypedValue::Bool(bool)),
                    Err(parse_error) => {
                        log::debug!("Could not cast {:?} to bool due to {:?}", string, parse_error);
                        Err(ImplicitCastError::invalid_input_syntax_for_type(target_type, string))
                    }
                },
                SqlType::Str { .. } => Ok(UntypedValue::String(string.clone())),
                SqlType::Num(_) => match BigDecimal::from_str(&string) {
                    Ok(num) => Ok(UntypedValue::Number(num)),
                    Err(parse_error) => {
                        log::debug!("Could not cast {:?} to bool due to {:?}", string, parse_error);
                        Err(ImplicitCastError::invalid_input_syntax_for_type(target_type, string))
                    }
                },
            },
            UntypedValue::Number(num) => match target_type {
                SqlType::Bool => {
                    if num.is_integer() {
                        if &BigDecimal::from(i32::MIN) <= num && num <= &BigDecimal::from(i32::MAX) {
                            Err(ImplicitCastError::datatype_mismatch(target_type, SqlType::integer()))
                        } else if &BigDecimal::from(i64::MIN) <= num && num <= &BigDecimal::from(i64::MAX) {
                            Err(ImplicitCastError::datatype_mismatch(target_type, SqlType::big_int()))
                        } else {
                            unimplemented!("NUMERIC types are not implemented")
                        }
                    } else if &BigDecimal::from_str(&f32::MIN.to_string()).unwrap() <= num
                        && num <= &BigDecimal::from_str(&f32::MAX.to_string()).unwrap()
                    {
                        Err(ImplicitCastError::datatype_mismatch(target_type, SqlType::real()))
                    } else if &BigDecimal::from_str(&f64::MIN.to_string()).unwrap() <= num
                        && num <= &BigDecimal::from_str(&f64::MAX.to_string()).unwrap()
                    {
                        Err(ImplicitCastError::datatype_mismatch(
                            target_type,
                            SqlType::double_precision(),
                        ))
                    } else {
                        unimplemented!("NUMERIC types are not implemented")
                    }
                }
                SqlType::Str { len, .. } => {
                    let r = num.to_string();
                    if r.len() as u64 <= len {
                        Ok(UntypedValue::String(r))
                    } else {
                        Err(ImplicitCastError::string_data_right_truncation(target_type))
                    }
                }
                SqlType::Num(_) => Ok(UntypedValue::Number(num.clone())),
            },
            UntypedValue::Null => Ok(UntypedValue::Null),
        }
    }
}

impl Display for UntypedValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            UntypedValue::String(s) => write!(f, "{}", s),
            UntypedValue::Number(n) => write!(f, "{}", n),
            UntypedValue::Bool(Bool(true)) => write!(f, "t"),
            UntypedValue::Bool(Bool(false)) => write!(f, "f"),
            UntypedValue::Null => write!(f, "NULL"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum StaticUntypedTree {
    UnOp {
        op: UnOperation,
        item: Box<StaticUntypedTree>,
    },
    BiOp {
        left: Box<StaticUntypedTree>,
        op: BiOperation,
        right: Box<StaticUntypedTree>,
    },
    Item(StaticUntypedItem),
}

impl StaticUntypedTree {
    pub fn kind(&self) -> Option<SqlTypeFamily> {
        match self {
            StaticUntypedTree::UnOp { .. } => None,
            StaticUntypedTree::BiOp { .. } => None,
            StaticUntypedTree::Item(StaticUntypedItem::Const(value)) => value.kind(),
            StaticUntypedTree::Item(StaticUntypedItem::Param(_)) => None,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum DynamicUntypedTree {
    Operation {
        left: Box<DynamicUntypedTree>,
        op: BiOperation,
        right: Box<DynamicUntypedTree>,
    },
    Item(DynamicUntypedItem),
}

#[cfg(test)]
mod tests;
