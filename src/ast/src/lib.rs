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

use std::{
    convert::TryFrom,
    ops::{Add, BitAnd, BitOr, Div, Mul, Rem, Shl, Shr, Sub},
};

use crate::values::{Bool, ScalarValue};
use bigdecimal::ToPrimitive;
use ordered_float::OrderedFloat;
use sql_model::sql_types::SqlType;
use sqlparser::ast::{DataType, Expr, Value};
use std::fmt::{self, Display, Formatter};

pub mod scalar;
pub mod values;

#[derive(Debug, PartialEq)]
pub enum ScalarError {
    NotSupportedType(DataType),
    NotSupportedValue(Value),
    NotHandled(Expr),
}

impl Display for ScalarError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ScalarError::NotSupportedType(data_type) => write!(f, "not supported type '{}'", data_type),
            ScalarError::NotSupportedValue(value) => write!(f, "not supported value '{}'", value),
            ScalarError::NotHandled(expr) => write!(f, "not handled Expression [{}]", expr),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct OperationError(Operation, Option<String>);

impl Display for OperationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "operation '{}' not supported", self.0)
    }
}

#[derive(Debug, PartialEq)]
pub enum Operation {
    Cast(Value, ScalarType),
    Minus,
    Plus,
    Not,
}

impl Display for Operation {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Operation::Cast(val, scalar_type) => write!(f, "casting value {} to {} type", val, scalar_type),
            Operation::Minus => write!(f, "unary minus"),
            Operation::Plus => write!(f, "unary plus"),
            Operation::Not => write!(f, "logical not"),
        }
    }
}

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
pub enum ScalarType {
    Int16,
    Int32,
    Int64,
    UInt64,
    Float32,
    Float64,
    Boolean,
    String,
}

impl TryFrom<&DataType> for ScalarType {
    type Error = ();

    fn try_from(value: &DataType) -> Result<Self, Self::Error> {
        match value {
            DataType::Char(_) => Ok(ScalarType::String),
            DataType::Varchar(_) => Ok(ScalarType::String),
            DataType::Uuid => Err(()),
            DataType::Clob(_) => Err(()),
            DataType::Binary(_) => Err(()),
            DataType::Varbinary(_) => Err(()),
            DataType::Blob(_) => Err(()),
            DataType::Decimal(_, _) => Err(()),
            DataType::Float(_) => Err(()),
            DataType::SmallInt => Ok(ScalarType::Int16),
            DataType::Int => Ok(ScalarType::Int32),
            DataType::BigInt => Ok(ScalarType::Int64),
            DataType::Real => Ok(ScalarType::Float32),
            DataType::Double => Ok(ScalarType::Float64),
            DataType::Boolean => Ok(ScalarType::Boolean),
            DataType::Date => Err(()),
            DataType::Time => Err(()),
            DataType::Timestamp => Err(()),
            DataType::Interval => Err(()),
            DataType::Regclass => Err(()),
            DataType::Text => Err(()),
            DataType::Bytea => Err(()),
            DataType::Custom(_) => Err(()),
            DataType::Array(_) => Err(()),
        }
    }
}

impl ScalarType {
    pub fn is_integer(&self) -> bool {
        match self {
            Self::Int64 | Self::Int32 | Self::Int16 => true,
            _ => false,
        }
    }

    pub fn is_float(&self) -> bool {
        match self {
            Self::Float64 | Self::Float32 => true,
            _ => false,
        }
    }

    pub fn is_string(&self) -> bool {
        *self == Self::String
    }

    pub fn is_boolean(&self) -> bool {
        *self == Self::Boolean
    }
}

impl Display for ScalarType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int16 => write!(f, "Int16"),
            Self::Int32 => write!(f, "Int32"),
            Self::Int64 => write!(f, "Int64"),
            Self::UInt64 => write!(f, "UInt64"),
            Self::Float32 => write!(f, "Float32"),
            Self::Float64 => write!(f, "Float64"),
            Self::Boolean => write!(f, "Bool"),
            Self::String => write!(f, "String"),
        }
    }
}

/// value shared by the row.
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub enum Datum<'a> {
    Null,
    True,
    False,
    Int16(i16),
    Int32(i32),
    Int64(i64),
    // should u16, u32 be implemented here?
    UInt64(u64),
    Float32(OrderedFloat<f32>),
    Float64(OrderedFloat<f64>),
    String(&'a str),
    // this should only be used when loading string into a database
    OwnedString(String),
    // Bytes(&'a [u8]),
    SqlType(SqlType),
    // fill in the rest of the types as they get implemented.
}

impl<'a> Datum<'a> {
    pub fn size(&self) -> usize {
        match self {
            Self::Null => 1,
            Self::True => std::mem::size_of::<u8>(),
            Self::False => std::mem::size_of::<u8>(),
            Self::Int16(_) => 1 + std::mem::size_of::<i16>(),
            Self::Int32(_) => 1 + std::mem::size_of::<i32>(),
            Self::Int64(_) => 1 + std::mem::size_of::<i64>(),
            Self::UInt64(_) => 1 + std::mem::size_of::<u64>(),
            Self::Float32(_) => 1 + std::mem::size_of::<f32>(),
            Self::Float64(_) => 1 + std::mem::size_of::<f64>(),
            Self::String(val) => 1 + std::mem::size_of::<usize>() + val.len(),
            Self::OwnedString(val) => 1 + std::mem::size_of::<usize>() + val.len(),
            Self::SqlType(_) => 1 + std::mem::size_of::<SqlType>(),
        }
    }

    pub fn from_null() -> Datum<'static> {
        Datum::Null
    }

    pub fn from_bool(val: bool) -> Datum<'static> {
        if val {
            Datum::True
        } else {
            Datum::False
        }
    }

    pub fn from_i16(val: i16) -> Datum<'static> {
        Datum::Int16(val)
    }

    pub fn from_i32(val: i32) -> Datum<'static> {
        Datum::Int32(val)
    }

    pub fn from_i64(val: i64) -> Datum<'static> {
        Datum::Int64(val)
    }

    pub fn from_u64(val: u64) -> Datum<'static> {
        Datum::UInt64(val)
    }

    pub fn from_f32(val: f32) -> Datum<'static> {
        Datum::Float32(val.into())
    }

    pub fn from_f64(val: f64) -> Datum<'static> {
        Datum::Float64(val.into())
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(val: &'a str) -> Datum<'a> {
        Datum::String(val)
    }

    pub fn from_string(val: String) -> Datum<'static> {
        Datum::OwnedString(val)
    }

    pub fn from_sql_type(val: SqlType) -> Datum<'static> {
        Datum::SqlType(val)
    }

    pub fn scalar_type(&self) -> Option<ScalarType> {
        match self {
            Datum::Null => None,
            Datum::True | Datum::False => Some(ScalarType::Boolean),
            Datum::Int16(_) => Some(ScalarType::Int16),
            Datum::Int32(_) => Some(ScalarType::Int32),
            Datum::Int64(_) => Some(ScalarType::Int64),
            Datum::Float32(_) => Some(ScalarType::Float32),
            Datum::Float64(_) => Some(ScalarType::Float64),
            Datum::String(_) | Datum::OwnedString(_) => Some(ScalarType::String),
            Datum::UInt64(_) => Some(ScalarType::UInt64),
            _ => None,
        }
    }

    // @TODO: Add accessor helper functions.
    pub fn as_i16(&self) -> i16 {
        match self {
            Self::Int16(val) => *val,
            _ => panic!("invalid use of Datum::as_i16"),
        }
    }

    pub fn as_i32(&self) -> i32 {
        match self {
            Self::Int32(val) => *val,
            _ => panic!("invalid use of Datum::as_i32"),
        }
    }

    pub fn as_i64(&self) -> i64 {
        match self {
            Self::Int64(val) => *val,
            _ => panic!("invalid use of Datum::as_i64"),
        }
    }

    pub fn as_u64(&self) -> u64 {
        match self {
            Self::UInt64(val) => *val,
            _ => panic!("invalid use of Datum::as_u64"),
        }
    }

    pub fn as_f32(&self) -> f32 {
        match self {
            Self::Float32(val) => **val,
            _ => panic!("invalid use of Datum::as_f32"),
        }
    }

    pub fn as_f64(&self) -> f64 {
        match self {
            Self::Float64(val) => **val,
            _ => panic!("invlaid use of Datum::as_f64"),
        }
    }

    pub fn as_bool(&self) -> bool {
        match self {
            Self::True => true,
            Self::False => false,
            _ => panic!("invalid use of Datum::as_bool"),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::String(s) => s,
            _ => panic!("invalid use of Datum::as_str"),
        }
    }

    pub fn as_string(&self) -> &str {
        match self {
            Self::OwnedString(s) => s,
            _ => panic!("invalid use of Datum::as_string"),
        }
    }

    pub fn as_sql_type(&self) -> SqlType {
        match self {
            Self::SqlType(sql_type) => *sql_type,
            _ => panic!("invalid use of Datum::as_sql_type"),
        }
    }

    pub fn is_integer(&self) -> bool {
        match self {
            Self::Int16(_) | Self::Int32(_) | Self::Int64(_) => true,
            _ => false,
        }
    }

    pub fn is_float(&self) -> bool {
        match self {
            Self::Float32(_) | Self::Float64(_) => true,
            _ => false,
        }
    }

    pub fn is_string(&self) -> bool {
        match self {
            Self::String(_) | Self::OwnedString(_) => true,
            _ => false,
        }
    }

    pub fn is_boolean(&self) -> bool {
        match self {
            Self::True | Self::False => true,
            _ => false,
        }
    }

    pub fn is_null(&self) -> bool {
        if let Self::Null = self {
            true
        } else {
            false
        }
    }

    pub fn is_type(&self) -> bool {
        if let Self::SqlType(_) = self {
            true
        } else {
            false
        }
    }

    // arithmetic operations
}

#[derive(Debug, Clone)]
pub enum EvalError {
    UnsupportedDatum(String),
    OutOfRangeNumeric(SqlType),
    UnsupportedOperation,
}

impl<'a> TryFrom<&ScalarValue> for Datum<'a> {
    type Error = EvalError;

    fn try_from(other: &ScalarValue) -> Result<Self, EvalError> {
        log::debug!("Datum::try_from({:?})", other);
        match other {
            ScalarValue::Number(val) => {
                // there has to be a better way of doing this.
                if val.is_integer() {
                    if let Some(val) = val.to_i32() {
                        Ok(Datum::from_i32(val))
                    } else if let Some(val) = val.to_i64() {
                        Ok(Datum::from_i64(val))
                    } else {
                        Err(EvalError::OutOfRangeNumeric(SqlType::Integer(i32::min_value())))
                    }
                } else if let Some(val) = val.to_f32() {
                    Ok(Datum::from_f32(val))
                } else if let Some(val) = val.to_f64() {
                    Ok(Datum::from_f64(val))
                } else {
                    Err(EvalError::OutOfRangeNumeric(SqlType::DoublePrecision))
                }
            }
            ScalarValue::String(value) => Ok(Datum::from_string(value.trim().to_owned())),
            ScalarValue::Bool(Bool(val)) => Ok(Datum::from_bool(*val)),
            ScalarValue::Null => Ok(Datum::from_null()),
        }
    }
}

impl<'a> TryFrom<&Value> for Datum<'a> {
    type Error = EvalError;

    fn try_from(other: &Value) -> Result<Self, EvalError> {
        log::debug!("Datum::try_from({})", other);
        match other {
            Value::Number(val) => {
                // there has to be a better way of doing this.
                if val.is_integer() {
                    if let Some(val) = val.to_i32() {
                        Ok(Datum::from_i32(val))
                    } else if let Some(val) = val.to_i64() {
                        Ok(Datum::from_i64(val))
                    } else {
                        Err(EvalError::OutOfRangeNumeric(SqlType::Integer(i32::min_value())))
                    }
                } else if let Some(val) = val.to_f32() {
                    Ok(Datum::from_f32(val))
                } else if let Some(val) = val.to_f64() {
                    Ok(Datum::from_f64(val))
                } else {
                    Err(EvalError::OutOfRangeNumeric(SqlType::DoublePrecision))
                }
            }
            Value::SingleQuotedString(value) => Ok(Datum::from_string(value.trim().to_owned())),
            Value::HexStringLiteral(value) => match i64::from_str_radix(value.as_str(), 16) {
                Ok(val) => Ok(Datum::from_i64(val)),
                Err(_) => panic!("Failed to parse hex string"),
            },
            Value::Boolean(val) => Ok(Datum::from_bool(*val)),
            Value::Null => Ok(Datum::from_null()),
            Value::Interval { .. } => Err(EvalError::UnsupportedDatum("Interval".to_string())),
            Value::NationalStringLiteral(_value) => {
                Err(EvalError::UnsupportedDatum("NationalStringLiteral".to_string()))
            }
        }
    }
}

impl ToString for Datum<'_> {
    fn to_string(&self) -> String {
        match self {
            Self::Null => "NULL".to_string(),
            Self::True => "t".to_string(),
            Self::False => "f".to_string(),
            Self::Int16(val) => val.to_string(),
            Self::Int32(val) => val.to_string(),
            Self::Int64(val) => val.to_string(),
            Self::UInt64(val) => val.to_string(),
            Self::Float32(val) => val.into_inner().to_string(),
            Self::Float64(val) => val.into_inner().to_string(),
            Self::String(val) => val.to_string(),
            Self::OwnedString(val) => val.clone(),
            Self::SqlType(val) => val.to_string(),
        }
    }
}

macro_rules! impl_op_integral {
    ($op:tt, $lhs:expr, $rhs:expr) => {
        match ($lhs, $rhs) {
            (Datum::Int16(lhs), Datum::Int16(rhs)) => Datum::Int16(lhs $op rhs),
            (Datum::Int32(lhs), Datum::Int32(rhs)) => Datum::Int32(lhs $op rhs),
            (Datum::Int64(lhs), Datum::Int64(rhs)) => Datum::Int64(lhs $op rhs),
            (Datum::UInt64(lhs), Datum::UInt64(rhs)) => Datum::UInt64(lhs $op rhs),
            (_, _) => panic!("{} can not be used for no arithmetic types", stringify!($op)),
        }
    }
}

macro_rules! impl_op {
    ($op:tt, $lhs:expr, $rhs:expr) => {
        match ($lhs, $rhs) {
            (Datum::Int16(lhs), Datum::Int16(rhs)) => Datum::Int16(lhs $op rhs),
            (Datum::Int32(lhs), Datum::Int32(rhs)) => Datum::Int32(lhs $op rhs),
            (Datum::Int64(lhs), Datum::Int64(rhs)) => Datum::Int64(lhs $op rhs),

            (Datum::UInt64(lhs), Datum::UInt64(rhs)) => Datum::UInt64(lhs $op rhs),

            (Datum::Float32(lhs), Datum::Float32(rhs)) => Datum::Float32(lhs $op rhs),
            (Datum::Float64(lhs), Datum::Float64(rhs)) => Datum::Float64(lhs $op rhs),
            (_, _) => panic!("{} can not be used for no arithmetic types", stringify!($op)),
        }
    }
}

macro_rules! impl_trait_integral {
    ($name:ident, $method:ident, $op:tt) => {
        impl<'a> $name<Self> for Datum<'a> {
            type Output = Self;

            fn $method(self, rhs: Datum<'a>) -> Self::Output {
                impl_op_integral!($op, self, rhs)
            }
        }
    };
}

macro_rules! impl_trait {
    ($name:ident, $method:ident, $op:tt) => {
        impl<'a> $name<Self> for Datum<'a> {
            type Output = Self;

            fn $method(self, rhs: Datum<'a>) -> Self::Output {
                impl_op!($op, self, rhs)
            }
        }
    };
}

impl_trait!(Add, add, +);
impl_trait!(Sub, sub, -);
impl_trait!(Div, div, /);
impl_trait!(Mul, mul, *);

impl_trait_integral!(BitAnd, bitand, &);
impl_trait_integral!(BitOr, bitor, |);
impl_trait_integral!(Rem, rem, %);

impl_trait_integral!(Shl, shl, <<);
impl_trait_integral!(Shr, shr, >>);
