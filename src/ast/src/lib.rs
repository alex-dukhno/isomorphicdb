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

pub mod operations;
pub mod values;

#[derive(Debug, PartialEq)]
pub struct NotHandled(Expr);

impl Display for NotHandled {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "not handled Expression [{}]", self.0)
    }
}

#[derive(Debug, PartialEq)]
pub struct OperationError(NotSupportedOperation);

impl Display for OperationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "operation '{}' not supported", self.0)
    }
}

#[derive(Debug, PartialEq)]
pub enum NotSupportedOperation {
    ExplicitCast(Value, DataType),
    ImplicitCast(ScalarValue, SqlType),
    Minus,
    Plus,
    Not,
}

impl Display for NotSupportedOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            NotSupportedOperation::ExplicitCast(val, scalar_type) => {
                write!(f, "explicit casting value {} to {} type", val, scalar_type)
            }
            NotSupportedOperation::ImplicitCast(val, sql_type) => {
                write!(f, "implicit casting value {} to {} type", val, sql_type)
            }
            NotSupportedOperation::Minus => write!(f, "unary minus"),
            NotSupportedOperation::Plus => write!(f, "unary plus"),
            NotSupportedOperation::Not => write!(f, "logical not"),
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
            _ => panic!("invalid use of Datum::as_f64"),
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
                    if let Some(val) = val.to_i16() {
                        Ok(Datum::from_i16(val))
                    } else if let Some(val) = val.to_i32() {
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

impl Display for Datum<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::True => write!(f, "t"),
            Self::False => write!(f, "f"),
            Self::Int16(val) => write!(f, "{}", val),
            Self::Int32(val) => write!(f, "{}", val),
            Self::Int64(val) => write!(f, "{}", val),
            Self::UInt64(val) => write!(f, "{}", val),
            Self::Float32(val) => write!(f, "{}", val.into_inner()),
            Self::Float64(val) => write!(f, "{}", val.into_inner()),
            Self::String(val) => write!(f, "{}", val),
            Self::OwnedString(val) => write!(f, "{}", val),
            Self::SqlType(val) => write!(f, "{}", val),
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
