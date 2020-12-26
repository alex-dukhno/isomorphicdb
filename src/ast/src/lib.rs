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

use crate::values::{Bool, ScalarValue};
use bigdecimal::BigDecimal;
use repr::Datum;
use sqlparser::ast::{DataType, Expr, Value};
use std::{
    convert::{From, TryFrom, TryInto},
    fmt::{self, Display, Formatter},
};
use types::SqlType;

pub mod operations;
pub mod predicates;
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

impl<'a> TryInto<ScalarValue> for &Datum<'a> {
    type Error = ();

    fn try_into(self) -> Result<ScalarValue, Self::Error> {
        match self {
            Datum::Null => Ok(ScalarValue::Null),
            Datum::True => Ok(ScalarValue::Bool(Bool(true))),
            Datum::False => Ok(ScalarValue::Bool(Bool(false))),
            Datum::Int16(num) => Ok(ScalarValue::Number(BigDecimal::from(*num))),
            Datum::Int32(num) => Ok(ScalarValue::Number(BigDecimal::from(*num))),
            Datum::Int64(num) => Ok(ScalarValue::Number(BigDecimal::from(*num))),
            Datum::Float32(num) => Ok(ScalarValue::Number(BigDecimal::try_from(**num).unwrap())),
            Datum::Float64(num) => Ok(ScalarValue::Number(BigDecimal::try_from(**num).unwrap())),
            Datum::String(str) => Ok(ScalarValue::String(str.to_string())),
            Datum::OwnedString(str) => Ok(ScalarValue::String(str.to_owned())),
        }
    }
}
