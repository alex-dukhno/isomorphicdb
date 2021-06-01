// Copyright 2020 - 2021 Alex Dukhno
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

use bigdecimal::{BigDecimal, ToPrimitive};
use binary::BinaryValue;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use operators::{UnArithmetic, UnOperator};
use ordered_float::OrderedFloat;
use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};
use types::{Bool, ParseBoolError, SqlType, SqlTypeFamilyOld};

#[derive(Debug, PartialEq)]
pub enum OperationError {
    InvalidTextRepresentation { sql_type: SqlType, value: String },
    CanNotCoerce { from: SqlType, to: SqlType },
    UndefinedFunction { sql_type: SqlType, op: UnOperator },
    AmbiguousFunction { sql_type: SqlType, op: UnOperator },
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum ScalarValue {
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Bool(bool),
    Real(OrderedFloat<f32>),
    Double(OrderedFloat<f64>),
    Time(NaiveTime),
    Date(NaiveDate),
    Timestamp(NaiveDateTime),
    TimestampTz(DateTime<Utc>),
    Numeric(BigDecimal),
    Text(String),
    VarChar(String),
    Char(String),
    StringLiteral(String),
    Interval(i64),
    Null(Option<SqlType>),
}

impl ScalarValue {
    pub fn cast_to(self, sql_type: SqlType) -> Result<ScalarValue, OperationError> {
        match self {
            ScalarValue::SmallInt(_) | ScalarValue::BigInt(_) => Err(OperationError::CanNotCoerce {
                from: self.sql_type(),
                to: sql_type,
            }),
            ScalarValue::Integer(value) => Ok(ScalarValue::Bool(value != 0)),
            ScalarValue::Bool(_) => Ok(self),
            ScalarValue::StringLiteral(value) => Bool::from_str(&value)
                .map(|Bool(value)| ScalarValue::Bool(value))
                .map_err(|ParseBoolError(value)| OperationError::InvalidTextRepresentation { sql_type, value }),
            _ => unimplemented!(),
        }
    }

    pub fn negate(self) -> Result<ScalarValue, OperationError> {
        match self {
            ScalarValue::SmallInt(value) => Ok(ScalarValue::SmallInt(-value)),
            ScalarValue::Integer(value) => Ok(ScalarValue::Integer(-value)),
            ScalarValue::BigInt(value) => Ok(ScalarValue::BigInt(-value)),
            ScalarValue::Bool(_) => Err(OperationError::UndefinedFunction {
                sql_type: self.sql_type(),
                op: UnOperator::Arithmetic(UnArithmetic::Neg),
            }),
            ScalarValue::StringLiteral(_) => Err(OperationError::AmbiguousFunction {
                sql_type: SqlType::Unknown,
                op: UnOperator::Arithmetic(UnArithmetic::Neg),
            }),
            _ => unimplemented!(),
        }
    }

    fn sql_type(&self) -> SqlType {
        match self {
            ScalarValue::SmallInt(_) => SqlType::small_int(),
            ScalarValue::Integer(_) => SqlType::integer(),
            ScalarValue::BigInt(_) => SqlType::big_int(),
            ScalarValue::Bool(_) => SqlType::bool(),
            _ => unimplemented!(),
        }
    }
}

impl Display for ScalarValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::SmallInt(value) => write!(f, "{}", value),
            ScalarValue::Integer(value) => write!(f, "{}", value),
            ScalarValue::BigInt(value) => write!(f, "{}", value),
            ScalarValue::Bool(true) => write!(f, "t"),
            ScalarValue::Bool(false) => write!(f, "f"),
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum ScalarValueOld {
    Num { value: BigDecimal, type_family: SqlTypeFamilyOld },
    String(String),
    Bool(bool),
    Null,
}

impl ScalarValueOld {
    pub fn type_family(&self) -> Option<SqlTypeFamilyOld> {
        match self {
            ScalarValueOld::Num { type_family, .. } => Some(*type_family),
            ScalarValueOld::String(_) => Some(SqlTypeFamilyOld::String),
            ScalarValueOld::Bool(_) => Some(SqlTypeFamilyOld::Bool),
            ScalarValueOld::Null => None,
        }
    }

    pub fn as_text(&self) -> String {
        match self {
            ScalarValueOld::Null => "NULL".to_owned(),
            ScalarValueOld::Bool(true) => "t".to_owned(),
            ScalarValueOld::Bool(false) => "f".to_owned(),
            ScalarValueOld::Num { value, .. } => value.to_string(),
            ScalarValueOld::String(val) => val.clone(),
        }
    }

    pub fn convert(self) -> BinaryValue {
        match self {
            ScalarValueOld::Num {
                value,
                type_family: SqlTypeFamilyOld::SmallInt,
            } => BinaryValue::from(value.to_i16().unwrap()),
            ScalarValueOld::Num {
                value,
                type_family: SqlTypeFamilyOld::Integer,
            } => BinaryValue::from(value.to_i32().unwrap()),
            ScalarValueOld::Num {
                value,
                type_family: SqlTypeFamilyOld::Real,
            } => BinaryValue::from(value.to_f32().unwrap()),
            ScalarValueOld::Num {
                value,
                type_family: SqlTypeFamilyOld::Double,
            } => BinaryValue::from(value.to_f64().unwrap()),
            ScalarValueOld::Num {
                value,
                type_family: SqlTypeFamilyOld::BigInt,
            } => BinaryValue::from(value.to_i64().unwrap()),
            ScalarValueOld::String(str) => BinaryValue::from(str),
            ScalarValueOld::Bool(boolean) => BinaryValue::from(boolean),
            ScalarValueOld::Null => BinaryValue::null(),
            _ => unreachable!(),
        }
    }
}

impl From<wire_protocol_payload::Value> for ScalarValueOld {
    fn from(value: wire_protocol_payload::Value) -> ScalarValueOld {
        match value {
            wire_protocol_payload::Value::Null => ScalarValueOld::Null,
            wire_protocol_payload::Value::Bool(value) => ScalarValueOld::Bool(value),
            wire_protocol_payload::Value::Int16(value) => ScalarValueOld::Num {
                value: BigDecimal::from(value),
                type_family: SqlTypeFamilyOld::SmallInt,
            },
            wire_protocol_payload::Value::Int32(value) => ScalarValueOld::Num {
                value: BigDecimal::from(value),
                type_family: SqlTypeFamilyOld::Integer,
            },
            wire_protocol_payload::Value::Int64(value) => ScalarValueOld::Num {
                value: BigDecimal::from(value),
                type_family: SqlTypeFamilyOld::BigInt,
            },
            wire_protocol_payload::Value::String(value) => ScalarValueOld::String(value),
        }
    }
}

impl From<query_ast::Value> for ScalarValueOld {
    fn from(value: query_ast::Value) -> Self {
        match value {
            query_ast::Value::Int(value) => ScalarValueOld::Num {
                value: BigDecimal::from(value),
                type_family: SqlTypeFamilyOld::Integer,
            },
            query_ast::Value::Number(value) => ScalarValueOld::Num {
                value: BigDecimal::from_str(&value).unwrap(),
                type_family: SqlTypeFamilyOld::Double,
            },
            query_ast::Value::String(value) => ScalarValueOld::String(value),
            query_ast::Value::Null => ScalarValueOld::Null,
        }
    }
}

impl Display for ScalarValueOld {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValueOld::Num { value, .. } => write!(f, "{}", value),
            ScalarValueOld::String(value) => write!(f, "{}", value),
            ScalarValueOld::Bool(value) => write!(f, "{}", value),
            ScalarValueOld::Null => write!(f, "NULL"),
        }
    }
}
