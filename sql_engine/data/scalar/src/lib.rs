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
use data_binary::repr::{Datum, ToDatum};
use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};
use types::SqlTypeFamily;

#[derive(Debug, PartialEq, Clone)]
pub enum ScalarValue {
    Num {
        value: BigDecimal,
        type_family: SqlTypeFamily,
    },
    String(String),
    Bool(bool),
    Null,
}

impl ScalarValue {
    pub fn type_family(&self) -> Option<SqlTypeFamily> {
        match self {
            ScalarValue::Num { type_family, .. } => Some(*type_family),
            ScalarValue::String(_) => Some(SqlTypeFamily::String),
            ScalarValue::Bool(_) => Some(SqlTypeFamily::Bool),
            ScalarValue::Null => None,
        }
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn as_to_datum(self) -> Box<dyn ToDatum> {
        Box::new(self)
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn as_text(self) -> String {
        match self {
            ScalarValue::Null => "NULL".to_owned(),
            ScalarValue::Bool(true) => "t".to_owned(),
            ScalarValue::Bool(false) => "f".to_owned(),
            ScalarValue::Num { value, .. } => value.to_string(),
            ScalarValue::String(val) => val,
        }
    }
}

impl From<pg_wire::Value> for ScalarValue {
    fn from(value: pg_wire::Value) -> ScalarValue {
        match value {
            pg_wire::Value::Null => ScalarValue::Null,
            pg_wire::Value::Bool(value) => ScalarValue::Bool(value),
            pg_wire::Value::Int16(value) => ScalarValue::Num {
                value: BigDecimal::from(value),
                type_family: SqlTypeFamily::SmallInt,
            },
            pg_wire::Value::Int32(value) => ScalarValue::Num {
                value: BigDecimal::from(value),
                type_family: SqlTypeFamily::Integer,
            },
            pg_wire::Value::Int64(value) => ScalarValue::Num {
                value: BigDecimal::from(value),
                type_family: SqlTypeFamily::BigInt,
            },
            pg_wire::Value::String(value) => ScalarValue::String(value),
        }
    }
}

impl From<query_ast::Value> for ScalarValue {
    fn from(value: query_ast::Value) -> Self {
        match value {
            query_ast::Value::Int(value) => ScalarValue::Num {
                value: BigDecimal::from(value),
                type_family: SqlTypeFamily::Integer,
            },
            query_ast::Value::Number(value) => ScalarValue::Num {
                value: BigDecimal::from_str(&value).unwrap(),
                type_family: SqlTypeFamily::Double,
            },
            query_ast::Value::String(value) => ScalarValue::String(value),
            query_ast::Value::Boolean(value) => ScalarValue::Bool(value),
            query_ast::Value::Null => ScalarValue::Null,
        }
    }
}

impl Display for ScalarValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::Num { value, .. } => write!(f, "{}", value),
            ScalarValue::String(value) => write!(f, "{}", value),
            ScalarValue::Bool(value) => write!(f, "{}", value),
            ScalarValue::Null => write!(f, "NULL"),
        }
    }
}

impl ToDatum for ScalarValue {
    fn convert(&self) -> Datum {
        match self {
            ScalarValue::Num {
                value,
                type_family: SqlTypeFamily::SmallInt,
            } => Datum::from_i16(value.to_i16().unwrap()),
            ScalarValue::Num {
                value,
                type_family: SqlTypeFamily::Integer,
            } => Datum::from_i32(value.to_i32().unwrap()),
            ScalarValue::Num {
                value,
                type_family: SqlTypeFamily::Real,
            } => Datum::from_f32(value.to_f32().unwrap()),
            ScalarValue::Num {
                value,
                type_family: SqlTypeFamily::Double,
            } => Datum::from_f64(value.to_f64().unwrap()),
            ScalarValue::Num {
                value,
                type_family: SqlTypeFamily::BigInt,
            } => Datum::from_i64(value.to_i64().unwrap()),
            ScalarValue::String(str) => Datum::from_string(str.clone()),
            ScalarValue::Bool(boolean) => Datum::from_bool(*boolean),
            ScalarValue::Null => Datum::from_null(),
            _ => unreachable!(),
        }
    }
}
