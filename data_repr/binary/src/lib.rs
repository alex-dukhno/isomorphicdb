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

use ordered_float::OrderedFloat;
use std::fmt::{self, Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub enum BinaryValue {
    Null,
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(OrderedFloat<f32>),
    Float64(OrderedFloat<f64>),
    String(String),
}

impl BinaryValue {
    pub fn null() -> BinaryValue {
        BinaryValue::Null
    }

    pub fn from_bool(val: bool) -> BinaryValue {
        BinaryValue::Bool(val)
    }

    pub fn from_u32(val: u32) -> BinaryValue {
        BinaryValue::from(val as i32)
    }

    pub fn from_u64(val: u64) -> BinaryValue {
        BinaryValue::from(val as i64)
    }

    pub fn from_string(val: String) -> BinaryValue {
        BinaryValue::String(val)
    }

    pub fn as_u32(&self) -> u32 {
        match self {
            Self::Int32(val) => *val as u32,
            _ => panic!("invalid use of Datum::as_u64"),
        }
    }

    pub fn as_u64(&self) -> u64 {
        match self {
            Self::Int64(val) => *val as u64,
            _ => panic!("invalid use of Datum::as_u64"),
        }
    }

    pub fn as_string(&self) -> String {
        match self {
            Self::String(s) => s.clone(),
            _ => panic!("invalid use of Datum::as_str"),
        }
    }
}

impl From<bool> for BinaryValue {
    fn from(value: bool) -> BinaryValue {
        BinaryValue::Bool(value)
    }
}

impl From<i16> for BinaryValue {
    fn from(value: i16) -> BinaryValue {
        BinaryValue::Int16(value)
    }
}

impl From<i32> for BinaryValue {
    fn from(value: i32) -> BinaryValue {
        BinaryValue::Int32(value)
    }
}

impl From<i64> for BinaryValue {
    fn from(value: i64) -> BinaryValue {
        BinaryValue::Int64(value)
    }
}

impl From<f32> for BinaryValue {
    fn from(value: f32) -> BinaryValue {
        BinaryValue::Float32(OrderedFloat::from(value))
    }
}

impl From<f64> for BinaryValue {
    fn from(value: f64) -> BinaryValue {
        BinaryValue::Float64(OrderedFloat::from(value))
    }
}

impl From<String> for BinaryValue {
    fn from(value: String) -> BinaryValue {
        BinaryValue::String(value)
    }
}

impl From<&str> for BinaryValue {
    fn from(value: &str) -> BinaryValue {
        BinaryValue::String(value.to_owned())
    }
}

impl Display for BinaryValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BinaryValue::Null => write!(f, "NULL"),
            BinaryValue::Bool(true) => write!(f, "t"),
            BinaryValue::Bool(false) => write!(f, "f"),
            BinaryValue::Int16(val) => write!(f, "{}", val),
            BinaryValue::Int32(val) => write!(f, "{}", val),
            BinaryValue::Int64(val) => write!(f, "{}", val),
            BinaryValue::Float32(val) => write!(f, "{}", val.into_inner()),
            BinaryValue::Float64(val) => write!(f, "{}", val.into_inner()),
            BinaryValue::String(val) => write!(f, "{}", val),
        }
    }
}

impl PartialEq<&str> for BinaryValue {
    fn eq(&self, other: &&str) -> bool {
        match self {
            BinaryValue::String(this) => this == other,
            _ => false,
        }
    }
}
