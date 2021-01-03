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

use ordered_float::OrderedFloat;
use std::fmt::{self, Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub enum Datum<'a> {
    Null,
    True,
    False,
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(OrderedFloat<f32>),
    Float64(OrderedFloat<f64>),
    String(&'a str),
    OwnedString(String),
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
            Self::Float32(_) => 1 + std::mem::size_of::<f32>(),
            Self::Float64(_) => 1 + std::mem::size_of::<f64>(),
            Self::String(val) => 1 + std::mem::size_of::<usize>() + val.len(),
            Self::OwnedString(val) => 1 + std::mem::size_of::<usize>() + val.len(),
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

    pub fn from_u32(val: u32) -> Datum<'static> {
        Datum::from_i32(val as i32)
    }

    pub const fn from_i64(val: i64) -> Datum<'static> {
        Datum::Int64(val)
    }

    pub const fn from_u64(val: u64) -> Datum<'static> {
        Datum::from_i64(val as i64)
    }

    pub fn from_f32(val: f32) -> Datum<'static> {
        Datum::Float32(OrderedFloat(val))
    }

    pub fn from_f64(val: f64) -> Datum<'static> {
        Datum::Float64(OrderedFloat(val))
    }

    #[allow(clippy::should_implement_trait)]
    pub const fn from_str(val: &'a str) -> Datum<'a> {
        Datum::String(val)
    }

    pub fn from_string(val: String) -> Datum<'static> {
        Datum::OwnedString(val)
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

    pub fn as_str(&self) -> &str {
        match self {
            Self::String(s) => s,
            _ => panic!("invalid use of Datum::as_str"),
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
            Self::Float32(val) => write!(f, "{}", val.into_inner()),
            Self::Float64(val) => write!(f, "{}", val.into_inner()),
            Self::String(val) => write!(f, "{}", val),
            Self::OwnedString(val) => write!(f, "{}", val),
        }
    }
}
