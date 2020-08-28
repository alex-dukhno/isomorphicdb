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

///! Runtime cell and row representation.
use bigdecimal::ToPrimitive;
use ordered_float::OrderedFloat;
use sql_types::SqlType;
use sqlparser::ast::Value;
use std::convert::TryFrom;

/// value shared by the row.
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub enum Datum<'a> {
    Null,
    True,
    False,
    Int16(i16),
    Int32(i32),
    Int64(i64),
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
}

#[derive(Debug, Clone)]
pub enum EvalError {
    InvalidExpressionInStaticContext,
    UnsupportedDatum(String),
    OutOfRangeNumeric,
    UnsupportedOperation,
}

impl<'a> TryFrom<&Value> for Datum<'a> {
    type Error = EvalError;

    fn try_from(other: &Value) -> Result<Self, EvalError> {
        match other {
            Value::Number(val) => {
                if val.is_integer() {
                    if let Some(val) = val.to_i32() {
                        Ok(Datum::from_i32(val))
                    } else if let Some(val) = val.to_i64() {
                        Ok(Datum::from_i64(val))
                    } else {
                        Err(EvalError::OutOfRangeNumeric)
                    }
                } else {
                    Err(EvalError::OutOfRangeNumeric)
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

#[repr(u8)]
enum TypeTag {
    Null = 0,
    True,
    False,
    I16,
    I32,
    I64,
    U64,
    F32,
    F64,
    Str,
    SqlType,
    // fill in the rest of the types.
}

fn assert_copy<T: Copy>(_t: T) {}

fn push_tag(data: &mut Vec<u8>, tag: TypeTag) {
    data.push(tag as u8);
}

macro_rules! push_copy {
    ($ptr:expr, $val:expr, $T:ty) => {{
        let t = $val;
        assert_copy(t);
        $ptr.extend_from_slice(&unsafe { std::mem::transmute::<_, [u8; std::mem::size_of::<$T>()]>(t) })
    }};
}

unsafe fn read<T>(data: &[u8], idx: &mut usize) -> T {
    debug_assert!(data.len() >= *idx + std::mem::size_of::<T>());
    let ptr = data.as_ptr().add(*idx);
    *idx += std::mem::size_of::<T>();
    (ptr as *const T).read_unaligned()
}

unsafe fn read_string<'a>(data: &'a [u8], idx: &mut usize) -> &'a str {
    let len = read::<usize>(data, idx);
    let data = &data[*idx..*idx + len];
    *idx += len;
    std::str::from_utf8_unchecked(data)
}

fn read_tag(data: &[u8], idx: &mut usize) -> TypeTag {
    unsafe { read::<TypeTag>(data, idx) }
}

/// in-memory representation of a table row. It is unable to deserialize
/// the row without knowing the types of each column, which makes this unsafe
/// however it is more memory efficient.
#[derive(Debug, Clone, PartialEq, Eq, Default, PartialOrd, Ord)]
pub struct Binary(Vec<u8>);

impl Binary {
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn to_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }

    pub fn with_data(data: Vec<u8>) -> Self {
        Self(data)
    }

    pub fn pack<'a>(other: &[Datum<'a>]) -> Self {
        use std::ops::Deref;
        let size = other.iter().fold(0usize, |acc, datum| acc + datum.size());
        let mut data = Vec::with_capacity(size);
        // this is not a very smart way of doing this, this just to get it working.
        for datum in other {
            match datum {
                Datum::<'a>::True => {
                    push_tag(&mut data, TypeTag::True);
                }
                Datum::<'a>::False => {
                    push_tag(&mut data, TypeTag::False);
                }
                Datum::<'a>::Int16(val) => {
                    push_tag(&mut data, TypeTag::I16);
                    push_copy!(&mut data, *val, i16);
                }
                Datum::<'a>::Int32(val) => {
                    push_tag(&mut data, TypeTag::I32);
                    push_copy!(&mut data, *val, i32);
                }
                Datum::<'a>::Int64(val) => {
                    push_tag(&mut data, TypeTag::I64);
                    push_copy!(&mut data, *val, i64);
                }
                Datum::<'a>::UInt64(val) => {
                    push_tag(&mut data, TypeTag::U64);
                    push_copy!(&mut data, *val, u64);
                }
                Datum::<'a>::Float32(val) => {
                    push_tag(&mut data, TypeTag::F32);
                    push_copy!(&mut data, *val.deref(), f32)
                }
                Datum::<'a>::Float64(val) => {
                    push_tag(&mut data, TypeTag::F64);
                    push_copy!(&mut data, *val.deref(), f64)
                }
                Datum::<'a>::String(val) => {
                    push_tag(&mut data, TypeTag::Str);
                    push_copy!(&mut data, val.len(), usize);
                    data.extend_from_slice(val.as_bytes());
                }
                Datum::<'a>::OwnedString(val) => {
                    push_tag(&mut data, TypeTag::Str);
                    push_copy!(&mut data, val.len(), usize);
                    data.extend_from_slice(val.as_bytes());
                }
                Datum::<'a>::Null => push_tag(&mut data, TypeTag::Null),
                Datum::<'a>::SqlType(sql_type) => {
                    push_tag(&mut data, TypeTag::SqlType);
                    push_copy!(&mut data, *sql_type, SqlType);
                }
            }
        }

        Self(data)
    }

    pub fn unpack(&self) -> Vec<Datum> {
        unpack_raw(self.0.as_slice())
    }
}

pub fn unpack_raw(data: &[u8]) -> Vec<Datum> {
    let mut index = 0;
    let mut res = Vec::new();
    while index < data.len() {
        let tag = read_tag(data, &mut index);
        let datum = match tag {
            TypeTag::Null => Datum::from_null(),
            TypeTag::True => Datum::from_bool(true),
            TypeTag::False => Datum::from_bool(false),
            TypeTag::Str => {
                let val = unsafe { read_string(data, &mut index) };
                Datum::String(val)
            }
            TypeTag::I16 => {
                let val = unsafe { read::<i16>(data, &mut index) };
                Datum::from_i16(val)
            }
            TypeTag::I32 => {
                let val = unsafe { read::<i32>(data, &mut index) };
                Datum::from_i32(val)
            }
            TypeTag::I64 => {
                let val = unsafe { read::<i64>(data, &mut index) };
                Datum::from_i64(val)
            }
            TypeTag::U64 => {
                let val = unsafe { read::<u64>(data, &mut index) };
                Datum::from_u64(val)
            }
            TypeTag::F32 => {
                let val = unsafe { read::<f32>(data, &mut index) };
                Datum::from_f32(val)
            }
            TypeTag::F64 => {
                let val = unsafe { read::<f64>(data, &mut index) };
                Datum::from_f64(val)
            }
            TypeTag::SqlType => {
                let val = unsafe { read::<SqlType>(data, &mut index) };
                Datum::from_sql_type(val)
            }
        };
        res.push(datum)
    }
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    mod pack_unpack_types {
        use super::*;

        #[test]
        fn null() {
            let data = vec![Datum::from_null()];
            let row = Binary::pack(&data);
            assert_eq!(data, row.unpack());
        }

        #[test]
        fn booleans() {
            let data = vec![Datum::from_bool(true)];
            let row = Binary::pack(&data);
            assert_eq!(data, row.unpack());
        }

        #[test]
        fn floats() {
            let data = vec![Datum::from_f32(1000.123), Datum::from_f64(100.134_219_234_555)];
            let row = Binary::pack(&data);
            assert_eq!(data, row.unpack());
        }

        #[test]
        fn integers() {
            let data = vec![Datum::from_i16(100), Datum::from_i32(1_000), Datum::from_i64(10_000)];
            let row = Binary::pack(&data);
            assert_eq!(data, row.unpack());
        }

        #[test]
        fn unsigned_integers() {
            let data = vec![Datum::from_u64(10_000)];
            let row = Binary::pack(&data);
            assert_eq!(data, row.unpack());
        }

        #[test]
        fn strings() {
            let data = vec![Datum::from_string("string".to_owned()), Datum::from_str("hello")];
            let row = Binary::pack(&data);
            assert_eq!(vec![Datum::from_str("string"), Datum::from_str("hello")], row.unpack());
        }

        #[test]
        fn sql_type() {
            let data = vec![Datum::from_sql_type(SqlType::VarChar(32))];
            let row = Binary::pack(&data);
            assert_eq!(data, row.unpack());
        }
    }
}
