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

use super::RelationType;
use ordered_float::OrderedFloat;
use sql_types::SqlType;

///! Runtime cell and row representation.

// owned parallel of Datum but owns the content.
// pub enum Value {
//     Null,
//     True,
//     False,
//     Int32(i32),
//     Int64(i64),
//     Float32(f32),
//     Float64(f64),
//     String(String),
//     // Bytes(Vec<u8>)
// }

/// value shared by the row.
#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Hash)]
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
    // Bytes(&'a [u8]),
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
            Self::Float32(_) => 1 + std::mem::size_of::<f32>(),
            Self::Float64(_) => 1 + std::mem::size_of::<f64>(),
            Self::String(val) => 1 + std::mem::size_of::<usize>() + val.len(),
        }
    }

    pub fn from_null() -> Datum<'static> {
        Datum::Null
    }

    pub fn from_bool(val: bool) -> Datum<'static> {
        if val {
            Datum::True
        }
        else {
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

    pub fn from_f32(val: f32) -> Datum<'static> {
        Datum::Float32(val.into())
    }

    pub fn from_f64(val: f64) -> Datum<'static> {
        Datum::Float64(val.into())
    }

    pub fn from_str(val: &'a str) -> Datum<'a> {
        Datum::String(val)
    }
}

/// in-memory representation of a table row. It is unable to deserialize
/// the row without knowing the types of each column, which makes this unsafe
/// however it is more memory efficient.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Row {
    // consider move to an version can store elements inline upto a point
    /// packed data.
    data: Vec<u8>
}

#[repr(u8)]
enum TypeTag {
    Null = 0,
    True,
    False,
    I16,
    I32,
    I64,
    F32,
    F64,
    Str,
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
        $ptr.extend_from_slice(&unsafe { std::mem::transmute::<_, [u8; std::mem::size_of::<$T>()]>(t)})
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

impl Row {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_data(data: Vec<u8>) -> Self {
        Self { data }
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
                    push_tag(&mut data,TypeTag::False);
                }
                Datum::<'a>::Int16(val) => {
                    push_tag(&mut data,TypeTag::I16);
                    push_copy!(&mut data, *val, i16);
                }
                Datum::<'a>::Int32(val) => {
                    push_tag(&mut data,TypeTag::I32);
                    push_copy!(&mut data, *val, i32);
                },
                Datum::<'a>::Int64(val) => {
                    push_tag(&mut data,TypeTag::I64);
                    push_copy!(&mut data, *val, i64);
                }
                Datum::<'a>::Float32(val) => {
                    push_tag(&mut data,TypeTag::F32);
                    push_copy!(&mut data, *val.deref(), f32)
                }
                Datum::<'a>::Float64(val) => {
                    push_tag(&mut data,TypeTag::F64);
                    push_copy!(&mut data, *val.deref(), f64)
                }
                Datum::<'a>::String(val) => {
                    push_tag(&mut data,TypeTag::Str);
                    push_copy!(&mut data, val.len(), usize);
                    data.extend_from_slice(val.as_bytes());
                },
                Datum::<'a>::Null => push_tag(&mut data, TypeTag::Null),
            }
        }

        Self {
            data
        }
    }

    pub fn unpack<'a>(&'a self) -> Vec<Datum<'a>> {
        let mut index = 0;
        let mut res = Vec::new();
        let data = self.data.as_slice();
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
                TypeTag::F32 => {
                    let val = unsafe { read::<f32>(data, &mut index) };
                    Datum::from_f32(val)
                }
                TypeTag::F64 => {
                    let val = unsafe { read::<f64>(data, &mut index) };
                    Datum::from_f64(val)
                }
                // SqlType::Decimal |
                // SqlType::Time |
                // SqlType::TimeWithTimeZone |
                // SqlType::Timestamp |
                // SqlType::TimestampWithTimeZone |
                // SqlType::Date |
                // SqlType::Interval => unimplemented!()
            };
            res.push(datum)
        }
        res
    }
}
