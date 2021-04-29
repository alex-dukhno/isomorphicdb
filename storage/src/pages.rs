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

use bigdecimal::{BigDecimal, FromPrimitive};
use scalar::ScalarValue;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use types::{Num, SqlType, SqlTypeFamily};

struct TableMetadata {
    column_types: Vec<SqlType>,
}

impl TableMetadata {
    fn index_in_tuple(&self, col_index: usize) -> (usize, SqlType) {
        (
            self.column_types.iter().take(col_index).map(SqlType::size).sum(),
            self.column_types[col_index],
        )
    }
}

struct Header {
    tx_id: AtomicU64,
    begin_ts: AtomicU64,
    end_ts: AtomicU64,
    pointer: Pointer,
}

struct Pointer {
    index: usize,
    page: Option<Arc<Page>>,
}

struct Tuple {
    header: Header,
    data: Vec<u8>,
}

enum FixedLenValue {
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Index(usize),
}

impl Tuple {
    fn extract(&self, index: usize, meta_data: &TableMetadata) -> FixedLenValue {
        let (data_at, sql_type) = meta_data.index_in_tuple(index);
        let datum = data[data_at..sql_type.size()];
        match sql_type {
            SqlType::Bool => FixedLenValue::Bool(datum[0] != 0),
            SqlType::Num(Num::SmallInt) => FixedLenValue::Int16(i16::from_be_bytes(datum.try_into().unwrap())),
            SqlType::Num(Num::Integer) => FixedLenValue::Int32(i32::from_be_bytes(datum.try_into().unwrap())),
            SqlType::Num(Num::BigInt) => FixedLenValue::Int64(i64::from_be_bytes(datum.try_into().unwrap())),
            SqlType::Num(Num::Real) => FixedLenValue::Float32(f32::from_be_bytes(datum.try_into().unwrap())),
            SqlType::Num(Num::Double) => FixedLenValue::Float64(f64::from_be_bytes(datum.try_into().unwrap())),
            SqlType::Str { .. } => FixedLenValue::Index(usize::from_be_bytes(datum.try_into().unwrap())),
        }
    }
}

impl From<FixedLenValue> for ScalarValue {
    fn from(value: FixedLenValue) -> ScalarValue {
        match value {
            FixedLenValue::Bool(v) => ScalarValue::Bool(v),
            FixedLenValue::Int16(v) => ScalarValue::Num {
                value: BigDecimal::from(v),
                type_family: SqlTypeFamily::SmallInt,
            },
            FixedLenValue::Int32(v) => ScalarValue::Num {
                value: BigDecimal::from(v),
                type_family: SqlTypeFamily::Integer,
            },
            FixedLenValue::Int64(v) => ScalarValue::Num {
                value: BigDecimal::from(v),
                type_family: SqlTypeFamily::BigInt,
            },
            FixedLenValue::Float32(v) => ScalarValue::Num {
                value: BigDecimal::from_f32(v).unwrap(),
                type_family: SqlTypeFamily::Real,
            },
            FixedLenValue::Float64(v) => ScalarValue::Num {
                value: BigDecimal::from_f64(v).unwrap(),
                type_family: SqlTypeFamily::Double,
            },
            FixedLenValue::Index(_) => unreachable!(),
        }
    }
}

struct Records {
    data: Vec<Option<String>>,
}

struct Page {
    fixed_len_data: [Option<Tuple>; 4096],
    dynamic_len_data: Records,
}

impl Page {
    fn scan(&self, columns: &[usize], meta_data: &TableMetadata) -> impl Iterator<Item = Vec<ScalarValue>> {
        PageIter {
            page: self,
            columns,
            meta_data,
            index: 0,
        }
    }

    fn read_at(&self, tuple_index: usize, columns: &[usize], meta_data: &TableMetadata) -> Option<Vec<ScalarValue>> {
        match self.fixed_len_data[tuple_index].as_ref() {
            None => None,
            Some(data) => {
                let mut tuple = vec![];
                for col_index in columns {
                    let fixed_len = self.fixed_len_data[tuple_index].extract(*col_index, meta_data);
                    let value = match fixed_len {
                        FixedLenValue::Index(index) => {
                            ScalarValue::String(unsafe { (*self.dynamic_len_data.data[index].as_ptr()).clone() })
                        }
                        others => ScalarValue::from(others),
                    };
                    tuple.push(value);
                }
                Some(tuple)
            }
        }
    }
}

struct PageIter<'p> {
    page: &'p Page,
    columns: &'p [usize],
    meta_data: &'p TableMetadata,
    index: usize,
}

impl<'a> Iterator for PageIter<'a> {
    type Item = Vec<ScalarValue>;

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.index;
        self.index += 1;
        self.page.read_at(index, self.columns, self.meta_data)
    }
}
