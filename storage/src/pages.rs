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

use bigdecimal::{BigDecimal, FromPrimitive, ToPrimitive};
use scalar::ScalarValue;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use types::{Num, SqlType, SqlTypeFamily};

struct TableMetadata {
    column_types: Vec<SqlType>,
}

impl TableMetadata {
    fn column_number(&self) -> usize {
        self.column_types.len()
    }

    fn index_in_tuple(&self, col_index: usize) -> (usize, SqlType) {
        (
            self.column_types.iter().take(col_index).map(SqlType::size).sum(),
            self.column_types[col_index],
        )
    }

    fn dynamic_value_indexes(&self) -> Vec<usize> {
        self.column_types
            .iter()
            .enumerate()
            .filter(|(_, sql_type)| sql_type.family() == SqlTypeFamily::String)
            .map(|(index, _)| index)
            .collect()
    }
}

#[derive(Default)]
struct Header {
    tx_id: AtomicU64,
    begin_ts: AtomicU64,
    end_ts: AtomicU64,
    pointer: Pointer,
}

#[derive(Default)]
struct Pointer {
    index: usize,
    page: Option<Arc<Page>>,
}

struct BitSet {
    bits: Vec<u8>,
}

impl BitSet {
    fn new(capacity: usize) -> BitSet {
        BitSet {
            bits: vec![0; capacity],
        }
    }

    fn is_set(&self, index: usize) -> bool {
        self.bits[index] == 1
    }

    fn set(&mut self, index: usize) {
        self.bits[index] = 1
    }

    fn unset(&mut self, index: usize) {
        self.bits[index] = 0
    }
}

struct Tuple {
    header: Header,
    nullable: BitSet,
    data: Vec<u8>,
}

impl Tuple {
    fn serialize(values: &[ScalarValue], meta_data: &TableMetadata) -> Tuple {
        debug_assert_eq!(
            values.len() == meta_data.column_number(),
            "number of columns and number of values should be equal"
        );
        let header = Header::default();
        let mut nullable = BitSet::new(values.len());
        let mut data = vec![];
        for (index, value) in values.iter().enumerate() {
            match value {
                ScalarValue::Null => {
                    nullable.set(index);
                    match meta_data.column_types[index] {
                        SqlType::Bool => data.push(0),
                        SqlType::Str { .. } => data.extend_from_slice(&0usize.to_be_bytes()),
                        SqlType::Num(Num::SmallInt) => data.extend_from_slice(&0i16.to_be_bytes()),
                        SqlType::Num(Num::Integer) => data.extend_from_slice(&0i32.to_be_bytes()),
                        SqlType::Num(Num::BigInt) => data.extend_from_slice(&0i64.to_be_bytes()),
                        SqlType::Num(Num::Real) => data.extend_from_slice(&0.0f32.to_be_bytes()),
                        SqlType::Num(Num::Double) => data.extend_from_slice(&0.0f64.to_be_bytes()),
                    }
                }
                ScalarValue::Num {
                    value,
                    type_family: SqlTypeFamily::SmallInt,
                } => {
                    data.extend_from_slice(&value.to_i16().unwrap().to_be_bytes());
                }
                ScalarValue::Num {
                    value,
                    type_family: SqlTypeFamily::Integer,
                } => {
                    data.extend_from_slice(&value.to_i32().unwrap().to_be_bytes());
                }
                ScalarValue::Num {
                    value,
                    type_family: SqlTypeFamily::BigInt,
                } => {
                    data.extend_from_slice(&value.to_i64().unwrap().to_be_bytes());
                }
                ScalarValue::Num {
                    value,
                    type_family: SqlTypeFamily::Real,
                } => {
                    data.extend_from_slice(&value.to_f32().unwrap().to_be_bytes());
                }
                ScalarValue::Num {
                    value,
                    type_family: SqlTypeFamily::Double,
                } => {
                    data.extend_from_slice(&value.to_f64().unwrap().to_be_bytes());
                }
                ScalarValue::String(_) => {}
                ScalarValue::Bool(_) => {}
                ScalarValue::Num { type_family, .. } => unreachable!("NUM with {:?} is impossible", type_family),
            }
        }
        Tuple { header, nullable, data }
    }

    fn extract(&self, index: usize, meta_data: &TableMetadata) -> ScalarValue {
        if self.nullable.is_set(index) {
            ScalarValue::Null
        } else {
            let (data_at, sql_type) = meta_data.index_in_tuple(index);
            let datum = data[data_at..sql_type.size()];
            match sql_type {
                SqlType::Bool => ScalarValue::Bool(datum[0] != 0),
                SqlType::Num(Num::SmallInt) => ScalarValue::Num {
                    value: BigDecimal::from(i16::from_be_bytes(datum.try_into().unwrap())),
                    type_family: SqlTypeFamily::SmallInt,
                },
                SqlType::Num(Num::Integer) => ScalarValue::Num {
                    value: BigDecimal::from(i32::from_be_bytes(datum.try_into().unwrap())),
                    type_family: SqlTypeFamily::Integer,
                },
                SqlType::Num(Num::BigInt) => ScalarValue::Num {
                    value: BigDecimal::from(i64::from_be_bytes(datum.try_into().unwrap())),
                    type_family: SqlTypeFamily::BigInt,
                },
                SqlType::Num(Num::Real) => ScalarValue::Num {
                    value: BigDecimal::from(f32::from_be_bytes(datum.try_into().unwrap())),
                    type_family: SqlTypeFamily::Real,
                },
                SqlType::Num(Num::Double) => ScalarValue::Num {
                    value: BigDecimal::from(f64::from_be_bytes(datum.try_into().unwrap())),
                    type_family: SqlTypeFamily::Double,
                },
                SqlType::Str { len, .. } => {
                    let index = usize::from_be_bytes(datum.try_into().unwrap());
                    ScalarValue::String(std::str::from_utf8(&self.data[index..len]).unwrap().to_owned())
                }
            }
        }
    }
}

struct Page {
    records: [Option<Tuple>; 4096],
    current_index: usize,
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
        match self.records[tuple_index].as_ref() {
            None => None,
            Some(data) => {
                let mut tuple = vec![];
                for col_index in columns {
                    let value = data.extract(*col_index, meta_data);
                    tuple.push(value);
                }
                Some(tuple)
            }
        }
    }

    fn delete_at(&mut self, tuple_index: usize) -> bool {
        match self.records[tuple_index].take() {
            None => false,
            Some(_) => true,
        }
    }

    fn write_at(&mut self, _tuple_index: usize, values: &[ScalarValue], meta_data: &TableMetadata) -> bool {
        self.records[self.current_index] = Some(Tuple::serialize(values, meta_data));
        self.current_index += 1;
        true
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
