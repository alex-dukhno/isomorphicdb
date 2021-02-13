// Copyright 2020 - present Alex Dukhno
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

use bigdecimal::ToPrimitive;
use catalog::SqlTable;
use data_binary::{repr::Datum, Binary};
use data_manipulation_query_result::QueryExecutionError;
use data_manipulation_typed_tree::StaticTypedTree;
use data_manipulation_typed_values::TypedValue;
use types::SqlTypeFamily;

pub struct TableInPlan {
    source: Box<StaticValues>,
    table: Box<dyn SqlTable>,
}

impl TableInPlan {
    pub fn new(static_values: StaticValues, table: Box<dyn SqlTable>) -> TableInPlan {
        TableInPlan {
            source: Box::new(static_values),
            table,
        }
    }

    pub fn execute(mut self) -> Result<usize, QueryExecutionError> {
        let mut len = 0;
        while let Some(raw_tuple) = self.source.next() {
            let mut data = vec![];
            for value in raw_tuple {
                let datum = match value {
                    None => Datum::from_null(),
                    Some(expr) => match expr.eval() {
                        Err(error) => return Err(error),
                        Ok(TypedValue::Num {
                            value,
                            type_family: SqlTypeFamily::SmallInt,
                        }) => Datum::from_i16(value.to_i16().unwrap()),
                        Ok(TypedValue::Num {
                            value,
                            type_family: SqlTypeFamily::Integer,
                        }) => Datum::from_i32(value.to_i32().unwrap()),
                        Ok(TypedValue::Num {
                            value,
                            type_family: SqlTypeFamily::BigInt,
                        }) => Datum::from_i64(value.to_i64().unwrap()),
                        Ok(TypedValue::String(str)) => Datum::from_string(str),
                        Ok(TypedValue::Bool(boolean)) => Datum::from_bool(boolean),
                        _ => unreachable!(),
                    },
                };
                data.push(datum);
            }
            self.table.write(Binary::pack(&data));
            len += 1;
        }
        Ok(len)
    }
}

pub struct StaticValues(Vec<Vec<Option<StaticTypedTree>>>);

impl StaticValues {
    pub fn from(mut values: Vec<Vec<Option<StaticTypedTree>>>) -> StaticValues {
        values.reverse();
        StaticValues(values)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Vec<Option<StaticTypedTree>>> {
        self.0.pop()
    }
}
