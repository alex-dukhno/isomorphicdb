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

use super::*;
use representation::{Binary, Datum};
use sql_types::SqlType;

#[rstest::fixture]
fn with_small_ints_table(default_schema_name: &str, storage_with_schema: CatalogManager) -> CatalogManager {
    storage_with_schema
        .create_table(
            default_schema_name,
            "table_name",
            &[
                ColumnDefinition::new("column_1", SqlType::SmallInt(i16::min_value())),
                ColumnDefinition::new("column_2", SqlType::SmallInt(i16::min_value())),
                ColumnDefinition::new("column_3", SqlType::SmallInt(i16::min_value())),
            ],
        )
        .expect("table is created");
    storage_with_schema
}

#[rstest::rstest]
fn select_all_from_table_with_many_columns(default_schema_name: &str, with_small_ints_table: CatalogManager) {
    with_small_ints_table
        .write_into(
            default_schema_name,
            "table_name",
            vec![(
                Binary::pack(&[Datum::from_u64(1)]),
                Binary::pack(&[Datum::from_i16(1), Datum::from_i16(2), Datum::from_i16(3)]),
            )],
        )
        .expect("values are inserted");

    assert_eq!(
        with_small_ints_table
            .full_scan(default_schema_name, "table_name")
            .map(|read| read
                .map(Result::unwrap)
                .map(Result::unwrap)
                .map(|(_key, values)| values)
                .collect()),
        Ok(vec![Binary::pack(&[
            Datum::from_i16(1),
            Datum::from_i16(2),
            Datum::from_i16(3)
        ])])
    );
}
