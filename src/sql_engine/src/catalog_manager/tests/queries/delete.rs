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
use representation::Datum;
use sql_types::SqlType;

#[rstest::rstest]
fn delete_all_from_table(default_schema_name: &str, storage_with_schema: CatalogManager) {
    storage_with_schema
        .create_table(
            default_schema_name,
            "table_name",
            &[ColumnDefinition::new(
                "column_test",
                SqlType::SmallInt(i16::min_value()),
            )],
        )
        .expect("table is created");

    storage_with_schema
        .write_into(
            default_schema_name,
            "table_name",
            vec![(
                Binary::pack(&[Datum::from_u64(1)]),
                Binary::pack(&[Datum::from_i16(123)]),
            )],
        )
        .expect("values are inserted");
    storage_with_schema
        .write_into(
            default_schema_name,
            "table_name",
            vec![(
                Binary::pack(&[Datum::from_u64(2)]),
                Binary::pack(&[Datum::from_i16(456)]),
            )],
        )
        .expect("values are inserted");
    storage_with_schema
        .write_into(
            default_schema_name,
            "table_name",
            vec![(
                Binary::pack(&[Datum::from_u64(3)]),
                Binary::pack(&[Datum::from_i16(789)]),
            )],
        )
        .expect("values are inserted");

    assert_eq!(
        storage_with_schema.delete_from(
            default_schema_name,
            "table_name",
            vec![
                Binary::pack(&[Datum::from_u64(1)]),
                Binary::pack(&[Datum::from_u64(2)]),
                Binary::pack(&[Datum::from_u64(3)])
            ]
        ),
        Ok(3)
    );

    assert_eq!(
        storage_with_schema
            .full_scan("schema_name", "table_name")
            .map(|iter| iter.map(Result::unwrap).map(Result::unwrap).collect()),
        Ok(vec![])
    );
}
