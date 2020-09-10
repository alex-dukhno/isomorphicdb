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

use representation::Binary;
use sql_model::sql_types::SqlType;

use super::*;
use ast::Datum;

#[rstest::rstest]
fn delete_all_from_table(data_manager_with_schema: DataManager) {
    let schema_id = data_manager_with_schema.schema_exists(&SCHEMA).expect("schema exists");
    let table_id = data_manager_with_schema
        .create_table(
            schema_id,
            "table_name",
            &[ColumnDefinition::new(
                "column_test",
                SqlType::SmallInt(i16::min_value()),
            )],
        )
        .expect("table is created");

    data_manager_with_schema
        .write_into(
            &Box::new((schema_id, table_id)),
            vec![(
                Binary::pack(&[Datum::from_u64(1)]),
                Binary::pack(&[Datum::from_i16(123)]),
            )],
        )
        .expect("values are inserted");
    data_manager_with_schema
        .write_into(
            &Box::new((schema_id, table_id)),
            vec![(
                Binary::pack(&[Datum::from_u64(2)]),
                Binary::pack(&[Datum::from_i16(456)]),
            )],
        )
        .expect("values are inserted");
    data_manager_with_schema
        .write_into(
            &Box::new((schema_id, table_id)),
            vec![(
                Binary::pack(&[Datum::from_u64(3)]),
                Binary::pack(&[Datum::from_i16(789)]),
            )],
        )
        .expect("values are inserted");

    assert_eq!(
        data_manager_with_schema.delete_from(
            &Box::new((schema_id, table_id)),
            vec![
                Binary::pack(&[Datum::from_u64(1)]),
                Binary::pack(&[Datum::from_u64(2)]),
                Binary::pack(&[Datum::from_u64(3)])
            ],
        ),
        Ok(3)
    );

    assert_eq!(
        data_manager_with_schema
            .full_scan(&Box::new((schema_id, table_id)))
            .map(|iter| iter.map(Result::unwrap).map(Result::unwrap).collect()),
        Ok(vec![])
    );
}

#[rstest::fixture]
fn with_small_ints_table(data_manager_with_schema: DataManager) -> DataManager {
    let schema_id = data_manager_with_schema.schema_exists(&SCHEMA).expect("schema exists");
    data_manager_with_schema
        .create_table(
            schema_id,
            "table_name",
            &[
                ColumnDefinition::new("column_1", SqlType::SmallInt(i16::min_value())),
                ColumnDefinition::new("column_2", SqlType::SmallInt(i16::min_value())),
                ColumnDefinition::new("column_3", SqlType::SmallInt(i16::min_value())),
            ],
        )
        .expect("table is created");
    data_manager_with_schema
}

#[rstest::rstest]
fn select_all_from_table_with_many_columns(with_small_ints_table: DataManager) {
    let full_table_id = with_small_ints_table
        .table_exists(&SCHEMA, &"table_name")
        .expect("schema exists");
    let schema_id = full_table_id.0;
    let table_id = full_table_id.1.expect("table exist");
    with_small_ints_table
        .write_into(
            &Box::new((schema_id, table_id)),
            vec![(
                Binary::pack(&[Datum::from_u64(1)]),
                Binary::pack(&[Datum::from_i16(1), Datum::from_i16(2), Datum::from_i16(3)]),
            )],
        )
        .expect("values are inserted");

    assert_eq!(
        with_small_ints_table
            .full_scan(&Box::new((schema_id, table_id)))
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
