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

use super::*;
use binary::Binary;
use repr::Datum;
use types::SqlType;

#[rstest::fixture]
fn with_small_ints_table(data_manager_with_schema: InMemory) -> InMemory {
    for op in create_table(
        SCHEMA,
        TABLE,
        &[
            ("column_1", SqlType::small_int()),
            ("column_2", SqlType::small_int()),
            ("column_3", SqlType::small_int()),
        ],
    ) {
        if data_manager_with_schema.execute(&op).is_ok() {}
    }
    data_manager_with_schema
}

#[rstest::rstest]
fn delete_all_from_table(data_manager_with_schema: InMemory) {
    let schema_id = data_manager_with_schema.schema_exists(SCHEMA).expect("schema exists");
    for op in create_table(SCHEMA, TABLE, &[("column_test", SqlType::small_int())]) {
        if data_manager_with_schema.execute(&op).is_ok() {}
    }

    let table_id = match data_manager_with_schema.table_exists(SCHEMA, TABLE) {
        Some((_, Some(table_id))) => table_id,
        _ => panic!(),
    };

    data_manager_with_schema
        .write_into(
            &(schema_id, table_id),
            vec![(
                Binary::pack(&[Datum::from_u64(1)]),
                Binary::pack(&[Datum::from_i16(123)]),
            )],
        )
        .expect("values are inserted");
    data_manager_with_schema
        .write_into(
            &(schema_id, table_id),
            vec![(
                Binary::pack(&[Datum::from_u64(2)]),
                Binary::pack(&[Datum::from_i16(456)]),
            )],
        )
        .expect("values are inserted");
    data_manager_with_schema
        .write_into(
            &(schema_id, table_id),
            vec![(
                Binary::pack(&[Datum::from_u64(3)]),
                Binary::pack(&[Datum::from_i16(789)]),
            )],
        )
        .expect("values are inserted");

    assert_eq!(
        data_manager_with_schema.delete_from(
            &(schema_id, table_id),
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
            .full_scan(&(schema_id, table_id))
            .map(|iter| iter.map(Result::unwrap).map(Result::unwrap).collect()),
        Ok(vec![])
    );
}

#[rstest::rstest]
fn select_all_from_table_with_many_columns(with_small_ints_table: InMemory) {
    let (schema_id, table_id) = match with_small_ints_table.table_exists(SCHEMA, "table_name") {
        Some((schema_id, Some(table_id))) => (schema_id, table_id),
        _ => panic!(),
    };
    with_small_ints_table
        .write_into(
            &(schema_id, table_id),
            vec![(
                Binary::pack(&[Datum::from_u64(1)]),
                Binary::pack(&[Datum::from_i16(1), Datum::from_i16(2), Datum::from_i16(3)]),
            )],
        )
        .expect("values are inserted");

    assert_eq!(
        with_small_ints_table.full_scan(&(schema_id, table_id)).map(|read| read
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
