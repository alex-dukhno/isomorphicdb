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
use sql_types::SqlType;

#[rstest::fixture]
fn with_small_ints_table(default_schema_name: &str, mut storage_with_schema: PersistentStorage) -> PersistentStorage {
    create_table(
        &mut storage_with_schema,
        default_schema_name,
        "table_name",
        vec![
            column_definition("column_1", SqlType::SmallInt(i16::min_value())),
            column_definition("column_2", SqlType::SmallInt(i16::min_value())),
            column_definition("column_3", SqlType::SmallInt(i16::min_value())),
        ],
    );
    storage_with_schema
}

#[rstest::rstest]
fn select_from_table_from_non_existent_schema(mut storage: PersistentStorage) {
    assert_eq!(
        storage
            .select_all_from("non_existent", "table_name", vec![])
            .expect("no system errors"),
        Err(OperationOnTableError::SchemaDoesNotExist)
    );
}

#[rstest::rstest]
fn select_from_table_that_does_not_exist(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
    let table_columns = storage_with_schema
        .table_columns(default_schema_name, "not_existed")
        .expect("no system errors")
        .into_iter()
        .map(|column_definition| column_definition.name())
        .collect();

    assert_eq!(
        storage_with_schema
            .select_all_from(default_schema_name, "not_existed", table_columns)
            .expect("no system errors"),
        Err(OperationOnTableError::TableDoesNotExist)
    );
}

#[rstest::rstest]
fn select_all_from_table_with_many_columns(default_schema_name: &str, mut with_small_ints_table: PersistentStorage) {
    let row = vec![Datum::from_i16(1), Datum::from_i16(2), Datum::from_i16( 3)];
    insert_into(
        &mut with_small_ints_table,
        default_schema_name,
        "table_name",
        row.clone()
    );

    let table_columns = with_small_ints_table
        .table_columns(default_schema_name, "table_name")
        .expect("no system errors")
        .into_iter()
        .map(|column_definition| column_definition.name())
        .collect();

    assert_eq!(
        with_small_ints_table
            .select_all_from(default_schema_name, "table_name", table_columns)
            .expect("no system errors"),
        Ok((
            vec![
                column_definition("column_1", SqlType::SmallInt(i16::min_value())),
                column_definition("column_2", SqlType::SmallInt(i16::min_value())),
                column_definition("column_3", SqlType::SmallInt(i16::min_value()))
            ],
            vec![Row::pack(&row).to_bytes()]
        ))
    );
}

#[rstest::rstest]
fn select_first_and_last_columns_from_table_with_multiple_columns(
    default_schema_name: &str,
    mut with_small_ints_table: PersistentStorage,
) {
    let row1 = vec![Datum::from_i16(1), Datum::from_i16(2), Datum::from_i16(3)];
    let row2 = vec![Datum::from_i16(4), Datum::from_i16(5), Datum::from_i16(6)];
    let row3 = vec![Datum::from_i16(7), Datum::from_i16(8), Datum::from_i16(9)];

    insert_into(
        &mut with_small_ints_table,
        default_schema_name,
        "table_name",
        row1.clone(),
    );
    insert_into(
        &mut with_small_ints_table,
        default_schema_name,
        "table_name",
        row2.clone(),
    );
    insert_into(
        &mut with_small_ints_table,
        default_schema_name,
        "table_name",
        row3.clone(),
    );

    assert_eq!(
        with_small_ints_table
            .select_all_from(
                default_schema_name,
                "table_name",
                vec!["column_1".to_owned(), "column_3".to_owned()]
            )
            .expect("no system errors"),
        Ok((
            vec![
                column_definition("column_1", SqlType::SmallInt(i16::min_value())),
                column_definition("column_3", SqlType::SmallInt(i16::min_value()))
            ],
            vec![
                Row::pack(&row1).to_bytes(),
                Row::pack(&row2).to_bytes(),
                Row::pack(&row3).to_bytes(),
            ],
        ))
    );
}

#[rstest::rstest]
fn select_all_columns_reordered_from_table_with_multiple_columns(
    default_schema_name: &str,
    mut with_small_ints_table: PersistentStorage,
) {
    let row1 = vec![Datum::from_i16(1), Datum::from_i16(2), Datum::from_i16(3)];
    let row2 = vec![Datum::from_i16(4), Datum::from_i16(5), Datum::from_i16(6)];
    let row3 = vec![Datum::from_i16(7), Datum::from_i16(8), Datum::from_i16(9)];

    insert_into(
        &mut with_small_ints_table,
        default_schema_name,
        "table_name",
        row1,
    );
    insert_into(
        &mut with_small_ints_table,
        default_schema_name,
        "table_name",
        row2
    );
    insert_into(
        &mut with_small_ints_table,
        default_schema_name,
        "table_name",
        row3,
    );

    assert_eq!(
        with_small_ints_table
            .select_all_from(
                default_schema_name,
                "table_name",
                vec!["column_3".to_owned(), "column_1".to_owned(), "column_2".to_owned()]
            )
            .expect("no system errors"),
        Ok((
            vec![
                column_definition("column_3", SqlType::SmallInt(i16::min_value())),
                column_definition("column_1", SqlType::SmallInt(i16::min_value())),
                column_definition("column_2", SqlType::SmallInt(i16::min_value()))
            ],
            vec![
                Row::pack(&[Datum::from_i16(3), Datum::from_i16(1), Datum::from_i16(2)]).to_bytes(),
                Row::pack(&[Datum::from_i16(6), Datum::from_i16(4), Datum::from_i16(5)]).to_bytes(),
                Row::pack(&[Datum::from_i16(9), Datum::from_i16(7), Datum::from_i16(8)]).to_bytes(),
            ],
        ))
    );
}

#[rstest::rstest]
fn select_with_column_name_duplication(default_schema_name: &str, mut with_small_ints_table: PersistentStorage) {
    let row1 = vec![Datum::from_i16(1), Datum::from_i16(2), Datum::from_i16(3)];
    let row2 = vec![Datum::from_i16(4), Datum::from_i16(5), Datum::from_i16(6)];
    let row3 = vec![Datum::from_i16(7), Datum::from_i16(8), Datum::from_i16(9)];

    insert_into(
        &mut with_small_ints_table,
        default_schema_name,
        "table_name",
        row1,
    );
    insert_into(
        &mut with_small_ints_table,
        default_schema_name,
        "table_name",
        row2,
    );
    insert_into(
        &mut with_small_ints_table,
        default_schema_name,
        "table_name",
        row3,
    );

    assert_eq!(
        with_small_ints_table
            .select_all_from(
                default_schema_name,
                "table_name",
                vec![
                    "column_3".to_owned(),
                    "column_2".to_owned(),
                    "column_1".to_owned(),
                    "column_3".to_owned(),
                    "column_2".to_owned()
                ]
            )
            .expect("no system errors"),
        Ok((
            vec![
                column_definition("column_3", SqlType::SmallInt(i16::min_value())),
                column_definition("column_2", SqlType::SmallInt(i16::min_value())),
                column_definition("column_1", SqlType::SmallInt(i16::min_value())),
                column_definition("column_3", SqlType::SmallInt(i16::min_value())),
                column_definition("column_2", SqlType::SmallInt(i16::min_value()))
            ],
            vec![
                Row::pack(&[
                    Datum::from_i16(3),
                    Datum::from_i16(2),
                    Datum::from_i16(1),
                    Datum::from_i16(3),
                    Datum::from_i16(2)
                ]).to_bytes(),
                Row::pack(&[
                    Datum::from_i16(6),
                    Datum::from_i16(5),
                    Datum::from_i16(4),
                    Datum::from_i16(6),
                    Datum::from_i16(5),
                ]).to_bytes(),
                Row::pack(&[
                    Datum::from_i16(9),
                    Datum::from_i16(8),
                    Datum::from_i16(7),
                    Datum::from_i16(9),
                    Datum::from_i16(8),
                ]).to_bytes(),
            ],
        ))
    );
}

#[rstest::rstest]
fn select_different_integer_types(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
    create_table(
        &mut storage_with_schema,
        default_schema_name,
        "table_name",
        vec![
            column_definition("small_int", SqlType::SmallInt(i16::min_value())),
            column_definition("integer", SqlType::Integer(i32::min_value())),
            column_definition("big_int", SqlType::BigInt(i64::min_value())),
        ],
    );
    let row1 = vec![Datum::from_i16(1000), Datum::from_i32(2000000), Datum::from_i64(3000000000)];
    let row2 = vec![Datum::from_i16(4000), Datum::from_i32(5000000), Datum::from_i64(6000000000)];
    let row3 = vec![Datum::from_i16(7000), Datum::from_i32(8000000), Datum::from_i64(9000000000)];

    insert_into(
        &mut storage_with_schema,
        default_schema_name,
        "table_name",
        row1.clone(),
    );
    insert_into(
        &mut storage_with_schema,
        default_schema_name,
        "table_name",
        row2.clone(),
    );
    insert_into(
        &mut storage_with_schema,
        default_schema_name,
        "table_name",
        row3.clone(),
    );

    assert_eq!(
        storage_with_schema
            .select_all_from(
                default_schema_name,
                "table_name",
                vec!["small_int".to_owned(), "integer".to_owned(), "big_int".to_owned()]
            )
            .expect("no system errors"),
        Ok((
            vec![
                column_definition("small_int", SqlType::SmallInt(i16::min_value())),
                column_definition("integer", SqlType::Integer(i32::min_value())),
                column_definition("big_int", SqlType::BigInt(i64::min_value())),
            ],
            vec![
                Row::pack(&row1).to_bytes(),
                Row::pack(&row2).to_bytes(),
                Row::pack(&row3).to_bytes(),
            ],
        ))
    );
}

#[rstest::rstest]
fn select_different_character_strings_types(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
    create_table(
        &mut storage_with_schema,
        default_schema_name,
        "table_name",
        vec![
            column_definition("char_10", SqlType::Char(10)),
            column_definition("var_char_20", SqlType::VarChar(20)),
        ],
    );
    let row1 = vec![Datum::from_str("1234567890"), Datum::from_str("12345678901234567890")];
    let row2 = vec![Datum::from_str("12345"), Datum::from_str("1234567890")];
    let row3 = vec![Datum::from_str("12345"), Datum::from_str("1234567890     ")];

    insert_into(
        &mut storage_with_schema,
        default_schema_name,
        "table_name",
        row1.clone(),
    );
    insert_into(
        &mut storage_with_schema,
        default_schema_name,
        "table_name",
        row2.clone(),
    );
    insert_into(
        &mut storage_with_schema,
        default_schema_name,
        "table_name",
        row3.clone(),
    );

    assert_eq!(
        storage_with_schema
            .select_all_from(
                default_schema_name,
                "table_name",
                vec!["char_10".to_owned(), "var_char_20".to_owned()]
            )
            .expect("no system errors"),
        Ok((
            vec![
                column_definition("char_10", SqlType::Char(10)),
                column_definition("var_char_20", SqlType::VarChar(20)),
            ],
            vec![
                Row::pack(&row1).to_bytes(),
                Row::pack(&row2).to_bytes(),
                Row::pack(&row3).to_bytes(),
            ],
        ))
    );
}
