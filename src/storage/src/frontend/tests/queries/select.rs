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
            .select_all_from("non_existent", "table_name")
            .expect("no system errors"),
        Err(OperationOnTableError::SchemaDoesNotExist)
    );
}

#[rstest::rstest]
fn select_from_table_that_does_not_exist(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
    assert_eq!(
        storage_with_schema
            .select_all_from(default_schema_name, "not_existed")
            .expect("no system errors"),
        Err(OperationOnTableError::TableDoesNotExist)
    );
}

#[rstest::rstest]
fn select_all_from_table_with_many_columns(default_schema_name: &str, mut with_small_ints_table: PersistentStorage) {
    insert_into(
        &mut with_small_ints_table,
        default_schema_name,
        "table_name",
        vec![(1, vec!["1", "2", "3"])],
    );

    assert_eq!(
        with_small_ints_table
            .select_all_from(default_schema_name, "table_name")
            .expect("no system errors"),
        Ok(vec![Binary::with_data(b"1|2|3".to_vec())])
    );
}

#[rstest::rstest]
fn select_columns_for_non_existent_table(mut storage_with_schema: PersistentStorage) {
    assert_eq!(
        storage_with_schema
            .select_all_from(
                "system",
                "columns",
                vec![
                    "schema_name".to_owned(),
                    "table_name".to_owned(),
                    "column_name".to_owned(),
                    "column_type".to_owned()
                ]
            )
            .expect("no system errors"),
        Ok((
            vec![
                column_definition("schema_name", SqlType::VarChar(100)),
                column_definition("table_name", SqlType::VarChar(100)),
                column_definition("column_name", SqlType::VarChar(100)),
                column_definition("column_type", SqlType::VarChar(100)),
            ],
            vec![],
        ))
    );
}

#[rstest::rstest]
fn select_columns_for_all_tables(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
    create_table(
        &mut storage_with_schema,
        default_schema_name,
        "table_name_1",
        vec![
            column_definition("big_int", SqlType::BigInt(0)),
            column_definition("char_10", SqlType::Char(10)),
        ],
    );

    assert_eq!(
        storage_with_schema
            .select_all_from(
                "system",
                "columns",
                vec![
                    "schema_name".to_owned(),
                    "table_name".to_owned(),
                    "column_name".to_owned(),
                    "column_type".to_owned()
                ]
            )
            .expect("no system errors"),
        Ok((
            vec![
                column_definition("schema_name", SqlType::VarChar(100)),
                column_definition("table_name", SqlType::VarChar(100)),
                column_definition("column_name", SqlType::VarChar(100)),
                column_definition("column_type", SqlType::VarChar(100)),
            ],
            vec![
                vec![
                    default_schema_name.to_owned(),
                    "table_name_1".to_owned(),
                    "big_int".to_owned(),
                    "BIGINT".to_owned()
                ],
                vec![
                    default_schema_name.to_owned(),
                    "table_name_1".to_owned(),
                    "char_10".to_owned(),
                    "CHAR (10)".to_owned()
                ],
            ],
        ))
    );

    create_table(
        &mut storage_with_schema,
        default_schema_name,
        "table_name_2",
        vec![
            column_definition("decimal", SqlType::Decimal),
            column_definition("var_char_20", SqlType::VarChar(20)),
        ],
    );

    assert_eq!(
        storage_with_schema
            .select_all_from(
                "system",
                "columns",
                vec![
                    "schema_name".to_owned(),
                    "table_name".to_owned(),
                    "column_name".to_owned(),
                    "column_type".to_owned()
                ]
            )
            .expect("no system errors"),
        Ok((
            vec![
                column_definition("schema_name", SqlType::VarChar(100)),
                column_definition("table_name", SqlType::VarChar(100)),
                column_definition("column_name", SqlType::VarChar(100)),
                column_definition("column_type", SqlType::VarChar(100)),
            ],
            vec![
                vec![
                    default_schema_name.to_owned(),
                    "table_name_1".to_owned(),
                    "big_int".to_owned(),
                    "BIGINT".to_owned()
                ],
                vec![
                    default_schema_name.to_owned(),
                    "table_name_1".to_owned(),
                    "char_10".to_owned(),
                    "CHAR (10)".to_owned()
                ],
                vec![
                    default_schema_name.to_owned(),
                    "table_name_2".to_owned(),
                    "decimal".to_owned(),
                    "DECIMAL".to_owned()
                ],
                vec![
                    default_schema_name.to_owned(),
                    "table_name_2".to_owned(),
                    "var_char_20".to_owned(),
                    "VARCHAR (20)".to_owned()
                ],
            ],
        ))
    );
}

#[rstest::rstest]
fn select_columns_after_drop(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
    create_table(
        &mut storage_with_schema,
        default_schema_name,
        "table_name_1",
        vec![
            column_definition("big_int", SqlType::BigInt(0)),
            column_definition("char_10", SqlType::Char(10)),
        ],
    );

    create_table(
        &mut storage_with_schema,
        default_schema_name,
        "table_name_2",
        vec![
            column_definition("decimal", SqlType::Decimal),
            column_definition("var_char_20", SqlType::VarChar(20)),
        ],
    );

    assert_eq!(
        storage_with_schema
            .select_all_from(
                "system",
                "columns",
                vec![
                    "schema_name".to_owned(),
                    "table_name".to_owned(),
                    "column_name".to_owned(),
                    "column_type".to_owned()
                ]
            )
            .expect("no system errors"),
        Ok((
            vec![
                column_definition("schema_name", SqlType::VarChar(100)),
                column_definition("table_name", SqlType::VarChar(100)),
                column_definition("column_name", SqlType::VarChar(100)),
                column_definition("column_type", SqlType::VarChar(100)),
            ],
            vec![
                vec![
                    default_schema_name.to_owned(),
                    "table_name_1".to_owned(),
                    "big_int".to_owned(),
                    "BIGINT".to_owned()
                ],
                vec![
                    default_schema_name.to_owned(),
                    "table_name_1".to_owned(),
                    "char_10".to_owned(),
                    "CHAR (10)".to_owned()
                ],
                vec![
                    default_schema_name.to_owned(),
                    "table_name_2".to_owned(),
                    "decimal".to_owned(),
                    "DECIMAL".to_owned()
                ],
                vec![
                    default_schema_name.to_owned(),
                    "table_name_2".to_owned(),
                    "var_char_20".to_owned(),
                    "VARCHAR (20)".to_owned()
                ],
            ],
        ))
    );

    assert_eq!(
        storage_with_schema
            .drop_table(default_schema_name, "table_name_1")
            .expect("no system errors"),
        Ok(())
    );

    assert_eq!(
        storage_with_schema
            .select_all_from(
                "system",
                "columns",
                vec![
                    "schema_name".to_owned(),
                    "table_name".to_owned(),
                    "column_name".to_owned(),
                    "column_type".to_owned()
                ]
            )
            .expect("no system errors"),
        Ok((
            vec![
                column_definition("schema_name", SqlType::VarChar(100)),
                column_definition("table_name", SqlType::VarChar(100)),
                column_definition("column_name", SqlType::VarChar(100)),
                column_definition("column_type", SqlType::VarChar(100)),
            ],
            vec![
                vec![
                    default_schema_name.to_owned(),
                    "table_name_2".to_owned(),
                    "decimal".to_owned(),
                    "DECIMAL".to_owned()
                ],
                vec![
                    default_schema_name.to_owned(),
                    "table_name_2".to_owned(),
                    "var_char_20".to_owned(),
                    "VARCHAR (20)".to_owned()
                ],
            ],
        ))
    );

    assert_eq!(
        storage_with_schema
            .drop_table(default_schema_name, "table_name_2")
            .expect("no system errors"),
        Ok(())
    );

    assert_eq!(
        storage_with_schema
            .select_all_from(
                "system",
                "columns",
                vec![
                    "schema_name".to_owned(),
                    "table_name".to_owned(),
                    "column_name".to_owned(),
                    "column_type".to_owned()
                ]
            )
            .expect("no system errors"),
        Ok((
            vec![
                column_definition("schema_name", SqlType::VarChar(100)),
                column_definition("table_name", SqlType::VarChar(100)),
                column_definition("column_name", SqlType::VarChar(100)),
                column_definition("column_type", SqlType::VarChar(100)),
            ],
            vec![],
        ))
    );
}
