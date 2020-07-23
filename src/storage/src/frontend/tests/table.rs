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

#[rstest::rstest]
fn create_tables_with_different_names(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
    assert_eq!(
        storage_with_schema
            .create_table(
                default_schema_name,
                "table_name_1",
                &[column_definition(
                    "column_rstest::rstest",
                    SqlType::SmallInt(i16::min_value())
                )]
            )
            .expect("no system errors"),
        Ok(())
    );
    assert_eq!(
        storage_with_schema
            .create_table(
                default_schema_name,
                "table_name_2",
                &[column_definition(
                    "column_rstest::rstest",
                    SqlType::SmallInt(i16::min_value())
                )]
            )
            .expect("no system errors"),
        Ok(())
    );
}

#[rstest::rstest]
fn create_table_with_the_same_name(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
    create_table(
        &mut storage_with_schema,
        default_schema_name,
        "table_name",
        vec![column_definition(
            "column_rstest::rstest",
            SqlType::SmallInt(i16::min_value()),
        )],
    );

    assert_eq!(
        storage_with_schema
            .create_table(
                default_schema_name,
                "table_name",
                &[column_definition(
                    "column_rstest::rstest",
                    SqlType::SmallInt(i16::min_value())
                )]
            )
            .expect("no system errors"),
        Err(CreateTableError::TableAlreadyExists)
    );
}

#[rstest::rstest]
fn create_table_with_the_same_name_in_different_schemas(mut storage: PersistentStorage) {
    create_schema(&mut storage, "schema_name_1");
    create_schema(&mut storage, "schema_name_2");
    assert_eq!(
        storage
            .create_table(
                "schema_name_1",
                "table_name",
                &[column_definition(
                    "column_rstest::rstest",
                    SqlType::SmallInt(i16::min_value())
                )]
            )
            .expect("no system errors"),
        Ok(())
    );
    assert_eq!(
        storage
            .create_table(
                "schema_name_2",
                "table_name",
                &[column_definition(
                    "column_rstest::rstest",
                    SqlType::SmallInt(i16::min_value())
                )]
            )
            .expect("no system errors"),
        Ok(())
    );
}

#[rstest::rstest]
fn drop_table(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
    create_table(
        &mut storage_with_schema,
        default_schema_name,
        "table_name",
        vec![column_definition(
            "column_rstest::rstest",
            SqlType::SmallInt(i16::min_value()),
        )],
    );
    assert_eq!(
        storage_with_schema
            .drop_table(default_schema_name, "table_name")
            .expect("no system errors"),
        Ok(())
    );
    assert_eq!(
        storage_with_schema
            .create_table(
                default_schema_name,
                "table_name",
                &[column_definition(
                    "column_rstest::rstest",
                    SqlType::SmallInt(i16::min_value())
                )]
            )
            .expect("no system errors"),
        Ok(())
    );
}

#[rstest::rstest]
fn table_columns_on_empty_table(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
    create_table(&mut storage_with_schema, default_schema_name, "table_name", vec![]);

    assert_eq!(
        storage_with_schema
            .table_columns(default_schema_name, "table_name")
            .expect("no system errors"),
        vec![]
    )
}

#[rstest::rstest]
fn drop_not_created_table(default_schema_name: &str, mut storage_with_schema: PersistentStorage) {
    assert_eq!(
        storage_with_schema
            .drop_table(default_schema_name, "not_existed_table")
            .expect("no system errors"),
        Err(DropTableError::TableDoesNotExist)
    );
}

#[rstest::rstest]
fn columns_on_system_table(storage: PersistentStorage) {
    assert_eq!(
        storage.table_columns("system", "columns").expect("no system errors"),
        vec![
            column_definition("schema_name", SqlType::VarChar(100)),
            column_definition("table_name", SqlType::VarChar(100)),
            column_definition("column_name", SqlType::VarChar(100)),
            column_definition("column_type", SqlType::VarChar(100)),
        ]
    )
}
