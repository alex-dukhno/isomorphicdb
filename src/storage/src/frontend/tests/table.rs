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
fn create_tables_with_different_names(mut storage: PersistentStorage) {
    create_schema(&mut storage, "schema_name");

    assert_eq!(
        storage
            .create_table(
                "schema_name",
                "table_name_1",
                vec![("column_rstest::rstest".to_owned(), SqlType::SmallInt(i16::min_value()))]
            )
            .expect("no system errors"),
        Ok(())
    );
    assert_eq!(
        storage
            .create_table(
                "schema_name",
                "table_name_2",
                vec![("column_rstest::rstest".to_owned(), SqlType::SmallInt(i16::min_value()))]
            )
            .expect("no system errors"),
        Ok(())
    );
}

#[rstest::rstest]
fn create_table_with_the_same_name(mut storage: PersistentStorage) {
    create_schema_with_table(
        &mut storage,
        "schema_name",
        "table_name",
        vec![("column_rstest::rstest", SqlType::SmallInt(i16::min_value()))],
    );

    assert_eq!(
        storage
            .create_table(
                "schema_name",
                "table_name",
                vec![("column_rstest::rstest".to_owned(), SqlType::SmallInt(i16::min_value()))]
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
                vec![("column_rstest::rstest".to_owned(), SqlType::SmallInt(i16::min_value()))]
            )
            .expect("no system errors"),
        Ok(())
    );
    assert_eq!(
        storage
            .create_table(
                "schema_name_2",
                "table_name",
                vec![("column_rstest::rstest".to_owned(), SqlType::SmallInt(i16::min_value()))]
            )
            .expect("no system errors"),
        Ok(())
    );
}

#[rstest::rstest]
fn drop_table(mut storage: PersistentStorage) {
    create_schema_with_table(
        &mut storage,
        "schema_name",
        "table_name",
        vec![("column_rstest::rstest", SqlType::SmallInt(i16::min_value()))],
    );
    assert_eq!(
        storage
            .drop_table("schema_name", "table_name")
            .expect("no system errors"),
        Ok(())
    );
    assert_eq!(
        storage
            .create_table(
                "schema_name",
                "table_name",
                vec![("column_rstest::rstest".to_owned(), SqlType::SmallInt(i16::min_value()))]
            )
            .expect("no system errors"),
        Ok(())
    );
}

#[rstest::rstest]
fn table_columns_on_empty_table(mut storage: PersistentStorage) {
    create_schema_with_table(&mut storage, "schema_name", "table_name", vec![]);

    assert_eq!(
        storage
            .table_columns("schema_name", "table_name")
            .expect("no system errors"),
        vec![]
    )
}

#[rstest::rstest]
fn drop_not_created_table(mut storage: PersistentStorage) {
    create_schema(&mut storage, "schema_name");
    assert_eq!(
        storage
            .drop_table("schema_name", "not_existed_table")
            .expect("no system errors"),
        Err(DropTableError::TableDoesNotExist)
    );
}
