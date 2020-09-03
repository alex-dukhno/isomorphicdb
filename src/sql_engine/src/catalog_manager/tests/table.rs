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
fn create_tables_with_different_names(default_schema_name: &str, storage_with_schema: CatalogManager) {
    assert_eq!(
        storage_with_schema.create_table(
            default_schema_name,
            "table_name_1",
            &[ColumnDefinition::new(
                "column_test",
                SqlType::SmallInt(i16::min_value())
            )]
        ),
        Ok(())
    );
    assert_eq!(
        storage_with_schema.create_table(
            default_schema_name,
            "table_name_2",
            &[ColumnDefinition::new(
                "column_test",
                SqlType::SmallInt(i16::min_value())
            )]
        ),
        Ok(())
    );
}

#[rstest::rstest]
fn create_table_with_the_same_name_in_different_schemas(storage: CatalogManager) {
    storage.create_schema("schema_name_1").expect("schema is created");
    storage.create_schema("schema_name_2").expect("schema is created");
    assert_eq!(
        storage.create_table(
            "schema_name_1",
            "table_name",
            &[ColumnDefinition::new(
                "column_test",
                SqlType::SmallInt(i16::min_value())
            )]
        ),
        Ok(())
    );
    assert_eq!(
        storage.create_table(
            "schema_name_2",
            "table_name",
            &[ColumnDefinition::new(
                "column_test",
                SqlType::SmallInt(i16::min_value())
            )]
        ),
        Ok(())
    );
}

#[rstest::rstest]
fn drop_table(default_schema_name: &str, storage_with_schema: CatalogManager) {
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
    assert_eq!(
        storage_with_schema.drop_table(default_schema_name, "table_name"),
        Ok(())
    );
    assert_eq!(
        storage_with_schema.create_table(
            default_schema_name,
            "table_name",
            &[ColumnDefinition::new(
                "column_test",
                SqlType::SmallInt(i16::min_value())
            )]
        ),
        Ok(())
    );
}

#[rstest::rstest]
fn table_columns_on_empty_table(default_schema_name: &str, storage_with_schema: CatalogManager) {
    let column_names = vec![];
    storage_with_schema
        .create_table(default_schema_name, "table_name", column_names.as_slice())
        .expect("table is created");

    assert_eq!(
        storage_with_schema
            .table_columns(default_schema_name, "table_name")
            .expect("no system errors"),
        vec![]
    );
}
