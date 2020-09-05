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
fn create_tables_with_different_names(catalog_manager_with_schema: CatalogManager) {
    let schema_id = catalog_manager_with_schema
        .schema_exists(SCHEMA)
        .expect("schema exists");
    assert!(matches!(
        catalog_manager_with_schema.create_table(
            schema_id,
            "table_name_1",
            &[ColumnDefinition::new(
                "column_test",
                SqlType::SmallInt(i16::min_value())
            )]
        ),
        Ok(_)
    ));
    assert!(matches!(
        catalog_manager_with_schema.create_table(
            schema_id,
            "table_name_2",
            &[ColumnDefinition::new(
                "column_test",
                SqlType::SmallInt(i16::min_value())
            )]
        ),
        Ok(_)
    ));
}

#[rstest::rstest]
fn create_table_with_the_same_name_in_different_schemas(catalog_manager: CatalogManager) {
    let schema_1_id = catalog_manager.create_schema(SCHEMA_1).expect("schema is created");
    let schema_2_id = catalog_manager.create_schema(SCHEMA_2).expect("schema is created");

    assert!(matches!(
        catalog_manager.create_table(
            schema_1_id,
            "table_name",
            &[ColumnDefinition::new(
                "column_test",
                SqlType::SmallInt(i16::min_value())
            )]
        ),
        Ok(_)
    ));

    assert!(matches!(
        catalog_manager.create_table(
            schema_2_id,
            "table_name",
            &[ColumnDefinition::new(
                "column_test",
                SqlType::SmallInt(i16::min_value())
            )]
        ),
        Ok(_)
    ));
}

#[rstest::rstest]
fn drop_table(catalog_manager_with_schema: CatalogManager) {
    let schema_id = catalog_manager_with_schema
        .schema_exists(SCHEMA)
        .expect("schema exists");
    let table_id = catalog_manager_with_schema
        .create_table(
            schema_id,
            "table_name",
            &[ColumnDefinition::new(
                "column_test",
                SqlType::SmallInt(i16::min_value()),
            )],
        )
        .expect("table is created");

    assert_eq!(catalog_manager_with_schema.drop_table(schema_id, table_id), Ok(()));
    assert!(matches!(
        catalog_manager_with_schema.create_table(
            schema_id,
            "table_name",
            &[ColumnDefinition::new(
                "column_test",
                SqlType::SmallInt(i16::min_value())
            )]
        ),
        Ok(_)
    ));
}

#[rstest::rstest]
fn table_columns_on_empty_table(catalog_manager_with_schema: CatalogManager) {
    let schema_id = catalog_manager_with_schema
        .schema_exists(SCHEMA)
        .expect("schema exists");
    let column_names = vec![];
    let table_id = catalog_manager_with_schema
        .create_table(schema_id, "table_name", column_names.as_slice())
        .expect("table is created");

    assert_eq!(
        catalog_manager_with_schema
            .table_columns(schema_id, table_id)
            .expect("no system errors"),
        vec![]
    );
}
