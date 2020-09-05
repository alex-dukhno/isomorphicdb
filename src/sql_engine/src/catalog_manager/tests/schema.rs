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
fn create_schemas_with_different_names(catalog_manager: CatalogManager) {
    assert_eq!(catalog_manager.create_schema(SCHEMA_1), Ok(()));
    assert_eq!(catalog_manager.create_schema(SCHEMA_2), Ok(()));
}

#[rstest::rstest]
fn same_table_names_with_different_columns_in_different_schemas(catalog_manager: CatalogManager) {
    catalog_manager.create_schema(SCHEMA_1).expect("schema is created");
    catalog_manager.create_schema(SCHEMA_2).expect("schema is created");

    let schema_1_id = catalog_manager.schema_exists(SCHEMA_1).expect("schema exists");
    catalog_manager
        .create_table(
            schema_1_id,
            "table_name",
            &[ColumnDefinition::new(
                "sn_1_column",
                SqlType::SmallInt(i16::min_value()),
            )],
        )
        .expect("table is created");

    let schema_2_id = catalog_manager.schema_exists(SCHEMA_2).expect("schema exists");
    catalog_manager
        .create_table(
            schema_2_id,
            "table_name",
            &[ColumnDefinition::new("sn_2_column", SqlType::BigInt(i64::min_value()))],
        )
        .expect("table is created");

    let table_1_id = catalog_manager
        .table_exists(SCHEMA_1, "table_name")
        .expect("schema exists")
        .1
        .expect("table exists");

    let table_2_id = catalog_manager
        .table_exists(SCHEMA_2, "table_name")
        .expect("schema exists")
        .1
        .expect("table exists");

    assert_eq!(
        catalog_manager.table_columns(schema_1_id, table_1_id),
        Ok(vec![ColumnDefinition::new(
            "sn_1_column",
            SqlType::SmallInt(i16::min_value())
        )])
    );
    assert_eq!(
        catalog_manager.table_columns(schema_2_id, table_2_id),
        Ok(vec![ColumnDefinition::new(
            "sn_2_column",
            SqlType::BigInt(i64::min_value())
        )])
    );
}

#[rstest::rstest]
fn drop_schema(catalog_manager_with_schema: CatalogManager) {
    let schema_id = catalog_manager_with_schema
        .schema_exists(SCHEMA)
        .expect("schema exists");
    assert_eq!(
        catalog_manager_with_schema
            .drop_schema(schema_id, DropStrategy::Restrict)
            .expect("no system errors"),
        Ok(())
    );
    assert_eq!(catalog_manager_with_schema.create_schema(SCHEMA), Ok(()));
}

#[rstest::rstest]
fn restrict_drop_schema_does_not_drop_schema_with_table(catalog_manager_with_schema: CatalogManager) {
    let schema_id = catalog_manager_with_schema
        .schema_exists(SCHEMA)
        .expect("schema exists");
    catalog_manager_with_schema
        .create_table(schema_id, "table_name", &[])
        .expect("no system errors");
    let schema_id = catalog_manager_with_schema
        .schema_exists(SCHEMA)
        .expect("schema exists");
    assert_eq!(
        catalog_manager_with_schema
            .drop_schema(schema_id, DropStrategy::Restrict)
            .expect("no system errors"),
        Err(DropSchemaError::HasDependentObjects)
    );
}

#[rstest::rstest]
fn cascade_drop_schema_drops_tables_in_it(catalog_manager_with_schema: CatalogManager) {
    let schema_id = catalog_manager_with_schema
        .schema_exists(SCHEMA)
        .expect("schema exists");
    catalog_manager_with_schema
        .create_table(
            schema_id,
            "table_name_1",
            &[ColumnDefinition::new(
                "column_test",
                SqlType::SmallInt(i16::min_value()),
            )],
        )
        .expect("table is created");
    catalog_manager_with_schema
        .create_table(
            schema_id,
            "table_name_2",
            &[ColumnDefinition::new(
                "column_test",
                SqlType::SmallInt(i16::min_value()),
            )],
        )
        .expect("table is created");

    assert_eq!(
        catalog_manager_with_schema
            .drop_schema(schema_id, DropStrategy::Cascade)
            .expect("no system errors"),
        Ok(())
    );
    assert_eq!(catalog_manager_with_schema.create_schema(SCHEMA), Ok(()));
    let schema_id = catalog_manager_with_schema
        .schema_exists(SCHEMA)
        .expect("schema exists");
    assert_eq!(
        catalog_manager_with_schema.create_table(
            schema_id,
            "table_name_1",
            &[ColumnDefinition::new(
                "column_test",
                SqlType::SmallInt(i16::min_value())
            )]
        ),
        Ok(())
    );
    assert_eq!(
        catalog_manager_with_schema.create_table(
            schema_id,
            "table_name_2",
            &[ColumnDefinition::new(
                "column_test",
                SqlType::SmallInt(i16::min_value())
            )]
        ),
        Ok(())
    );
}
