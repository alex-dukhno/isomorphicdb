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

use crate::{catalog_manager::CatalogManager, ColumnDefinition};
use representation::{Binary, Datum};
use sql_types::SqlType;
use std::path::PathBuf;
use storage::Row;
use tempfile::TempDir;

#[rstest::fixture]
fn persistent() -> (CatalogManager, TempDir) {
    let root_path = tempfile::tempdir().expect("to create temp folder");
    (
        CatalogManager::persistent(PathBuf::from(root_path.path())).expect("to create catalog manager"),
        root_path,
    )
}

#[rstest::rstest]
fn created_schema_is_preserved_after_restart(persistent: (CatalogManager, TempDir)) {
    let (catalog_manager, root_path) = persistent;
    catalog_manager
        .create_schema("schema_name")
        .expect("to create a schema");
    assert!(catalog_manager.schema_exists("schema_name"));

    drop(catalog_manager);

    let catalog_manager = CatalogManager::persistent(root_path.into_path()).expect("to create catalog manager");

    assert!(catalog_manager.schema_exists("schema_name"));
}

#[rstest::rstest]
fn created_table_is_preserved_after_restart(persistent: (CatalogManager, TempDir)) {
    let (catalog_manager, root_path) = persistent;
    catalog_manager
        .create_schema("schema_name")
        .expect("to create a schema");
    catalog_manager
        .create_table(
            "schema_name",
            "table_name",
            &[ColumnDefinition::new("col_test", SqlType::Bool)],
        )
        .expect("to create a table");
    assert!(catalog_manager.table_exists("schema_name", "table_name"));

    drop(catalog_manager);

    let catalog_manager = CatalogManager::persistent(root_path.into_path()).expect("to create catalog manager");

    assert!(catalog_manager.table_exists("schema_name", "table_name"));
    assert_eq!(
        catalog_manager
            .table_columns("schema_name", "table_name")
            .expect("to have a columns"),
        vec![ColumnDefinition::new("col_test", SqlType::Bool)]
    )
}

#[rstest::rstest]
fn stored_data_is_preserved_after_restart(persistent: (CatalogManager, TempDir)) {
    let (catalog_manager, root_path) = persistent;
    catalog_manager
        .create_schema("schema_name")
        .expect("to create a schema");
    catalog_manager
        .create_table(
            "schema_name",
            "table_name",
            &[ColumnDefinition::new("col_test", SqlType::Bool)],
        )
        .expect("to create a table");
    catalog_manager
        .insert_into(
            "schema_name",
            "table_name",
            vec![(
                Binary::pack(&[Datum::from_u64(0)]),
                Binary::pack(&[Datum::from_bool(true)]),
            )],
        )
        .expect("values are inserted");

    assert_eq!(
        catalog_manager
            .table_scan("schema_name", "table_name")
            .expect("to scan a table")
            .map(|item| item.expect("no io error").expect("no platform error"))
            .collect::<Vec<Row>>(),
        vec![(
            Binary::pack(&[Datum::from_u64(0)]),
            Binary::pack(&[Datum::from_bool(true)]),
        )],
    );
    drop(catalog_manager);

    let catalog_manager = CatalogManager::persistent(root_path.into_path()).expect("to create catalog manager");

    assert_eq!(
        catalog_manager
            .table_scan("schema_name", "table_name")
            .expect("to scan a table")
            .map(|item| item.expect("no io error").expect("no platform error"))
            .collect::<Vec<Row>>(),
        vec![(
            Binary::pack(&[Datum::from_u64(0)]),
            Binary::pack(&[Datum::from_bool(true)]),
        )],
    );
}
