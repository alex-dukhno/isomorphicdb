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
use binary::Binary;
use repr::Datum;
use sql_model::sql_types::SqlType;
use std::path::PathBuf;
use storage::Row;
use tempfile::TempDir;

type Persistent = DataManager<PersistentDatabase>;

#[rstest::fixture]
fn persistent() -> (Persistent, TempDir) {
    let root_path = tempfile::tempdir().expect("to create temp folder");
    (
        Persistent::persistent(PathBuf::from(root_path.path())).expect("to create catalog manager"),
        root_path,
    )
}

#[rstest::rstest]
fn created_schema_is_preserved_after_restart(persistent: (Persistent, TempDir)) {
    let (data_manager, root_path) = persistent;
    data_manager.create_schema(SCHEMA).expect("to create a schema");
    assert!(matches!(data_manager.schema_exists(&SCHEMA), Some(_)));

    drop(data_manager);

    let data_manager = Persistent::persistent(root_path.into_path()).expect("to create catalog manager");

    assert!(matches!(data_manager.schema_exists(&SCHEMA), Some(_)));
}

#[rstest::rstest]
fn created_table_is_preserved_after_restart(persistent: (Persistent, TempDir)) {
    let (data_manager, root_path) = persistent;
    let schema_id = data_manager.create_schema(&SCHEMA).expect("to create a schema");
    let table_id = data_manager
        .create_table(
            schema_id,
            "table_name",
            &[ColumnDefinition::new("col_test", SqlType::Bool)],
        )
        .expect("to create a table");
    assert!(matches!(
        data_manager.table_exists(&SCHEMA, &"table_name"),
        Some((_, Some(_)))
    ));

    drop(data_manager);

    let data_manager = Persistent::persistent(root_path.into_path()).expect("to create catalog manager");

    assert!(matches!(
        data_manager.table_exists(&SCHEMA, &"table_name"),
        Some((_, Some(_)))
    ));
    assert_eq!(
        data_manager
            .table_columns(&Box::new((schema_id, table_id)))
            .expect("to have a columns"),
        vec![ColumnDefinition::new("col_test", SqlType::Bool)]
    )
}

#[rstest::rstest]
fn stored_data_is_preserved_after_restart(persistent: (Persistent, TempDir)) {
    let (data_manager, root_path) = persistent;
    let schema_id = data_manager.create_schema(SCHEMA).expect("to create a schema");
    let table_id = data_manager
        .create_table(
            schema_id,
            "table_name",
            &[ColumnDefinition::new("col_test", SqlType::Bool)],
        )
        .expect("to create a table");
    data_manager
        .write_into(
            &Box::new((schema_id, table_id)),
            vec![(
                Binary::pack(&[Datum::from_u64(0)]),
                Binary::pack(&[Datum::from_bool(true)]),
            )],
        )
        .expect("values are inserted");

    assert_eq!(
        data_manager
            .full_scan(&Box::new((schema_id, table_id)))
            .expect("to scan a table")
            .map(|item| item.expect("no io error").expect("no platform error"))
            .collect::<Vec<Row>>(),
        vec![(
            Binary::pack(&[Datum::from_u64(0)]),
            Binary::pack(&[Datum::from_bool(true)]),
        )],
    );
    drop(data_manager);

    let data_manager = Persistent::persistent(root_path.into_path()).expect("to create catalog manager");

    assert_eq!(
        data_manager
            .full_scan(&Box::new((schema_id, table_id)))
            .expect("to scan a table")
            .map(|item| item.expect("no io error").expect("no platform error"))
            .collect::<Vec<Row>>(),
        vec![(
            Binary::pack(&[Datum::from_u64(0)]),
            Binary::pack(&[Datum::from_bool(true)]),
        )],
    );
}
