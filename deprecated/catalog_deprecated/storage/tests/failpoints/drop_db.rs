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

use binary::StorageError;
use common::{scenario, OBJECT, SCHEMA};
use fail::FailScenario;
use storage::{Database, PersistentDatabase};

mod common;

#[rstest::fixture]
fn database() -> PersistentDatabase {
    let root_path = tempfile::tempdir().expect("to create temporary folder");
    let storage = PersistentDatabase::new(root_path.into_path());
    storage
        .create_schema(SCHEMA)
        .expect("no io error")
        .expect("no platform errors");
    storage
        .create_object(SCHEMA, OBJECT)
        .expect("no io error")
        .expect("no platform errors")
        .expect("to create object");
    storage
}

#[rstest::rstest]
fn io_error(database: PersistentDatabase, scenario: FailScenario) {
    fail::cfg("sled-fail-to-drop-db", "return(io)").unwrap();

    assert_eq!(
        database.drop_schema(SCHEMA).expect("no io error"),
        Err(StorageError::CascadeIo(vec![OBJECT.to_owned()]))
    );

    scenario.teardown();
}

#[rstest::rstest]
fn corruption_error(database: PersistentDatabase, scenario: FailScenario) {
    fail::cfg("sled-fail-to-drop-db", "return(corruption)").unwrap();

    assert_eq!(
        database.drop_schema(SCHEMA).expect("no io error"),
        Err(StorageError::Storage)
    );

    scenario.teardown();
}

#[rstest::rstest]
fn reportable_bug(database: PersistentDatabase, scenario: FailScenario) {
    fail::cfg("sled-fail-to-drop-db", "return(bug)").unwrap();

    assert_eq!(
        database.drop_schema(SCHEMA).expect("no io error"),
        Err(StorageError::Storage)
    );

    scenario.teardown();
}

#[rstest::rstest]
fn unsupported_operation_core_structure(database: PersistentDatabase, scenario: FailScenario) {
    fail::cfg("sled-fail-to-drop-db", "return(unsupported(core_structure))").unwrap();

    assert_eq!(database.drop_schema(SCHEMA).expect("no io error"), Ok(true));

    scenario.teardown();
}

#[rstest::rstest]
fn unsupported_operation(database: PersistentDatabase, scenario: FailScenario) {
    fail::cfg("sled-fail-to-drop-db", "return(unsupported)").unwrap();

    assert_eq!(
        database.drop_schema(SCHEMA).expect("no io error"),
        Err(StorageError::Storage)
    );

    scenario.teardown();
}

#[rstest::rstest]
fn collection_not_found(database: PersistentDatabase, scenario: FailScenario) {
    fail::cfg("sled-fail-to-drop-db", "return(collection_not_found)").unwrap();

    assert_eq!(
        database.drop_schema(SCHEMA).expect("no io error"),
        Err(StorageError::Storage)
    );

    scenario.teardown();
}
