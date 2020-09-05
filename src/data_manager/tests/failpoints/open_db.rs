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

use fail::FailScenario;

use common::{scenario, SCHEMA};
use data_manager::{Database, StorageError};
use data_manager::persistent::PersistentDatabase;

mod common;

#[rstest::fixture]
fn database() -> PersistentDatabase {
    let root_path = tempfile::tempdir().expect("to create temporary folder");
    PersistentDatabase::new(root_path.into_path())
}

#[rstest::rstest]
fn io_error(database: PersistentDatabase, scenario: FailScenario) {
    fail::cfg("sled-fail-to-open-db", "return(io)").unwrap();

    assert!(matches!(database.create_schema(SCHEMA), Err(_)));

    scenario.teardown();
}

#[rstest::rstest]
fn corruption_error(database: PersistentDatabase, scenario: FailScenario) {
    fail::cfg("sled-fail-to-open-db", "return(corruption)").unwrap();

    assert_eq!(
        database.create_schema(SCHEMA).expect("no io error"),
        Err(StorageError::Storage)
    );

    scenario.teardown();
}

#[rstest::rstest]
fn reportable_bug(database: PersistentDatabase, scenario: FailScenario) {
    fail::cfg("sled-fail-to-open-db", "return(bug)").unwrap();

    assert_eq!(
        database.create_schema(SCHEMA).expect("no io error"),
        Err(StorageError::Storage)
    );

    scenario.teardown();
}

#[rstest::rstest]
fn unsupported_operation(database: PersistentDatabase, scenario: FailScenario) {
    fail::cfg("sled-fail-to-open-db", "return(unsupported)").unwrap();

    assert_eq!(
        database.create_schema(SCHEMA).expect("no io error"),
        Err(StorageError::Storage)
    );

    scenario.teardown();
}

#[rstest::rstest]
fn collection_not_found(database: PersistentDatabase, scenario: FailScenario) {
    fail::cfg("sled-fail-to-open-db", "return(collection_not_found)").unwrap();

    assert_eq!(
        database.create_schema(SCHEMA).expect("no io error"),
        Err(StorageError::Storage)
    );

    scenario.teardown();
}
