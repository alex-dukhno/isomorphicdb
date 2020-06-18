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

#[test]
fn create_schemas_with_different_names() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    assert_eq!(storage.create_schema("schema_1").expect("no system errors"), Ok(()));
    assert_eq!(storage.create_schema("schema_2").expect("no system errors"), Ok(()));
}

#[test]
fn create_schema_with_existing_name() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    storage
        .create_schema("schema_name")
        .expect("no system errors")
        .expect("schema is created");

    assert_eq!(
        storage.create_schema("schema_name").expect("no system errors"),
        Err(SchemaAlreadyExists)
    );
}

#[test]
fn drop_schema() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    storage
        .create_schema("schema_name")
        .expect("no system errors")
        .expect("schema is created");

    assert_eq!(storage.drop_schema("schema_name").expect("no system errors"), Ok(()));
    assert_eq!(storage.create_schema("schema_name").expect("no system errors"), Ok(()));
}

#[test]
fn drop_schema_that_was_not_created() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    assert_eq!(
        storage.drop_schema("does_not_exists").expect("no system errors"),
        Err(SchemaDoesNotExist)
    );
}

#[test]
#[ignore]
// TODO store tables and columns into "system" schema
//      but simple select by predicate has to be implemented
fn drop_schema_drops_tables_in_it() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    storage
        .create_schema("schema_name")
        .expect("no system errors")
        .expect("schema is created");
    storage
        .create_table("schema_name", "table_name_1", vec!["column_test".to_owned()])
        .expect("no system errors")
        .expect("values are inserted");
    storage
        .create_table("schema_name", "table_name_2", vec!["column_test".to_owned()])
        .expect("no system errors")
        .expect("values are inserted");

    assert_eq!(storage.drop_schema("schema_name").expect("no system errors"), Ok(()));
    assert_eq!(storage.create_schema("schema_name").expect("no system errors"), Ok(()));
    assert_eq!(
        storage
            .create_table("schema_name", "table_name_1", vec!["column_test".to_owned()])
            .expect("no system errors"),
        Ok(())
    );
    assert_eq!(
        storage
            .create_table("schema_name", "table_name_2", vec!["column_test".to_owned()])
            .expect("no system errors"),
        Ok(())
    );
}
