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
fn create_tables_with_different_names() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    storage
        .create_schema("schema_name")
        .expect("no system errors")
        .expect("schema is created");

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

#[test]
fn create_table_with_the_same_name() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    create_table(&mut storage, "schema_name", "table_name", vec!["column_test"]);

    assert_eq!(
        storage
            .create_table("schema_name", "table_name", vec!["column_test".to_owned()])
            .expect("no system errors"),
        Err(CreateTableError::TableAlreadyExists)
    );
}

#[test]
fn create_table_with_the_same_name_in_different_schemas() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    storage
        .create_schema("schema_name_1")
        .expect("no system errors")
        .expect("schema is created");
    storage
        .create_schema("schema_name_2")
        .expect("no system errors")
        .expect("schema is created");
    assert_eq!(
        storage
            .create_table("schema_name_1", "table_name", vec!["column_test".to_owned()])
            .expect("no system errors"),
        Ok(())
    );
    assert_eq!(
        storage
            .create_table("schema_name_2", "table_name", vec!["column_test".to_owned()])
            .expect("no system errors"),
        Ok(())
    );
}

#[test]
fn drop_table() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    create_table(&mut storage, "schema_name", "table_name", vec!["column_test"]);
    assert_eq!(
        storage
            .drop_table("schema_name", "table_name")
            .expect("no system errors"),
        Ok(())
    );
    assert_eq!(
        storage
            .create_table("schema_name", "table_name", vec!["column_test".to_owned()])
            .expect("no system errors"),
        Ok(())
    );
}

#[test]
fn drop_not_created_table() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    storage
        .create_schema("schema_name")
        .expect("no system errors")
        .expect("schema is created");
    assert_eq!(
        storage
            .drop_table("schema_name", "not_existed_table")
            .expect("no system errors"),
        Err(DropTableError::TableDoesNotExist)
    );
}
