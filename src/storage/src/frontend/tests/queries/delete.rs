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
fn delete_all_from_not_existed_table() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    storage
        .create_schema("schema_name")
        .expect("no system errors")
        .expect("schema is created");

    assert_eq!(
        storage
            .delete_all_from("schema_name", "table_name")
            .expect("no system errors"),
        Err(OperationOnTableError::TableDoesNotExist)
    );
}

#[test]
fn delete_all_from_table() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    create_table(&mut storage, "schema_name", "table_name", vec!["column_test"]);
    storage
        .insert_into("schema_name", "table_name", vec![vec!["123".to_owned()]])
        .expect("no system errors")
        .expect("values are inserted");
    storage
        .insert_into("schema_name", "table_name", vec![vec!["456".to_owned()]])
        .expect("no system errors")
        .expect("values are inserted");
    storage
        .insert_into("schema_name", "table_name", vec![vec!["789".to_owned()]])
        .expect("no system errors")
        .expect("values are inserted");

    assert_eq!(
        storage
            .delete_all_from("schema_name", "table_name")
            .expect("no system errors"),
        Ok(3)
    );

    let table_columns = storage
        .table_columns("schema_name", "table_name")
        .expect("no system errors")
        .expect("table has columns");

    assert_eq!(
        storage
            .select_all_from("schema_name", "table_name", table_columns)
            .expect("no system errors"),
        Ok((vec!["column_test".to_owned()], vec![]))
    );
}
