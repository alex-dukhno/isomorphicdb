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

#[test]
fn insert_into_non_existent_table() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    storage
        .create_schema("schema_name")
        .expect("no system errors")
        .expect("schema is created");
    assert_eq!(
        storage
            .insert_into("schema_name", "not_existed", vec![vec!["123".to_owned()]],)
            .expect("no system errors"),
        Err(OperationOnTableError::TableDoesNotExist)
    );
}

#[test]
fn insert_many_rows_into_table() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    create_table(
        &mut storage,
        "schema_name",
        "table_name",
        vec![("column_test", SqlType::Int2)],
    );
    storage
        .insert_into("schema_name", "table_name", vec![vec!["123".to_owned()]])
        .expect("no system errors")
        .expect("values are inserted");
    storage
        .insert_into("schema_name", "table_name", vec![vec!["456".to_owned()]])
        .expect("no system errors")
        .expect("values are inserted");

    let table_columns = storage
        .table_columns("schema_name", "table_name")
        .expect("no system errors")
        .expect("table has columns")
        .into_iter()
        .map(|(name, _sql_type)| name)
        .collect();

    assert_eq!(
        storage
            .select_all_from("schema_name", "table_name", table_columns)
            .expect("no system errors"),
        Ok((
            vec![("column_test".to_owned(), SqlType::Int2)],
            vec![vec!["123".to_owned()], vec!["456".to_owned()]]
        ))
    );
}

#[test]
fn insert_multiple_rows() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    create_table(
        &mut storage,
        "schema_name",
        "table_name",
        vec![
            ("column_1", SqlType::Int2),
            ("column_2", SqlType::Int2),
            ("column_3", SqlType::Int2),
        ],
    );
    storage
        .insert_into(
            "schema_name",
            "table_name",
            vec![
                vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
                vec!["4".to_owned(), "5".to_owned(), "6".to_owned()],
                vec!["7".to_owned(), "8".to_owned(), "9".to_owned()],
            ],
        )
        .expect("no system errors")
        .expect("values are inserted");

    let table_columns = storage
        .table_columns("schema_name", "table_name")
        .expect("no system errors")
        .expect("table has columns")
        .into_iter()
        .map(|(name, _sql_type)| name)
        .collect();

    assert_eq!(
        storage
            .select_all_from("schema_name", "table_name", table_columns)
            .expect("no system errors"),
        Ok((
            vec![
                ("column_1".to_owned(), SqlType::Int2),
                ("column_2".to_owned(), SqlType::Int2),
                ("column_3".to_owned(), SqlType::Int2)
            ],
            vec![
                vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
                vec!["4".to_owned(), "5".to_owned(), "6".to_owned()],
                vec!["7".to_owned(), "8".to_owned(), "9".to_owned()],
            ],
        ))
    );
}

#[test]
fn insert_row_into_table() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    create_table(
        &mut storage,
        "schema_name",
        "table_name",
        vec![("column_test", SqlType::Int2)],
    );
    assert_eq!(
        storage
            .insert_into("schema_name", "table_name", vec![vec!["123".to_owned()]],)
            .expect("no system errors"),
        Ok(())
    );

    let table_columns = storage
        .table_columns("schema_name", "table_name")
        .expect("no system errors")
        .expect("table has columns")
        .into_iter()
        .map(|(name, _sql_type)| name)
        .collect();

    assert_eq!(
        storage
            .select_all_from("schema_name", "table_name", table_columns)
            .expect("no system errors"),
        Ok((
            vec![("column_test".to_owned(), SqlType::Int2)],
            vec![vec!["123".to_owned()]]
        ))
    );
}
