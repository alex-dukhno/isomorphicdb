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

    create_schema(&mut storage, "schema_name");

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

    create_schema_with_table(
        &mut storage,
        "schema_name",
        "table_name",
        vec![("column_test", SqlType::SmallInt)],
    );

    insert_into(&mut storage, "schema_name", "table_name", vec!["123"]);
    insert_into(&mut storage, "schema_name", "table_name", vec!["456"]);

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
            vec![("column_test".to_owned(), SqlType::SmallInt)],
            vec![vec!["123".to_owned()], vec!["456".to_owned()]]
        ))
    );
}

#[test]
fn insert_multiple_values_rows() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    create_schema_with_table(
        &mut storage,
        "schema_name",
        "table_name",
        vec![
            ("column_1", SqlType::SmallInt),
            ("column_2", SqlType::SmallInt),
            ("column_3", SqlType::SmallInt),
        ],
    );

    insert_into(&mut storage, "schema_name", "table_name", vec!["1", "2", "3"]);
    insert_into(&mut storage, "schema_name", "table_name", vec!["4", "5", "6"]);
    insert_into(&mut storage, "schema_name", "table_name", vec!["7", "8", "9"]);

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
                ("column_1".to_owned(), SqlType::SmallInt),
                ("column_2".to_owned(), SqlType::SmallInt),
                ("column_3".to_owned(), SqlType::SmallInt)
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

    create_schema_with_table(
        &mut storage,
        "schema_name",
        "table_name",
        vec![("column_test", SqlType::SmallInt)],
    );
    assert_eq!(
        storage
            .insert_into("schema_name", "table_name", vec![vec!["123".to_owned()]])
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
            vec![("column_test".to_owned(), SqlType::SmallInt)],
            vec![vec!["123".to_owned()]]
        ))
    );
}

#[test]
fn insert_values_in_limit() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    create_schema_with_table(
        &mut storage,
        "schema_name",
        "table_name",
        vec![("column_1", SqlType::SmallInt), ("column_2", SqlType::Integer)],
    );

    assert_eq!(
        storage
            .insert_into(
                "schema_name",
                "table_name",
                vec![vec!["100".to_owned(), "100".to_owned()]]
            )
            .expect("no system errors"),
        Ok(())
    )
}

// #[ignore]
#[test]
fn insert_values_less_then_minimal_limit() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    create_schema_with_table(
        &mut storage,
        "schema_name",
        "table_name",
        vec![("column_1", SqlType::SmallInt), ("column_2", SqlType::Integer)],
    );

    assert_eq!(
        storage
            .insert_into(
                "schema_name",
                "table_name",
                vec![vec!["-32769".to_owned(), "100".to_owned()]]
            )
            .expect("no system errors"),
        Err(OperationOnTableError::ColumnOutOfRange(vec![(
            "column_1".to_owned(),
            SqlType::SmallInt
        )]))
    )
}
