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
fn update_all_records() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    create_schema_with_table(
        &mut storage,
        "schema_name",
        "table_name",
        vec![("column_test", SqlType::SmallInt)],
    );

    insert_into(&mut storage, "schema_name", "table_name", vec!["123"]);
    insert_into(&mut storage, "schema_name", "table_name", vec!["456"]);
    insert_into(&mut storage, "schema_name", "table_name", vec!["789"]);

    assert_eq!(
        storage
            .update_all(
                "schema_name",
                "table_name",
                vec![("column_test".to_owned(), "567".to_owned())]
            )
            .expect("no system errors"),
        Ok(3)
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
            vec![vec!["567".to_owned()], vec!["567".to_owned()], vec!["567".to_owned()]]
        ))
    );
}

#[test]
fn update_not_existed_table() {
    let mut storage = FrontendStorage::default().expect("no system errors");

    create_schema(&mut storage, "schema_name");

    assert_eq!(
        storage
            .update_all("schema_name", "not_existed", vec![])
            .expect("no system errors"),
        Err(OperationOnTableError::TableDoesNotExist)
    );
}

#[cfg(test)]
mod constraints {
    use super::*;

    #[rstest::fixture]
    fn storage_with_ints_table(mut storage: PersistentStorage) -> PersistentStorage {
        create_schema_with_table(
            &mut storage,
            "schema_name",
            "table_name",
            vec![
                ("column_si", SqlType::SmallInt),
                ("column_i", SqlType::Integer),
                ("column_bi", SqlType::BigInt),
            ],
        );
        storage
    }

    #[rstest::fixture]
    fn storage_with_chars_table(mut storage: PersistentStorage) -> PersistentStorage {
        create_schema_with_table(
            &mut storage,
            "schema_name",
            "table_name",
            vec![("column_c", SqlType::Char(10)), ("column_vc", SqlType::VarChar(10))],
        );
        storage
    }

    #[rstest::rstest]
    fn out_of_range_violation(mut storage_with_ints_table: PersistentStorage) {
        storage_with_ints_table
            .insert_into(
                "schema_name",
                "table_name",
                vec![vec!["100".to_owned(), "100".to_owned(), "100".to_owned()]],
            )
            .expect("no system errors")
            .expect("record inserted");
        assert_eq!(
            storage_with_ints_table
                .update_all(
                    "schema_name",
                    "table_name",
                    vec![
                        ("column_si".to_owned(), "-32769".to_owned()),
                        ("column_i".to_owned(), "100".to_owned()),
                        ("column_bi".to_owned(), "100".to_owned())
                    ]
                )
                .expect("no system errors"),
            Err(constraint_violations(
                ConstraintError::OutOfRange,
                vec![vec![("column_si".to_owned(), SqlType::SmallInt)]]
            ))
        );
    }

    #[rstest::rstest]
    fn not_an_int_violation(mut storage_with_ints_table: PersistentStorage) {
        storage_with_ints_table
            .insert_into(
                "schema_name",
                "table_name",
                vec![vec!["100".to_owned(), "100".to_owned(), "100".to_owned()]],
            )
            .expect("no system errors")
            .expect("record inserted");
        assert_eq!(
            storage_with_ints_table
                .update_all(
                    "schema_name",
                    "table_name",
                    vec![
                        ("column_si".to_owned(), "abc".to_owned()),
                        ("column_i".to_owned(), "100".to_owned()),
                        ("column_bi".to_owned(), "100".to_owned())
                    ]
                )
                .expect("no system errors"),
            Err(constraint_violations(
                ConstraintError::NotAnInt,
                vec![vec![("column_si".to_owned(), SqlType::SmallInt)]]
            ))
        );
    }

    #[rstest::rstest]
    fn value_too_long_violation(mut storage_with_chars_table: PersistentStorage) {
        storage_with_chars_table
            .insert_into(
                "schema_name",
                "table_name",
                vec![vec!["100".to_owned(), "100".to_owned()]],
            )
            .expect("no system errors")
            .expect("record inserted");
        assert_eq!(
            storage_with_chars_table
                .update_all(
                    "schema_name",
                    "table_name",
                    vec![
                        ("column_c".to_owned(), "12345678901".to_owned()),
                        ("column_vc".to_owned(), "100".to_owned())
                    ]
                )
                .expect("no system errors"),
            Err(constraint_violations(
                ConstraintError::ValueTooLong,
                vec![vec![("column_c".to_owned(), SqlType::Char(10))]]
            ))
        );
    }

    #[rstest::rstest]
    fn multiple_columns_violation(mut storage_with_ints_table: PersistentStorage) {
        storage_with_ints_table
            .insert_into(
                "schema_name",
                "table_name",
                vec![vec!["100".to_owned(), "100".to_owned(), "100".to_owned()]],
            )
            .expect("no system errors")
            .expect("records inserted");

        assert_eq!(
            storage_with_ints_table
                .update_all(
                    "schema_name",
                    "table_name",
                    vec![
                        ("column_si".to_owned(), "-32769".to_owned()),
                        ("column_i".to_owned(), "-2147483649".to_owned()),
                        ("column_bi".to_owned(), "100".to_owned())
                    ]
                )
                .expect("no system errors"),
            Err(constraint_violations(
                ConstraintError::OutOfRange,
                vec![vec![
                    ("column_si".to_owned(), SqlType::SmallInt),
                    ("column_i".to_owned(), SqlType::Integer)
                ]]
            ))
        )
    }

    fn constraint_violations(error: ConstraintError, columns: Vec<Vec<(String, SqlType)>>) -> OperationOnTableError {
        let mut map = HashMap::new();
        map.insert(error, columns);
        OperationOnTableError::ConstraintViolation(map)
    }
}
