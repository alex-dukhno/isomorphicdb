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

#[rstest::rstest]
fn insert_into_non_existent_schema(mut storage: PersistentStorage) {
    assert_eq!(
        storage
            .insert_into("non_existent", "not_existed", vec![], vec![vec!["123".to_owned()]])
            .expect("no system errors"),
        Err(OperationOnTableError::SchemaDoesNotExist)
    );
}

#[rstest::rstest]
fn insert_into_non_existent_table(mut storage: PersistentStorage) {
    create_schema(&mut storage, "schema_name");

    assert_eq!(
        storage
            .insert_into("schema_name", "not_existed", vec![], vec![vec!["123".to_owned()]])
            .expect("no system errors"),
        Err(OperationOnTableError::TableDoesNotExist)
    );
}

#[rstest::rstest]
fn insert_many_rows_into_table(mut storage: PersistentStorage) {
    create_schema_with_table(
        &mut storage,
        "schema_name",
        "table_name",
        vec![("column_test", SqlType::SmallInt(u16::min_value()))],
    );

    insert_into(&mut storage, "schema_name", "table_name", vec![], vec!["123"]);
    insert_into(&mut storage, "schema_name", "table_name", vec![], vec!["456"]);

    let table_columns = storage
        .table_columns("schema_name", "table_name")
        .expect("no system errors")
        .into_iter()
        .map(|(name, _sql_type)| name)
        .collect();

    assert_eq!(
        storage
            .select_all_from("schema_name", "table_name", table_columns)
            .expect("no system errors"),
        Ok((
            vec![("column_test".to_owned(), SqlType::SmallInt(u16::min_value()))],
            vec![vec!["123".to_owned()], vec!["456".to_owned()]]
        ))
    );
}

#[rstest::rstest]
fn insert_multiple_values_rows(mut storage: PersistentStorage) {
    create_schema_with_table(
        &mut storage,
        "schema_name",
        "table_name",
        vec![
            ("column_1", SqlType::SmallInt(u16::min_value())),
            ("column_2", SqlType::SmallInt(u16::min_value())),
            ("column_3", SqlType::SmallInt(u16::min_value())),
        ],
    );

    insert_into(&mut storage, "schema_name", "table_name", vec![], vec!["1", "2", "3"]);
    insert_into(&mut storage, "schema_name", "table_name", vec![], vec!["4", "5", "6"]);
    insert_into(&mut storage, "schema_name", "table_name", vec![], vec!["7", "8", "9"]);

    let table_columns = storage
        .table_columns("schema_name", "table_name")
        .expect("no system errors")
        .into_iter()
        .map(|(name, _sql_type)| name)
        .collect();

    assert_eq!(
        storage
            .select_all_from("schema_name", "table_name", table_columns)
            .expect("no system errors"),
        Ok((
            vec![
                ("column_1".to_owned(), SqlType::SmallInt(u16::min_value())),
                ("column_2".to_owned(), SqlType::SmallInt(u16::min_value())),
                ("column_3".to_owned(), SqlType::SmallInt(u16::min_value()))
            ],
            vec![
                vec!["1".to_owned(), "2".to_owned(), "3".to_owned()],
                vec!["4".to_owned(), "5".to_owned(), "6".to_owned()],
                vec!["7".to_owned(), "8".to_owned(), "9".to_owned()],
            ],
        ))
    );
}

#[rstest::rstest]
fn insert_named_columns(mut storage: PersistentStorage) {
    create_schema_with_table(
        &mut storage,
        "schema_name",
        "table_name",
        vec![
            ("column_1", SqlType::SmallInt(u16::min_value())),
            ("column_2", SqlType::Char(10)),
            ("column_3", SqlType::BigInt(u64::min_value())),
        ],
    );

    let columns = vec!["column_3", "column_2", "column_1"];

    insert_into(
        &mut storage,
        "schema_name",
        "table_name",
        columns.clone(),
        vec!["1", "2", "3"],
    );
    insert_into(
        &mut storage,
        "schema_name",
        "table_name",
        columns.clone(),
        vec!["4", "5", "6"],
    );
    insert_into(
        &mut storage,
        "schema_name",
        "table_name",
        columns.clone(),
        vec!["7", "8", "9"],
    );

    let table_columns = storage
        .table_columns("schema_name", "table_name")
        .expect("no system errors")
        .into_iter()
        .map(|(name, _sql_type)| name)
        .collect();

    assert_eq!(
        storage
            .select_all_from("schema_name", "table_name", table_columns)
            .expect("no system errors"),
        Ok((
            vec![
                ("column_1".to_owned(), SqlType::SmallInt(u16::min_value())),
                ("column_2".to_owned(), SqlType::Char(10)),
                ("column_3".to_owned(), SqlType::BigInt(u64::min_value()))
            ],
            vec![
                vec!["3".to_owned(), "2".to_owned(), "1".to_owned()],
                vec!["6".to_owned(), "5".to_owned(), "4".to_owned()],
                vec!["9".to_owned(), "8".to_owned(), "7".to_owned()],
            ],
        ))
    );
}

#[rstest::rstest]
fn insert_named_not_existed_column(mut storage: PersistentStorage) {
    create_schema_with_table(
        &mut storage,
        "schema_name",
        "table_name",
        vec![
            ("column_1", SqlType::SmallInt(u16::min_value())),
            ("column_2", SqlType::Char(10)),
            ("column_3", SqlType::BigInt(u64::min_value())),
        ],
    );

    let columns = vec![
        "column_3".to_owned(),
        "column_2".to_owned(),
        "column_1".to_owned(),
        "not_existed".to_owned(),
    ];

    assert_eq!(
        storage
            .insert_into(
                "schema_name",
                "table_name",
                columns,
                vec![vec!["1".to_owned(), "2".to_owned(), "3".to_owned(), "4".to_owned()]],
            )
            .expect("no system errors"),
        Err(OperationOnTableError::ColumnDoesNotExist(
            vec!["not_existed".to_owned()]
        ))
    )
}

#[rstest::rstest]
fn insert_row_into_table(mut storage: PersistentStorage) {
    create_schema_with_table(
        &mut storage,
        "schema_name",
        "table_name",
        vec![("column_test", SqlType::SmallInt(u16::min_value()))],
    );
    assert_eq!(
        storage
            .insert_into("schema_name", "table_name", vec![], vec![vec!["123".to_owned()]])
            .expect("no system errors"),
        Ok(())
    );

    let table_columns = storage
        .table_columns("schema_name", "table_name")
        .expect("no system errors")
        .into_iter()
        .map(|(name, _sql_type)| name)
        .collect();

    assert_eq!(
        storage
            .select_all_from("schema_name", "table_name", table_columns)
            .expect("no system errors"),
        Ok((
            vec![("column_test".to_owned(), SqlType::SmallInt(u16::min_value()))],
            vec![vec!["123".to_owned()]]
        ))
    );
}

#[rstest::rstest]
fn insert_too_many_expressions(mut storage: PersistentStorage) {
    create_schema_with_table(
        &mut storage,
        "schema_name",
        "table_name",
        vec![
            ("column_1", SqlType::SmallInt),
            ("column_2", SqlType::Char(10)),
            ("column_3", SqlType::BigInt),
        ],
    );

    let columns = vec![];

    assert_eq!(
        storage
            .insert_into(
                "schema_name",
                "table_name",
                columns,
                vec![vec!["1".to_owned(), "2".to_owned(), "3".to_owned(), "4".to_owned()]],
            )
            .expect("no system errors"),
        Err(OperationOnTableError::InsertTooManyExpressions)
    );

    let table_columns = storage
        .table_columns("schema_name", "table_name")
        .expect("no system errors")
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
                ("column_2".to_owned(), SqlType::Char(10)),
                ("column_3".to_owned(), SqlType::BigInt),
            ],
            vec![]
        ))
    );
}

#[rstest::rstest]
fn insert_too_many_expressions_labeled(mut storage: PersistentStorage) {
    create_schema_with_table(
        &mut storage,
        "schema_name",
        "table_name",
        vec![
            ("column_1", SqlType::SmallInt),
            ("column_2", SqlType::Char(10)),
            ("column_3", SqlType::BigInt),
        ],
    );

    let columns = vec!["column_3".to_owned(), "column_2".to_owned(), "column_1".to_owned()];

    assert_eq!(
        storage
            .insert_into(
                "schema_name",
                "table_name",
                columns,
                vec![vec!["1".to_owned(), "2".to_owned(), "3".to_owned(), "4".to_owned()]],
            )
            .expect("no system errors"),
        Err(OperationOnTableError::InsertTooManyExpressions)
    );

    let table_columns = storage
        .table_columns("schema_name", "table_name")
        .expect("no system errors")
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
                ("column_2".to_owned(), SqlType::Char(10)),
                ("column_3".to_owned(), SqlType::BigInt),
            ],
            vec![]
        ))
    );
}

#[cfg(test)]
mod constraints {
    use super::*;
    use sql_types::ConstraintError;

    #[rstest::fixture]
    fn storage_with_ints_table(mut storage: PersistentStorage) -> PersistentStorage {
        create_schema_with_table(
            &mut storage,
            "schema_name",
            "table_name",
            vec![
                ("column_si", SqlType::SmallInt(u16::min_value())),
                ("column_i", SqlType::Integer(u32::min_value())),
                ("column_bi", SqlType::BigInt(u64::min_value())),
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
        assert_eq!(
            storage_with_ints_table
                .insert_into(
                    "schema_name",
                    "table_name",
                    vec![],
                    vec![vec!["-32769".to_owned(), "100".to_owned(), "100".to_owned()]],
                )
                .expect("no system errors"),
            Err(OperationOnTableError::ConstraintViolations(vec![(
                ConstraintError::OutOfRange,
                "column_si".to_owned(),
                SqlType::SmallInt(u16::min_value())
            )]))
        );
    }

    #[rstest::rstest]
    fn not_an_int_violation(mut storage_with_ints_table: PersistentStorage) {
        assert_eq!(
            storage_with_ints_table
                .insert_into(
                    "schema_name",
                    "table_name",
                    vec![],
                    vec![vec!["abc".to_owned(), "100".to_owned(), "100".to_owned()]],
                )
                .expect("no system errors"),
            Err(OperationOnTableError::ConstraintViolations(vec![(
                ConstraintError::NotAnInt,
                "column_si".to_owned(),
                SqlType::SmallInt(u16::min_value())
            )]))
        )
    }

    #[rstest::rstest]
    fn value_too_long_violation(mut storage_with_chars_table: PersistentStorage) {
        assert_eq!(
            storage_with_chars_table
                .insert_into(
                    "schema_name",
                    "table_name",
                    vec![],
                    vec![vec!["12345678901".to_owned(), "100".to_owned()]],
                )
                .expect("no system errors"),
            Err(OperationOnTableError::ConstraintViolations(vec![(
                ConstraintError::ValueTooLong,
                "column_c".to_owned(),
                SqlType::Char(10)
            )]))
        )
    }

    #[rstest::rstest]
    fn multiple_columns_single_row_violation(mut storage_with_ints_table: PersistentStorage) {
        assert_eq!(
            storage_with_ints_table
                .insert_into(
                    "schema_name",
                    "table_name",
                    vec![],
                    vec![vec!["-32769".to_owned(), "-2147483649".to_owned(), "100".to_owned()]],
                )
                .expect("no system errors"),
            Err(OperationOnTableError::ConstraintViolations(vec![
                (ConstraintError::OutOfRange, "column_si".to_owned(), SqlType::SmallInt(u16::min_value())),
                (ConstraintError::OutOfRange, "column_i".to_owned(), SqlType::Integer(u32::min_value()))
            ]))
        )
    }

    #[rstest::rstest]
    fn multiple_columns_multiple_row_violation(mut storage_with_ints_table: PersistentStorage) {
        assert_eq!(
            storage_with_ints_table
                .insert_into(
                    "schema_name",
                    "table_name",
                    vec![],
                    vec![
                        vec!["-32769".to_owned(), "-2147483649".to_owned(), "100".to_owned()],
                        vec![
                            "100".to_owned(),
                            "-2147483649".to_owned(),
                            "-9223372036854775809".to_owned()
                        ],
                    ],
                )
                .expect("no system errors"),
            Err(OperationOnTableError::ConstraintViolations(vec![
                (ConstraintError::OutOfRange, "column_si".to_owned(), SqlType::SmallInt(u16::min_value())),
                (ConstraintError::OutOfRange, "column_i".to_owned(), SqlType::SmallInt(u32::min_value())),
                (ConstraintError::OutOfRange, "column_i".to_owned(), SqlType::SmallInt(u32::min_value())),
                (ConstraintError::OutOfRange, "column_bi".to_owned(), SqlType::BigInt(u64::min_value())),
            ]))
        )
    }
}
