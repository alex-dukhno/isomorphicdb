// Copyright 2020 - 2021 Alex Dukhno
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

#[rstest::rstest]
fn insert_value_in_non_existent_column(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (column_test smallint);",
        vec![Outbound::TableCreated, Outbound::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "insert into schema_name.table_name (non_existent) values (123);",
        vec![
            QueryError::column_does_not_exist("non_existent").into(),
            Outbound::ReadyForQuery,
        ],
    );

    txn.commit();
}

#[rstest::rstest]
fn insert_and_select_single_row(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (column_test smallint);",
        vec![Outbound::TableCreated, Outbound::ReadyForQuery],
    );

    assert_statement(
        &txn,
        "insert into schema_name.table_name values (123);",
        vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select * from schema_name.table_name;",
        vec![
            Outbound::RowDescription(vec![("column_test".to_owned(), SMALLINT)]),
            Outbound::DataRow(vec![small_int(123)]),
            Outbound::RecordsSelected(1),
            Outbound::ReadyForQuery,
        ],
    );
}

#[rstest::rstest]
fn insert_and_select_multiple_rows(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (column_test smallint);",
        vec![Outbound::TableCreated, Outbound::ReadyForQuery],
    );

    assert_statement(
        &txn,
        "insert into schema_name.table_name values (123);",
        vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "insert into schema_name.table_name values (456);",
        vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select * from schema_name.table_name;",
        vec![
            Outbound::RowDescription(vec![("column_test".to_owned(), SMALLINT)]),
            Outbound::DataRow(vec![small_int(123)]),
            Outbound::DataRow(vec![small_int(456)]),
            Outbound::RecordsSelected(2),
            Outbound::ReadyForQuery,
        ],
    );

    txn.commit();
}

#[rstest::rstest]
fn insert_and_select_named_columns(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (col1 smallint, col2 smallint, col3 smallint);",
        vec![Outbound::TableCreated, Outbound::ReadyForQuery],
    );

    assert_statement(
        &txn,
        "insert into schema_name.table_name (col2, col3, col1) values (1, 2, 3), (4, 5, 6);",
        vec![Outbound::RecordsInserted(2), Outbound::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select * from schema_name.table_name;",
        vec![
            Outbound::RowDescription(vec![
                ("col1".to_owned(), SMALLINT),
                ("col2".to_owned(), SMALLINT),
                ("col3".to_owned(), SMALLINT),
            ]),
            Outbound::DataRow(vec![small_int(3), small_int(1), small_int(2)]),
            Outbound::DataRow(vec![small_int(6), small_int(4), small_int(5)]),
            Outbound::RecordsSelected(2),
            Outbound::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[rstest::rstest]
fn insert_multiple_rows(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);",
        vec![Outbound::TableCreated, Outbound::ReadyForQuery],
    );

    assert_statement(
        &txn,
        "insert into schema_name.table_name values (1, 4, 7), (2, 5, 8), (3, 6, 9);",
        vec![Outbound::RecordsInserted(3), Outbound::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select * from schema_name.table_name;",
        vec![
            Outbound::RowDescription(vec![
                ("column_1".to_owned(), SMALLINT),
                ("column_2".to_owned(), SMALLINT),
                ("column_3".to_owned(), SMALLINT),
            ]),
            Outbound::DataRow(vec![small_int(1), small_int(4), small_int(7)]),
            Outbound::DataRow(vec![small_int(2), small_int(5), small_int(8)]),
            Outbound::DataRow(vec![small_int(3), small_int(6), small_int(9)]),
            Outbound::RecordsSelected(3),
            Outbound::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[rstest::rstest]
fn insert_and_select_different_integer_types(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (column_si smallint, column_i integer, column_bi bigint);",
        vec![Outbound::TableCreated, Outbound::ReadyForQuery],
    );

    assert_statement(
        &txn,
        "insert into schema_name.table_name values(-32768, -2147483648, -9223372036854775808);",
        vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "insert into schema_name.table_name values(32767, 2147483647, 9223372036854775807);",
        vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select * from schema_name.table_name;",
        vec![
            Outbound::RowDescription(vec![
                ("column_si".to_owned(), SMALLINT),
                ("column_i".to_owned(), INT),
                ("column_bi".to_owned(), BIGINT),
            ]),
            Outbound::DataRow(vec![small_int(i16::MIN), integer(i32::MIN), big_int(i64::MIN)]),
            Outbound::DataRow(vec![small_int(i16::MAX), integer(i32::MAX), big_int(i64::MAX)]),
            Outbound::RecordsSelected(2),
            Outbound::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[rstest::rstest]
fn insert_and_select_different_character_types(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (column_c char(10), column_vc varchar(10));",
        vec![Outbound::TableCreated, Outbound::ReadyForQuery],
    );

    assert_statement(
        &txn,
        "insert into schema_name.table_name values('12345abcde', '12345abcde');",
        vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "insert into schema_name.table_name values('12345abcde', 'abcde');",
        vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select * from schema_name.table_name;",
        vec![
            Outbound::RowDescription(vec![("column_c".to_owned(), CHAR), ("column_vc".to_owned(), VARCHAR)]),
            Outbound::DataRow(vec![string("12345abcde"), string("12345abcde")]),
            Outbound::DataRow(vec![string("12345abcde"), string("abcde")]),
            Outbound::RecordsSelected(2),
            Outbound::ReadyForQuery,
        ],
    );
}

#[rstest::rstest]
fn insert_booleans(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (b boolean);",
        vec![Outbound::TableCreated, Outbound::ReadyForQuery],
    );

    assert_statement(
        &txn,
        "insert into schema_name.table_name values(true);",
        vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "insert into schema_name.table_name values(TRUE::boolean);",
        vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "insert into schema_name.table_name values('true'::boolean);",
        vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
    );

    txn.commit();
}

#[cfg(test)]
mod operators {
    use super::*;

    #[cfg(test)]
    mod integers {
        use super::*;

        #[rstest::fixture]
        fn with_table(with_schema: TransactionManager) -> TransactionManager {
            let txn = with_schema.start_transaction();

            assert_statement(
                &txn,
                "create table schema_name.table_name(column_si smallint);",
                vec![Outbound::TableCreated, Outbound::ReadyForQuery],
            );
            txn.commit();

            with_schema
        }

        #[rstest::rstest]
        fn addition(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "insert into schema_name.table_name values (1 + 2);",
                vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    Outbound::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    Outbound::DataRow(vec![small_int(3)]),
                    Outbound::RecordsSelected(1),
                    Outbound::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn subtraction(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "insert into schema_name.table_name values (1 - 2);",
                vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    Outbound::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    Outbound::DataRow(vec![small_int(-1)]),
                    Outbound::RecordsSelected(1),
                    Outbound::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn multiplication(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "insert into schema_name.table_name values (3 * 2);",
                vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
            );

            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    Outbound::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    Outbound::DataRow(vec![small_int(6)]),
                    Outbound::RecordsSelected(1),
                    Outbound::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn division(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "insert into schema_name.table_name values (8 / 2);",
                vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    Outbound::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    Outbound::DataRow(vec![small_int(4)]),
                    Outbound::RecordsSelected(1),
                    Outbound::ReadyForQuery,
                ],
            );
        }

        #[rstest::rstest]
        fn modulo(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "insert into schema_name.table_name values (8 % 2);",
                vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    Outbound::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    Outbound::DataRow(vec![small_int(0)]),
                    Outbound::RecordsSelected(1),
                    Outbound::ReadyForQuery,
                ],
            );
        }

        #[rstest::rstest]
        fn exponentiation(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "insert into schema_name.table_name values (8 ^ 2);",
                vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    Outbound::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    Outbound::DataRow(vec![small_int(64)]),
                    Outbound::RecordsSelected(1),
                    Outbound::ReadyForQuery,
                ],
            );
        }

        #[rstest::rstest]
        #[ignore] //TODO: TypeInference#infer_static is not implemented
        fn square_root(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "insert into schema_name.table_name values (|/ 16);",
                vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    Outbound::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    Outbound::DataRow(vec![small_int(4)]),
                    Outbound::RecordsSelected(1),
                    Outbound::ReadyForQuery,
                ],
            );
        }

        #[rstest::rstest]
        #[ignore] //TODO: TypeInference#infer_static is not implemented
        fn cube_root(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "insert into schema_name.table_name values (||/ 8);",
                vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    Outbound::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    Outbound::DataRow(vec![small_int(2)]),
                    Outbound::RecordsSelected(1),
                    Outbound::ReadyForQuery,
                ],
            );
        }

        #[rstest::rstest]
        fn factorial(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "insert into schema_name.table_name values (5!);",
                vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    Outbound::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    Outbound::DataRow(vec![small_int(120)]),
                    Outbound::RecordsSelected(1),
                    Outbound::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn prefix_factorial(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "insert into schema_name.table_name values (!!5);",
                vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    Outbound::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    Outbound::DataRow(vec![small_int(120)]),
                    Outbound::RecordsSelected(1),
                    Outbound::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn absolute_value(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "insert into schema_name.table_name values (@ -5);",
                vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    Outbound::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    Outbound::DataRow(vec![small_int(5)]),
                    Outbound::RecordsSelected(1),
                    Outbound::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn bitwise_and(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "insert into schema_name.table_name values (5 & 1);",
                vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    Outbound::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    Outbound::DataRow(vec![small_int(1)]),
                    Outbound::RecordsSelected(1),
                    Outbound::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn bitwise_or(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "insert into schema_name.table_name values (5 | 2);",
                vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    Outbound::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    Outbound::DataRow(vec![small_int(7)]),
                    Outbound::RecordsSelected(1),
                    Outbound::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn bitwise_not(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "insert into schema_name.table_name values (~1);",
                vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    Outbound::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    Outbound::DataRow(vec![small_int(-2)]),
                    Outbound::RecordsSelected(1),
                    Outbound::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn bitwise_shift_left(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "insert into schema_name.table_name values (1 << 4);",
                vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    Outbound::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    Outbound::DataRow(vec![small_int(16)]),
                    Outbound::RecordsSelected(1),
                    Outbound::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn bitwise_right_left(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "insert into schema_name.table_name values (8 >> 2);",
                vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    Outbound::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    Outbound::DataRow(vec![small_int(2)]),
                    Outbound::RecordsSelected(1),
                    Outbound::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn evaluate_many_operations(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "insert into schema_name.table_name values (5 & 13 % 10 + 1 * 20 - 40 / 4);",
                vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    Outbound::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    Outbound::DataRow(vec![small_int(5)]),
                    Outbound::RecordsSelected(1),
                    Outbound::ReadyForQuery,
                ],
            );
            txn.commit();
        }
    }

    #[cfg(test)]
    mod string {
        use super::*;

        #[rstest::fixture]
        fn with_table(with_schema: TransactionManager) -> TransactionManager {
            let txn = with_schema.start_transaction();

            assert_statement(
                &txn,
                "create table schema_name.table_name(strings char(5));",
                vec![Outbound::TableCreated, Outbound::ReadyForQuery],
            );
            txn.commit();

            with_schema
        }

        #[rstest::rstest]
        fn concatenation(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "insert into schema_name.table_name values ('123' || '45');",
                vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    Outbound::RowDescription(vec![("strings".to_owned(), CHAR)]),
                    Outbound::DataRow(vec![string("12345")]),
                    Outbound::RecordsSelected(1),
                    Outbound::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        #[ignore] //TODO: TypeInference#infer_static is not implemented
        fn concatenation_with_number(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "insert into schema_name.table_name values (1 || '45');",
                vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "insert into schema_name.table_name values ('45' || 1);",
                vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    Outbound::RowDescription(vec![("strings".to_owned(), CHAR)]),
                    Outbound::DataRow(vec![string("145")]),
                    Outbound::DataRow(vec![string("451")]),
                    Outbound::RecordsSelected(2),
                    Outbound::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn non_string_concatenation_not_supported(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "insert into schema_name.table_name values (1 || 2);",
                vec![
                    QueryError::undefined_function("||".to_owned(), "smallint".to_owned(), "smallint".to_owned())
                        .into(),
                    Outbound::ReadyForQuery,
                ],
            );
            txn.commit();
        }
    }
}
