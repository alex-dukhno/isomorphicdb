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
fn update_all_records(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (column_test smallint);",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "insert into schema_name.table_name values (123), (456);",
        vec![OutboundMessage::RecordsInserted(2), OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select * from schema_name.table_name;",
        vec![
            OutboundMessage::RowDescription(vec![("column_test".to_owned(), SMALLINT)]),
            OutboundMessage::DataRow(vec![small_int(123)]),
            OutboundMessage::DataRow(vec![small_int(456)]),
            OutboundMessage::RecordsSelected(2),
            OutboundMessage::ReadyForQuery,
        ],
    );
    assert_statement(
        &txn,
        "update schema_name.table_name set column_test=789;",
        vec![OutboundMessage::RecordsUpdated(2), OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select * from schema_name.table_name;",
        vec![
            OutboundMessage::RowDescription(vec![("column_test".to_owned(), SMALLINT)]),
            OutboundMessage::DataRow(vec![small_int(789)]),
            OutboundMessage::DataRow(vec![small_int(789)]),
            OutboundMessage::RecordsSelected(2),
            OutboundMessage::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[rstest::rstest]
fn update_single_column_of_all_records(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (col1 smallint, col2 smallint);",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "insert into schema_name.table_name values (123, 789), (456, 789);",
        vec![OutboundMessage::RecordsInserted(2), OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select * from schema_name.table_name;",
        vec![
            OutboundMessage::RowDescription(vec![("col1".to_owned(), SMALLINT), ("col2".to_owned(), SMALLINT)]),
            OutboundMessage::DataRow(vec![small_int(123), small_int(789)]),
            OutboundMessage::DataRow(vec![small_int(456), small_int(789)]),
            OutboundMessage::RecordsSelected(2),
            OutboundMessage::ReadyForQuery,
        ],
    );
    assert_statement(
        &txn,
        "update schema_name.table_name set col2=357;",
        vec![OutboundMessage::RecordsUpdated(2), OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select * from schema_name.table_name;",
        vec![
            OutboundMessage::RowDescription(vec![("col1".to_owned(), SMALLINT), ("col2".to_owned(), SMALLINT)]),
            OutboundMessage::DataRow(vec![small_int(123), small_int(357)]),
            OutboundMessage::DataRow(vec![small_int(456), small_int(357)]),
            OutboundMessage::RecordsSelected(2),
            OutboundMessage::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[rstest::rstest]
fn update_multiple_columns_of_all_records(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (col1 smallint, col2 smallint, col3 smallint);",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "insert into schema_name.table_name values (111, 222, 333), (444, 555, 666);",
        vec![OutboundMessage::RecordsInserted(2), OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select * from schema_name.table_name;",
        vec![
            OutboundMessage::RowDescription(vec![
                ("col1".to_owned(), SMALLINT),
                ("col2".to_owned(), SMALLINT),
                ("col3".to_owned(), SMALLINT),
            ]),
            OutboundMessage::DataRow(vec![small_int(111), small_int(222), small_int(333)]),
            OutboundMessage::DataRow(vec![small_int(444), small_int(555), small_int(666)]),
            OutboundMessage::RecordsSelected(2),
            OutboundMessage::ReadyForQuery,
        ],
    );
    assert_statement(
        &txn,
        "update schema_name.table_name set col3=777, col1=999;",
        vec![OutboundMessage::RecordsUpdated(2), OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select * from schema_name.table_name;",
        vec![
            OutboundMessage::RowDescription(vec![
                ("col1".to_owned(), SMALLINT),
                ("col2".to_owned(), SMALLINT),
                ("col3".to_owned(), SMALLINT),
            ]),
            OutboundMessage::DataRow(vec![small_int(999), small_int(222), small_int(777)]),
            OutboundMessage::DataRow(vec![small_int(999), small_int(555), small_int(777)]),
            OutboundMessage::RecordsSelected(2),
            OutboundMessage::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[rstest::rstest]
fn update_all_records_in_multiple_columns(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6), (7, 8, 9);",
        vec![OutboundMessage::RecordsInserted(3), OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select * from schema_name.table_name;",
        vec![
            OutboundMessage::RowDescription(vec![
                ("column_1".to_owned(), SMALLINT),
                ("column_2".to_owned(), SMALLINT),
                ("column_3".to_owned(), SMALLINT),
            ]),
            OutboundMessage::DataRow(vec![small_int(1), small_int(2), small_int(3)]),
            OutboundMessage::DataRow(vec![small_int(4), small_int(5), small_int(6)]),
            OutboundMessage::DataRow(vec![small_int(7), small_int(8), small_int(9)]),
            OutboundMessage::RecordsSelected(3),
            OutboundMessage::ReadyForQuery,
        ],
    );
    assert_statement(
        &txn,
        "update schema_name.table_name set column_1=10, column_2=20, column_3=30;",
        vec![OutboundMessage::RecordsUpdated(3), OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select * from schema_name.table_name;",
        vec![
            OutboundMessage::RowDescription(vec![
                ("column_1".to_owned(), SMALLINT),
                ("column_2".to_owned(), SMALLINT),
                ("column_3".to_owned(), SMALLINT),
            ]),
            OutboundMessage::DataRow(vec![small_int(10), small_int(20), small_int(30)]),
            OutboundMessage::DataRow(vec![small_int(10), small_int(20), small_int(30)]),
            OutboundMessage::DataRow(vec![small_int(10), small_int(20), small_int(30)]),
            OutboundMessage::RecordsSelected(3),
            OutboundMessage::ReadyForQuery,
        ],
    );
}

#[rstest::rstest]
fn update_non_existent_columns_of_records(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (column_test smallint);",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "insert into schema_name.table_name values (123);",
        vec![OutboundMessage::RecordsInserted(1), OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select * from schema_name.table_name;",
        vec![
            OutboundMessage::RowDescription(vec![("column_test".to_owned(), SMALLINT)]),
            OutboundMessage::DataRow(vec![small_int(123)]),
            OutboundMessage::RecordsSelected(1),
            OutboundMessage::ReadyForQuery,
        ],
    );
    assert_statement(
        &txn,
        "update schema_name.table_name set col1=456, col2=789;",
        vec![
            QueryError::column_does_not_exist("col1").into(),
            OutboundMessage::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[rstest::rstest]
fn test_update_with_dynamic_expression(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (\
            si_column_1 smallint, \
            si_column_2 smallint, \
            si_column_3 smallint);",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6), (7, 8, 9);",
        vec![OutboundMessage::RecordsInserted(3), OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select * from schema_name.table_name;",
        vec![
            OutboundMessage::RowDescription(vec![
                ("si_column_1".to_owned(), SMALLINT),
                ("si_column_2".to_owned(), SMALLINT),
                ("si_column_3".to_owned(), SMALLINT),
            ]),
            OutboundMessage::DataRow(vec![small_int(1), small_int(2), small_int(3)]),
            OutboundMessage::DataRow(vec![small_int(4), small_int(5), small_int(6)]),
            OutboundMessage::DataRow(vec![small_int(7), small_int(8), small_int(9)]),
            OutboundMessage::RecordsSelected(3),
            OutboundMessage::ReadyForQuery,
        ],
    );
    assert_statement(
        &txn,
        "update schema_name.table_name \
        set \
            si_column_1 = 2 * si_column_1, \
            si_column_2 = 2 * (si_column_1 + si_column_2), \
            si_column_3 = (si_column_3 + (2 * (si_column_1 + si_column_2)));",
        vec![OutboundMessage::RecordsUpdated(3), OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select * from schema_name.table_name;",
        vec![
            OutboundMessage::RowDescription(vec![
                ("si_column_1".to_owned(), SMALLINT),
                ("si_column_2".to_owned(), SMALLINT),
                ("si_column_3".to_owned(), SMALLINT),
            ]),
            OutboundMessage::DataRow(vec![small_int(2), small_int(2 * (1 + 2)), small_int(3 + (2 * (1 + 2)))]),
            OutboundMessage::DataRow(vec![
                small_int(2 * 4),
                small_int(2 * (4 + 5)),
                small_int(6 + (2 * (4 + 5))),
            ]),
            OutboundMessage::DataRow(vec![
                small_int(2 * 7),
                small_int(2 * (7 + 8)),
                small_int(9 + (2 * (7 + 8))),
            ]),
            OutboundMessage::RecordsSelected(3),
            OutboundMessage::ReadyForQuery,
        ],
    );
}

#[rstest::rstest]
fn update_value_by_predicate_on_single_field(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (col1 smallint, col2 smallint, col3 smallint);",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6), (7, 8, 9);",
        vec![OutboundMessage::RecordsInserted(3), OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "update schema_name.table_name set col1 = 7 where col1 = 4;",
        vec![OutboundMessage::RecordsUpdated(1), OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select * from schema_name.table_name where col1 = 4",
        vec![
            OutboundMessage::RowDescription(vec![
                ("col1".to_owned(), SMALLINT),
                ("col2".to_owned(), SMALLINT),
                ("col3".to_owned(), SMALLINT),
            ]),
            OutboundMessage::RecordsSelected(0),
            OutboundMessage::ReadyForQuery,
        ],
    );
    assert_statement(
        &txn,
        "select * from schema_name.table_name where col1 = 7",
        vec![
            OutboundMessage::RowDescription(vec![
                ("col1".to_owned(), SMALLINT),
                ("col2".to_owned(), SMALLINT),
                ("col3".to_owned(), SMALLINT),
            ]),
            OutboundMessage::DataRow(vec![small_int(7), small_int(5), small_int(6)]),
            OutboundMessage::DataRow(vec![small_int(7), small_int(8), small_int(9)]),
            OutboundMessage::RecordsSelected(2),
            OutboundMessage::ReadyForQuery,
        ],
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
                vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "insert into schema_name.table_name values (2);",
                vec![OutboundMessage::RecordsInserted(1), OutboundMessage::ReadyForQuery],
            );
            txn.commit();

            with_schema
        }

        #[rstest::rstest]
        fn addition(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "update schema_name.table_name set column_si = 1 + 2;",
                vec![OutboundMessage::RecordsUpdated(1), OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    OutboundMessage::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    OutboundMessage::DataRow(vec![small_int(3)]),
                    OutboundMessage::RecordsSelected(1),
                    OutboundMessage::ReadyForQuery,
                ],
            );

            txn.commit();
        }

        #[rstest::rstest]
        fn subtraction(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "update schema_name.table_name set column_si = 1 - 2;",
                vec![OutboundMessage::RecordsUpdated(1), OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    OutboundMessage::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    OutboundMessage::DataRow(vec![small_int(-1)]),
                    OutboundMessage::RecordsSelected(1),
                    OutboundMessage::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn multiplication(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "update schema_name.table_name set column_si = 3 * 2;",
                vec![OutboundMessage::RecordsUpdated(1), OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    OutboundMessage::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    OutboundMessage::DataRow(vec![small_int(6)]),
                    OutboundMessage::RecordsSelected(1),
                    OutboundMessage::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn division(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "update schema_name.table_name set column_si = 8 / 2;",
                vec![OutboundMessage::RecordsUpdated(1), OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    OutboundMessage::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    OutboundMessage::DataRow(vec![small_int(4)]),
                    OutboundMessage::RecordsSelected(1),
                    OutboundMessage::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn modulo(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "update schema_name.table_name set column_si = 8 % 2;",
                vec![OutboundMessage::RecordsUpdated(1), OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    OutboundMessage::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    OutboundMessage::DataRow(vec![small_int(0)]),
                    OutboundMessage::RecordsSelected(1),
                    OutboundMessage::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn exponentiation(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "update schema_name.table_name set column_si = 8 ^ 2;",
                vec![OutboundMessage::RecordsUpdated(1), OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    OutboundMessage::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    OutboundMessage::DataRow(vec![small_int(64)]),
                    OutboundMessage::RecordsSelected(1),
                    OutboundMessage::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        #[ignore] //TODO: TypeInference#infer_static is not implemented
        fn square_root(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "update schema_name.table_name set column_si = |/ 16;",
                vec![OutboundMessage::RecordsUpdated(1), OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    OutboundMessage::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    OutboundMessage::DataRow(vec![small_int(4)]),
                    OutboundMessage::RecordsSelected(1),
                    OutboundMessage::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        #[ignore] //TODO: TypeInference#infer_static is not implemented
        fn cube_root(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "update schema_name.table_name set column_si = ||/ 8;",
                vec![OutboundMessage::RecordsUpdated(1), OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    OutboundMessage::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    OutboundMessage::DataRow(vec![small_int(1)]),
                    OutboundMessage::RecordsSelected(1),
                    OutboundMessage::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn factorial(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "update schema_name.table_name set column_si = 5!;",
                vec![OutboundMessage::RecordsUpdated(1), OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    OutboundMessage::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    OutboundMessage::DataRow(vec![small_int(120)]),
                    OutboundMessage::RecordsSelected(1),
                    OutboundMessage::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn prefix_factorial(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "update schema_name.table_name set column_si = !!5;",
                vec![OutboundMessage::RecordsUpdated(1), OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    OutboundMessage::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    OutboundMessage::DataRow(vec![small_int(120)]),
                    OutboundMessage::RecordsSelected(1),
                    OutboundMessage::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn absolute_value(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "update schema_name.table_name set column_si = @ -5;",
                vec![OutboundMessage::RecordsUpdated(1), OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    OutboundMessage::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    OutboundMessage::DataRow(vec![small_int(5)]),
                    OutboundMessage::RecordsSelected(1),
                    OutboundMessage::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn bitwise_and(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "update schema_name.table_name set column_si = 5 & 1;",
                vec![OutboundMessage::RecordsUpdated(1), OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    OutboundMessage::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    OutboundMessage::DataRow(vec![small_int(1)]),
                    OutboundMessage::RecordsSelected(1),
                    OutboundMessage::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn bitwise_or(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "update schema_name.table_name set column_si = 5 | 2;",
                vec![OutboundMessage::RecordsUpdated(1), OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    OutboundMessage::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    OutboundMessage::DataRow(vec![small_int(7)]),
                    OutboundMessage::RecordsSelected(1),
                    OutboundMessage::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn bitwise_not(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "update schema_name.table_name set column_si = ~1;",
                vec![OutboundMessage::RecordsUpdated(1), OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    OutboundMessage::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    OutboundMessage::DataRow(vec![small_int(-2)]),
                    OutboundMessage::RecordsSelected(1),
                    OutboundMessage::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn bitwise_shift_left(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "update schema_name.table_name set column_si = 1 << 4;",
                vec![OutboundMessage::RecordsUpdated(1), OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    OutboundMessage::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    OutboundMessage::DataRow(vec![small_int(16)]),
                    OutboundMessage::RecordsSelected(1),
                    OutboundMessage::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn bitwise_right_left(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "update schema_name.table_name set column_si = 8 >> 2;",
                vec![OutboundMessage::RecordsUpdated(1), OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    OutboundMessage::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    OutboundMessage::DataRow(vec![small_int(2)]),
                    OutboundMessage::RecordsSelected(1),
                    OutboundMessage::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn evaluate_many_operations(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "update schema_name.table_name set column_si = 5 & 13 % 10 + 1 * 20 - 40 / 4;",
                vec![OutboundMessage::RecordsUpdated(1), OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    OutboundMessage::RowDescription(vec![("column_si".to_owned(), SMALLINT)]),
                    OutboundMessage::DataRow(vec![small_int(5)]),
                    OutboundMessage::RecordsSelected(1),
                    OutboundMessage::ReadyForQuery,
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
                vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "insert into schema_name.table_name values ('x');",
                vec![OutboundMessage::RecordsInserted(1), OutboundMessage::ReadyForQuery],
            );
            txn.commit();

            with_schema
        }

        #[rstest::rstest]
        fn concatenation(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "update schema_name.table_name set strings = '123' || '45';",
                vec![OutboundMessage::RecordsUpdated(1), OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    OutboundMessage::RowDescription(vec![("strings".to_owned(), CHAR)]),
                    OutboundMessage::DataRow(vec![string("12345")]),
                    OutboundMessage::RecordsSelected(1),
                    OutboundMessage::ReadyForQuery,
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
                "update schema_name.table_name set strings = 1 || '45';",
                vec![OutboundMessage::RecordsUpdated(1), OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    OutboundMessage::RowDescription(vec![("strings".to_owned(), CHAR)]),
                    OutboundMessage::DataRow(vec![string("145")]),
                    OutboundMessage::RecordsSelected(1),
                    OutboundMessage::ReadyForQuery,
                ],
            );
            assert_statement(
                &txn,
                "update schema_name.table_name set strings = '45' || 1;",
                vec![OutboundMessage::RecordsUpdated(1), OutboundMessage::ReadyForQuery],
            );
            assert_statement(
                &txn,
                "select * from schema_name.table_name;",
                vec![
                    OutboundMessage::RowDescription(vec![("strings".to_owned(), CHAR)]),
                    OutboundMessage::DataRow(vec![string("451")]),
                    OutboundMessage::RecordsSelected(1),
                    OutboundMessage::ReadyForQuery,
                ],
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn non_string_concatenation_not_supported(with_table: TransactionManager) {
            let txn = with_table.start_transaction();

            assert_statement(
                &txn,
                "update schema_name.table_name set strings = 1 || 2;",
                vec![
                    QueryError::undefined_function("||".to_owned(), "smallint".to_owned(), "smallint".to_owned())
                        .into(),
                    OutboundMessage::ReadyForQuery,
                ],
            );
            txn.commit();
        }
    }
}
