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
fn select_all_from_table_with_multiple_columns(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "insert into schema_name.table_name values (123, 456, 789);",
        vec![OutboundMessage::RecordsInserted(1), OutboundMessage::ReadyForQuery],
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
            OutboundMessage::DataRow(vec![small_int(123), small_int(456), small_int(789)]),
            OutboundMessage::RecordsSelected(1),
            OutboundMessage::ReadyForQuery,
        ],
    );
}

#[rstest::rstest]
fn select_not_all_columns(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "insert into schema_name.table_name values (1, 4, 7), (2, 5, 8), (3, 6, 9);",
        vec![OutboundMessage::RecordsInserted(3), OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select column_3, column_2 from schema_name.table_name;",
        vec![
            OutboundMessage::RowDescription(vec![("column_3".to_owned(), SMALLINT), ("column_2".to_owned(), SMALLINT)]),
            OutboundMessage::DataRow(vec![small_int(7), small_int(4)]),
            OutboundMessage::DataRow(vec![small_int(8), small_int(5)]),
            OutboundMessage::DataRow(vec![small_int(9), small_int(6)]),
            OutboundMessage::RecordsSelected(3),
            OutboundMessage::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[rstest::rstest]
fn select_non_existing_columns_from_table(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (column_in_table smallint);",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select column_not_in_table1, column_not_in_table2 from schema_name.table_name;",
        vec![
            QueryError::column_does_not_exist("column_not_in_table1").into(),
            OutboundMessage::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[rstest::rstest]
fn select_first_and_last_columns_from_table_with_multiple_columns(with_schema: TransactionManager) {
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
        "select column_3, column_1 from schema_name.table_name;",
        vec![
            OutboundMessage::RowDescription(vec![("column_3".to_owned(), SMALLINT), ("column_1".to_owned(), SMALLINT)]),
            OutboundMessage::DataRow(vec![small_int(3), small_int(1)]),
            OutboundMessage::DataRow(vec![small_int(6), small_int(4)]),
            OutboundMessage::DataRow(vec![small_int(9), small_int(7)]),
            OutboundMessage::RecordsSelected(3),
            OutboundMessage::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[rstest::rstest]
fn select_all_columns_reordered_from_table_with_multiple_columns(with_schema: TransactionManager) {
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
        "select column_3, column_1, column_2 from schema_name.table_name;",
        vec![
            OutboundMessage::RowDescription(vec![
                ("column_3".to_owned(), SMALLINT),
                ("column_1".to_owned(), SMALLINT),
                ("column_2".to_owned(), SMALLINT),
            ]),
            OutboundMessage::DataRow(vec![small_int(3), small_int(1), small_int(2)]),
            OutboundMessage::DataRow(vec![small_int(6), small_int(4), small_int(5)]),
            OutboundMessage::DataRow(vec![small_int(9), small_int(7), small_int(8)]),
            OutboundMessage::RecordsSelected(3),
            OutboundMessage::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[rstest::rstest]
fn select_with_column_name_duplication(with_schema: TransactionManager) {
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
        "select column_3, column_2, column_1, column_3, column_2 from schema_name.table_name;",
        vec![
            OutboundMessage::RowDescription(vec![
                ("column_3".to_owned(), SMALLINT),
                ("column_2".to_owned(), SMALLINT),
                ("column_1".to_owned(), SMALLINT),
                ("column_3".to_owned(), SMALLINT),
                ("column_2".to_owned(), SMALLINT),
            ]),
            OutboundMessage::DataRow(vec![small_int(3), small_int(2), small_int(1), small_int(3), small_int(2)]),
            OutboundMessage::DataRow(vec![small_int(6), small_int(5), small_int(4), small_int(6), small_int(5)]),
            OutboundMessage::DataRow(vec![small_int(9), small_int(8), small_int(7), small_int(9), small_int(8)]),
            OutboundMessage::RecordsSelected(3),
            OutboundMessage::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[rstest::rstest]
fn select_different_integer_types(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (column_si smallint, column_i integer, column_bi bigint);",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "insert into schema_name.table_name values (1000, 2000000, 3000000000), (4000, 5000000, 6000000000), (7000, 8000000, 9000000000);",
        vec![OutboundMessage::RecordsInserted(3), OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select * from schema_name.table_name;",
        vec![
            OutboundMessage::RowDescription(vec![
                ("column_si".to_owned(), SMALLINT),
                ("column_i".to_owned(), INT),
                ("column_bi".to_owned(), BIGINT),
            ]),
            OutboundMessage::DataRow(vec![small_int(1_000), integer(2_000_000), big_int(3_000_000_000)]),
            OutboundMessage::DataRow(vec![small_int(4_000), integer(5_000_000), big_int(6_000_000_000)]),
            OutboundMessage::DataRow(vec![small_int(7_000), integer(8_000_000), big_int(9_000_000_000)]),
            OutboundMessage::RecordsSelected(3),
            OutboundMessage::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[rstest::rstest]
fn select_different_character_strings_types(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (char_10 char(10), var_char_20 varchar(20));",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "insert into schema_name.table_name values ('1234567890', '12345678901234567890'), ('12345', '1234567890');",
        vec![OutboundMessage::RecordsInserted(2), OutboundMessage::ReadyForQuery],
    );
    // TODO: string type is not recognizable on SqlTable level
    // assert_statement(
    //     &txn,
    //     "insert into schema_name.table_name values ('12345', '1234567890     ');",
    //     vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
    // );
    assert_statement(
        &txn,
        "select * from schema_name.table_name;",
        vec![
            OutboundMessage::RowDescription(vec![("char_10".to_owned(), CHAR), ("var_char_20".to_owned(), VARCHAR)]),
            OutboundMessage::DataRow(vec![string("1234567890"), string("12345678901234567890")]),
            OutboundMessage::DataRow(vec![string("12345"), string("1234567890")]),
            // Outbound::DataRow(vec![string("12345"), string("1234567890")]),
            OutboundMessage::RecordsSelected(2),
            // Outbound::RecordsSelected(3),
            OutboundMessage::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[rstest::rstest]
fn select_value_by_predicate_on_single_field(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (col1 smallint, col2 smallint, col3 smallint);",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6);",
        vec![OutboundMessage::RecordsInserted(2), OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "select * from schema_name.table_name where col1 = 1",
        vec![
            OutboundMessage::RowDescription(vec![
                ("col1".to_owned(), SMALLINT),
                ("col2".to_owned(), SMALLINT),
                ("col3".to_owned(), SMALLINT),
            ]),
            OutboundMessage::DataRow(vec![small_int(1), small_int(2), small_int(3)]),
            OutboundMessage::RecordsSelected(1),
            OutboundMessage::ReadyForQuery,
        ],
    );
    txn.commit();
}
