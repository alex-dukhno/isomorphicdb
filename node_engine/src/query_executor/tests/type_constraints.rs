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

#[rstest::fixture]
fn int_table(with_schema: TransactionManager) -> TransactionManager {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name(col smallint);",
        vec![Outbound::TableCreated, Outbound::ReadyForQuery],
    );
    txn.commit();

    with_schema
}

#[rstest::fixture]
fn multiple_ints_table(with_schema: TransactionManager) -> TransactionManager {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name(column_si smallint, column_i integer, column_bi bigint);",
        vec![Outbound::TableCreated, Outbound::ReadyForQuery],
    );
    txn.commit();

    with_schema
}

#[rstest::fixture]
fn str_table(with_schema: TransactionManager) -> TransactionManager {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name(col varchar(5));",
        vec![Outbound::TableCreated, Outbound::ReadyForQuery],
    );
    txn.commit();

    with_schema
}

#[cfg(test)]
mod insert {
    use super::*;
    use types::SqlType;

    #[rstest::rstest]
    fn out_of_range(int_table: TransactionManager) {
        let txn = int_table.start_transaction();

        assert_statement(
            &txn,
            "insert into schema_name.table_name values (32768);",
            vec![
                QueryError::out_of_range_2("smallint", "col".to_string(), 1).into(),
                Outbound::ReadyForQuery,
            ],
        );
        txn.commit();
    }

    #[rstest::rstest]
    fn type_mismatch(int_table: TransactionManager) {
        let txn = int_table.start_transaction();

        assert_statement(
            &txn,
            "insert into schema_name.table_name values ('str');",
            vec![
                QueryError::invalid_text_representation_2(SqlType::small_int(), &"str").into(),
                Outbound::ReadyForQuery,
            ],
        );
        txn.commit();
    }

    #[rstest::rstest]
    fn multiple_columns_multiple_row_violation(multiple_ints_table: TransactionManager) {
        let txn = multiple_ints_table.start_transaction();

        // assert_statement(
        //     &txn,
        //     "insert into schema_name.table_name values (-32769, -2147483649, 100), (100, -2147483649, -9223372036854775809);",
        //     Err(QueryError::out_of_range(SMALLINT, "column_si", 1)),
        //     Err(QueryError::out_of_range(INT, "column_i", 1))
        // );
        assert_statement(
            &txn,
            "insert into schema_name.table_name values (-32769, -2147483649, 100), (100, -2147483649, -9223372036854775809);",
            vec![QueryError::out_of_range_2(SqlType::small_int(), "column_si", 1).into(), Outbound::ReadyForQuery],
        );
        txn.commit();
    }

    #[rstest::rstest]
    fn violation_in_the_second_row(multiple_ints_table: TransactionManager) {
        let txn = multiple_ints_table.start_transaction();

        assert_statement(
            &txn,
            "insert into schema_name.table_name values (-32768, -2147483648, 100), (100, -2147483649, -9223372036854775808);",
            vec![QueryError::out_of_range_2(SqlType::integer(), "column_i".to_owned(), 2).into(), Outbound::ReadyForQuery]
        );
        txn.commit();
    }

    #[rstest::rstest]
    #[ignore] // TODO: string length is not checked
    fn value_too_long(str_table: TransactionManager) {
        let txn = str_table.start_transaction();

        assert_statement(
            &txn,
            "insert into schema_name.table_name values ('123457890');",
            vec![
                QueryError::string_length_mismatch(VARCHAR, 5, "col".to_string(), 1).into(),
                Outbound::ReadyForQuery,
            ],
        );
        txn.commit();
    }
}

#[cfg(test)]
mod update {
    use super::*;
    use types::SqlType;

    #[rstest::rstest]
    fn out_of_range(int_table: TransactionManager) {
        let txn = int_table.start_transaction();

        assert_statement(
            &txn,
            "insert into schema_name.table_name values (32767);",
            vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
        );
        assert_statement(
            &txn,
            "update schema_name.table_name set col = 32768;",
            vec![
                QueryError::out_of_range_2(SqlType::small_int(), "col".to_string(), 1).into(),
                Outbound::ReadyForQuery,
            ],
        );
        txn.commit();
    }

    #[rstest::rstest]
    fn type_mismatch(int_table: TransactionManager) {
        let txn = int_table.start_transaction();

        assert_statement(
            &txn,
            "insert into schema_name.table_name values (32767);",
            vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
        );
        assert_statement(
            &txn,
            "update schema_name.table_name set col = 'str';",
            vec![
                QueryError::invalid_text_representation_2(SqlType::small_int(), &"str").into(),
                Outbound::ReadyForQuery,
            ],
        );
        txn.commit();
    }

    #[rstest::rstest]
    #[ignore] // TODO: string length is not checked
    fn value_too_long(str_table: TransactionManager) {
        let txn = str_table.start_transaction();

        assert_statement(
            &txn,
            "insert into schema_name.table_name values ('str');",
            vec![Outbound::RecordsInserted(1), Outbound::ReadyForQuery],
        );
        assert_statement(
            &txn,
            "update schema_name.table_name set col = '123457890';",
            vec![
                QueryError::string_length_mismatch(VARCHAR, 5, "col".to_string(), 1).into(),
                Outbound::ReadyForQuery,
            ],
        );
        txn.commit();
    }

    #[rstest::rstest]
    fn multiple_columns_violation(multiple_ints_table: TransactionManager) {
        let txn = multiple_ints_table.start_transaction();

        assert_statement(
            &txn,
            "insert into schema_name.table_name values (100, 100, 100), (100, 100, 100);",
            vec![Outbound::RecordsInserted(2), Outbound::ReadyForQuery],
        );
        // assert_statement(
        //      &txn,
        //      "update schema_name.table_name set column_si = -32769, column_i= -2147483649, column_bi=100;",
        //      vec![
        //          Err(QueryError::out_of_range(SMALLINT, "column_si".to_owned(), 1)),
        //          Err(QueryError::out_of_range(INT, "column_i".to_owned(), 1)),
        //      ]
        // );
        assert_statement(
            &txn,
            "update schema_name.table_name set column_si = -32769, column_i= -2147483649, column_bi=100;",
            vec![
                QueryError::out_of_range_2(SqlType::small_int(), "column_si".to_string(), 1).into(),
                Outbound::ReadyForQuery,
            ],
        );
    }
}
