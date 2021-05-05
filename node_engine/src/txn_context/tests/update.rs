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
fn update_all_records(with_schema: QueryEngine) {
    let txn = with_schema.start_transaction();

    assert_definition(
        &txn,
        "create table schema_name.table_name (column_test smallint);",
        Ok(QueryEvent::TableCreated),
    );
    assert_query(
        &txn,
        "insert into schema_name.table_name values (123), (456);",
        Ok(QueryExecutionResult::Inserted(2)),
    );
    assert_query(
        &txn,
        "select * from schema_name.table_name;",
        Ok(QueryExecutionResult::Selected((
            vec![("column_test".to_owned(), SMALLINT)],
            vec![vec![small_int(123)], vec![small_int(456)]],
        ))),
    );
    assert_query(
        &txn,
        "update schema_name.table_name set column_test=789;",
        Ok(QueryExecutionResult::Updated(2)),
    );
    assert_query(
        &txn,
        "select * from schema_name.table_name;",
        Ok(QueryExecutionResult::Selected((
            vec![("column_test".to_owned(), SMALLINT)],
            vec![vec![small_int(789)], vec![small_int(789)]],
        ))),
    );
    txn.commit();
}

#[rstest::rstest]
fn update_single_column_of_all_records(with_schema: QueryEngine) {
    let txn = with_schema.start_transaction();

    assert_definition(
        &txn,
        "create table schema_name.table_name (col1 smallint, col2 smallint);",
        Ok(QueryEvent::TableCreated),
    );
    assert_query(
        &txn,
        "insert into schema_name.table_name values (123, 789), (456, 789);",
        Ok(QueryExecutionResult::Inserted(2)),
    );
    assert_query(
        &txn,
        "select * from schema_name.table_name;",
        Ok(QueryExecutionResult::Selected((
            vec![("col1".to_owned(), SMALLINT), ("col2".to_owned(), SMALLINT)],
            vec![
                vec![small_int(123), small_int(789)],
                vec![small_int(456), small_int(789)],
            ],
        ))),
    );
    assert_query(
        &txn,
        "update schema_name.table_name set col2=357;",
        Ok(QueryExecutionResult::Updated(2)),
    );
    assert_query(
        &txn,
        "select * from schema_name.table_name;",
        Ok(QueryExecutionResult::Selected((
            vec![("col1".to_owned(), SMALLINT), ("col2".to_owned(), SMALLINT)],
            vec![
                vec![small_int(123), small_int(357)],
                vec![small_int(456), small_int(357)],
            ],
        ))),
    );
    txn.commit();
}

#[rstest::rstest]
fn update_multiple_columns_of_all_records(with_schema: QueryEngine) {
    let txn = with_schema.start_transaction();

    assert_definition(
        &txn,
        "create table schema_name.table_name (col1 smallint, col2 smallint, col3 smallint);",
        Ok(QueryEvent::TableCreated),
    );
    assert_query(
        &txn,
        "insert into schema_name.table_name values (111, 222, 333), (444, 555, 666);",
        Ok(QueryExecutionResult::Inserted(2)),
    );
    assert_query(
        &txn,
        "select * from schema_name.table_name;",
        Ok(QueryExecutionResult::Selected((
            vec![
                ("col1".to_owned(), SMALLINT),
                ("col2".to_owned(), SMALLINT),
                ("col3".to_owned(), SMALLINT),
            ],
            vec![
                vec![small_int(111), small_int(222), small_int(333)],
                vec![small_int(444), small_int(555), small_int(666)],
            ],
        ))),
    );
    assert_query(
        &txn,
        "update schema_name.table_name set col3=777, col1=999;",
        Ok(QueryExecutionResult::Updated(2)),
    );
    assert_query(
        &txn,
        "select * from schema_name.table_name;",
        Ok(QueryExecutionResult::Selected((
            vec![
                ("col1".to_owned(), SMALLINT),
                ("col2".to_owned(), SMALLINT),
                ("col3".to_owned(), SMALLINT),
            ],
            vec![
                vec![small_int(999), small_int(222), small_int(777)],
                vec![small_int(999), small_int(555), small_int(777)],
            ],
        ))),
    );
    txn.commit();
}

#[rstest::rstest]
fn update_all_records_in_multiple_columns(with_schema: QueryEngine) {
    let txn = with_schema.start_transaction();

    assert_definition(
        &txn,
        "create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);",
        Ok(QueryEvent::TableCreated),
    );
    assert_query(
        &txn,
        "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6), (7, 8, 9);",
        Ok(QueryExecutionResult::Inserted(3)),
    );
    assert_query(
        &txn,
        "select * from schema_name.table_name;",
        Ok(QueryExecutionResult::Selected((
            vec![
                ("column_1".to_owned(), SMALLINT),
                ("column_2".to_owned(), SMALLINT),
                ("column_3".to_owned(), SMALLINT),
            ],
            vec![
                vec![small_int(1), small_int(2), small_int(3)],
                vec![small_int(4), small_int(5), small_int(6)],
                vec![small_int(7), small_int(8), small_int(9)],
            ],
        ))),
    );
    assert_query(
        &txn,
        "update schema_name.table_name set column_1=10, column_2=20, column_3=30;",
        Ok(QueryExecutionResult::Updated(3)),
    );
    assert_query(
        &txn,
        "select * from schema_name.table_name;",
        Ok(QueryExecutionResult::Selected((
            vec![
                ("column_1".to_owned(), SMALLINT),
                ("column_2".to_owned(), SMALLINT),
                ("column_3".to_owned(), SMALLINT),
            ],
            vec![
                vec![small_int(10), small_int(20), small_int(30)],
                vec![small_int(10), small_int(20), small_int(30)],
                vec![small_int(10), small_int(20), small_int(30)],
            ],
        ))),
    );
}

#[rstest::rstest]
fn update_non_existent_columns_of_records(with_schema: QueryEngine) {
    let txn = with_schema.start_transaction();

    assert_definition(
        &txn,
        "create table schema_name.table_name (column_test smallint);",
        Ok(QueryEvent::TableCreated),
    );
    assert_query(
        &txn,
        "insert into schema_name.table_name values (123);",
        Ok(QueryExecutionResult::Inserted(1)),
    );
    assert_query(
        &txn,
        "select * from schema_name.table_name;",
        Ok(QueryExecutionResult::Selected((
            vec![("column_test".to_owned(), SMALLINT)],
            vec![vec![small_int(123)]],
        ))),
    );
    assert_query(
        &txn,
        "update schema_name.table_name set col1=456, col2=789;",
        Err(QueryError::column_does_not_exist("col1")),
    );
    txn.commit();
}

#[rstest::rstest]
#[ignore] // TODO: type coercion
fn test_update_with_dynamic_expression(with_schema: QueryEngine) {
    let txn = with_schema.start_transaction();

    assert_definition(
        &txn,
        "create table schema_name.table_name (\
            si_column_1 smallint, \
            si_column_2 smallint, \
            si_column_3 smallint);",
        Ok(QueryEvent::TableCreated),
    );
    assert_query(
        &txn,
        "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6), (7, 8, 9);",
        Ok(QueryExecutionResult::Inserted(3)),
    );
    assert_query(
        &txn,
        "select * from schema_name.table_name;",
        Ok(QueryExecutionResult::Selected((
            vec![
                ("si_column_1".to_owned(), SMALLINT),
                ("si_column_2".to_owned(), SMALLINT),
                ("si_column_3".to_owned(), SMALLINT),
            ],
            vec![
                vec![small_int(1), small_int(2), small_int(3)],
                vec![small_int(4), small_int(5), small_int(6)],
                vec![small_int(7), small_int(8), small_int(9)],
            ],
        ))),
    );
    assert_query(
        &txn,
        "update schema_name.table_name \
        set \
            si_column_1 = 2 * si_column_1, \
            si_column_2 = 2 * (si_column_1 + si_column_2), \
            si_column_3 = (si_column_3 + (2 * (si_column_1 + si_column_2)));",
        Ok(QueryExecutionResult::Updated(3)),
    );
    assert_query(
        &txn,
        "select * from schema_name.table_name;",
        Ok(QueryExecutionResult::Selected((
            vec![
                ("si_column_1".to_owned(), SMALLINT),
                ("si_column_2".to_owned(), SMALLINT),
                ("si_column_3".to_owned(), SMALLINT),
            ],
            vec![
                vec![small_int(2), small_int(2 * (1 + 2)), small_int(3 + (2 * (1 + 2)))],
                vec![small_int(2 * 4), small_int(2 * (4 + 5)), small_int(6 + (2 * (4 + 5)))],
                vec![small_int(2 * 7), small_int(2 * (7 + 8)), small_int(9 + (2 * (7 + 8)))],
            ],
        ))),
    );
}

#[rstest::rstest]
fn update_value_by_predicate_on_single_field(with_schema: QueryEngine) {
    let txn = with_schema.start_transaction();

    assert_definition(
        &txn,
        "create table schema_name.table_name (col1 smallint, col2 smallint, col3 smallint);",
        Ok(QueryEvent::TableCreated),
    );
    assert_query(
        &txn,
        "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6), (7, 8, 9);",
        Ok(QueryExecutionResult::Inserted(3)),
    );
    assert_query(
        &txn,
        "update schema_name.table_name set col1 = 7 where col1 = 4;",
        Ok(QueryExecutionResult::Updated(1)),
    );
    assert_query(
        &txn,
        "select * from schema_name.table_name where col1 = 4",
        Ok(QueryExecutionResult::Selected((
            vec![
                ("col1".to_owned(), SMALLINT),
                ("col2".to_owned(), SMALLINT),
                ("col3".to_owned(), SMALLINT),
            ],
            vec![],
        ))),
    );
    assert_query(
        &txn,
        "select * from schema_name.table_name where col1 = 7",
        Ok(QueryExecutionResult::Selected((
            vec![
                ("col1".to_owned(), SMALLINT),
                ("col2".to_owned(), SMALLINT),
                ("col3".to_owned(), SMALLINT),
            ],
            vec![
                vec![small_int(7), small_int(5), small_int(6)],
                vec![small_int(7), small_int(8), small_int(9)],
            ],
        ))),
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
        fn with_table(with_schema: QueryEngine) -> QueryEngine {
            let txn = with_schema.start_transaction();

            assert_definition(
                &txn,
                "create table schema_name.table_name(column_si smallint);",
                Ok(QueryEvent::TableCreated),
            );
            assert_query(
                &txn,
                "insert into schema_name.table_name values (2);",
                Ok(QueryExecutionResult::Inserted(1)),
            );
            txn.commit();

            with_schema
        }

        #[rstest::rstest]
        #[ignore] // TODO: type coercion
        fn addition(with_table: QueryEngine) {
            let txn = with_table.start_transaction();

            assert_query(
                &txn,
                "update schema_name.table_name set column_si = 1 + 2;",
                Ok(QueryExecutionResult::Updated(1)),
            );
            assert_query(
                &txn,
                "select * from schema_name.table_name;",
                Ok(QueryExecutionResult::Selected((
                    vec![("column_si".to_owned(), SMALLINT)],
                    vec![vec![small_int(3)]],
                ))),
            );

            txn.commit();
        }

        #[rstest::rstest]
        #[ignore] // TODO: type coercion
        fn subtraction(with_table: QueryEngine) {
            let txn = with_table.start_transaction();

            assert_query(
                &txn,
                "update schema_name.table_name set column_si = 1 - 2;",
                Ok(QueryExecutionResult::Updated(1)),
            );
            assert_query(
                &txn,
                "select * from schema_name.table_name;",
                Ok(QueryExecutionResult::Selected((
                    vec![("column_si".to_owned(), SMALLINT)],
                    vec![vec![small_int(-1)]],
                ))),
            );
            txn.commit();
        }

        #[rstest::rstest]
        #[ignore] // TODO: type coercion
        fn multiplication(with_table: QueryEngine) {
            let txn = with_table.start_transaction();

            assert_query(
                &txn,
                "update schema_name.table_name set column_si = 3 * 2;",
                Ok(QueryExecutionResult::Updated(1)),
            );
            assert_query(
                &txn,
                "select * from schema_name.table_name;",
                Ok(QueryExecutionResult::Selected((
                    vec![("column_si".to_owned(), SMALLINT)],
                    vec![vec![small_int(6)]],
                ))),
            );
            txn.commit();
        }

        #[rstest::rstest]
        #[ignore] // TODO: type coercion
        fn division(with_table: QueryEngine) {
            let txn = with_table.start_transaction();

            assert_query(
                &txn,
                "update schema_name.table_name set column_si = 8 / 2;",
                Ok(QueryExecutionResult::Updated(1)),
            );
            assert_query(
                &txn,
                "select * from schema_name.table_name;",
                Ok(QueryExecutionResult::Selected((
                    vec![("column_si".to_owned(), SMALLINT)],
                    vec![vec![small_int(4)]],
                ))),
            );
            txn.commit();
        }

        #[rstest::rstest]
        #[ignore] // TODO: type coercion
        fn modulo(with_table: QueryEngine) {
            let txn = with_table.start_transaction();

            assert_query(
                &txn,
                "update schema_name.table_name set column_si = 8 % 2;",
                Ok(QueryExecutionResult::Updated(1)),
            );
            assert_query(
                &txn,
                "select * from schema_name.table_name;",
                Ok(QueryExecutionResult::Selected((
                    vec![("column_si".to_owned(), SMALLINT)],
                    vec![vec![small_int(0)]],
                ))),
            );
            txn.commit();
        }

        #[rstest::rstest]
        #[ignore] // TODO: type coercion
        fn exponentiation(with_table: QueryEngine) {
            let txn = with_table.start_transaction();

            assert_query(
                &txn,
                "update schema_name.table_name set column_si = 8 ^ 2;",
                Ok(QueryExecutionResult::Updated(1)),
            );
            assert_query(
                &txn,
                "select * from schema_name.table_name;",
                Ok(QueryExecutionResult::Selected((
                    vec![("column_si".to_owned(), SMALLINT)],
                    vec![vec![small_int(64)]],
                ))),
            );
            txn.commit();
        }

        #[rstest::rstest]
        #[ignore] //TODO: TypeInference#infer_static is not implemented
        fn square_root(with_table: QueryEngine) {
            let txn = with_table.start_transaction();

            assert_query(
                &txn,
                "update schema_name.table_name set column_si = |/ 16;",
                Ok(QueryExecutionResult::Updated(1)),
            );
            assert_query(
                &txn,
                "select * from schema_name.table_name;",
                Ok(QueryExecutionResult::Selected((
                    vec![("column_si".to_owned(), SMALLINT)],
                    vec![vec![small_int(4)]],
                ))),
            );
            txn.commit();
        }

        #[rstest::rstest]
        #[ignore] //TODO: TypeInference#infer_static is not implemented
        fn cube_root(with_table: QueryEngine) {
            let txn = with_table.start_transaction();

            assert_query(
                &txn,
                "update schema_name.table_name set column_si = ||/ 8;",
                Ok(QueryExecutionResult::Updated(1)),
            );
            assert_query(
                &txn,
                "select * from schema_name.table_name;",
                Ok(QueryExecutionResult::Selected((
                    vec![("column_si".to_owned(), SMALLINT)],
                    vec![vec![small_int(1)]],
                ))),
            );
            txn.commit();
        }

        #[rstest::rstest]
        #[ignore] // TODO: type coercion
        fn factorial(with_table: QueryEngine) {
            let txn = with_table.start_transaction();

            assert_query(
                &txn,
                "update schema_name.table_name set column_si = 5!;",
                Ok(QueryExecutionResult::Updated(1)),
            );
            assert_query(
                &txn,
                "select * from schema_name.table_name;",
                Ok(QueryExecutionResult::Selected((
                    vec![("column_si".to_owned(), SMALLINT)],
                    vec![vec![small_int(120)]],
                ))),
            );
            txn.commit();
        }

        #[rstest::rstest]
        #[ignore] // TODO: type coercion
        fn prefix_factorial(with_table: QueryEngine) {
            let txn = with_table.start_transaction();

            assert_query(
                &txn,
                "update schema_name.table_name set column_si = !!5;",
                Ok(QueryExecutionResult::Updated(1)),
            );
            assert_query(
                &txn,
                "select * from schema_name.table_name;",
                Ok(QueryExecutionResult::Selected((
                    vec![("column_si".to_owned(), SMALLINT)],
                    vec![vec![small_int(120)]],
                ))),
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn absolute_value(with_table: QueryEngine) {
            let txn = with_table.start_transaction();

            assert_query(
                &txn,
                "update schema_name.table_name set column_si = @ -5;",
                Ok(QueryExecutionResult::Updated(1)),
            );
            assert_query(
                &txn,
                "select * from schema_name.table_name;",
                Ok(QueryExecutionResult::Selected((
                    vec![("column_si".to_owned(), SMALLINT)],
                    vec![vec![small_int(5)]],
                ))),
            );
            txn.commit();
        }

        #[rstest::rstest]
        #[ignore] // TODO: type coercion
        fn bitwise_and(with_table: QueryEngine) {
            let txn = with_table.start_transaction();

            assert_query(
                &txn,
                "update schema_name.table_name set column_si = 5 & 1;",
                Ok(QueryExecutionResult::Updated(1)),
            );
            assert_query(
                &txn,
                "select * from schema_name.table_name;",
                Ok(QueryExecutionResult::Selected((
                    vec![("column_si".to_owned(), SMALLINT)],
                    vec![vec![small_int(1)]],
                ))),
            );
            txn.commit();
        }

        #[rstest::rstest]
        #[ignore] // TODO: type coercion
        fn bitwise_or(with_table: QueryEngine) {
            let txn = with_table.start_transaction();

            assert_query(
                &txn,
                "update schema_name.table_name set column_si = 5 | 2;",
                Ok(QueryExecutionResult::Updated(1)),
            );
            assert_query(
                &txn,
                "select * from schema_name.table_name;",
                Ok(QueryExecutionResult::Selected((
                    vec![("column_si".to_owned(), SMALLINT)],
                    vec![vec![small_int(7)]],
                ))),
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn bitwise_not(with_table: QueryEngine) {
            let txn = with_table.start_transaction();

            assert_query(
                &txn,
                "update schema_name.table_name set column_si = ~1;",
                Ok(QueryExecutionResult::Updated(1)),
            );
            assert_query(
                &txn,
                "select * from schema_name.table_name;",
                Ok(QueryExecutionResult::Selected((
                    vec![("column_si".to_owned(), SMALLINT)],
                    vec![vec![small_int(-2)]],
                ))),
            );
            txn.commit();
        }

        #[rstest::rstest]
        #[ignore] // TODO: type coercion
        fn bitwise_shift_left(with_table: QueryEngine) {
            let txn = with_table.start_transaction();

            assert_query(
                &txn,
                "update schema_name.table_name set column_si = 1 << 4;",
                Ok(QueryExecutionResult::Updated(1)),
            );
            assert_query(
                &txn,
                "select * from schema_name.table_name;",
                Ok(QueryExecutionResult::Selected((
                    vec![("column_si".to_owned(), SMALLINT)],
                    vec![vec![small_int(16)]],
                ))),
            );
            txn.commit();
        }

        #[rstest::rstest]
        #[ignore] // TODO: type coercion
        fn bitwise_right_left(with_table: QueryEngine) {
            let txn = with_table.start_transaction();

            assert_query(
                &txn,
                "update schema_name.table_name set column_si = 8 >> 2;",
                Ok(QueryExecutionResult::Updated(1)),
            );
            assert_query(
                &txn,
                "select * from schema_name.table_name;",
                Ok(QueryExecutionResult::Selected((
                    vec![("column_si".to_owned(), SMALLINT)],
                    vec![vec![small_int(2)]],
                ))),
            );
            txn.commit();
        }

        #[rstest::rstest]
        #[ignore] // TODO: type coercion
        fn evaluate_many_operations(with_table: QueryEngine) {
            let txn = with_table.start_transaction();

            assert_query(
                &txn,
                "update schema_name.table_name set column_si = 5 & 13 % 10 + 1 * 20 - 40 / 4;",
                Ok(QueryExecutionResult::Updated(1)),
            );
            assert_query(
                &txn,
                "select * from schema_name.table_name;",
                Ok(QueryExecutionResult::Selected((
                    vec![("column_si".to_owned(), SMALLINT)],
                    vec![vec![small_int(5)]],
                ))),
            );
            txn.commit();
        }
    }

    #[cfg(test)]
    mod string {
        use super::*;

        #[rstest::fixture]
        fn with_table(with_schema: QueryEngine) -> QueryEngine {
            let txn = with_schema.start_transaction();

            assert_definition(
                &txn,
                "create table schema_name.table_name(strings char(5));",
                Ok(QueryEvent::TableCreated),
            );
            assert_query(
                &txn,
                "insert into schema_name.table_name values ('x');",
                Ok(QueryExecutionResult::Inserted(1)),
            );
            txn.commit();

            with_schema
        }

        #[rstest::rstest]
        fn concatenation(with_table: QueryEngine) {
            let txn = with_table.start_transaction();

            assert_query(
                &txn,
                "update schema_name.table_name set strings = '123' || '45';",
                Ok(QueryExecutionResult::Updated(1)),
            );
            assert_query(
                &txn,
                "select * from schema_name.table_name;",
                Ok(QueryExecutionResult::Selected((
                    vec![("strings".to_owned(), CHAR)],
                    vec![vec![string("12345")]],
                ))),
            );
            txn.commit();
        }

        #[rstest::rstest]
        #[ignore] //TODO: TypeInference#infer_static is not implemented
        fn concatenation_with_number(with_table: QueryEngine) {
            let txn = with_table.start_transaction();

            assert_query(
                &txn,
                "update schema_name.table_name set strings = 1 || '45';",
                Ok(QueryExecutionResult::Updated(1)),
            );
            assert_query(
                &txn,
                "select * from schema_name.table_name;",
                Ok(QueryExecutionResult::Selected((
                    vec![("strings".to_owned(), CHAR)],
                    vec![vec![string("145")]],
                ))),
            );
            assert_query(
                &txn,
                "update schema_name.table_name set strings = '45' || 1;",
                Ok(QueryExecutionResult::Updated(1)),
            );
            assert_query(
                &txn,
                "select * from schema_name.table_name;",
                Ok(QueryExecutionResult::Selected((
                    vec![("strings".to_owned(), CHAR)],
                    vec![vec![string("451")]],
                ))),
            );
            txn.commit();
        }

        #[rstest::rstest]
        fn non_string_concatenation_not_supported(with_table: QueryEngine) {
            let txn = with_table.start_transaction();

            assert_query(
                &txn,
                "update schema_name.table_name set strings = 1 || 2;",
                Err(QueryError::undefined_function(
                    "||".to_owned(),
                    "smallint".to_owned(),
                    "smallint".to_owned(),
                )),
            );
            txn.commit();
        }
    }
}
