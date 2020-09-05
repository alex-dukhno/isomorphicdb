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
use protocol::sql_types::PostgreSqlType;

#[rstest::rstest]
fn insert_into_nonexistent_table(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("insert into schema_name.table_name values (123);")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Err(QueryError::table_does_not_exist("schema_name.table_name".to_owned())),
        Ok(QueryEvent::QueryComplete),
    ]);
}

#[rstest::rstest]
fn insert_value_in_non_existent_column(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_test smallint);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name (non_existent) values (123);")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::QueryComplete),
        Err(QueryError::column_does_not_exist("non_existent".to_owned())),
        Ok(QueryEvent::QueryComplete),
    ]);
}

#[rstest::rstest]
fn insert_and_select_single_row(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_test smallint);")
        .expect("no system errors");

    engine
        .execute("insert into schema_name.table_name values (123);")
        .expect("no system errors");
    engine
        .execute("select * from schema_name.table_name;")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsSelected((
            vec![("column_test".to_owned(), PostgreSqlType::SmallInt)],
            vec![vec!["123".to_owned()]],
        ))),
        Ok(QueryEvent::QueryComplete),
    ]);
}

#[rstest::rstest]
fn insert_and_select_multiple_rows(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_test smallint);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values (123);")
        .expect("no system errors");
    engine
        .execute("select * from schema_name.table_name;")
        .expect("no system errors");

    engine
        .execute("insert into schema_name.table_name values (456);")
        .expect("no system errors");
    engine
        .execute("select * from schema_name.table_name;")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsSelected((
            vec![("column_test".to_owned(), PostgreSqlType::SmallInt)],
            vec![vec!["123".to_owned()]],
        ))),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsSelected((
            vec![("column_test".to_owned(), PostgreSqlType::SmallInt)],
            vec![vec!["123".to_owned()], vec!["456".to_owned()]],
        ))),
        Ok(QueryEvent::QueryComplete),
    ]);
}

#[rstest::rstest]
fn insert_and_select_named_columns(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (col1 smallint, col2 smallint, col3 smallint);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name (col2, col3, col1) values (1, 2, 3), (4, 5, 6);")
        .expect("no system errors");
    engine
        .execute("select * from schema_name.table_name;")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsInserted(2)),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsSelected((
            vec![
                ("col1".to_owned(), PostgreSqlType::SmallInt),
                ("col2".to_owned(), PostgreSqlType::SmallInt),
                ("col3".to_owned(), PostgreSqlType::SmallInt),
            ],
            vec![
                vec!["3".to_owned(), "1".to_owned(), "2".to_owned()],
                vec!["6".to_owned(), "4".to_owned(), "5".to_owned()],
            ],
        ))),
        Ok(QueryEvent::QueryComplete),
    ]);
}

#[rstest::rstest]
fn insert_multiple_rows(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values (1, 4, 7), (2, 5, 8), (3, 6, 9);")
        .expect("no system errors");
    engine
        .execute("select * from schema_name.table_name;")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsInserted(3)),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsSelected((
            vec![
                ("column_1".to_owned(), PostgreSqlType::SmallInt),
                ("column_2".to_owned(), PostgreSqlType::SmallInt),
                ("column_3".to_owned(), PostgreSqlType::SmallInt),
            ],
            vec![
                vec!["1".to_owned(), "4".to_owned(), "7".to_owned()],
                vec!["2".to_owned(), "5".to_owned(), "8".to_owned()],
                vec!["3".to_owned(), "6".to_owned(), "9".to_owned()],
            ],
        ))),
        Ok(QueryEvent::QueryComplete),
    ]);
}

#[rstest::rstest]
fn insert_and_select_different_integer_types(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_si smallint, column_i integer, column_bi bigint, column_serial serial);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values(-32768, -2147483648, -9223372036854775808, 1);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values(32767, 2147483647, 9223372036854775807, 1);")
        .expect("no system errors");
    engine
        .execute("select * from schema_name.table_name;")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsSelected((
            vec![
                ("column_si".to_owned(), PostgreSqlType::SmallInt),
                ("column_i".to_owned(), PostgreSqlType::Integer),
                ("column_bi".to_owned(), PostgreSqlType::BigInt),
                ("column_serial".to_owned(), PostgreSqlType::Integer),
            ],
            vec![
                vec![
                    "-32768".to_owned(),
                    "-2147483648".to_owned(),
                    "-9223372036854775808".to_owned(),
                    "1".to_owned(),
                ],
                vec![
                    "32767".to_owned(),
                    "2147483647".to_owned(),
                    "9223372036854775807".to_owned(),
                    "1".to_owned(),
                ],
            ],
        ))),
        Ok(QueryEvent::QueryComplete),
    ]);
}

#[rstest::rstest]
fn insert_and_select_different_character_types(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_c char(10), column_vc varchar(10));")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values('12345abcde', '12345abcde');")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values('12345abcde', 'abcde');")
        .expect("no system errors");
    engine
        .execute("select * from schema_name.table_name;")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsSelected((
            vec![
                ("column_c".to_owned(), PostgreSqlType::Char),
                ("column_vc".to_owned(), PostgreSqlType::VarChar),
            ],
            vec![
                vec!["12345abcde".to_owned(), "12345abcde".to_owned()],
                vec!["12345abcde".to_owned(), "abcde".to_owned()],
            ],
        ))),
        Ok(QueryEvent::QueryComplete),
    ]);
}

#[rstest::rstest]
fn insert_booleans(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (b boolean);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values(true);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values(TRUE::boolean);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values('true'::boolean);")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::QueryComplete),
    ]);
}

#[cfg(test)]
mod operators {
    use super::*;

    #[cfg(test)]
    mod mathematical {
        use super::*;

        #[cfg(test)]
        mod integers {
            use super::*;

            #[rstest::fixture]
            fn with_table(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) -> (QueryExecutor, Arc<Collector>) {
                let (mut engine, collector) = sql_engine_with_schema;
                engine
                    .execute("create table schema_name.table_name(column_si smallint);")
                    .expect("no system errors");

                (engine, collector)
            }

            #[rstest::rstest]
            fn addition(with_table: (QueryExecutor, Arc<Collector>)) {
                let (mut engine, collector) = with_table;
                engine
                    .execute("insert into schema_name.table_name values (1 + 2);")
                    .expect("no system errors");
                engine
                    .execute("select * from schema_name.table_name;")
                    .expect("no system errors");

                collector.assert_content_for_single_queries(vec![
                    Ok(QueryEvent::SchemaCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::TableCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsInserted(1)),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsSelected((
                        vec![("column_si".to_owned(), PostgreSqlType::SmallInt)],
                        vec![vec!["3".to_owned()]],
                    ))),
                    Ok(QueryEvent::QueryComplete),
                ]);
            }

            #[rstest::rstest]
            fn subtraction(with_table: (QueryExecutor, Arc<Collector>)) {
                let (mut engine, collector) = with_table;
                engine
                    .execute("insert into schema_name.table_name values (1 - 2);")
                    .expect("no system errors");
                engine
                    .execute("select * from schema_name.table_name;")
                    .expect("no system errors");

                collector.assert_content_for_single_queries(vec![
                    Ok(QueryEvent::SchemaCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::TableCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsInserted(1)),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsSelected((
                        vec![("column_si".to_owned(), PostgreSqlType::SmallInt)],
                        vec![vec!["-1".to_owned()]],
                    ))),
                    Ok(QueryEvent::QueryComplete),
                ]);
            }

            #[rstest::rstest]
            fn multiplication(with_table: (QueryExecutor, Arc<Collector>)) {
                let (mut engine, collector) = with_table;
                engine
                    .execute("insert into schema_name.table_name values (3 * 2);")
                    .expect("no system errors");
                engine
                    .execute("select * from schema_name.table_name;")
                    .expect("no system errors");

                collector.assert_content_for_single_queries(vec![
                    Ok(QueryEvent::SchemaCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::TableCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsInserted(1)),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsSelected((
                        vec![("column_si".to_owned(), PostgreSqlType::SmallInt)],
                        vec![vec!["6".to_owned()]],
                    ))),
                    Ok(QueryEvent::QueryComplete),
                ]);
            }

            #[rstest::rstest]
            fn division(with_table: (QueryExecutor, Arc<Collector>)) {
                let (mut engine, collector) = with_table;
                engine
                    .execute("insert into schema_name.table_name values (8 / 2);")
                    .expect("no system errors");
                engine
                    .execute("select * from schema_name.table_name;")
                    .expect("no system errors");

                collector.assert_content_for_single_queries(vec![
                    Ok(QueryEvent::SchemaCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::TableCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsInserted(1)),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsSelected((
                        vec![("column_si".to_owned(), PostgreSqlType::SmallInt)],
                        vec![vec!["4".to_owned()]],
                    ))),
                    Ok(QueryEvent::QueryComplete),
                ]);
            }

            #[rstest::rstest]
            fn modulo(with_table: (QueryExecutor, Arc<Collector>)) {
                let (mut engine, collector) = with_table;
                engine
                    .execute("insert into schema_name.table_name values (8 % 2);")
                    .expect("no system errors");
                engine
                    .execute("select * from schema_name.table_name;")
                    .expect("no system errors");

                collector.assert_content_for_single_queries(vec![
                    Ok(QueryEvent::SchemaCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::TableCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsInserted(1)),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsSelected((
                        vec![("column_si".to_owned(), PostgreSqlType::SmallInt)],
                        vec![vec!["0".to_owned()]],
                    ))),
                    Ok(QueryEvent::QueryComplete),
                ]);
            }

            #[rstest::rstest]
            #[ignore]
            // TODO ^ is bitwise in SQL standard
            //      # is bitwise in PostgreSQL and it does not supported in sqlparser-rs
            fn exponentiation(with_table: (QueryExecutor, Arc<Collector>)) {
                let (mut engine, collector) = with_table;
                engine
                    .execute("insert into schema_name.table_name values (8 ^ 2);")
                    .expect("no system errors");
                engine
                    .execute("select * from schema_name.table_name;")
                    .expect("no system errors");

                collector.assert_content_for_single_queries(vec![
                    Ok(QueryEvent::SchemaCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::TableCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsInserted(1)),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsSelected((
                        vec![("column_si".to_owned(), PostgreSqlType::SmallInt)],
                        vec![vec!["64".to_owned()]],
                    ))),
                    Ok(QueryEvent::QueryComplete),
                ]);
            }

            #[rstest::rstest]
            #[ignore]
            // TODO |/<n> is square root in PostgreSQL and it does not supported in sqlparser-rs
            fn square_root(with_table: (QueryExecutor, Arc<Collector>)) {
                let (mut engine, collector) = with_table;
                engine
                    .execute("insert into schema_name.table_name values (|/ 16);")
                    .expect("no system errors");
                engine
                    .execute("select * from schema_name.table_name;")
                    .expect("no system errors");

                collector.assert_content_for_single_queries(vec![
                    Ok(QueryEvent::SchemaCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::TableCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsInserted(1)),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsSelected((
                        vec![("column_si".to_owned(), PostgreSqlType::SmallInt)],
                        vec![vec!["4".to_owned()]],
                    ))),
                    Ok(QueryEvent::QueryComplete),
                ]);
            }

            #[rstest::rstest]
            #[ignore]
            // TODO ||/<n> is cube root in PostgreSQL and it does not supported in sqlparser-rs
            fn cube_root(with_table: (QueryExecutor, Arc<Collector>)) {
                let (mut engine, collector) = with_table;
                engine
                    .execute("insert into schema_name.table_name values (||/ 8);")
                    .expect("no system errors");
                engine
                    .execute("select * from schema_name.table_name;")
                    .expect("no system errors");

                collector.assert_content_for_single_queries(vec![
                    Ok(QueryEvent::SchemaCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::TableCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsInserted(1)),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsSelected((
                        vec![("column_si".to_owned(), PostgreSqlType::SmallInt)],
                        vec![vec!["2".to_owned()]],
                    ))),
                    Ok(QueryEvent::QueryComplete),
                ]);
            }

            #[rstest::rstest]
            #[ignore]
            // TODO <n>! is factorial in PostgreSQL and it does not supported in sqlparser-rs
            fn factorial(with_table: (QueryExecutor, Arc<Collector>)) {
                let (mut engine, collector) = with_table;
                engine
                    .execute("insert into schema_name.table_name values (5!);")
                    .expect("no system errors");
                engine
                    .execute("select * from schema_name.table_name;")
                    .expect("no system errors");

                collector.assert_content_for_single_queries(vec![
                    Ok(QueryEvent::SchemaCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::TableCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsInserted(1)),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsSelected((
                        vec![("column_si".to_owned(), PostgreSqlType::SmallInt)],
                        vec![vec!["120".to_owned()]],
                    ))),
                    Ok(QueryEvent::QueryComplete),
                ]);
            }

            #[rstest::rstest]
            #[ignore]
            // TODO !!<n> is prefix factorial in PostgreSQL and it does not supported in sqlparser-rs
            fn prefix_factorial(with_table: (QueryExecutor, Arc<Collector>)) {
                let (mut engine, collector) = with_table;
                engine
                    .execute("insert into schema_name.table_name values (!!5);")
                    .expect("no system errors");
                engine
                    .execute("select * from schema_name.table_name;")
                    .expect("no system errors");

                collector.assert_content_for_single_queries(vec![
                    Ok(QueryEvent::SchemaCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::TableCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsInserted(1)),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsSelected((
                        vec![("column_si".to_owned(), PostgreSqlType::SmallInt)],
                        vec![vec!["120".to_owned()]],
                    ))),
                    Ok(QueryEvent::QueryComplete),
                ]);
            }

            #[rstest::rstest]
            #[ignore]
            // TODO @<n> is absolute value in PostgreSQL and it does not supported in sqlparser-rs
            fn absolute_value(with_table: (QueryExecutor, Arc<Collector>)) {
                let (mut engine, collector) = with_table;
                engine
                    .execute("insert into schema_name.table_name values (@-5);")
                    .expect("no system errors");
                engine
                    .execute("select * from schema_name.table_name;")
                    .expect("no system errors");

                collector.assert_content_for_single_queries(vec![
                    Ok(QueryEvent::SchemaCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::TableCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsInserted(1)),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsSelected((
                        vec![("column_si".to_owned(), PostgreSqlType::SmallInt)],
                        vec![vec!["5".to_owned()]],
                    ))),
                    Ok(QueryEvent::QueryComplete),
                ]);
            }

            #[rstest::rstest]
            fn bitwise_and(with_table: (QueryExecutor, Arc<Collector>)) {
                let (mut engine, collector) = with_table;
                engine
                    .execute("insert into schema_name.table_name values (5 & 1);")
                    .expect("no system errors");
                engine
                    .execute("select * from schema_name.table_name;")
                    .expect("no system errors");

                collector.assert_content_for_single_queries(vec![
                    Ok(QueryEvent::SchemaCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::TableCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsInserted(1)),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsSelected((
                        vec![("column_si".to_owned(), PostgreSqlType::SmallInt)],
                        vec![vec!["1".to_owned()]],
                    ))),
                    Ok(QueryEvent::QueryComplete),
                ]);
            }

            #[rstest::rstest]
            fn bitwise_or(with_table: (QueryExecutor, Arc<Collector>)) {
                let (mut engine, collector) = with_table;
                engine
                    .execute("insert into schema_name.table_name values (5 | 2);")
                    .expect("no system errors");
                engine
                    .execute("select * from schema_name.table_name;")
                    .expect("no system errors");

                collector.assert_content_for_single_queries(vec![
                    Ok(QueryEvent::SchemaCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::TableCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsInserted(1)),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsSelected((
                        vec![("column_si".to_owned(), PostgreSqlType::SmallInt)],
                        vec![vec!["7".to_owned()]],
                    ))),
                    Ok(QueryEvent::QueryComplete),
                ]);
            }

            #[rstest::rstest]
            #[ignore]
            // TODO ~ <n> is bitwise NOT in PostgreSQL and it does not supported in sqlparser-rs
            fn bitwise_not(with_table: (QueryExecutor, Arc<Collector>)) {
                let (mut engine, collector) = with_table;
                engine
                    .execute("insert into schema_name.table_name values (~1);")
                    .expect("no system errors");
                engine
                    .execute("select * from schema_name.table_name;")
                    .expect("no system errors");

                collector.assert_content_for_single_queries(vec![
                    Ok(QueryEvent::SchemaCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::TableCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsInserted(1)),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsSelected((
                        vec![("column_si".to_owned(), PostgreSqlType::SmallInt)],
                        vec![vec!["-2".to_owned()]],
                    ))),
                    Ok(QueryEvent::QueryComplete),
                ]);
            }

            #[rstest::rstest]
            #[ignore]
            // TODO <n> << <m> is bitwise SHIFT LEFT in PostgreSQL and it does not supported in sqlparser-rs
            fn bitwise_shift_left(with_table: (QueryExecutor, Arc<Collector>)) {
                let (mut engine, collector) = with_table;
                engine
                    .execute("insert into schema_name.table_name values (1 << 4);")
                    .expect("no system errors");
                engine
                    .execute("select * from schema_name.table_name;")
                    .expect("no system errors");

                collector.assert_content_for_single_queries(vec![
                    Ok(QueryEvent::SchemaCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::TableCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsInserted(1)),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsSelected((
                        vec![("column_si".to_owned(), PostgreSqlType::SmallInt)],
                        vec![vec!["16".to_owned()]],
                    ))),
                    Ok(QueryEvent::QueryComplete),
                ]);
            }

            #[rstest::rstest]
            #[ignore]
            // TODO <n> >> <m> is bitwise SHIFT RIGHT in PostgreSQL and it does not supported in sqlparser-rs
            fn bitwise_shift_right(with_table: (QueryExecutor, Arc<Collector>)) {
                let (mut engine, collector) = with_table;
                engine
                    .execute("insert into schema_name.table_name values (8 >> 2);")
                    .expect("no system errors");
                engine
                    .execute("select * from schema_name.table_name;")
                    .expect("no system errors");

                collector.assert_content_for_single_queries(vec![
                    Ok(QueryEvent::SchemaCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::TableCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsInserted(1)),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsSelected((
                        vec![("column_si".to_owned(), PostgreSqlType::SmallInt)],
                        vec![vec!["2".to_owned()]],
                    ))),
                    Ok(QueryEvent::QueryComplete),
                ]);
            }

            #[rstest::rstest]
            fn evaluate_many_operations(with_table: (QueryExecutor, Arc<Collector>)) {
                let (mut engine, collector) = with_table;
                engine
                    .execute("insert into schema_name.table_name values (5 & 13 % 10 + 1 * 20 - 40 / 4);")
                    .expect("no system errors");
                engine
                    .execute("select * from schema_name.table_name;")
                    .expect("no system errors");

                collector.assert_content_for_single_queries(vec![
                    Ok(QueryEvent::SchemaCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::TableCreated),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsInserted(1)),
                    Ok(QueryEvent::QueryComplete),
                    Ok(QueryEvent::RecordsSelected((
                        vec![("column_si".to_owned(), PostgreSqlType::SmallInt)],
                        vec![vec!["5".to_owned()]],
                    ))),
                    Ok(QueryEvent::QueryComplete),
                ]);
            }
        }
    }

    #[cfg(test)]
    mod string {
        use super::*;

        #[rstest::fixture]
        fn with_table(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) -> (QueryExecutor, Arc<Collector>) {
            let (mut engine, collector) = sql_engine_with_schema;
            engine
                .execute("create table schema_name.table_name(strings char(5));")
                .expect("no system errors");

            (engine, collector)
        }

        #[rstest::rstest]
        fn concatenation(with_table: (QueryExecutor, Arc<Collector>)) {
            let (mut engine, collector) = with_table;
            engine
                .execute("insert into schema_name.table_name values ('123' || '45');")
                .expect("no system errors");
            engine
                .execute("select * from schema_name.table_name;")
                .expect("no system errors");

            collector.assert_content_for_single_queries(vec![
                Ok(QueryEvent::SchemaCreated),
                Ok(QueryEvent::QueryComplete),
                Ok(QueryEvent::TableCreated),
                Ok(QueryEvent::QueryComplete),
                Ok(QueryEvent::RecordsInserted(1)),
                Ok(QueryEvent::QueryComplete),
                Ok(QueryEvent::RecordsSelected((
                    vec![("strings".to_owned(), PostgreSqlType::Char)],
                    vec![vec!["12345".to_owned()]],
                ))),
                Ok(QueryEvent::QueryComplete),
            ]);
        }

        #[rstest::rstest]
        fn concatenation_with_number(with_table: (QueryExecutor, Arc<Collector>)) {
            let (mut engine, collector) = with_table;
            engine
                .execute("insert into schema_name.table_name values (1 || '45');")
                .expect("no system errors");
            engine
                .execute("insert into schema_name.table_name values ('45' || 1);")
                .expect("no system errors");
            engine
                .execute("select * from schema_name.table_name;")
                .expect("no system errors");

            collector.assert_content_for_single_queries(vec![
                Ok(QueryEvent::SchemaCreated),
                Ok(QueryEvent::QueryComplete),
                Ok(QueryEvent::TableCreated),
                Ok(QueryEvent::QueryComplete),
                Ok(QueryEvent::RecordsInserted(1)),
                Ok(QueryEvent::QueryComplete),
                Ok(QueryEvent::RecordsInserted(1)),
                Ok(QueryEvent::QueryComplete),
                Ok(QueryEvent::RecordsSelected((
                    vec![("strings".to_owned(), PostgreSqlType::Char)],
                    vec![vec!["145".to_owned()], vec!["451".to_owned()]],
                ))),
                Ok(QueryEvent::QueryComplete),
            ]);
        }

        #[rstest::rstest]
        fn non_string_concatenation_not_supported(with_table: (QueryExecutor, Arc<Collector>)) {
            let (mut engine, collector) = with_table;

            engine
                .execute("insert into schema_name.table_name values (1 || 2);")
                .expect("no system errors");

            collector.assert_content_for_single_queries(vec![
                Ok(QueryEvent::SchemaCreated),
                Ok(QueryEvent::QueryComplete),
                Ok(QueryEvent::TableCreated),
                Ok(QueryEvent::QueryComplete),
                Err(QueryError::undefined_function(
                    "||".to_owned(),
                    "NUMBER".to_owned(),
                    "NUMBER".to_owned(),
                )),
                Ok(QueryEvent::QueryComplete),
            ]);
        }
    }
}
