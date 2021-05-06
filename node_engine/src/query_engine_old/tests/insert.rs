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
fn insert_into_nonexistent_table(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;

    engine
        .execute(InboundMessage::Query {
            sql: "insert into schema_name.table_name values (123);".to_owned(),
        })
        .expect("query executed");

    collector
        .lock()
        .unwrap()
        .assert_receive_single(Err(QueryError::table_does_not_exist("schema_name.table_name")));
}

#[rstest::rstest]
fn insert_value_in_non_existent_column(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;

    engine
        .execute(InboundMessage::Query {
            sql: "create table schema_name.table_name (column_test smallint);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(InboundMessage::Query {
            sql: "insert into schema_name.table_name (non_existent) values (123);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Err(QueryError::column_does_not_exist("non_existent")));
}

#[rstest::rstest]
fn insert_and_select_single_row(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;

    engine
        .execute(InboundMessage::Query {
            sql: "create table schema_name.table_name (column_test smallint);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(InboundMessage::Query {
            sql: "insert into schema_name.table_name values (123);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

    engine
        .execute(InboundMessage::Query {
            sql: "select * from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.lock().unwrap().assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![("column_test".to_owned(), SMALLINT)])),
        Ok(QueryEvent::DataRow(vec!["123".to_owned()])),
        Ok(QueryEvent::RecordsSelected(1)),
    ]);
}

#[rstest::rstest]
fn insert_and_select_multiple_rows(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;

    engine
        .execute(InboundMessage::Query {
            sql: "create table schema_name.table_name (column_test smallint);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(InboundMessage::Query {
            sql: "insert into schema_name.table_name values (123);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

    engine
        .execute(InboundMessage::Query {
            sql: "insert into schema_name.table_name values (456);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

    engine
        .execute(InboundMessage::Query {
            sql: "select * from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.lock().unwrap().assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![("column_test".to_owned(), SMALLINT)])),
        Ok(QueryEvent::DataRow(vec!["123".to_owned()])),
        Ok(QueryEvent::DataRow(vec!["456".to_owned()])),
        Ok(QueryEvent::RecordsSelected(2)),
    ]);
}

#[rstest::rstest]
fn insert_and_select_named_columns(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;

    engine
        .execute(InboundMessage::Query {
            sql: "create table schema_name.table_name (col1 smallint, col2 smallint, col3 smallint);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(InboundMessage::Query {
            sql: "insert into schema_name.table_name (col2, col3, col1) values (1, 2, 3), (4, 5, 6);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::RecordsInserted(2)));

    engine
        .execute(InboundMessage::Query {
            sql: "select * from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.lock().unwrap().assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ("col1".to_owned(), SMALLINT),
            ("col2".to_owned(), SMALLINT),
            ("col3".to_owned(), SMALLINT),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "3".to_owned(),
            "1".to_owned(),
            "2".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "6".to_owned(),
            "4".to_owned(),
            "5".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(2)),
    ]);
}

#[rstest::rstest]
fn insert_multiple_rows(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;

    engine
        .execute(InboundMessage::Query {
            sql: "create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);"
                .to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(InboundMessage::Query {
            sql: "insert into schema_name.table_name values (1, 4, 7), (2, 5, 8), (3, 6, 9);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::RecordsInserted(3)));

    engine
        .execute(InboundMessage::Query {
            sql: "select * from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");

    collector.lock().unwrap().assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ("column_1".to_owned(), SMALLINT),
            ("column_2".to_owned(), SMALLINT),
            ("column_3".to_owned(), SMALLINT),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "1".to_owned(),
            "4".to_owned(),
            "7".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "2".to_owned(),
            "5".to_owned(),
            "8".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "3".to_owned(),
            "6".to_owned(),
            "9".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(3)),
    ]);
}

#[rstest::rstest]
fn insert_and_select_different_integer_types(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(InboundMessage::Query {
            sql: "create table schema_name.table_name (column_si smallint, column_i integer, column_bi bigint);"
                .to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(InboundMessage::Query {
            sql: "insert into schema_name.table_name values(-32768, -2147483648, -9223372036854775808);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

    engine
        .execute(InboundMessage::Query {
            sql: "insert into schema_name.table_name values(32767, 2147483647, 9223372036854775807);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

    engine
        .execute(InboundMessage::Query {
            sql: "select * from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.lock().unwrap().assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ("column_si".to_owned(), SMALLINT),
            ("column_i".to_owned(), INT),
            ("column_bi".to_owned(), BIGINT),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "-32768".to_owned(),
            "-2147483648".to_owned(),
            "-9223372036854775808".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "32767".to_owned(),
            "2147483647".to_owned(),
            "9223372036854775807".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(2)),
    ]);
}

#[rstest::rstest]
fn insert_and_select_different_character_types(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(InboundMessage::Query {
            sql: "create table schema_name.table_name (column_c char(10), column_vc varchar(10));".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(InboundMessage::Query {
            sql: "insert into schema_name.table_name values('12345abcde', '12345abcde');".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

    engine
        .execute(InboundMessage::Query {
            sql: "insert into schema_name.table_name values('12345abcde', 'abcde');".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

    engine
        .execute(InboundMessage::Query {
            sql: "select * from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.lock().unwrap().assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ("column_c".to_owned(), CHAR),
            ("column_vc".to_owned(), VARCHAR),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "12345abcde".to_owned(),
            "12345abcde".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec!["12345abcde".to_owned(), "abcde".to_owned()])),
        Ok(QueryEvent::RecordsSelected(2)),
    ]);
}

#[rstest::rstest]
fn insert_booleans(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(InboundMessage::Query {
            sql: "create table schema_name.table_name (b boolean);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(InboundMessage::Query {
            sql: "insert into schema_name.table_name values(true);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

    engine
        .execute(InboundMessage::Query {
            sql: "insert into schema_name.table_name values(TRUE::boolean);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

    engine
        .execute(InboundMessage::Query {
            sql: "insert into schema_name.table_name values('true'::boolean);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));
}

#[cfg(test)]
mod operators {
    use super::*;

    #[cfg(test)]
    mod integers {
        use super::*;

        #[rstest::fixture]
        fn with_table(database_with_schema: (InMemory, ResultCollector)) -> (InMemory, ResultCollector) {
            let (mut engine, collector) = database_with_schema;

            engine
                .execute(InboundMessage::Query {
                    sql: "create table schema_name.table_name(column_si smallint);".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_till_this_moment(vec![Ok(QueryEvent::TableCreated), Ok(QueryEvent::QueryComplete)]);

            (engine, collector)
        }

        #[rstest::rstest]
        fn addition(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;

            engine
                .execute(InboundMessage::Query {
                    sql: "insert into schema_name.table_name values (1 + 2);".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(InboundMessage::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.lock().unwrap().assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![("column_si".to_owned(), SMALLINT)])),
                Ok(QueryEvent::DataRow(vec!["3".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn subtraction(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;

            engine
                .execute(InboundMessage::Query {
                    sql: "insert into schema_name.table_name values (1 - 2);".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(InboundMessage::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.lock().unwrap().assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![("column_si".to_owned(), SMALLINT)])),
                Ok(QueryEvent::DataRow(vec!["-1".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn multiplication(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;

            engine
                .execute(InboundMessage::Query {
                    sql: "insert into schema_name.table_name values (3 * 2);".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(InboundMessage::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.lock().unwrap().assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![("column_si".to_owned(), SMALLINT)])),
                Ok(QueryEvent::DataRow(vec!["6".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn division(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;

            engine
                .execute(InboundMessage::Query {
                    sql: "insert into schema_name.table_name values (8 / 2);".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(InboundMessage::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.lock().unwrap().assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![("column_si".to_owned(), SMALLINT)])),
                Ok(QueryEvent::DataRow(vec!["4".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn modulo(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;

            engine
                .execute(InboundMessage::Query {
                    sql: "insert into schema_name.table_name values (8 % 2);".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(InboundMessage::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.lock().unwrap().assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![("column_si".to_owned(), SMALLINT)])),
                Ok(QueryEvent::DataRow(vec!["0".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn exponentiation(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;

            engine
                .execute(InboundMessage::Query {
                    sql: "insert into schema_name.table_name values (8 ^ 2);".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(InboundMessage::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.lock().unwrap().assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![("column_si".to_owned(), SMALLINT)])),
                Ok(QueryEvent::DataRow(vec!["64".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        #[ignore] //TODO: TypeInference#infer_static is not implemented
        fn square_root(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;

            engine
                .execute(InboundMessage::Query {
                    sql: "insert into schema_name.table_name values (|/ 16);".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(InboundMessage::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.lock().unwrap().assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![("column_si".to_owned(), SMALLINT)])),
                Ok(QueryEvent::DataRow(vec!["4".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        #[ignore] //TODO: TypeInference#infer_static is not implemented
        fn cube_root(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;

            engine
                .execute(InboundMessage::Query {
                    sql: "insert into schema_name.table_name values (||/ 8);".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(InboundMessage::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.lock().unwrap().assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![("column_si".to_owned(), SMALLINT)])),
                Ok(QueryEvent::DataRow(vec!["2".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn factorial(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;

            engine
                .execute(InboundMessage::Query {
                    sql: "insert into schema_name.table_name values (5!);".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(InboundMessage::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.lock().unwrap().assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![("column_si".to_owned(), SMALLINT)])),
                Ok(QueryEvent::DataRow(vec!["120".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn prefix_factorial(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;

            engine
                .execute(InboundMessage::Query {
                    sql: "insert into schema_name.table_name values (!!5);".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(InboundMessage::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.lock().unwrap().assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![("column_si".to_owned(), SMALLINT)])),
                Ok(QueryEvent::DataRow(vec!["120".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn absolute_value(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;

            engine
                .execute(InboundMessage::Query {
                    sql: "insert into schema_name.table_name values (@ -5);".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(InboundMessage::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.lock().unwrap().assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![("column_si".to_owned(), SMALLINT)])),
                Ok(QueryEvent::DataRow(vec!["5".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn bitwise_and(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;

            engine
                .execute(InboundMessage::Query {
                    sql: "insert into schema_name.table_name values (5 & 1);".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(InboundMessage::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.lock().unwrap().assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![("column_si".to_owned(), SMALLINT)])),
                Ok(QueryEvent::DataRow(vec!["1".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn bitwise_or(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;

            engine
                .execute(InboundMessage::Query {
                    sql: "insert into schema_name.table_name values (5 | 2);".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(InboundMessage::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.lock().unwrap().assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![("column_si".to_owned(), SMALLINT)])),
                Ok(QueryEvent::DataRow(vec!["7".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn bitwise_not(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;

            engine
                .execute(InboundMessage::Query {
                    sql: "insert into schema_name.table_name values (~1);".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(InboundMessage::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.lock().unwrap().assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![("column_si".to_owned(), SMALLINT)])),
                Ok(QueryEvent::DataRow(vec!["-2".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn bitwise_shift_left(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;

            engine
                .execute(InboundMessage::Query {
                    sql: "insert into schema_name.table_name values (1 << 4);".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(InboundMessage::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.lock().unwrap().assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![("column_si".to_owned(), SMALLINT)])),
                Ok(QueryEvent::DataRow(vec!["16".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn bitwise_right_left(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;

            engine
                .execute(InboundMessage::Query {
                    sql: "insert into schema_name.table_name values (8 >> 2);".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(InboundMessage::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.lock().unwrap().assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![("column_si".to_owned(), SMALLINT)])),
                Ok(QueryEvent::DataRow(vec!["2".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn evaluate_many_operations(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;

            engine
                .execute(InboundMessage::Query {
                    sql: "insert into schema_name.table_name values (5 & 13 % 10 + 1 * 20 - 40 / 4);".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(InboundMessage::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.lock().unwrap().assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![("column_si".to_owned(), SMALLINT)])),
                Ok(QueryEvent::DataRow(vec!["5".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }
    }

    #[cfg(test)]
    mod string {
        use super::*;

        #[rstest::fixture]
        fn with_table(database_with_schema: (InMemory, ResultCollector)) -> (InMemory, ResultCollector) {
            let (mut engine, collector) = database_with_schema;

            engine
                .execute(InboundMessage::Query {
                    sql: "create table schema_name.table_name(strings char(5));".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_till_this_moment(vec![Ok(QueryEvent::TableCreated), Ok(QueryEvent::QueryComplete)]);

            (engine, collector)
        }

        #[rstest::rstest]
        fn concatenation(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;

            engine
                .execute(InboundMessage::Query {
                    sql: "insert into schema_name.table_name values ('123' || '45');".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(InboundMessage::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.lock().unwrap().assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![("strings".to_owned(), CHAR)])),
                Ok(QueryEvent::DataRow(vec!["12345".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        #[ignore] //TODO: TypeInference#infer_static is not implemented
        fn concatenation_with_number(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;

            engine
                .execute(InboundMessage::Query {
                    sql: "insert into schema_name.table_name values (1 || '45');".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(InboundMessage::Query {
                    sql: "insert into schema_name.table_name values ('45' || 1);".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(InboundMessage::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.lock().unwrap().assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![("strings".to_owned(), CHAR)])),
                Ok(QueryEvent::DataRow(vec!["145".to_owned()])),
                Ok(QueryEvent::DataRow(vec!["451".to_owned()])),
                Ok(QueryEvent::RecordsSelected(2)),
            ]);
        }

        #[rstest::rstest]
        fn non_string_concatenation_not_supported(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;

            engine
                .execute(InboundMessage::Query {
                    sql: "insert into schema_name.table_name values (1 || 2);".to_owned(),
                })
                .expect("query executed");
            collector
                .lock()
                .unwrap()
                .assert_receive_single(Err(QueryError::undefined_function(
                    "||".to_owned(),
                    "smallint".to_owned(),
                    "smallint".to_owned(),
                )));
        }
    }
}
