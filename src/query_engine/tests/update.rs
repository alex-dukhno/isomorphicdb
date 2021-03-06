// Copyright 2020 - present Alex Dukhno
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
fn update_all_records(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(Command::Query {
            sql: "create table schema_name.table_name (column_test smallint);".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(Command::Query {
            sql: "insert into schema_name.table_name values (123), (456);".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(2)));

    engine
        .execute(Command::Query {
            sql: "select * from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
            "column_test",
            PgType::SmallInt,
        )])),
        Ok(QueryEvent::DataRow(vec!["123".to_owned()])),
        Ok(QueryEvent::DataRow(vec!["456".to_owned()])),
        Ok(QueryEvent::RecordsSelected(2)),
    ]);

    engine
        .execute(Command::Query {
            sql: "update schema_name.table_name set column_test=789;".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(2)));

    engine
        .execute(Command::Query {
            sql: "select * from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
            "column_test",
            PgType::SmallInt,
        )])),
        Ok(QueryEvent::DataRow(vec!["789".to_owned()])),
        Ok(QueryEvent::DataRow(vec!["789".to_owned()])),
        Ok(QueryEvent::RecordsSelected(2)),
    ]);
}

#[rstest::rstest]
fn update_single_column_of_all_records(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(Command::Query {
            sql: "create table schema_name.table_name (col1 smallint, col2 smallint);".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(Command::Query {
            sql: "insert into schema_name.table_name values (123, 789), (456, 789);".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(2)));

    engine
        .execute(Command::Query {
            sql: "select * from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ColumnMetadata::new("col1", PgType::SmallInt),
            ColumnMetadata::new("col2", PgType::SmallInt),
        ])),
        Ok(QueryEvent::DataRow(vec!["123".to_owned(), "789".to_owned()])),
        Ok(QueryEvent::DataRow(vec!["456".to_owned(), "789".to_owned()])),
        Ok(QueryEvent::RecordsSelected(2)),
    ]);

    engine
        .execute(Command::Query {
            sql: "update schema_name.table_name set col2=357;".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(2)));

    engine
        .execute(Command::Query {
            sql: "select * from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ColumnMetadata::new("col1", PgType::SmallInt),
            ColumnMetadata::new("col2", PgType::SmallInt),
        ])),
        Ok(QueryEvent::DataRow(vec!["123".to_owned(), "357".to_owned()])),
        Ok(QueryEvent::DataRow(vec!["456".to_owned(), "357".to_owned()])),
        Ok(QueryEvent::RecordsSelected(2)),
    ]);
}

#[rstest::rstest]
fn update_multiple_columns_of_all_records(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(Command::Query {
            sql: "create table schema_name.table_name (col1 smallint, col2 smallint, col3 smallint);".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(Command::Query {
            sql: "insert into schema_name.table_name values (111, 222, 333), (444, 555, 666);".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(2)));

    engine
        .execute(Command::Query {
            sql: "select * from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ColumnMetadata::new("col1", PgType::SmallInt),
            ColumnMetadata::new("col2", PgType::SmallInt),
            ColumnMetadata::new("col3", PgType::SmallInt),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "111".to_owned(),
            "222".to_owned(),
            "333".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "444".to_owned(),
            "555".to_owned(),
            "666".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(2)),
    ]);

    engine
        .execute(Command::Query {
            sql: "update schema_name.table_name set col3=777, col1=999;".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(2)));

    engine
        .execute(Command::Query {
            sql: "select * from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");

    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ColumnMetadata::new("col1", PgType::SmallInt),
            ColumnMetadata::new("col2", PgType::SmallInt),
            ColumnMetadata::new("col3", PgType::SmallInt),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "999".to_owned(),
            "222".to_owned(),
            "777".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "999".to_owned(),
            "555".to_owned(),
            "777".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(2)),
    ]);
}

#[rstest::rstest]
fn update_all_records_in_multiple_columns(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(Command::Query {
            sql: "create table schema_name.table_name (column_1 smallint, column_2 smallint, column_3 smallint);"
                .to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(Command::Query {
            sql: "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6), (7, 8, 9);".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(3)));

    engine
        .execute(Command::Query {
            sql: "select * from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ColumnMetadata::new("column_1", PgType::SmallInt),
            ColumnMetadata::new("column_2", PgType::SmallInt),
            ColumnMetadata::new("column_3", PgType::SmallInt),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "1".to_owned(),
            "2".to_owned(),
            "3".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "4".to_owned(),
            "5".to_owned(),
            "6".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "7".to_owned(),
            "8".to_owned(),
            "9".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(3)),
    ]);

    engine
        .execute(Command::Query {
            sql: "update schema_name.table_name set column_1=10, column_2=20, column_3=30;".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(3)));

    engine
        .execute(Command::Query {
            sql: "select * from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");

    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ColumnMetadata::new("column_1", PgType::SmallInt),
            ColumnMetadata::new("column_2", PgType::SmallInt),
            ColumnMetadata::new("column_3", PgType::SmallInt),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "10".to_owned(),
            "20".to_owned(),
            "30".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "10".to_owned(),
            "20".to_owned(),
            "30".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "10".to_owned(),
            "20".to_owned(),
            "30".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(3)),
    ]);
}

#[rstest::rstest]
fn update_records_in_nonexistent_table(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(Command::Query {
            sql: "update schema_name.table_name set column_test=789;".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Err(QueryError::table_does_not_exist("schema_name.table_name")));
}

#[rstest::rstest]
fn update_non_existent_columns_of_records(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(Command::Query {
            sql: "create table schema_name.table_name (column_test smallint);".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(Command::Query {
            sql: "insert into schema_name.table_name values (123);".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

    engine
        .execute(Command::Query {
            sql: "select * from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
            "column_test",
            PgType::SmallInt,
        )])),
        Ok(QueryEvent::DataRow(vec!["123".to_owned()])),
        Ok(QueryEvent::RecordsSelected(1)),
    ]);

    engine
        .execute(Command::Query {
            sql: "update schema_name.table_name set col1=456, col2=789;".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_many(vec![Err(QueryError::column_does_not_exist("col1"))]);
}

#[rstest::rstest]
fn test_update_with_dynamic_expression(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(Command::Query {
            sql: "create table schema_name.table_name (\
            si_column_1 smallint, \
            si_column_2 smallint, \
            si_column_3 smallint);"
                .to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(Command::Query {
            sql: "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6), (7, 8, 9);".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(3)));

    engine
        .execute(Command::Query {
            sql: "select * from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ColumnMetadata::new("si_column_1", PgType::SmallInt),
            ColumnMetadata::new("si_column_2", PgType::SmallInt),
            ColumnMetadata::new("si_column_3", PgType::SmallInt),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "1".to_owned(),
            "2".to_owned(),
            "3".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "4".to_owned(),
            "5".to_owned(),
            "6".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "7".to_owned(),
            "8".to_owned(),
            "9".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(3)),
    ]);

    engine
        .execute(Command::Query {
            sql: "update schema_name.table_name \
        set \
            si_column_1 = 2 * si_column_1, \
            si_column_2 = 2 * (si_column_1 + si_column_2), \
            si_column_3 = (si_column_3 + (2 * (si_column_1 + si_column_2)));"
                .to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(3)));

    engine
        .execute(Command::Query {
            sql: "select * from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ColumnMetadata::new("si_column_1", PgType::SmallInt),
            ColumnMetadata::new("si_column_2", PgType::SmallInt),
            ColumnMetadata::new("si_column_3", PgType::SmallInt),
        ])),
        Ok(QueryEvent::DataRow(vec![
            2.to_string(),
            (2 * (1 + 2)).to_string(),
            (3 + (2 * (1 + 2))).to_string(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            (2 * 4).to_string(),
            (2 * (4 + 5)).to_string(),
            (6 + (2 * (4 + 5))).to_string(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            (2 * 7).to_string(),
            (2 * (7 + 8)).to_string(),
            (9 + (2 * (7 + 8))).to_string(),
        ])),
        Ok(QueryEvent::RecordsSelected(3)),
    ]);
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
                .execute(Command::Query {
                    sql: "create table schema_name.table_name(column_si smallint);".to_owned(),
                })
                .expect("query executed");
            engine
                .execute(Command::Query {
                    sql: "insert into schema_name.table_name values (2);".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_till_this_moment(vec![
                Ok(QueryEvent::TableCreated),
                Ok(QueryEvent::QueryComplete),
                Ok(QueryEvent::RecordsInserted(1)),
                Ok(QueryEvent::QueryComplete),
            ]);

            (engine, collector)
        }

        #[rstest::rstest]
        fn addition(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;
            engine
                .execute(Command::Query {
                    sql: "update schema_name.table_name set column_si = 1 + 2;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));

            engine
                .execute(Command::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
                    "column_si",
                    PgType::SmallInt,
                )])),
                Ok(QueryEvent::DataRow(vec!["3".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn subtraction(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;
            engine
                .execute(Command::Query {
                    sql: "update schema_name.table_name set column_si = 1 - 2;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));

            engine
                .execute(Command::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");

            collector.assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
                    "column_si",
                    PgType::SmallInt,
                )])),
                Ok(QueryEvent::DataRow(vec!["-1".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn multiplication(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;
            engine
                .execute(Command::Query {
                    sql: "update schema_name.table_name set column_si = 3 * 2;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));

            engine
                .execute(Command::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");

            collector.assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
                    "column_si",
                    PgType::SmallInt,
                )])),
                Ok(QueryEvent::DataRow(vec!["6".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn division(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;
            engine
                .execute(Command::Query {
                    sql: "update schema_name.table_name set column_si = 8 / 2;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));

            engine
                .execute(Command::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
                    "column_si",
                    PgType::SmallInt,
                )])),
                Ok(QueryEvent::DataRow(vec!["4".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn modulo(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;
            engine
                .execute(Command::Query {
                    sql: "update schema_name.table_name set column_si = 8 % 2;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));

            engine
                .execute(Command::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
                    "column_si",
                    PgType::SmallInt,
                )])),
                Ok(QueryEvent::DataRow(vec!["0".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn exponentiation(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;
            engine
                .execute(Command::Query {
                    sql: "update schema_name.table_name set column_si = 8 ^ 2;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));

            engine
                .execute(Command::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
                    "column_si",
                    PgType::SmallInt,
                )])),
                Ok(QueryEvent::DataRow(vec!["64".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        #[ignore] //TODO: TypeInference#infer_static is not implemented
        fn square_root(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;
            engine
                .execute(Command::Query {
                    sql: "update schema_name.table_name set column_si = |/ 16;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));

            engine
                .execute(Command::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
                    "column_si",
                    PgType::SmallInt,
                )])),
                Ok(QueryEvent::DataRow(vec!["4".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        #[ignore] //TODO: TypeInference#infer_static is not implemented
        fn cube_root(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;
            engine
                .execute(Command::Query {
                    sql: "update schema_name.table_name set column_si = ||/ 8;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));

            engine
                .execute(Command::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
                    "column_si",
                    PgType::SmallInt,
                )])),
                Ok(QueryEvent::DataRow(vec!["2".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn factorial(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;
            engine
                .execute(Command::Query {
                    sql: "update schema_name.table_name set column_si = 5!;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));

            engine
                .execute(Command::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
                    "column_si",
                    PgType::SmallInt,
                )])),
                Ok(QueryEvent::DataRow(vec!["120".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn prefix_factorial(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;
            engine
                .execute(Command::Query {
                    sql: "update schema_name.table_name set column_si = !!5;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));

            engine
                .execute(Command::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
                    "column_si",
                    PgType::SmallInt,
                )])),
                Ok(QueryEvent::DataRow(vec!["120".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn absolute_value(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;
            engine
                .execute(Command::Query {
                    sql: "update schema_name.table_name set column_si = @ -5;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));

            engine
                .execute(Command::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
                    "column_si",
                    PgType::SmallInt,
                )])),
                Ok(QueryEvent::DataRow(vec!["5".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn bitwise_and(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;
            engine
                .execute(Command::Query {
                    sql: "update schema_name.table_name set column_si = 5 & 1;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));

            engine
                .execute(Command::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
                    "column_si",
                    PgType::SmallInt,
                )])),
                Ok(QueryEvent::DataRow(vec!["1".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn bitwise_or(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;
            engine
                .execute(Command::Query {
                    sql: "update schema_name.table_name set column_si = 5 | 2;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));

            engine
                .execute(Command::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
                    "column_si",
                    PgType::SmallInt,
                )])),
                Ok(QueryEvent::DataRow(vec!["7".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn bitwise_not(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;
            engine
                .execute(Command::Query {
                    sql: "update schema_name.table_name set column_si = ~1;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));

            engine
                .execute(Command::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
                    "column_si",
                    PgType::SmallInt,
                )])),
                Ok(QueryEvent::DataRow(vec!["-2".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn bitwise_shift_left(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;
            engine
                .execute(Command::Query {
                    sql: "update schema_name.table_name set column_si = 1 << 4;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));

            engine
                .execute(Command::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
                    "column_si",
                    PgType::SmallInt,
                )])),
                Ok(QueryEvent::DataRow(vec!["16".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn bitwise_right_left(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;
            engine
                .execute(Command::Query {
                    sql: "update schema_name.table_name set column_si = 8 >> 2;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));

            engine
                .execute(Command::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
                    "column_si",
                    PgType::SmallInt,
                )])),
                Ok(QueryEvent::DataRow(vec!["2".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn evaluate_many_operations(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;
            engine
                .execute(Command::Query {
                    sql: "update schema_name.table_name set column_si = 5 & 13 % 10 + 1 * 20 - 40 / 4;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));

            engine
                .execute(Command::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
                    "column_si",
                    PgType::SmallInt,
                )])),
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
                .execute(Command::Query {
                    sql: "create table schema_name.table_name(strings char(5));".to_owned(),
                })
                .expect("query executed");
            engine
                .execute(Command::Query {
                    sql: "insert into schema_name.table_name values ('x');".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_till_this_moment(vec![
                Ok(QueryEvent::TableCreated),
                Ok(QueryEvent::QueryComplete),
                Ok(QueryEvent::RecordsInserted(1)),
                Ok(QueryEvent::QueryComplete),
            ]);

            (engine, collector)
        }

        #[rstest::rstest]
        fn concatenation(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;
            engine
                .execute(Command::Query {
                    sql: "update schema_name.table_name set strings = '123' || '45';".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));

            engine
                .execute(Command::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
                    "strings",
                    PgType::Char,
                )])),
                Ok(QueryEvent::DataRow(vec!["12345".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        #[ignore] //TODO: TypeInference#infer_static is not implemented
        fn concatenation_with_number(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;
            engine
                .execute(Command::Query {
                    sql: "update schema_name.table_name set strings = 1 || '45';".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));

            engine
                .execute(Command::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
                    "strings",
                    PgType::Char,
                )])),
                Ok(QueryEvent::DataRow(vec!["145".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);

            engine
                .execute(Command::Query {
                    sql: "update schema_name.table_name set strings = '45' || 1;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));

            engine
                .execute(Command::Query {
                    sql: "select * from schema_name.table_name;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_many(vec![
                Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
                    "strings",
                    PgType::Char,
                )])),
                Ok(QueryEvent::DataRow(vec!["451".to_owned()])),
                Ok(QueryEvent::RecordsSelected(1)),
            ]);
        }

        #[rstest::rstest]
        fn non_string_concatenation_not_supported(with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = with_table;
            engine
                .execute(Command::Query {
                    sql: "update schema_name.table_name set strings = 1 || 2;".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Err(QueryError::undefined_function(
                "||".to_owned(),
                "smallint".to_owned(),
                "smallint".to_owned(),
            )));
        }
    }
}
