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
fn select_from_not_existed_table(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(InboundMessage::Query {
            sql: "select * from schema_name.non_existent;".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Err(QueryError::table_does_not_exist("schema_name.non_existent")));
}

#[rstest::rstest]
fn select_named_columns_from_non_existent_table(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(InboundMessage::Query {
            sql: "select column_1 from schema_name.non_existent;".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Err(QueryError::table_does_not_exist("schema_name.non_existent")));
}

#[rstest::rstest]
fn select_all_from_table_with_multiple_columns(database_with_schema: (InMemory, ResultCollector)) {
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
            sql: "insert into schema_name.table_name values (123, 456, 789);".to_owned(),
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
            ("column_1".to_owned(), SMALLINT),
            ("column_2".to_owned(), SMALLINT),
            ("column_3".to_owned(), SMALLINT),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "123".to_owned(),
            "456".to_owned(),
            "789".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(1)),
    ]);
}

#[rstest::rstest]
fn select_not_all_columns(database_with_schema: (InMemory, ResultCollector)) {
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
            sql: "select column_3, column_2 from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.lock().unwrap().assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ("column_3".to_owned(), SMALLINT),
            ("column_2".to_owned(), SMALLINT),
        ])),
        Ok(QueryEvent::DataRow(vec!["7".to_owned(), "4".to_owned()])),
        Ok(QueryEvent::DataRow(vec!["8".to_owned(), "5".to_owned()])),
        Ok(QueryEvent::DataRow(vec!["9".to_owned(), "6".to_owned()])),
        Ok(QueryEvent::RecordsSelected(3)),
    ]);
}

#[rstest::rstest]
fn select_non_existing_columns_from_table(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(InboundMessage::Query {
            sql: "create table schema_name.table_name (column_in_table smallint);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(InboundMessage::Query {
            sql: "select column_not_in_table1, column_not_in_table2 from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_many(vec![Err(QueryError::column_does_not_exist("column_not_in_table1"))]);
}

#[rstest::rstest]
fn select_first_and_last_columns_from_table_with_multiple_columns(database_with_schema: (InMemory, ResultCollector)) {
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
            sql: "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6), (7, 8, 9);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::RecordsInserted(3)));

    engine
        .execute(InboundMessage::Query {
            sql: "select column_3, column_1 from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.lock().unwrap().assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ("column_3".to_owned(), SMALLINT),
            ("column_1".to_owned(), SMALLINT),
        ])),
        Ok(QueryEvent::DataRow(vec!["3".to_owned(), "1".to_owned()])),
        Ok(QueryEvent::DataRow(vec!["6".to_owned(), "4".to_owned()])),
        Ok(QueryEvent::DataRow(vec!["9".to_owned(), "7".to_owned()])),
        Ok(QueryEvent::RecordsSelected(3)),
    ]);
}

#[rstest::rstest]
fn select_all_columns_reordered_from_table_with_multiple_columns(database_with_schema: (InMemory, ResultCollector)) {
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
            sql: "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6), (7, 8, 9);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::RecordsInserted(3)));

    engine
        .execute(InboundMessage::Query {
            sql: "select column_3, column_1, column_2 from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.lock().unwrap().assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ("column_3".to_owned(), SMALLINT),
            ("column_1".to_owned(), SMALLINT),
            ("column_2".to_owned(), SMALLINT),
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
        Ok(QueryEvent::DataRow(vec![
            "9".to_owned(),
            "7".to_owned(),
            "8".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(3)),
    ]);
}

#[rstest::rstest]
fn select_with_column_name_duplication(database_with_schema: (InMemory, ResultCollector)) {
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
            sql: "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6), (7, 8, 9);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::RecordsInserted(3)));

    engine
        .execute(InboundMessage::Query {
            sql: "select column_3, column_2, column_1, column_3, column_2 from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.lock().unwrap().assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ("column_3".to_owned(), SMALLINT),
            ("column_2".to_owned(), SMALLINT),
            ("column_1".to_owned(), SMALLINT),
            ("column_3".to_owned(), SMALLINT),
            ("column_2".to_owned(), SMALLINT),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "3".to_owned(),
            "2".to_owned(),
            "1".to_owned(),
            "3".to_owned(),
            "2".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "6".to_owned(),
            "5".to_owned(),
            "4".to_owned(),
            "6".to_owned(),
            "5".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "9".to_owned(),
            "8".to_owned(),
            "7".to_owned(),
            "9".to_owned(),
            "8".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(3)),
    ]);
}

#[rstest::rstest]
fn select_different_integer_types(database_with_schema: (InMemory, ResultCollector)) {
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
        .execute(InboundMessage::Query { sql: "insert into schema_name.table_name values (1000, 2000000, 3000000000), (4000, 5000000, 6000000000), (7000, 8000000, 9000000000);".to_owned()}).expect("query executed");
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
            ("column_si".to_owned(), SMALLINT),
            ("column_i".to_owned(), INT),
            ("column_bi".to_owned(), BIGINT),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "1000".to_owned(),
            "2000000".to_owned(),
            "3000000000".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "4000".to_owned(),
            "5000000".to_owned(),
            "6000000000".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "7000".to_owned(),
            "8000000".to_owned(),
            "9000000000".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(3)),
    ]);
}

#[rstest::rstest]
fn select_different_character_strings_types(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(InboundMessage::Query {
            sql: "create table schema_name.table_name (char_10 char(10), var_char_20 varchar(20));".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(InboundMessage::Query { sql: "insert into schema_name.table_name values ('1234567890', '12345678901234567890'), ('12345', '1234567890');".to_owned()}).expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::RecordsInserted(2)));

    // TODO: string type is not recognizable on SqlTable level
    // engine
    //     .execute(Request::Query { sql: "insert into schema_name.table_name values ('12345', '1234567890     ');".to_owned()}).expect("query executed");
    // collector.lock().unwrap().assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

    engine
        .execute(InboundMessage::Query {
            sql: "select * from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.lock().unwrap().assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ("char_10".to_owned(), CHAR),
            ("var_char_20".to_owned(), VARCHAR),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "1234567890".to_owned(),
            "12345678901234567890".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec!["12345".to_owned(), "1234567890".to_owned()])),
        // Ok(QueryEvent::DataRow(vec!["12345".to_owned(), "1234567890".to_owned()])),
        Ok(QueryEvent::RecordsSelected(2)),
    ]);
}
