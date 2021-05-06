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
fn select_value_by_predicate_on_single_field(database_with_schema: (InMemory, ResultCollector)) {
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
            sql: "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::RecordsInserted(2)));

    engine
        .execute(InboundMessage::Query {
            sql: "select * from schema_name.table_name where col1 = 1".to_owned(),
        })
        .expect("query executed");

    collector.lock().unwrap().assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ("col1".to_owned(), SMALLINT),
            ("col2".to_owned(), SMALLINT),
            ("col3".to_owned(), SMALLINT),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "1".to_owned(),
            "2".to_owned(),
            "3".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(1)),
    ]);
}

#[rstest::rstest]
fn update_value_by_predicate_on_single_field(database_with_schema: (InMemory, ResultCollector)) {
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
            sql: "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6), (7, 8, 9);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::RecordsInserted(3)));

    engine
        .execute(InboundMessage::Query {
            sql: "update schema_name.table_name set col1 = 7 where col1 = 4;".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));

    engine
        .execute(InboundMessage::Query {
            sql: "select * from schema_name.table_name where col1 = 4".to_owned(),
        })
        .expect("query executed");

    collector.lock().unwrap().assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ("col1".to_owned(), SMALLINT),
            ("col2".to_owned(), SMALLINT),
            ("col3".to_owned(), SMALLINT),
        ])),
        Ok(QueryEvent::RecordsSelected(0)),
    ]);

    engine
        .execute(InboundMessage::Query {
            sql: "select * from schema_name.table_name where col1 = 7".to_owned(),
        })
        .expect("query executed");
    collector.lock().unwrap().assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ("col1".to_owned(), SMALLINT),
            ("col2".to_owned(), SMALLINT),
            ("col3".to_owned(), SMALLINT),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "7".to_owned(),
            "5".to_owned(),
            "6".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "7".to_owned(),
            "8".to_owned(),
            "9".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(2)),
    ]);
}

#[rstest::rstest]
fn delete_value_by_predicate_on_single_field(database_with_schema: (InMemory, ResultCollector)) {
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
            sql: "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6), (7, 8, 9);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::RecordsInserted(3)));

    engine
        .execute(InboundMessage::Query {
            sql: "delete from schema_name.table_name where col2 = 5;".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::RecordsDeleted(1)));

    engine
        .execute(InboundMessage::Query {
            sql: "select * from schema_name.table_name".to_owned(),
        })
        .expect("query executed");

    collector.lock().unwrap().assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![
            ("col1".to_owned(), SMALLINT),
            ("col2".to_owned(), SMALLINT),
            ("col3".to_owned(), SMALLINT),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "1".to_owned(),
            "2".to_owned(),
            "3".to_owned(),
        ])),
        Ok(QueryEvent::DataRow(vec![
            "7".to_owned(),
            "8".to_owned(),
            "9".to_owned(),
        ])),
        Ok(QueryEvent::RecordsSelected(2)),
    ]);
}
