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
fn create_schema(empty_database: (InMemory, ResultCollector)) {
    let (mut engine, collector) = empty_database;
    engine
        .execute(Request::Query {
            sql: "create schema schema_name;".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::SchemaCreated));
}

#[rstest::rstest]
fn create_same_schema(empty_database: (InMemory, ResultCollector)) {
    let (mut engine, collector) = empty_database;
    engine
        .execute(Request::Query {
            sql: "create schema schema_name;".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::SchemaCreated));

    engine
        .execute(Request::Query {
            sql: "create schema schema_name;".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Err(QueryError::schema_already_exists("schema_name")));
}

#[rstest::rstest]
fn drop_schema(empty_database: (InMemory, ResultCollector)) {
    let (mut engine, collector) = empty_database;
    engine
        .execute(Request::Query {
            sql: "create schema schema_name;".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::SchemaCreated));

    engine
        .execute(Request::Query {
            sql: "drop schema schema_name;".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::SchemaDropped));
}

#[rstest::rstest]
fn drop_non_existent_schema(empty_database: (InMemory, ResultCollector)) {
    let (mut engine, collector) = empty_database;

    engine
        .execute(Request::Query {
            sql: "drop schema non_existent;".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Err(QueryError::schema_does_not_exist("non_existent")));
}

#[rstest::rstest]
fn drop_if_exists_non_existent_schema(empty_database: (InMemory, ResultCollector)) {
    let (mut engine, collector) = empty_database;

    engine
        .execute(Request::Query {
            sql: "drop schema if exists non_existent;".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::SchemaDropped));
}

#[rstest::rstest]
fn drop_if_exists_existent_and_non_existent_schema(empty_database: (InMemory, ResultCollector)) {
    let (mut engine, collector) = empty_database;

    engine
        .execute(Request::Query {
            sql: "create schema existent_schema;".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::SchemaCreated));

    engine
        .execute(Request::Query {
            sql: "drop schema if exists non_existent, existent_schema;".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::SchemaDropped));

    engine
        .execute(Request::Query {
            sql: "create schema existent_schema;".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::SchemaCreated));
}

#[rstest::rstest]
fn select_from_nonexistent_schema(empty_database: (InMemory, ResultCollector)) {
    let (mut engine, collector) = empty_database;

    engine
        .execute(Request::Query {
            sql: "select * from non_existent.some_table;".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Err(QueryError::schema_does_not_exist("non_existent")));
}

#[rstest::rstest]
fn select_named_columns_from_nonexistent_schema(empty_database: (InMemory, ResultCollector)) {
    let (mut engine, collector) = empty_database;
    engine
        .execute(Request::Query {
            sql: "select column_1 from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");

    collector
        .lock()
        .unwrap()
        .assert_receive_single(Err(QueryError::schema_does_not_exist("schema_name")));
}

#[rstest::rstest]
fn insert_into_table_in_nonexistent_schema(empty_database: (InMemory, ResultCollector)) {
    let (mut engine, collector) = empty_database;
    engine
        .execute(Request::Query {
            sql: "insert into schema_name.table_name values (123);".to_owned(),
        })
        .expect("query executed");

    collector
        .lock()
        .unwrap()
        .assert_receive_single(Err(QueryError::schema_does_not_exist("schema_name")));
}

#[rstest::rstest]
fn update_records_in_table_from_non_existent_schema(empty_database: (InMemory, ResultCollector)) {
    let (mut engine, collector) = empty_database;
    engine
        .execute(Request::Query {
            sql: "update schema_name.table_name set column_test=789;".to_owned(),
        })
        .expect("query executed");

    collector
        .lock()
        .unwrap()
        .assert_receive_single(Err(QueryError::schema_does_not_exist("schema_name")));
}

#[rstest::rstest]
fn delete_from_table_in_nonexistent_schema(empty_database: (InMemory, ResultCollector)) {
    let (mut engine, collector) = empty_database;
    engine
        .execute(Request::Query {
            sql: "delete from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");

    collector
        .lock()
        .unwrap()
        .assert_receive_single(Err(QueryError::schema_does_not_exist("schema_name")));
}
