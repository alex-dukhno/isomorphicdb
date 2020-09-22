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

mod common;
use common::{empty_database, ResultCollector};
use parser::QueryParser;
use protocol::results::{QueryError, QueryEvent};
use sql_engine::QueryExecutor;

#[rstest::rstest]
fn create_schema(empty_database: (QueryExecutor, QueryParser, ResultCollector)) {
    let (engine, parser, collector) = empty_database;
    engine.execute(&parser.parse("create schema schema_name;").expect("parsed"));
    collector.assert_receive_single(Ok(QueryEvent::SchemaCreated));
}

#[rstest::rstest]
fn create_same_schema(empty_database: (QueryExecutor, QueryParser, ResultCollector)) {
    let (engine, parser, collector) = empty_database;
    engine.execute(&parser.parse("create schema schema_name;").expect("parsed"));
    collector.assert_receive_single(Ok(QueryEvent::SchemaCreated));

    engine.execute(&parser.parse("create schema schema_name;").expect("parsed"));
    collector.assert_receive_single(Err(QueryError::schema_already_exists("schema_name")));
}

#[rstest::rstest]
fn drop_schema(empty_database: (QueryExecutor, QueryParser, ResultCollector)) {
    let (engine, parser, collector) = empty_database;
    engine.execute(&parser.parse("create schema schema_name;").expect("parsed"));
    collector.assert_receive_single(Ok(QueryEvent::SchemaCreated));

    engine.execute(&parser.parse("drop schema schema_name;").expect("parsed"));
    collector.assert_receive_single(Ok(QueryEvent::SchemaDropped));
}

#[rstest::rstest]
fn drop_non_existent_schema(empty_database: (QueryExecutor, QueryParser, ResultCollector)) {
    let (engine, parser, collector) = empty_database;

    engine.execute(&parser.parse("drop schema non_existent;").expect("parsed"));
    collector.assert_receive_single(Err(QueryError::schema_does_not_exist("non_existent")));
}

#[rstest::rstest]
fn select_from_nonexistent_schema(empty_database: (QueryExecutor, QueryParser, ResultCollector)) {
    let (engine, parser, collector) = empty_database;

    engine.execute(&parser.parse("select * from non_existent.some_table;").expect("parsed"));
    collector.assert_receive_single(Err(QueryError::schema_does_not_exist("non_existent")));
}

#[rstest::rstest]
fn select_named_columns_from_nonexistent_schema(empty_database: (QueryExecutor, QueryParser, ResultCollector)) {
    let (engine, parser, collector) = empty_database;
    engine.execute(
        &parser
            .parse("select column_1 from schema_name.table_name;")
            .expect("parsed"),
    );

    collector.assert_receive_single(Err(QueryError::schema_does_not_exist("schema_name")));
}

#[rstest::rstest]
fn insert_into_table_in_nonexistent_schema(empty_database: (QueryExecutor, QueryParser, ResultCollector)) {
    let (engine, parser, collector) = empty_database;
    engine.execute(
        &parser
            .parse("insert into schema_name.table_name values (123);")
            .expect("parsed"),
    );

    collector.assert_receive_single(Err(QueryError::schema_does_not_exist("schema_name")));
}

#[rstest::rstest]
fn update_records_in_table_from_non_existent_schema(empty_database: (QueryExecutor, QueryParser, ResultCollector)) {
    let (engine, parser, collector) = empty_database;
    engine.execute(
        &parser
            .parse("update schema_name.table_name set column_test=789;")
            .expect("parsed"),
    );

    collector.assert_receive_single(Err(QueryError::schema_does_not_exist("schema_name")));
}

#[rstest::rstest]
fn delete_from_table_in_nonexistent_schema(empty_database: (QueryExecutor, QueryParser, ResultCollector)) {
    let (engine, parser, collector) = empty_database;
    engine.execute(&parser.parse("delete from schema_name.table_name;").expect("parsed"));

    collector.assert_receive_single(Err(QueryError::schema_does_not_exist("schema_name")));
}
