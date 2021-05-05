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

#[test]
fn create_schema() {
    let database = Database::new("IN_MEMORY");
    let query_engine = QueryEngine::new(database);

    let txn = query_engine.start_transaction();
    assert_definition(&txn, "create schema schema_name;", Ok(QueryEvent::SchemaCreated));
    txn.commit();
}

#[test]
fn create_same_schema() {
    let database = Database::new("IN_MEMORY");
    let query_engine = QueryEngine::new(database);

    let txn = query_engine.start_transaction();
    assert_definition(&txn, "create schema schema_name;", Ok(QueryEvent::SchemaCreated));
    assert_definition(
        &txn,
        "create schema schema_name;",
        Err(QueryError::schema_already_exists("schema_name")),
    );
    txn.commit();
}

#[test]
fn drop_schema() {
    let database = Database::new("IN_MEMORY");
    let query_engine = QueryEngine::new(database);

    let txn = query_engine.start_transaction();
    assert_definition(&txn, "create schema schema_name;", Ok(QueryEvent::SchemaCreated));
    assert_definition(&txn, "drop schema schema_name;", Ok(QueryEvent::SchemaDropped));
    txn.commit();
}

#[test]
fn drop_non_existent_schema() {
    let database = Database::new("IN_MEMORY");
    let query_engine = QueryEngine::new(database);

    let txn = query_engine.start_transaction();
    assert_definition(
        &txn,
        "drop schema non_existent;",
        Err(QueryError::schema_does_not_exist("non_existent")),
    );
    txn.commit();
}

#[test]
fn drop_if_exists_non_existent_schema() {
    let database = Database::new("IN_MEMORY");
    let query_engine = QueryEngine::new(database);

    let txn = query_engine.start_transaction();
    assert_definition(
        &txn,
        "drop schema if exists non_existent;",
        Ok(QueryEvent::SchemaDropped),
    );
    txn.commit();
}

#[test]
fn drop_if_exists_existent_and_non_existent_schema() {
    let database = Database::new("IN_MEMORY");
    let query_engine = QueryEngine::new(database);

    let txn = query_engine.start_transaction();
    assert_definition(&txn, "create schema existent_schema;", Ok(QueryEvent::SchemaCreated));
    assert_definition(
        &txn,
        "drop schema if exists non_existent, existent_schema;",
        Ok(QueryEvent::SchemaDropped),
    );
    assert_definition(&txn, "create schema existent_schema;", Ok(QueryEvent::SchemaCreated));
    txn.commit();
}

#[test]
fn select_from_nonexistent_schema() {
    let database = Database::new("IN_MEMORY");
    let query_engine = QueryEngine::new(database);

    let txn = query_engine.start_transaction();
    assert_query(
        &txn,
        "select * from non_existent.some_table;",
        Err(QueryError::schema_does_not_exist("non_existent")),
    );
    txn.commit();
}

#[test]
fn select_named_columns_from_nonexistent_schema() {
    let database = Database::new("IN_MEMORY");
    let query_engine = QueryEngine::new(database);

    let txn = query_engine.start_transaction();
    assert_query(
        &txn,
        "select column_1 from schema_name.table_name;",
        Err(QueryError::schema_does_not_exist("schema_name")),
    );
    txn.commit();
}

#[test]
fn insert_into_table_in_nonexistent_schema() {
    let database = Database::new("IN_MEMORY");
    let query_engine = QueryEngine::new(database);

    let txn = query_engine.start_transaction();
    assert_query(
        &txn,
        "insert into schema_name.table_name values (123);",
        Err(QueryError::schema_does_not_exist("schema_name")),
    );
    txn.commit();
}

#[test]
fn update_records_in_table_from_non_existent_schema() {
    let database = Database::new("IN_MEMORY");
    let query_engine = QueryEngine::new(database);

    let txn = query_engine.start_transaction();
    assert_query(
        &txn,
        "update schema_name.table_name set column_test=789;",
        Err(QueryError::schema_does_not_exist("schema_name")),
    );
    txn.commit();
}

#[test]
fn delete_from_table_in_nonexistent_schema() {
    let database = Database::new("IN_MEMORY");
    let query_engine = QueryEngine::new(database);

    let txn = query_engine.start_transaction();
    assert_query(
        &txn,
        "delete from schema_name.table_name;",
        Err(QueryError::schema_does_not_exist("schema_name")),
    );
    txn.commit();
}
