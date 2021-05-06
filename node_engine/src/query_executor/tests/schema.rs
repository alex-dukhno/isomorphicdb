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
    let query_engine = TransactionManager::new(database);

    let txn = query_engine.start_transaction();
    assert_statement(
        &txn,
        "create schema schema_name;",
        vec![Outbound::SchemaCreated, Outbound::ReadyForQuery],
    );
    txn.commit();
}

#[test]
fn create_same_schema() {
    let database = Database::new("IN_MEMORY");
    let query_engine = TransactionManager::new(database);

    let txn = query_engine.start_transaction();
    assert_statement(
        &txn,
        "create schema schema_name;",
        vec![Outbound::SchemaCreated, Outbound::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "create schema schema_name;",
        vec![
            QueryError::schema_already_exists("schema_name").into(),
            Outbound::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[test]
fn drop_schema() {
    let database = Database::new("IN_MEMORY");
    let query_engine = TransactionManager::new(database);

    let txn = query_engine.start_transaction();
    assert_statement(
        &txn,
        "create schema schema_name;",
        vec![Outbound::SchemaCreated, Outbound::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "drop schema schema_name;",
        vec![Outbound::SchemaDropped, Outbound::ReadyForQuery],
    );
    txn.commit();
}

#[test]
fn drop_non_existent_schema() {
    let database = Database::new("IN_MEMORY");
    let query_engine = TransactionManager::new(database);

    let txn = query_engine.start_transaction();
    assert_statement(
        &txn,
        "drop schema non_existent;",
        vec![
            QueryError::schema_does_not_exist("non_existent").into(),
            Outbound::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[test]
fn drop_if_exists_non_existent_schema() {
    let database = Database::new("IN_MEMORY");
    let query_engine = TransactionManager::new(database);

    let txn = query_engine.start_transaction();
    assert_statement(
        &txn,
        "drop schema if exists non_existent;",
        vec![Outbound::SchemaDropped, Outbound::ReadyForQuery],
    );
    txn.commit();
}

#[test]
fn drop_if_exists_existent_and_non_existent_schema() {
    let database = Database::new("IN_MEMORY");
    let query_engine = TransactionManager::new(database);

    let txn = query_engine.start_transaction();
    assert_statement(
        &txn,
        "create schema existent_schema;",
        vec![Outbound::SchemaCreated, Outbound::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "drop schema if exists non_existent, existent_schema;",
        vec![Outbound::SchemaDropped, Outbound::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "create schema existent_schema;",
        vec![Outbound::SchemaCreated, Outbound::ReadyForQuery],
    );
    txn.commit();
}

#[test]
fn select_from_nonexistent_schema() {
    let database = Database::new("IN_MEMORY");
    let query_engine = TransactionManager::new(database);

    let txn = query_engine.start_transaction();
    assert_statement(
        &txn,
        "select * from non_existent.some_table;",
        vec![
            QueryError::schema_does_not_exist("non_existent").into(),
            Outbound::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[test]
fn select_named_columns_from_nonexistent_schema() {
    let database = Database::new("IN_MEMORY");
    let query_engine = TransactionManager::new(database);

    let txn = query_engine.start_transaction();
    assert_statement(
        &txn,
        "select column_1 from schema_name.table_name;",
        vec![
            QueryError::schema_does_not_exist("schema_name").into(),
            Outbound::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[test]
fn insert_into_table_in_nonexistent_schema() {
    let database = Database::new("IN_MEMORY");
    let query_engine = TransactionManager::new(database);

    let txn = query_engine.start_transaction();
    assert_statement(
        &txn,
        "insert into schema_name.table_name values (123);",
        vec![
            QueryError::schema_does_not_exist("schema_name").into(),
            Outbound::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[test]
fn update_records_in_table_from_non_existent_schema() {
    let database = Database::new("IN_MEMORY");
    let query_engine = TransactionManager::new(database);

    let txn = query_engine.start_transaction();
    assert_statement(
        &txn,
        "update schema_name.table_name set column_test=789;",
        vec![
            QueryError::schema_does_not_exist("schema_name").into(),
            Outbound::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[test]
fn delete_from_table_in_nonexistent_schema() {
    let database = Database::new("IN_MEMORY");
    let query_engine = TransactionManager::new(database);

    let txn = query_engine.start_transaction();
    assert_statement(
        &txn,
        "delete from schema_name.table_name;",
        vec![
            QueryError::schema_does_not_exist("schema_name").into(),
            Outbound::ReadyForQuery,
        ],
    );
    txn.commit();
}
