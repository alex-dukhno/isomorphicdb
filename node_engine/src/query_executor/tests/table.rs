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

#[cfg(test)]
mod schemaless {
    use super::*;

    #[test]
    fn create_table_in_non_existent_schema() {
        let database = Database::new("IN_MEMORY");
        let query_engine = TransactionManager::new(database);

        let txn = query_engine.start_transaction();
        assert_statement(
            &txn,
            "create table schema_name.table_name (column_name smallint);",
            vec![
                QueryError::schema_does_not_exist("schema_name").into(),
                OutboundMessage::ReadyForQuery,
            ],
        );
        txn.commit();
    }

    #[test]
    fn drop_table_from_non_existent_schema() {
        let database = Database::new("IN_MEMORY");
        let query_engine = TransactionManager::new(database);

        let txn = query_engine.start_transaction();
        assert_statement(
            &txn,
            "drop table schema_name.table_name;",
            vec![
                QueryError::schema_does_not_exist("schema_name").into(),
                OutboundMessage::ReadyForQuery,
            ],
        );
        txn.commit();
    }
}

#[rstest::rstest]
fn create_table(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (column_name smallint);",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );
    txn.commit();
}

#[rstest::rstest]
fn create_same_table(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (column_name smallint);",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "create table schema_name.table_name (column_name smallint);",
        vec![
            QueryError::table_already_exists("schema_name.table_name").into(),
            OutboundMessage::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[rstest::rstest]
fn drop_table(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.table_name (column_name smallint);",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "drop table schema_name.table_name;",
        vec![OutboundMessage::TableDropped, OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "create table schema_name.table_name (column_name smallint);",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );
}

#[rstest::rstest]
fn drop_non_existent_table(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "drop table schema_name.non_existent;",
        vec![
            QueryError::table_does_not_exist("schema_name.non_existent").into(),
            OutboundMessage::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[rstest::rstest]
fn drop_if_exists_non_existent_table(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "drop table if exists schema_name.non_existent;",
        vec![OutboundMessage::TableDropped, OutboundMessage::ReadyForQuery],
    );
    txn.commit();
}

#[rstest::rstest]
fn drop_if_exists_existent_and_non_existent_table(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "create table schema_name.existent_table();",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "drop table if exists schema_name.non_existent, schema_name.existent_table;",
        vec![OutboundMessage::TableDropped, OutboundMessage::ReadyForQuery],
    );
    assert_statement(
        &txn,
        "create table schema_name.existent_table();",
        vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
    );
    txn.commit();
}

#[rstest::rstest]
fn delete_from_nonexistent_table(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();
    assert_statement(
        &txn,
        "delete from schema_name.table_name;",
        vec![
            QueryError::table_does_not_exist("schema_name.table_name").into(),
            OutboundMessage::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[rstest::rstest]
fn insert_into_nonexistent_table(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "insert into schema_name.table_name values (123);",
        vec![
            QueryError::table_does_not_exist("schema_name.table_name").into(),
            OutboundMessage::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[rstest::rstest]
fn select_from_not_existed_table(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "select * from schema_name.non_existent;",
        vec![
            QueryError::table_does_not_exist("schema_name.non_existent").into(),
            OutboundMessage::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[rstest::rstest]
fn select_named_columns_from_non_existent_table(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();

    assert_statement(
        &txn,
        "select column_1 from schema_name.non_existent;",
        vec![
            QueryError::table_does_not_exist("schema_name.non_existent").into(),
            OutboundMessage::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[rstest::rstest]
fn update_records_in_nonexistent_table(with_schema: TransactionManager) {
    let txn = with_schema.start_transaction();
    assert_statement(
        &txn,
        "update schema_name.table_name set column_test=789;",
        vec![
            QueryError::table_does_not_exist("schema_name.table_name").into(),
            OutboundMessage::ReadyForQuery,
        ],
    );
    txn.commit();
}

#[cfg(test)]
mod different_types {
    use super::*;

    #[rstest::rstest]
    fn ints(with_schema: TransactionManager) {
        let txn = with_schema.start_transaction();

        assert_statement(
            &txn,
            "create table schema_name.table_name (\
                column_si smallint,\
                column_i integer,\
                column_bi bigint
            );",
            vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
        );
        txn.commit();
    }

    #[rstest::rstest]
    fn strings(with_schema: TransactionManager) {
        let txn = with_schema.start_transaction();

        assert_statement(
            &txn,
            "create table schema_name.table_name (\
                column_c char(10),\
                column_vc varchar(10)\
            );",
            vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
        );
        txn.commit();
    }

    #[rstest::rstest]
    fn boolean(with_schema: TransactionManager) {
        let txn = with_schema.start_transaction();

        assert_statement(
            &txn,
            "create table schema_name.table_name (\
                column_b boolean\
            );",
            vec![OutboundMessage::TableCreated, OutboundMessage::ReadyForQuery],
        );
        txn.commit();
    }
}
