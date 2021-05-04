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
        let query_engine = QueryEngine::new(database);

        let txn = query_engine.start_transaction();
        assert_definition(
            &txn,
            "create table schema_name.table_name (column_name smallint);",
            Err(QueryError::schema_does_not_exist("schema_name")),
        );
        txn.commit();
    }

    #[test]
    fn drop_table_from_non_existent_schema() {
        let database = Database::new("IN_MEMORY");
        let query_engine = QueryEngine::new(database);

        let txn = query_engine.start_transaction();
        assert_definition(
            &txn,
            "drop table schema_name.table_name;",
            Err(QueryError::schema_does_not_exist("schema_name")),
        );
        txn.commit();
    }
}

#[rstest::rstest]
fn create_table(with_schema: QueryEngine) {
    let txn = with_schema.start_transaction();

    assert_definition(
        &txn,
        "create table schema_name.table_name (column_name smallint);",
        Ok(QueryEvent::TableCreated),
    );
    txn.commit();
}

#[rstest::rstest]
fn create_same_table(with_schema: QueryEngine) {
    let txn = with_schema.start_transaction();

    assert_definition(
        &txn,
        "create table schema_name.table_name (column_name smallint);",
        Ok(QueryEvent::TableCreated),
    );
    assert_definition(
        &txn,
        "create table schema_name.table_name (column_name smallint);",
        Err(QueryError::table_already_exists("schema_name.table_name")),
    );
    txn.commit();
}

#[rstest::rstest]
fn drop_table(with_schema: QueryEngine) {
    let txn = with_schema.start_transaction();

    assert_definition(
        &txn,
        "create table schema_name.table_name (column_name smallint);",
        Ok(QueryEvent::TableCreated),
    );
    assert_definition(&txn, "drop table schema_name.table_name;", Ok(QueryEvent::TableDropped));
    assert_definition(
        &txn,
        "create table schema_name.table_name (column_name smallint);",
        Ok(QueryEvent::TableCreated),
    );
}

#[rstest::rstest]
fn drop_non_existent_table(with_schema: QueryEngine) {
    let txn = with_schema.start_transaction();

    assert_definition(
        &txn,
        "drop table schema_name.non_existent;",
        Err(QueryError::table_does_not_exist("schema_name.non_existent")),
    );
    txn.commit();
}

#[rstest::rstest]
fn drop_if_exists_non_existent_table(with_schema: QueryEngine) {
    let txn = with_schema.start_transaction();

    assert_definition(
        &txn,
        "drop table if exists schema_name.non_existent;",
        Ok(QueryEvent::TableDropped),
    );
    txn.commit();
}

#[rstest::rstest]
fn drop_if_exists_existent_and_non_existent_table(with_schema: QueryEngine) {
    let txn = with_schema.start_transaction();

    assert_definition(
        &txn,
        "create table schema_name.existent_table();",
        Ok(QueryEvent::TableCreated),
    );
    assert_definition(
        &txn,
        "drop table if exists schema_name.non_existent, schema_name.existent_table;",
        Ok(QueryEvent::TableDropped),
    );
    assert_definition(
        &txn,
        "create table schema_name.existent_table();",
        Ok(QueryEvent::TableCreated),
    );
    txn.commit();
}

#[rstest::rstest]
fn delete_from_nonexistent_table(with_schema: QueryEngine) {
    let txn = with_schema.start_transaction();
    assert_query(
        &txn,
        "delete from schema_name.table_name;",
        Err(QueryError::table_does_not_exist("schema_name.table_name")),
    );
    txn.commit();
}

#[rstest::rstest]
fn insert_into_nonexistent_table(with_schema: QueryEngine) {
    let txn = with_schema.start_transaction();

    assert_query(
        &txn,
        "insert into schema_name.table_name values (123);",
        Err(QueryError::table_does_not_exist("schema_name.table_name")),
    );
    txn.commit();
}

#[rstest::rstest]
fn select_from_not_existed_table(with_schema: QueryEngine) {
    let txn = with_schema.start_transaction();

    assert_query(
        &txn,
        "select * from schema_name.non_existent;",
        Err(QueryError::table_does_not_exist("schema_name.non_existent")),
    );
    txn.commit();
}

#[rstest::rstest]
fn select_named_columns_from_non_existent_table(with_schema: QueryEngine) {
    let txn = with_schema.start_transaction();

    assert_query(
        &txn,
        "select column_1 from schema_name.non_existent;",
        Err(QueryError::table_does_not_exist("schema_name.non_existent")),
    );
    txn.commit();
}

#[rstest::rstest]
fn update_records_in_nonexistent_table(with_schema: QueryEngine) {
    let txn = with_schema.start_transaction();
    assert_query(
        &txn,
        "update schema_name.table_name set column_test=789;",
        Err(QueryError::table_does_not_exist("schema_name.table_name")),
    );
    txn.commit();
}

#[cfg(test)]
mod different_types {
    use super::*;

    #[rstest::rstest]
    fn ints(with_schema: QueryEngine) {
        let txn = with_schema.start_transaction();

        assert_definition(
            &txn,
            "create table schema_name.table_name (\
                column_si smallint,\
                column_i integer,\
                column_bi bigint
            );",
            Ok(QueryEvent::TableCreated),
        );
        txn.commit();
    }

    #[rstest::rstest]
    fn strings(with_schema: QueryEngine) {
        let txn = with_schema.start_transaction();

        assert_definition(
            &txn,
            "create table schema_name.table_name (\
                column_c char(10),\
                column_vc varchar(10)\
            );",
            Ok(QueryEvent::TableCreated),
        );
        txn.commit();
    }

    #[rstest::rstest]
    fn boolean(with_schema: QueryEngine) {
        let txn = with_schema.start_transaction();

        assert_definition(
            &txn,
            "create table schema_name.table_name (\
                column_b boolean\
            );",
            Ok(QueryEvent::TableCreated),
        );
        txn.commit();
    }
}
