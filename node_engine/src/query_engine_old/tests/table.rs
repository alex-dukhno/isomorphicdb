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

    #[rstest::rstest]
    fn create_table_in_non_existent_schema(empty_database: (InMemory, ResultCollector)) {
        let (mut engine, collector) = empty_database;
        engine
            .execute(Inbound::Query {
                sql: "create table schema_name.table_name (column_name smallint);".to_owned(),
            })
            .expect("query executed");
        collector
            .lock()
            .unwrap()
            .assert_receive_single(Err(QueryError::schema_does_not_exist("schema_name")));
    }

    #[rstest::rstest]
    fn drop_table_from_non_existent_schema(empty_database: (InMemory, ResultCollector)) {
        let (mut engine, collector) = empty_database;
        engine
            .execute(Inbound::Query {
                sql: "drop table schema_name.table_name;".to_owned(),
            })
            .expect("query executed");
        collector
            .lock()
            .unwrap()
            .assert_receive_single(Err(QueryError::schema_does_not_exist("schema_name")));
    }
}

#[rstest::rstest]
fn create_table(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(Inbound::Query {
            sql: "create table schema_name.table_name (column_name smallint);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::TableCreated));
}

#[rstest::rstest]
fn create_same_table(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(Inbound::Query {
            sql: "create table schema_name.table_name (column_name smallint);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(Inbound::Query {
            sql: "create table schema_name.table_name (column_name smallint);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Err(QueryError::table_already_exists("schema_name.table_name")));
}

#[rstest::rstest]
fn drop_table(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(Inbound::Query {
            sql: "create table schema_name.table_name (column_name smallint);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(Inbound::Query {
            sql: "drop table schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::TableDropped));

    engine
        .execute(Inbound::Query {
            sql: "create table schema_name.table_name (column_name smallint);".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::TableCreated));
}

#[rstest::rstest]
fn drop_non_existent_table(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(Inbound::Query {
            sql: "drop table schema_name.non_existent;".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Err(QueryError::table_does_not_exist("schema_name.non_existent")));
}

#[rstest::rstest]
fn drop_if_exists_non_existent_table(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;

    engine
        .execute(Inbound::Query {
            sql: "drop table if exists schema_name.non_existent;".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::TableDropped));
}

#[rstest::rstest]
fn drop_if_exists_existent_and_non_existent_table(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;

    engine
        .execute(Inbound::Query {
            sql: "create table schema_name.existent_table();".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(Inbound::Query {
            sql: "drop table if exists schema_name.non_existent, schema_name.existent_table;".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::TableDropped));

    engine
        .execute(Inbound::Query {
            sql: "create table schema_name.existent_table();".to_owned(),
        })
        .expect("query executed");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::TableCreated));
}

#[cfg(test)]
mod different_types {
    use super::*;

    #[rstest::rstest]
    fn ints(database_with_schema: (InMemory, ResultCollector)) {
        let (mut engine, collector) = database_with_schema;
        engine
            .execute(Inbound::Query {
                sql: "create table schema_name.table_name (\
            column_si smallint,\
            column_i integer,\
            column_bi bigint
            );"
                .to_owned(),
            })
            .expect("query executed");

        collector
            .lock()
            .unwrap()
            .assert_receive_single(Ok(QueryEvent::TableCreated));
    }

    #[rstest::rstest]
    fn strings(database_with_schema: (InMemory, ResultCollector)) {
        let (mut engine, collector) = database_with_schema;
        engine
            .execute(Inbound::Query {
                sql: "create table schema_name.table_name (\
            column_c char(10),\
            column_vc varchar(10)\
            );"
                .to_owned(),
            })
            .expect("query executed");

        collector
            .lock()
            .unwrap()
            .assert_receive_single(Ok(QueryEvent::TableCreated));
    }

    #[rstest::rstest]
    fn boolean(database_with_schema: (InMemory, ResultCollector)) {
        let (mut engine, collector) = database_with_schema;
        engine
            .execute(Inbound::Query {
                sql: "create table schema_name.table_name (\
            column_b boolean\
            );"
                .to_owned(),
            })
            .expect("query executed");

        collector
            .lock()
            .unwrap()
            .assert_receive_single(Ok(QueryEvent::TableCreated));
    }
}
