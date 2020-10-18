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

use super::*;
use pg_model::pg_types::PostgreSqlType;
use protocol::{
    messages::ColumnMetadata,
    results::{QueryError, QueryEvent},
};

#[rstest::rstest]
fn delete_from_nonexistent_table(database_with_schema: (InMemory, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;

    engine
        .execute(Command::Query {
            sql: "delete from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Err(QueryError::table_does_not_exist("schema_name.table_name")));
}

#[rstest::rstest]
fn delete_all_records(database_with_schema: (InMemory, ResultCollector)) {
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
            sql: "insert into schema_name.table_name values (456);".to_owned(),
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
            PostgreSqlType::SmallInt,
        )])),
        Ok(QueryEvent::DataRow(vec!["123".to_owned()])),
        Ok(QueryEvent::DataRow(vec!["456".to_owned()])),
        Ok(QueryEvent::RecordsSelected(2)),
    ]);

    engine
        .execute(Command::Query {
            sql: "delete from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::RecordsDeleted(2)));

    engine
        .execute(Command::Query {
            sql: "select * from schema_name.table_name;".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
            "column_test",
            PostgreSqlType::SmallInt,
        )])),
        Ok(QueryEvent::RecordsSelected(0)),
    ]);
}
