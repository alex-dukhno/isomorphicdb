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

use protocol::{pgsql_types::PostgreSqlType, results::QueryEvent};

use sql_engine::QueryExecutor;

use common::{database_with_schema, ResultCollector};
use parser::QueryParser;
use protocol::messages::ColumnMetadata;
use protocol::results::QueryError;
mod common;

#[rstest::rstest]
fn delete_from_nonexistent_table(database_with_schema: (QueryExecutor, QueryParser, ResultCollector)) {
    let (engine, parser, collector) = database_with_schema;

    engine.execute(&parser.parse("delete from schema_name.table_name;").expect("parsed"));
    collector.assert_receive_single(Err(QueryError::table_does_not_exist("schema_name.table_name")));
}

#[rstest::rstest]
fn delete_all_records(database_with_schema: (QueryExecutor, QueryParser, ResultCollector)) {
    let (engine, parser, collector) = database_with_schema;

    engine.execute(
        &parser
            .parse("create table schema_name.table_name (column_test smallint);")
            .expect("parsed"),
    );
    collector.assert_receive_single(Ok(QueryEvent::TableCreated));

    engine.execute(
        &parser
            .parse("insert into schema_name.table_name values (123);")
            .expect("parsed"),
    );
    collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

    engine.execute(
        &parser
            .parse("insert into schema_name.table_name values (456);")
            .expect("parsed"),
    );
    collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

    engine.execute(&parser.parse("select * from schema_name.table_name;").expect("parsed"));
    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
            "column_test",
            PostgreSqlType::SmallInt,
        )])),
        Ok(QueryEvent::DataRow(vec!["123".to_owned()])),
        Ok(QueryEvent::DataRow(vec!["456".to_owned()])),
        Ok(QueryEvent::RecordsSelected(2)),
    ]);

    engine.execute(&parser.parse("delete from schema_name.table_name;").expect("parsed"));
    collector.assert_receive_single(Ok(QueryEvent::RecordsDeleted(2)));

    engine.execute(&parser.parse("select * from schema_name.table_name;").expect("parsed"));
    collector.assert_receive_many(vec![
        Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
            "column_test",
            PostgreSqlType::SmallInt,
        )])),
        Ok(QueryEvent::RecordsSelected(0)),
    ]);
}
