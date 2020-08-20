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
use crate::{tests::Collector, QueryExecutor};
use protocol::{
    results::{QueryErrorBuilder, QueryEvent},
    sql_types::PostgreSqlType,
};
use std::sync::Arc;

#[rstest::rstest]
fn delete_from_nonexistent_table(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("delete from schema_name.table_name;")
        .expect("no system errors");

    collector.assert_content(vec![
        Ok(QueryEvent::SchemaCreated),
        Err(QueryErrorBuilder::new()
            .table_does_not_exist("schema_name.table_name".to_owned())
            .build()),
    ]);
}

#[rstest::rstest]
fn delete_all_records(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_test smallint);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values (123);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values (456);")
        .expect("no system errors");
    engine
        .execute("select * from schema_name.table_name;")
        .expect("no system errors");
    engine
        .execute("delete from schema_name.table_name;")
        .expect("no system errors");
    engine
        .execute("select * from schema_name.table_name;")
        .expect("no system errors");

    collector.assert_content(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::RecordsSelected((
            vec![("column_test".to_owned(), PostgreSqlType::SmallInt)],
            vec![vec!["123".to_owned()], vec!["456".to_owned()]],
        ))),
        Ok(QueryEvent::RecordsDeleted(2)),
        Ok(QueryEvent::RecordsSelected((
            vec![("column_test".to_owned(), PostgreSqlType::SmallInt)],
            vec![],
        ))),
    ])
}
