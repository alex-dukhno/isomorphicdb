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

use protocol::pgsql_types::PostgreSqlType;

use super::*;

#[rstest::rstest]
fn execute_insert_portal(sql_engine_with_schema: (QueryExecutor, ResultCollector)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint);")
        .expect("no system errors");
    collector.assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .parse_prepared_statement(
            "statement_name",
            "insert into schema_name.table_name values ($1, $2);",
            &[PostgreSqlType::SmallInt, PostgreSqlType::SmallInt],
        )
        .expect("no system errors");
    collector.assert_receive_intermediate(Ok(QueryEvent::ParseComplete));

    engine
        .bind_prepared_statement_to_portal(
            "portal_name",
            "statement_name",
            &[PostgreSqlFormat::Binary, PostgreSqlFormat::Text],
            &[Some(vec![0, 1]), Some(b"2".to_vec())],
            &[],
        )
        .expect("no system errors");
    collector.assert_receive_intermediate(Ok(QueryEvent::BindComplete));

    engine.execute_portal("portal_name", 0).expect("no system errors");
    collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));
}

#[rstest::rstest]
fn execute_update_portal(sql_engine_with_schema: (QueryExecutor, ResultCollector)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint);")
        .expect("no system errors");
    collector.assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute("insert into schema_name.table_name values (1, 2);")
        .expect("no system errors");
    collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

    engine
        .parse_prepared_statement(
            "statement_name",
            "update schema_name.table_name set column_1 = $1, column_2 = $2;",
            &[PostgreSqlType::SmallInt, PostgreSqlType::SmallInt],
        )
        .expect("no system errors");
    collector.assert_receive_intermediate(Ok(QueryEvent::ParseComplete));

    engine
        .bind_prepared_statement_to_portal(
            "portal_name",
            "statement_name",
            &[PostgreSqlFormat::Binary, PostgreSqlFormat::Text],
            &[Some(vec![0, 1]), Some(b"2".to_vec())],
            &[],
        )
        .expect("no system errors");
    collector.assert_receive_intermediate(Ok(QueryEvent::BindComplete));

    engine.execute_portal("portal_name", 0).expect("no system errors");
    collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));
}
