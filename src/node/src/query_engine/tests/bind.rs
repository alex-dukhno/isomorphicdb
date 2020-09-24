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
use protocol::pgsql_types::{PostgreSqlFormat, PostgreSqlType};

#[rstest::rstest]
#[ignore] // TODO: parse, describe and bind inserts
fn bind_insert_raw_statement(database_with_schema: (QueryEngine, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;

    engine
        .execute(Command::Query {
            sql: "create table schema_name.table_name (column_1 smallint, column_2 smallint);".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(Command::Parse {
            statement_name: "statement_name".to_owned(),
            sql: "insert into schema_name.table_name values ($1, $2)".to_owned(),
            param_types: vec![PostgreSqlType::Integer, PostgreSqlType::VarChar],
        })
        .expect("query executed");
    collector.assert_receive_intermediate(Ok(QueryEvent::ParseComplete));

    engine
        .execute(Command::Bind {
            portal_name: "portal_name".to_owned(),
            statement_name: "statement_name".to_owned(),
            param_formats: vec![PostgreSqlFormat::Binary, PostgreSqlFormat::Text],
            raw_params: vec![Some(vec![0, 0, 0, 1]), Some(b"2".to_vec())],
            result_formats: vec![],
        })
        .expect("query executed");
    collector.assert_receive_intermediate(Ok(QueryEvent::BindComplete));
}

#[rstest::rstest]
#[ignore] // TODO: parse, describe and bind updates
fn bind_update_raw_statement(database_with_schema: (QueryEngine, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;

    engine
        .execute(Command::Query {
            sql: "create table schema_name.table_name (column_1 smallint, column_2 smallint);".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::TableCreated));

    engine
        .execute(Command::Parse {
            statement_name: "statement_name".to_owned(),
            sql: "update schema_name.table_name set column_1 = $1, column_2 = $2".to_owned(),
            param_types: vec![PostgreSqlType::Integer, PostgreSqlType::VarChar],
        })
        .expect("query executed");
    collector.assert_receive_intermediate(Ok(QueryEvent::ParseComplete));

    engine
        .execute(Command::Bind {
            portal_name: "portal_name".to_owned(),
            statement_name: "statement_name".to_owned(),
            param_formats: vec![PostgreSqlFormat::Binary, PostgreSqlFormat::Text],
            raw_params: vec![Some(vec![0, 0, 0, 1]), Some(b"2".to_vec())],
            result_formats: vec![],
        })
        .expect("query executed");
    collector.assert_receive_intermediate(Ok(QueryEvent::BindComplete));
}
