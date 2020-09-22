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
use data_manager::DataManager;
use parser::QueryParser;
use protocol::pgsql_types::{PostgreSqlFormat, PostgreSqlType, PostgreSqlValue};
use query_executor::query::bind::ParamBinder;
use std::sync::Arc;

#[rstest::rstest]
fn bind_insert_raw_statement(empty_database: (QueryEngine, ResultCollector)) -> Result<(), ()> {
    let (mut executor, collector) = empty_database;
    executor.execute(Command::Parse {
        statement_name: "statement_name".to_owned(),
        sql: "insert into schema_name.table_name values ($1, $2)".to_owned(),
        param_types: vec![PostgreSqlType::Integer, PostgreSqlType::VarChar],
    })?;

    executor.execute(Command::Bind {
        portal_name: "portal_name".to_owned(),
        statement_name: "statement_name".to_owned(),
        param_formats: vec![PostgreSqlFormat::Binary, PostgreSqlFormat::Text],
        raw_params: vec![Some(vec![0, 1]), Some(b"2".to_vec())],
        result_formats: vec![],
    })?;

    Ok(())
}

#[rstest::rstest]
fn bind_update_raw_statement(sender: ResultCollector) {
    let query_parser = QueryParser::new(
        sender.clone(),
        Arc::new(DataManager::in_memory().expect("create data manager")),
    );
    let mut statement = query_parser
        .parse("update schema_name.table_name set column_1 = $1, column_2 = $2")
        .expect("query parsed");

    ParamBinder::new(sender)
        .bind(
            &mut statement,
            &[PostgreSqlValue::Int16(1), PostgreSqlValue::String("abc".into())],
        )
        .unwrap();

    assert_eq!(
        statement.to_string(),
        "UPDATE schema_name.table_name SET column_1 = 1, column_2 = 'abc'"
    );
}
