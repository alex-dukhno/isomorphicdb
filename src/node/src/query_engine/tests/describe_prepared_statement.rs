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
use protocol::{
    pgsql_types::PostgreSqlType,
    results::{QueryError, QueryEvent},
};

#[rstest::rstest]
fn describe_select_statement(database_with_schema: (QueryEngine, ResultCollector)) {
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
            sql: "select * from schema_name.table_name where column = $1 and column_2 = $2;".to_owned(),
            param_types: vec![PostgreSqlType::SmallInt, PostgreSqlType::SmallInt],
        })
        .expect("statement parsed");
    collector.assert_receive_intermediate(Ok(QueryEvent::ParseComplete));

    engine
        .execute(Command::DescribeStatement {
            name: "statement_name".to_owned(),
        })
        .expect("statement described");
    collector.assert_receive_intermediate(Ok(QueryEvent::StatementDescription(vec![
        ("column_1".to_owned(), PostgreSqlType::SmallInt),
        ("column_2".to_owned(), PostgreSqlType::SmallInt),
    ])));
    collector.assert_receive_intermediate(Ok(QueryEvent::StatementParameters(vec![
        PostgreSqlType::SmallInt,
        PostgreSqlType::SmallInt,
    ])));
}

#[rstest::rstest]
fn describe_update_statement(database_with_schema: (QueryEngine, ResultCollector)) {
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
            sql: "update schema_name.table_name set column_1 = $1 where column_2 = $2;".to_owned(),
            param_types: vec![PostgreSqlType::SmallInt, PostgreSqlType::SmallInt],
        })
        .expect("statement parsed");
    collector.assert_receive_intermediate(Ok(QueryEvent::ParseComplete));

    engine
        .execute(Command::DescribeStatement {
            name: "statement_name".to_owned(),
        })
        .expect("statement described");
    collector.assert_receive_intermediate(Ok(QueryEvent::StatementDescription(vec![])));
    collector.assert_receive_intermediate(Ok(QueryEvent::StatementParameters(vec![
        PostgreSqlType::SmallInt,
        PostgreSqlType::SmallInt,
    ])));
}

#[rstest::rstest]
fn describe_not_existed_statement(database_with_schema: (QueryEngine, ResultCollector)) {
    let (mut engine, collector) = database_with_schema;

    engine
        .execute(Command::DescribeStatement {
            name: "non_existent".to_owned(),
        })
        .expect("no errors");
    collector.assert_receive_intermediate(Err(QueryError::prepared_statement_does_not_exist("non_existent")));
}
