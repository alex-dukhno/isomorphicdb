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
use kernel::SystemError;
use protocol::sql_types::PostgreSqlType;

#[rstest::rstest]
fn parse_select_statement(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint);")
        .expect("no system errors");
    engine
        .parse(
            "statement_name",
            "select * from schema_name.table_name where column = $1 and column_2 = $2;",
            &[PostgreSqlType::SmallInt, PostgreSqlType::SmallInt],
        )
        .expect("no system errors");

    collector.assert_content(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::ParseComplete),
    ]);
}

#[rstest::rstest]
fn parse_select_statement_with_not_existed_table(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    let error = engine
        .parse(
            "statement_name",
            "select * from schema_name.non_existent where column_1 = $1;",
            &[],
        )
        .unwrap_err();
    assert_eq!(
        error,
        SystemError::runtime_check_failure("Table Does Not Exist".to_owned())
    );

    collector.assert_content(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Err(QueryError::table_does_not_exist("schema_name.non_existent".to_owned())),
    ]);
}

#[rstest::rstest]
fn parse_select_statement_with_not_existed_column(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint);")
        .expect("no system errors");
    let error = engine
        .parse(
            "statement_name",
            "select column_not_in_table from schema_name.table_name where column_1 = $1;",
            &[],
        )
        .unwrap_err();
    assert_eq!(
        error,
        SystemError::runtime_check_failure("Column Does Not Exist".to_owned())
    );

    collector.assert_content(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::QueryComplete),
        Err(QueryError::column_does_not_exist(
            "column_not_in_table".to_owned()
        )),
    ]);
}

#[rstest::rstest]
fn parse_update_statement(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint);")
        .expect("no system errors");
    engine
        .parse(
            "statement_name",
            "update schema_name.table_name set column_1 = $1 where column_2 = $2;",
            &[PostgreSqlType::SmallInt, PostgreSqlType::SmallInt],
        )
        .expect("no system errors");

    collector.assert_content(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::ParseComplete),
    ]);
}
