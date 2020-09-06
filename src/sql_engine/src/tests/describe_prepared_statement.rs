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
fn describe_select_statement(sql_engine_with_schema: (QueryExecutor, ResultCollector)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint);")
        .expect("no system errors");
    engine
        .parse_prepared_statement(
            "statement_name",
            "select * from schema_name.table_name where column = $1 and column_2 = $2;",
            &[PostgreSqlType::SmallInt, PostgreSqlType::SmallInt],
        )
        .expect("no system errors");
    engine
        .describe_prepared_statement("statement_name")
        .expect("no system errors");
    collector.assert_content(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::ParseComplete),
        Ok(QueryEvent::PreparedStatementDescribed(
            vec![PostgreSqlType::SmallInt, PostgreSqlType::SmallInt],
            vec![
                ("column_1".to_owned(), PostgreSqlType::SmallInt),
                ("column_2".to_owned(), PostgreSqlType::SmallInt),
            ],
        )),
    ]);
}

#[rstest::rstest]
fn describe_update_statement(sql_engine_with_schema: (QueryExecutor, ResultCollector)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint);")
        .expect("no system errors");
    engine
        .parse_prepared_statement(
            "statement_name",
            "update schema_name.table_name set column_1 = $1 where column_2 = $2;",
            &[PostgreSqlType::SmallInt, PostgreSqlType::SmallInt],
        )
        .expect("no system errors");
    engine
        .describe_prepared_statement("statement_name")
        .expect("no system errors");

    collector.assert_content(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::ParseComplete),
        Ok(QueryEvent::PreparedStatementDescribed(
            vec![PostgreSqlType::SmallInt, PostgreSqlType::SmallInt],
            vec![],
        )),
    ]);
}

#[rstest::rstest]
fn describe_not_existed_statement(sql_engine_with_schema: (QueryExecutor, ResultCollector)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .describe_prepared_statement("non_existent")
        .expect("no system errors");

    collector.assert_content(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Err(QueryError::prepared_statement_does_not_exist("non_existent".to_owned())),
    ]);
}
