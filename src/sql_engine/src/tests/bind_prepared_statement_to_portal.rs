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

use protocol::sql_types::PostgreSqlType;

use super::*;

#[rstest::rstest]
fn bind_insert_statement_to_portal(sql_engine_with_schema: (QueryExecutor, ResultCollector)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint);")
        .expect("no system errors");
    engine
        .parse_prepared_statement(
            "statement_name",
            "insert into schema_name.table_name values ($1, $2);",
            &[PostgreSqlType::SmallInt, PostgreSqlType::SmallInt],
        )
        .expect("no system errors");
    engine
        .bind_prepared_statement_to_portal(
            "portal_name",
            "statement_name",
            &[PostgreSqlFormat::Binary, PostgreSqlFormat::Text],
            &[Some(vec![0, 1]), Some(b"2".to_vec())],
            &[],
        )
        .expect("no system errors");

    collector.assert_content(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::ParseComplete),
        Ok(QueryEvent::BindComplete),
    ]);
}

#[rstest::rstest]
fn bind_update_statement_to_portal(sql_engine_with_schema: (QueryExecutor, ResultCollector)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_1 smallint, column_2 smallint);")
        .expect("no system errors");
    engine
        .execute("insert into schema_name.table_name values (1, 2);")
        .expect("no system errors");
    engine
        .parse_prepared_statement(
            "statement_name",
            "update schema_name.table_name set column_1 = $1, column_2 = $2;",
            &[PostgreSqlType::SmallInt, PostgreSqlType::SmallInt],
        )
        .expect("no system errors");
    engine
        .bind_prepared_statement_to_portal(
            "portal_name",
            "statement_name",
            &[PostgreSqlFormat::Binary, PostgreSqlFormat::Text],
            &[Some(vec![0, 1]), Some(b"2".to_vec())],
            &[],
        )
        .expect("no system errors");

    collector.assert_content(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::ParseComplete),
        Ok(QueryEvent::BindComplete),
    ]);
}
