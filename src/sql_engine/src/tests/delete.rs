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

use crate::QueryExecutor;

use super::*;
use protocol::messages::ColumnMetadata;

#[rstest::rstest]
fn delete_from_nonexistent_table(sql_engine_with_schema: (QueryExecutor, ResultCollector)) {
    let (engine, collector) = sql_engine_with_schema;
    engine
        .execute("delete from schema_name.table_name;")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Err(QueryError::table_does_not_exist("schema_name.table_name")),
        Ok(QueryEvent::QueryComplete),
    ]);
}

#[rstest::rstest]
fn delete_all_records(sql_engine_with_schema: (QueryExecutor, ResultCollector)) {
    let (engine, collector) = sql_engine_with_schema;
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

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsInserted(1)),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
            "column_test",
            PostgreSqlType::SmallInt,
        )])),
        Ok(QueryEvent::DataRow(vec!["123".to_owned()])),
        Ok(QueryEvent::DataRow(vec!["456".to_owned()])),
        Ok(QueryEvent::RecordsSelected(2)),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RecordsDeleted(2)),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::RowDescription(vec![ColumnMetadata::new(
            "column_test",
            PostgreSqlType::SmallInt,
        )])),
        Ok(QueryEvent::RecordsSelected(0)),
        Ok(QueryEvent::QueryComplete),
    ])
}
