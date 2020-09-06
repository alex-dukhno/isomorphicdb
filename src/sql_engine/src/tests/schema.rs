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

#[rstest::rstest]
fn create_schema(sql_engine: (QueryExecutor, ResultCollector)) {
    let (mut engine, collector) = sql_engine;
    engine.execute("create schema schema_name;").expect("no system errors");

    collector.assert_content_for_single_queries(vec![Ok(QueryEvent::SchemaCreated), Ok(QueryEvent::QueryComplete)]);
}

#[rstest::rstest]
fn create_same_schema(sql_engine: (QueryExecutor, ResultCollector)) {
    let (mut engine, collector) = sql_engine;
    engine.execute("create schema schema_name;").expect("no system errors");
    engine.execute("create schema schema_name;").expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Err(QueryError::schema_already_exists("schema_name".to_owned())),
        Ok(QueryEvent::QueryComplete),
    ]);
}

#[rstest::rstest]
fn drop_schema(sql_engine: (QueryExecutor, ResultCollector)) {
    let (mut engine, collector) = sql_engine;
    engine.execute("create schema schema_name;").expect("no system errors");
    engine.execute("drop schema schema_name;").expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::QueryComplete),
        Ok(QueryEvent::SchemaDropped),
        Ok(QueryEvent::QueryComplete),
    ]);
}

#[rstest::rstest]
fn drop_non_existent_schema(sql_engine: (QueryExecutor, ResultCollector)) {
    let (mut engine, collector) = sql_engine;

    engine.execute("drop schema non_existent;").expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Err(QueryError::schema_does_not_exist("non_existent")),
        Ok(QueryEvent::QueryComplete),
    ]);
}

#[rstest::rstest]
fn select_from_nonexistent_schema(sql_engine: (QueryExecutor, ResultCollector)) {
    let (mut engine, collector) = sql_engine;

    engine
        .execute("select * from non_existent.some_table;")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Err(QueryError::schema_does_not_exist("non_existent")),
        Ok(QueryEvent::QueryComplete),
    ]);
}

#[rstest::rstest]
fn select_named_columns_from_nonexistent_schema(sql_engine: (QueryExecutor, ResultCollector)) {
    let (mut engine, collector) = sql_engine;
    engine
        .execute("select column_1 from schema_name.table_name;")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Err(QueryError::schema_does_not_exist("schema_name")),
        Ok(QueryEvent::QueryComplete),
    ]);
}

#[rstest::rstest]
fn insert_into_table_in_nonexistent_schema(sql_engine: (QueryExecutor, ResultCollector)) {
    let (mut engine, collector) = sql_engine;
    engine
        .execute("insert into schema_name.table_name values (123);")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Err(QueryError::schema_does_not_exist("schema_name")),
        Ok(QueryEvent::QueryComplete),
    ]);
}

#[rstest::rstest]
fn update_records_in_table_from_non_existent_schema(sql_engine: (QueryExecutor, ResultCollector)) {
    let (mut engine, collector) = sql_engine;
    engine
        .execute("update schema_name.table_name set column_test=789;")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Err(QueryError::schema_does_not_exist("schema_name")),
        Ok(QueryEvent::QueryComplete),
    ]);
}

#[rstest::rstest]
fn delete_from_table_in_nonexistent_schema(sql_engine: (QueryExecutor, ResultCollector)) {
    let (mut engine, collector) = sql_engine;
    engine
        .execute("delete from schema_name.table_name;")
        .expect("no system errors");

    collector.assert_content_for_single_queries(vec![
        Err(QueryError::schema_does_not_exist("schema_name")),
        Ok(QueryEvent::QueryComplete),
    ]);
}
