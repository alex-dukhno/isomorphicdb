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
use protocol::{results::QueryErrorBuilder, sql_types::PostgreSqlType};

#[rstest::fixture]
fn int_table(
    sql_engine_with_schema: (QueryExecutor<InMemoryStorage>, Arc<Collector>),
) -> (QueryExecutor<InMemoryStorage>, Arc<Collector>) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name(col smallint);")
        .expect("no system errors");

    (engine, collector)
}

#[rstest::fixture]
fn str_table(
    sql_engine_with_schema: (QueryExecutor<InMemoryStorage>, Arc<Collector>),
) -> (QueryExecutor<InMemoryStorage>, Arc<Collector>) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name(col varchar(5));")
        .expect("no system errors");

    (engine, collector)
}

#[cfg(test)]
mod insert {
    use super::*;

    #[rstest::rstest]
    fn out_of_range(int_table: (QueryExecutor<InMemoryStorage>, Arc<Collector>)) {
        let (mut engine, collector) = int_table;
        let mut builder = QueryErrorBuilder::new();
        builder.out_of_range(PostgreSqlType::SmallInt, "col".to_string(), 1);

        engine
            .execute("insert into schema_name.table_name values (32768);")
            .expect("no system errors");

        collector.assert_content(vec![
            Ok(QueryEvent::SchemaCreated),
            Ok(QueryEvent::TableCreated),
            Err(builder.build()),
        ]);
    }

    #[rstest::rstest]
    fn type_mismatch(int_table: (QueryExecutor<InMemoryStorage>, Arc<Collector>)) {
        let (mut engine, collector) = int_table;
        let mut builder = QueryErrorBuilder::new();
        builder.type_mismatch("str", PostgreSqlType::SmallInt, "col".to_string(), 1);

        engine
            .execute("insert into schema_name.table_name values ('str');")
            .expect("no system errors");

        collector.assert_content(vec![
            Ok(QueryEvent::SchemaCreated),
            Ok(QueryEvent::TableCreated),
            Err(builder.build()),
        ]);
    }

    #[rstest::rstest]
    fn value_too_long(str_table: (QueryExecutor<InMemoryStorage>, Arc<Collector>)) {
        let (mut engine, collector) = str_table;
        let mut builder = QueryErrorBuilder::new();
        builder.string_length_mismatch(PostgreSqlType::VarChar, 5, "col".to_string(), 1);
        engine
            .execute("insert into schema_name.table_name values ('123457890');")
            .expect("no system errors");

        collector.assert_content(vec![
            Ok(QueryEvent::SchemaCreated),
            Ok(QueryEvent::TableCreated),
            Err(builder.build()),
        ]);
    }
}

#[cfg(test)]
mod update {
    use super::*;

    #[rstest::rstest]
    fn out_of_range(int_table: (QueryExecutor<InMemoryStorage>, Arc<Collector>)) {
        let (mut engine, collector) = int_table;
        let mut builder = QueryErrorBuilder::new();
        builder.out_of_range(PostgreSqlType::SmallInt, "col".to_string(), 1);

        engine
            .execute("insert into schema_name.table_name values (32767);")
            .expect("no system errors");
        engine
            .execute("update schema_name.table_name set col = 32768;")
            .expect("no system errors");

        collector.assert_content(vec![
            Ok(QueryEvent::SchemaCreated),
            Ok(QueryEvent::TableCreated),
            Ok(QueryEvent::RecordsInserted(1)),
            Err(builder.build()),
        ]);
    }

    #[rstest::rstest]
    fn type_mismatch(int_table: (QueryExecutor<InMemoryStorage>, Arc<Collector>)) {
        let (mut engine, collector) = int_table;
        let mut builder = QueryErrorBuilder::new();
        builder.type_mismatch("str", PostgreSqlType::SmallInt, "col".to_string(), 1);
        engine
            .execute("insert into schema_name.table_name values (32767);")
            .expect("no system errors");
        engine
            .execute("update schema_name.table_name set col = 'str';")
            .expect("no system errors");

        collector.assert_content(vec![
            Ok(QueryEvent::SchemaCreated),
            Ok(QueryEvent::TableCreated),
            Ok(QueryEvent::RecordsInserted(1)),
            Err(builder.build()),
        ]);
    }

    #[rstest::rstest]
    fn value_too_long(str_table: (QueryExecutor<InMemoryStorage>, Arc<Collector>)) {
        let (mut engine, collector) = str_table;
        let mut builder = QueryErrorBuilder::new();
        builder.string_length_mismatch(PostgreSqlType::VarChar, 5, "col".to_string(), 1);

        engine
            .execute("insert into schema_name.table_name values ('str');")
            .expect("no system errors");
        engine
            .execute("update schema_name.table_name set col = '123457890';")
            .expect("no system errors");

        collector.assert_content(vec![
            Ok(QueryEvent::SchemaCreated),
            Ok(QueryEvent::TableCreated),
            Ok(QueryEvent::RecordsInserted(1)),
            Err(builder.build()),
        ]);
    }
}
