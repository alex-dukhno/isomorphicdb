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

#[cfg(test)]
mod schemaless {
    use super::*;

    #[rstest::rstest]
    fn create_table_in_non_existent_schema(sql_engine: (QueryExecutor, Arc<Collector>)) {
        let (mut engine, collector) = sql_engine;

        engine
            .execute("create table schema_name.table_name (column_name smallint);")
            .expect("no system errors");

        collector.assert_content(vec![Err(QueryErrorBuilder::new()
            .schema_does_not_exist("schema_name".to_owned())
            .build())]);
    }

    #[rstest::rstest]
    fn drop_table_from_non_existent_schema(sql_engine: (QueryExecutor, Arc<Collector>)) {
        let (mut engine, collector) = sql_engine;
        engine
            .execute("drop table schema_name.table_name;")
            .expect("no system errors");

        collector.assert_content(vec![Err(QueryErrorBuilder::new()
            .schema_does_not_exist("schema_name".to_owned())
            .build())]);
    }
}

#[rstest::rstest]
fn create_table(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;

    engine
        .execute("create table schema_name.table_name (column_name smallint);")
        .expect("no system errors");

    collector.assert_content(vec![Ok(QueryEvent::SchemaCreated), Ok(QueryEvent::TableCreated)]);
}

#[rstest::rstest]
fn create_same_table(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_name smallint);")
        .expect("no system errors");
    engine
        .execute("create table schema_name.table_name (column_name smallint);")
        .expect("no system errors");

    collector.assert_content(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::TableCreated),
        Err(QueryErrorBuilder::new()
            .table_already_exists("schema_name.table_name".to_owned())
            .build()),
    ]);
}

#[rstest::rstest]
fn drop_table(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name (column_name smallint);")
        .expect("no system errors");
    engine
        .execute("drop table schema_name.table_name;")
        .expect("no system errors");
    engine
        .execute("create table schema_name.table_name (column_name smallint);")
        .expect("no system errors");

    collector.assert_content(vec![
        Ok(QueryEvent::SchemaCreated),
        Ok(QueryEvent::TableCreated),
        Ok(QueryEvent::TableDropped),
        Ok(QueryEvent::TableCreated),
    ]);
}

#[rstest::rstest]
fn drop_non_existent_table(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("drop table schema_name.table_name;")
        .expect("no system errors");

    collector.assert_content(vec![
        Ok(QueryEvent::SchemaCreated),
        Err(QueryErrorBuilder::new()
            .table_does_not_exist("schema_name.table_name".to_owned())
            .build()),
    ]);
}

#[cfg(test)]
mod different_types {
    use super::*;

    #[rstest::rstest]
    fn ints(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
        let (mut engine, collector) = sql_engine_with_schema;
        engine
            .execute(
                "create table schema_name.table_name (\
            column_si smallint,\
            column_i integer,\
            column_bi bigint
            );",
            )
            .expect("no system errors");

        collector.assert_content(vec![Ok(QueryEvent::SchemaCreated), Ok(QueryEvent::TableCreated)]);
    }

    #[rstest::rstest]
    fn strings(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
        let (mut engine, collector) = sql_engine_with_schema;
        engine
            .execute(
                "create table schema_name.table_name (\
            column_c char(10),\
            column_vc varchar(10)\
            );",
            )
            .expect("no system errors");

        collector.assert_content(vec![Ok(QueryEvent::SchemaCreated), Ok(QueryEvent::TableCreated)]);
    }

    #[rstest::rstest]
    fn boolean(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
        let (mut engine, collector) = sql_engine_with_schema;
        engine
            .execute(
                "create table schema_name.table_name (\
            column_b boolean\
            );",
            )
            .expect("no system errors");

        collector.assert_content(vec![Ok(QueryEvent::SchemaCreated), Ok(QueryEvent::TableCreated)]);
    }

    #[rstest::rstest]
    fn serials(sql_engine_with_schema: (QueryExecutor, Arc<Collector>)) {
        let (mut engine, collector) = sql_engine_with_schema;
        engine
            .execute(
                "create table schema_name.table_name (\
            column_smalls smallserial,\
            column_s serial,\
            column_bigs bigserial\
            );",
            )
            .expect("no system errors");

        collector.assert_content(vec![Ok(QueryEvent::SchemaCreated), Ok(QueryEvent::TableCreated)]);
    }
}
