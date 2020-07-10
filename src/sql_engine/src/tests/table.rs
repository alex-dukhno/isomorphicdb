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
    fn create_table_in_non_existent_schema(mut sql_engine: InMemorySqlEngine) {
        assert_eq!(
            sql_engine
                .execute("create table schema_name.table_name (column_name smallint);")
                .expect("no system errors"),
            Err(QueryError::schema_does_not_exist("schema_name".to_owned()))
        );
    }

    #[rstest::rstest]
    fn drop_table_from_non_existent_schema(mut sql_engine: InMemorySqlEngine) {
        assert_eq!(
            sql_engine
                .execute("drop table schema_name.table_name;")
                .expect("no system errors"),
            Err(QueryError::schema_does_not_exist("schema_name".to_owned()))
        );
    }
}

#[rstest::rstest]
fn create_table(mut sql_engine_with_schema: InMemorySqlEngine) {
    assert_eq!(
        sql_engine_with_schema
            .execute("create table schema_name.table_name (column_name smallint);")
            .expect("no system errors"),
        Ok(QueryEvent::TableCreated)
    );
}

#[rstest::rstest]
fn create_same_table(mut sql_engine_with_schema: InMemorySqlEngine) {
    sql_engine_with_schema
        .execute("create table schema_name.table_name (column_name smallint);")
        .expect("no system errors")
        .expect("table created");
    assert_eq!(
        sql_engine_with_schema
            .execute("create table schema_name.table_name (column_name smallint);")
            .expect("no system errors"),
        Err(QueryError::table_already_exists("schema_name.table_name".to_owned()))
    );
}

#[rstest::rstest]
fn drop_table(mut sql_engine_with_schema: InMemorySqlEngine) {
    sql_engine_with_schema
        .execute("create table schema_name.table_name (column_name smallint);")
        .expect("no system errors")
        .expect("table created");

    assert_eq!(
        sql_engine_with_schema
            .execute("drop table schema_name.table_name;")
            .expect("no system errors"),
        Ok(QueryEvent::TableDropped)
    );
    assert_eq!(
        sql_engine_with_schema
            .execute("create table schema_name.table_name (column_name smallint);")
            .expect("no system errors"),
        Ok(QueryEvent::TableCreated)
    );
}

#[rstest::rstest]
fn drop_non_existent_table(mut sql_engine_with_schema: InMemorySqlEngine) {
    assert_eq!(
        sql_engine_with_schema
            .execute("drop table schema_name.table_name;")
            .expect("no system errors"),
        Err(QueryError::table_does_not_exist("schema_name.table_name".to_owned()))
    );
}

#[rstest::rstest]
fn create_table_with_different_types(mut sql_engine: InMemorySqlEngine) {
    sql_engine
        .execute("create schema schema_name;")
        .expect("no system errors")
        .expect("schema created");

    assert_eq!(
        sql_engine
            .execute(
                "create table schema_name.table_name (\
            column_si smallint,\
            column_i integer,\
            column_bi bigint,\
            column_c char(10),\
            column_vc varchar(10),\
            column_b boolean,\
            column_smalls smallserial,\
            column_s serial,\
            column_bigs bigserial\
            );"
            )
            .expect("no system errors"),
        Ok(QueryEvent::TableCreated)
    )
}
