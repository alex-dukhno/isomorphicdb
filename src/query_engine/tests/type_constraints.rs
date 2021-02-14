// Copyright 2020 - present Alex Dukhno
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
use pg_model::Command;
use pg_result::{QueryError, QueryEvent};

#[rstest::fixture]
fn int_table(database_with_schema: (InMemory, ResultCollector)) -> (InMemory, ResultCollector) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(Command::Query {
            sql: "create table schema_name.table_name(col smallint);".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_till_this_moment(vec![Ok(QueryEvent::TableCreated), Ok(QueryEvent::QueryComplete)]);

    (engine, collector)
}

#[rstest::fixture]
fn multiple_ints_table(database_with_schema: (InMemory, ResultCollector)) -> (InMemory, ResultCollector) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(Command::Query {
            sql: "create table schema_name.table_name(column_si smallint, column_i integer, column_bi bigint);"
                .to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_till_this_moment(vec![Ok(QueryEvent::TableCreated), Ok(QueryEvent::QueryComplete)]);

    (engine, collector)
}

#[rstest::fixture]
fn str_table(database_with_schema: (InMemory, ResultCollector)) -> (InMemory, ResultCollector) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(Command::Query {
            sql: "create table schema_name.table_name(col varchar(5));".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_till_this_moment(vec![Ok(QueryEvent::TableCreated), Ok(QueryEvent::QueryComplete)]);

    (engine, collector)
}

#[cfg(test)]
mod insert {
    use super::*;

    #[rstest::rstest]
    fn out_of_range(int_table: (InMemory, ResultCollector)) {
        let (mut engine, collector) = int_table;

        engine
            .execute(Command::Query {
                sql: "insert into schema_name.table_name values (32768);".to_owned(),
            })
            .expect("query executed");
        collector.assert_receive_single(Err(QueryError::out_of_range(PgType::SmallInt, "col".to_string(), 1)));
    }

    #[rstest::rstest]
    fn type_mismatch(int_table: (InMemory, ResultCollector)) {
        let (mut engine, collector) = int_table;

        engine
            .execute(Command::Query {
                sql: "insert into schema_name.table_name values ('str');".to_owned(),
            })
            .expect("query executed");
        collector.assert_receive_single(Err(QueryError::invalid_text_representation(PgType::SmallInt, "str")));
    }

    #[rstest::rstest]
    fn multiple_columns_multiple_row_violation(multiple_ints_table: (InMemory, ResultCollector)) {
        let (mut engine, collector) = multiple_ints_table;
        engine
            .execute(Command::Query { sql: "insert into schema_name.table_name values (-32769, -2147483649, 100), (100, -2147483649, -9223372036854775809);".to_owned()}).expect("query executed");
        collector.assert_receive_many(vec![
            Err(QueryError::out_of_range(PgType::SmallInt, "column_si", 1)),
            Err(QueryError::out_of_range(PgType::Integer, "column_i", 1)),
        ]);
    }

    #[rstest::rstest]
    fn violation_in_the_second_row(multiple_ints_table: (InMemory, ResultCollector)) {
        let (mut engine, collector) = multiple_ints_table;
        engine
            .execute(Command::Query { sql: "insert into schema_name.table_name values (-32768, -2147483648, 100), (100, -2147483649, -9223372036854775809);".to_owned()}).expect("query executed");
        collector.assert_receive_many(vec![
            Err(QueryError::out_of_range(PgType::Integer, "column_i".to_owned(), 2)),
            Err(QueryError::out_of_range(PgType::BigInt, "column_bi".to_owned(), 2)),
        ]);
    }

    #[rstest::rstest]
    fn value_too_long(str_table: (InMemory, ResultCollector)) {
        let (mut engine, collector) = str_table;
        engine
            .execute(Command::Query {
                sql: "insert into schema_name.table_name values ('123457890');".to_owned(),
            })
            .expect("query executed");
        collector.assert_receive_single(Err(QueryError::string_length_mismatch(
            PgType::VarChar,
            5,
            "col".to_string(),
            1,
        )));
    }
}

#[cfg(test)]
mod update {
    use super::*;

    #[rstest::rstest]
    fn out_of_range(int_table: (InMemory, ResultCollector)) {
        let (mut engine, collector) = int_table;
        engine
            .execute(Command::Query {
                sql: "insert into schema_name.table_name values (32767);".to_owned(),
            })
            .expect("query executed");
        collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

        engine
            .execute(Command::Query {
                sql: "update schema_name.table_name set col = 32768;".to_owned(),
            })
            .expect("query executed");

        collector.assert_receive_single(Err(QueryError::out_of_range(PgType::SmallInt, "col".to_string(), 1)));
    }

    #[rstest::rstest]
    fn type_mismatch(int_table: (InMemory, ResultCollector)) {
        let (mut engine, collector) = int_table;
        engine
            .execute(Command::Query {
                sql: "insert into schema_name.table_name values (32767);".to_owned(),
            })
            .expect("query executed");
        collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

        engine
            .execute(Command::Query {
                sql: "update schema_name.table_name set col = 'str';".to_owned(),
            })
            .expect("query executed");

        collector.assert_receive_single(Err(QueryError::invalid_text_representation(PgType::SmallInt, "str")));
    }

    #[rstest::rstest]
    fn value_too_long(str_table: (InMemory, ResultCollector)) {
        let (mut engine, collector) = str_table;

        engine
            .execute(Command::Query {
                sql: "insert into schema_name.table_name values ('str');".to_owned(),
            })
            .expect("query executed");
        collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

        engine
            .execute(Command::Query {
                sql: "update schema_name.table_name set col = '123457890';".to_owned(),
            })
            .expect("query executed");
        collector.assert_receive_single(Err(QueryError::string_length_mismatch(
            PgType::VarChar,
            5,
            "col".to_string(),
            1,
        )));
    }

    #[rstest::rstest]
    fn multiple_columns_violation(multiple_ints_table: (InMemory, ResultCollector)) {
        let (mut engine, collector) = multiple_ints_table;

        engine
            .execute(Command::Query {
                sql: "insert into schema_name.table_name values (100, 100, 100), (100, 100, 100);".to_owned(),
            })
            .expect("query executed");
        collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(2)));

        engine
            .execute(Command::Query {
                sql: "update schema_name.table_name set column_si = -32769, column_i= -2147483649, column_bi=100;"
                    .to_owned(),
            })
            .expect("query executed");
        collector.assert_receive_many(vec![
            Err(QueryError::out_of_range(PgType::SmallInt, "column_si".to_owned(), 1)),
            Err(QueryError::out_of_range(PgType::Integer, "column_i".to_owned(), 1)),
        ]);
    }
}
