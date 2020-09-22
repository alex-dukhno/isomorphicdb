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

mod common;
use common::{database_with_schema, ResultCollector};
use parser::QueryParser;
use protocol::pgsql_types::PostgreSqlType;
use protocol::results::QueryError;
use protocol::results::QueryEvent;
use sql_engine::QueryExecutor;

#[rstest::fixture]
fn int_table(
    database_with_schema: (QueryExecutor, QueryParser, ResultCollector),
) -> (QueryExecutor, QueryParser, ResultCollector) {
    let (engine, parser, collector) = database_with_schema;
    engine.execute(
        &parser
            .parse("create table schema_name.table_name(col smallint);")
            .expect("parsed"),
    );
    collector.assert_receive_till_this_moment(vec![Ok(QueryEvent::TableCreated), Ok(QueryEvent::QueryComplete)]);

    (engine, parser, collector)
}

#[rstest::fixture]
fn multiple_ints_table(
    database_with_schema: (QueryExecutor, QueryParser, ResultCollector),
) -> (QueryExecutor, QueryParser, ResultCollector) {
    let (engine, parser, collector) = database_with_schema;
    engine.execute(
        &parser
            .parse("create table schema_name.table_name(column_si smallint, column_i integer, column_bi bigint);")
            .expect("parsed"),
    );
    collector.assert_receive_till_this_moment(vec![Ok(QueryEvent::TableCreated), Ok(QueryEvent::QueryComplete)]);

    (engine, parser, collector)
}

#[rstest::fixture]
fn str_table(
    database_with_schema: (QueryExecutor, QueryParser, ResultCollector),
) -> (QueryExecutor, QueryParser, ResultCollector) {
    let (engine, parser, collector) = database_with_schema;
    engine.execute(
        &parser
            .parse("create table schema_name.table_name(col varchar(5));")
            .expect("parsed"),
    );
    collector.assert_receive_till_this_moment(vec![Ok(QueryEvent::TableCreated), Ok(QueryEvent::QueryComplete)]);

    (engine, parser, collector)
}

#[cfg(test)]
mod insert {
    use super::*;

    #[rstest::rstest]
    fn out_of_range(int_table: (QueryExecutor, QueryParser, ResultCollector)) {
        let (engine, parser, collector) = int_table;

        engine.execute(
            &parser
                .parse("insert into schema_name.table_name values (32768);")
                .expect("parsed"),
        );
        collector.assert_receive_single(Err(QueryError::out_of_range(
            PostgreSqlType::SmallInt,
            "col".to_string(),
            1,
        )));
    }

    #[rstest::rstest]
    fn type_mismatch(int_table: (QueryExecutor, QueryParser, ResultCollector)) {
        let (engine, parser, collector) = int_table;

        engine.execute(
            &parser
                .parse("insert into schema_name.table_name values ('str');")
                .expect("parsed"),
        );

        collector.assert_receive_single(Err(QueryError::invalid_text_representation(
            PostgreSqlType::SmallInt,
            "str",
        )));
    }

    #[rstest::rstest]
    fn multiple_columns_multiple_row_violation(multiple_ints_table: (QueryExecutor, QueryParser, ResultCollector)) {
        let (engine, parser, collector) = multiple_ints_table;
        engine
            .execute(&parser.parse("insert into schema_name.table_name values (-32769, -2147483649, 100), (100, -2147483649, -9223372036854775809);").expect("parsed"));
        collector.assert_receive_many(vec![
            Err(QueryError::out_of_range(PostgreSqlType::SmallInt, "column_si", 1)),
            Err(QueryError::out_of_range(PostgreSqlType::Integer, "column_i", 1)),
        ]);
    }

    #[rstest::rstest]
    fn violation_in_the_second_row(multiple_ints_table: (QueryExecutor, QueryParser, ResultCollector)) {
        let (engine, parser, collector) = multiple_ints_table;
        engine
            .execute(&parser.parse("insert into schema_name.table_name values (-32768, -2147483648, 100), (100, -2147483649, -9223372036854775809);").expect("parsed"));
        collector.assert_receive_many(vec![
            Err(QueryError::out_of_range(
                PostgreSqlType::Integer,
                "column_i".to_owned(),
                2,
            )),
            Err(QueryError::out_of_range(
                PostgreSqlType::BigInt,
                "column_bi".to_owned(),
                2,
            )),
        ]);
    }

    #[rstest::rstest]
    // #[ignore] // TODO constraints is going to be reworked
    fn value_too_long(str_table: (QueryExecutor, QueryParser, ResultCollector)) {
        let (engine, parser, collector) = str_table;
        engine.execute(
            &parser
                .parse("insert into schema_name.table_name values ('123457890');")
                .expect("parsed"),
        );
        collector.assert_receive_single(Err(QueryError::string_length_mismatch(
            PostgreSqlType::VarChar,
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
    fn out_of_range(int_table: (QueryExecutor, QueryParser, ResultCollector)) {
        let (engine, parser, collector) = int_table;
        engine.execute(
            &parser
                .parse("insert into schema_name.table_name values (32767);")
                .expect("parsed"),
        );
        collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

        engine.execute(
            &parser
                .parse("update schema_name.table_name set col = 32768;")
                .expect("parsed"),
        );

        collector.assert_receive_single(Err(QueryError::out_of_range(
            PostgreSqlType::SmallInt,
            "col".to_string(),
            1,
        )));
    }

    #[rstest::rstest]
    fn type_mismatch(int_table: (QueryExecutor, QueryParser, ResultCollector)) {
        let (engine, parser, collector) = int_table;
        engine.execute(
            &parser
                .parse("insert into schema_name.table_name values (32767);")
                .expect("parsed"),
        );
        collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

        engine.execute(
            &parser
                .parse("update schema_name.table_name set col = 'str';")
                .expect("parsed"),
        );

        collector.assert_receive_single(Err(QueryError::invalid_text_representation(
            PostgreSqlType::SmallInt,
            "str",
        )));
    }

    #[rstest::rstest]
    fn value_too_long(str_table: (QueryExecutor, QueryParser, ResultCollector)) {
        let (engine, parser, collector) = str_table;

        engine.execute(
            &parser
                .parse("insert into schema_name.table_name values ('str');")
                .expect("parsed"),
        );
        collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

        engine.execute(
            &parser
                .parse("update schema_name.table_name set col = '123457890';")
                .expect("parsed"),
        );
        collector.assert_receive_single(Err(QueryError::string_length_mismatch(
            PostgreSqlType::VarChar,
            5,
            "col".to_string(),
            1,
        )));
    }

    #[rstest::rstest]
    fn multiple_columns_violation(multiple_ints_table: (QueryExecutor, QueryParser, ResultCollector)) {
        let (engine, parser, collector) = multiple_ints_table;

        engine.execute(
            &parser
                .parse("insert into schema_name.table_name values (100, 100, 100), (100, 100, 100);")
                .expect("parsed"),
        );
        collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(2)));

        engine.execute(
            &parser
                .parse("update schema_name.table_name set column_si = -32769, column_i= -2147483649, column_bi=100;")
                .expect("parsed"),
        );
        collector.assert_receive_many(vec![
            Err(QueryError::out_of_range(
                PostgreSqlType::SmallInt,
                "column_si".to_owned(),
                1,
            )),
            Err(QueryError::out_of_range(
                PostgreSqlType::Integer,
                "column_i".to_owned(),
                1,
            )),
        ]);
    }
}
