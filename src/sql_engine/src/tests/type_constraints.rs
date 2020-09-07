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

#[rstest::fixture]
fn int_table(sql_engine_with_schema: (QueryExecutor, ResultCollector)) -> (QueryExecutor, ResultCollector) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name(col smallint);")
        .expect("no system errors");

    (engine, collector)
}

#[rstest::fixture]
fn multiple_ints_table(sql_engine_with_schema: (QueryExecutor, ResultCollector)) -> (QueryExecutor, ResultCollector) {
    let (mut engine, collector) = sql_engine_with_schema;
    engine
        .execute("create table schema_name.table_name(column_si smallint, column_i integer, column_bi bigint);")
        .expect("no system errors");

    (engine, collector)
}

#[rstest::fixture]
fn str_table(sql_engine_with_schema: (QueryExecutor, ResultCollector)) -> (QueryExecutor, ResultCollector) {
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
    fn out_of_range(int_table: (QueryExecutor, ResultCollector)) {
        let (mut engine, collector) = int_table;

        engine
            .execute("insert into schema_name.table_name values (32768);")
            .expect("no system errors");

        collector.assert_content_for_single_queries(vec![
            Ok(QueryEvent::SchemaCreated),
            Ok(QueryEvent::QueryComplete),
            Ok(QueryEvent::TableCreated),
            Ok(QueryEvent::QueryComplete),
            Err(QueryError::out_of_range(PostgreSqlType::SmallInt, "col".to_string(), 1)),
            Ok(QueryEvent::QueryComplete),
        ]);
    }

    #[rstest::rstest]
    fn type_mismatch(int_table: (QueryExecutor, ResultCollector)) {
        let (mut engine, collector) = int_table;

        engine
            .execute("insert into schema_name.table_name values ('str');")
            .expect("no system errors");

        collector.assert_content_for_single_queries(vec![
            Ok(QueryEvent::SchemaCreated),
            Ok(QueryEvent::QueryComplete),
            Ok(QueryEvent::TableCreated),
            Ok(QueryEvent::QueryComplete),
            Err(QueryError::type_mismatch("str", PostgreSqlType::SmallInt, "col", 1)),
            Ok(QueryEvent::QueryComplete),
        ]);
    }

    #[rstest::rstest]
    #[ignore]
    // currently the ExpressionEvaluator doesn't have contextual information to create the right
    // error messages.
    fn multiple_columns_multiple_row_violation(multiple_ints_table: (QueryExecutor, Arc<Collector>)) {
        let (mut engine, collector) = multiple_ints_table;
        engine
            .execute("insert into schema_name.table_name values (-32769, -2147483649, 100), (100, -2147483649, -9223372036854775809);")
            .expect("no system errors");

        collector.assert_content_for_single_queries(vec![
            Ok(QueryEvent::SchemaCreated),
            Ok(QueryEvent::QueryComplete),
            Ok(QueryEvent::TableCreated),
            Ok(QueryEvent::QueryComplete),
            Err(QueryError::out_of_range(PostgreSqlType::SmallInt, "column_si", 1)),
            Err(QueryError::out_of_range(PostgreSqlType::Integer, "column_i", 1)),
            Ok(QueryEvent::QueryComplete),
        ]);
    }

    #[rstest::rstest]
    fn violation_in_the_second_row(multiple_ints_table: (QueryExecutor, ResultCollector)) {
        let (mut engine, collector) = multiple_ints_table;
        engine
            .execute("insert into schema_name.table_name values (-32768, -2147483648, 100), (100, -2147483649, -9223372036854775809);")
            .expect("no system errors");

        collector.assert_content_for_single_queries(vec![
            Ok(QueryEvent::SchemaCreated),
            Ok(QueryEvent::QueryComplete),
            Ok(QueryEvent::TableCreated),
            Ok(QueryEvent::QueryComplete),
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
            Ok(QueryEvent::QueryComplete),
        ]);
    }

    #[rstest::rstest]
    fn value_too_long(str_table: (QueryExecutor, ResultCollector)) {
        let (mut engine, collector) = str_table;
        engine
            .execute("insert into schema_name.table_name values ('123457890');")
            .expect("no system errors");

        collector.assert_content_for_single_queries(vec![
            Ok(QueryEvent::SchemaCreated),
            Ok(QueryEvent::QueryComplete),
            Ok(QueryEvent::TableCreated),
            Ok(QueryEvent::QueryComplete),
            Err(QueryError::string_length_mismatch(
                PostgreSqlType::VarChar,
                5,
                "col".to_string(),
                1,
            )),
            Ok(QueryEvent::QueryComplete),
        ]);
    }
}

#[cfg(test)]
mod update {
    use super::*;

    #[rstest::rstest]
    fn out_of_range(int_table: (QueryExecutor, ResultCollector)) {
        let (mut engine, collector) = int_table;

        engine
            .execute("insert into schema_name.table_name values (32767);")
            .expect("no system errors");
        engine
            .execute("update schema_name.table_name set col = 32768;")
            .expect("no system errors");

        collector.assert_content_for_single_queries(vec![
            Ok(QueryEvent::SchemaCreated),
            Ok(QueryEvent::QueryComplete),
            Ok(QueryEvent::TableCreated),
            Ok(QueryEvent::QueryComplete),
            Ok(QueryEvent::RecordsInserted(1)),
            Ok(QueryEvent::QueryComplete),
            Err(QueryError::out_of_range(PostgreSqlType::SmallInt, "col".to_string(), 1)),
            Ok(QueryEvent::QueryComplete),
        ]);
    }

    #[rstest::rstest]
    fn type_mismatch(int_table: (QueryExecutor, ResultCollector)) {
        let (mut engine, collector) = int_table;
        engine
            .execute("insert into schema_name.table_name values (32767);")
            .expect("no system errors");
        engine
            .execute("update schema_name.table_name set col = 'str';")
            .expect("no system errors");

        collector.assert_content_for_single_queries(vec![
            Ok(QueryEvent::SchemaCreated),
            Ok(QueryEvent::QueryComplete),
            Ok(QueryEvent::TableCreated),
            Ok(QueryEvent::QueryComplete),
            Ok(QueryEvent::RecordsInserted(1)),
            Ok(QueryEvent::QueryComplete),
            Err(QueryError::type_mismatch("str", PostgreSqlType::SmallInt, "col", 1)),
            Ok(QueryEvent::QueryComplete),
        ]);
    }

    #[rstest::rstest]
    fn value_too_long(str_table: (QueryExecutor, ResultCollector)) {
        let (mut engine, collector) = str_table;

        engine
            .execute("insert into schema_name.table_name values ('str');")
            .expect("no system errors");
        engine
            .execute("update schema_name.table_name set col = '123457890';")
            .expect("no system errors");

        collector.assert_content_for_single_queries(vec![
            Ok(QueryEvent::SchemaCreated),
            Ok(QueryEvent::QueryComplete),
            Ok(QueryEvent::TableCreated),
            Ok(QueryEvent::QueryComplete),
            Ok(QueryEvent::RecordsInserted(1)),
            Ok(QueryEvent::QueryComplete),
            Err(QueryError::string_length_mismatch(
                PostgreSqlType::VarChar,
                5,
                "col".to_string(),
                1,
            )),
            Ok(QueryEvent::QueryComplete),
        ]);
    }

    #[rstest::rstest]
    fn multiple_columns_violation(multiple_ints_table: (QueryExecutor, ResultCollector)) {
        let (mut engine, collector) = multiple_ints_table;

        engine
            .execute("insert into schema_name.table_name values (100, 100, 100), (100, 100, 100);")
            .expect("no system errors");

        engine
            .execute("update schema_name.table_name set column_si = -32769, column_i= -2147483649, column_bi=100;")
            .expect("no system errors");

        collector.assert_content_for_single_queries(vec![
            Ok(QueryEvent::SchemaCreated),
            Ok(QueryEvent::QueryComplete),
            Ok(QueryEvent::TableCreated),
            Ok(QueryEvent::QueryComplete),
            Ok(QueryEvent::RecordsInserted(2)),
            Ok(QueryEvent::QueryComplete),
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
            Ok(QueryEvent::QueryComplete),
        ]);
    }
}
