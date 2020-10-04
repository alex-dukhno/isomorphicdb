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
use protocol::pgsql_types::{PostgreSqlFormat, PostgreSqlType};

#[rstest::fixture]
fn database_with_table(database_with_schema: (InMemory, ResultCollector)) -> (InMemory, ResultCollector) {
    let (mut engine, collector) = database_with_schema;

    engine
        .execute(Command::Query {
            sql: "create table schema_name.table_name (column_1 smallint, column_2 smallint);".to_owned(),
        })
        .expect("query executed");
    collector.assert_receive_single(Ok(QueryEvent::TableCreated));

    (engine, collector)
}

#[cfg(test)]
mod statement_description {
    use super::*;

    #[rstest::rstest]
    fn statement_description(database_with_table: (InMemory, ResultCollector)) {
        let (mut engine, collector) = database_with_table;

        engine
            .execute(Command::Parse {
                statement_name: "statement_name".to_owned(),
                sql: "select * from schema_name.table_name;".to_owned(),
                param_types: vec![],
            })
            .expect("statement parsed");
        collector.assert_receive_intermediate(Ok(QueryEvent::ParseComplete));

        engine
            .execute(Command::DescribeStatement {
                name: "statement_name".to_owned(),
            })
            .expect("statement described");
        collector.assert_receive_intermediate(Ok(QueryEvent::StatementDescription(vec![
            ("column_1".to_owned(), PostgreSqlType::SmallInt),
            ("column_2".to_owned(), PostgreSqlType::SmallInt),
        ])));
        collector.assert_receive_intermediate(Ok(QueryEvent::StatementParameters(vec![])));
    }

    #[rstest::rstest]
    fn statement_parameters(database_with_table: (InMemory, ResultCollector)) {
        let (mut engine, collector) = database_with_table;

        engine
            .execute(Command::Parse {
                statement_name: "statement_name".to_owned(),
                sql: "update schema_name.table_name set column_1 = $1 where column_2 = $2;".to_owned(),
                param_types: vec![PostgreSqlType::SmallInt, PostgreSqlType::SmallInt],
            })
            .expect("statement parsed");
        collector.assert_receive_intermediate(Ok(QueryEvent::ParseComplete));

        engine
            .execute(Command::DescribeStatement {
                name: "statement_name".to_owned(),
            })
            .expect("statement described");
        collector.assert_receive_intermediate(Ok(QueryEvent::StatementDescription(vec![])));
        collector.assert_receive_intermediate(Ok(QueryEvent::StatementParameters(vec![
            PostgreSqlType::SmallInt,
            PostgreSqlType::SmallInt,
        ])));
    }

    #[rstest::rstest]
    fn unsuccessful_statement_description(database_with_table: (InMemory, ResultCollector)) {
        let (mut engine, collector) = database_with_table;

        engine
            .execute(Command::DescribeStatement {
                name: "non_existent".to_owned(),
            })
            .expect("no errors");
        collector.assert_receive_intermediate(Err(QueryError::prepared_statement_does_not_exist("non_existent")));
    }
}

#[cfg(test)]
mod parse_bind_execute {
    use super::*;

    #[cfg(test)]
    mod simple_queries {
        use super::*;

        #[rstest::rstest]
        fn insert(database_with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = database_with_table;

            engine
                .execute(Command::Parse {
                    statement_name: "statement_name".to_owned(),
                    sql: "insert into schema_name.table_name values ($1, $2);".to_owned(),
                    param_types: vec![PostgreSqlType::SmallInt, PostgreSqlType::SmallInt],
                })
                .expect("statement parsed");
            collector.assert_receive_intermediate(Ok(QueryEvent::ParseComplete));

            engine
                .execute(Command::Bind {
                    statement_name: "statement_name".to_owned(),
                    portal_name: "portal_name".to_owned(),
                    param_formats: vec![PostgreSqlFormat::Binary, PostgreSqlFormat::Text],
                    raw_params: vec![Some(vec![0, 1]), Some(b"2".to_vec())],
                    result_formats: vec![],
                })
                .expect("statement bound to portal");
            collector.assert_receive_intermediate(Ok(QueryEvent::BindComplete));

            engine
                .execute(Command::Execute {
                    portal_name: "portal_name".to_owned(),
                    max_rows: 0,
                })
                .expect("portal executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));
        }

        #[rstest::rstest]
        fn update(database_with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = database_with_table;

            engine
                .execute(Command::Query {
                    sql: "insert into schema_name.table_name values (1, 2);".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(Command::Parse {
                    statement_name: "statement_name".to_owned(),
                    sql: "update schema_name.table_name set column_1 = $1, column_2 = $2".to_owned(),
                    param_types: vec![PostgreSqlType::Integer, PostgreSqlType::VarChar],
                })
                .expect("query parsed");
            collector.assert_receive_intermediate(Ok(QueryEvent::ParseComplete));

            engine
                .execute(Command::Bind {
                    portal_name: "portal_name".to_owned(),
                    statement_name: "statement_name".to_owned(),
                    param_formats: vec![PostgreSqlFormat::Binary, PostgreSqlFormat::Text],
                    raw_params: vec![Some(vec![0, 0, 0, 1]), Some(b"2".to_vec())],
                    result_formats: vec![],
                })
                .expect("statement bound to portal");
            collector.assert_receive_intermediate(Ok(QueryEvent::BindComplete));

            engine
                .execute(Command::Execute {
                    portal_name: "portal_name".to_owned(),
                    max_rows: 0,
                })
                .expect("portal executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsUpdated(1)));
        }

        #[rstest::rstest]
        fn select(database_with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = database_with_table;

            engine
                .execute(Command::Query {
                    sql: "insert into schema_name.table_name values (1, 2);".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(1)));

            engine
                .execute(Command::Parse {
                    statement_name: "statement_name".to_owned(),
                    sql: "select * from schema_name.table_name".to_owned(),
                    param_types: vec![],
                })
                .expect("query parsed");
            collector.assert_receive_intermediate(Ok(QueryEvent::ParseComplete));

            engine
                .execute(Command::Bind {
                    portal_name: "portal_name".to_owned(),
                    statement_name: "statement_name".to_owned(),
                    param_formats: vec![],
                    raw_params: vec![],
                    result_formats: vec![],
                })
                .expect("statement bound to portal");
            collector.assert_receive_intermediate(Ok(QueryEvent::BindComplete));

            engine
                .execute(Command::Execute {
                    portal_name: "portal_name".to_owned(),
                    max_rows: 0,
                })
                .expect("portal executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsSelected(1)));
        }
    }
}
