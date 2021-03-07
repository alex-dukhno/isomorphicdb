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

#[cfg(test)]
mod jdbc_flow {
    use super::*;

    #[rstest::rstest]
    fn insert(database_with_table: (InMemory, ResultCollector)) {
        let (mut engine, collector) = database_with_table;

        engine
            .execute(Command::Parse {
                statement_name: "".to_owned(),
                sql: "insert into schema_name.table_name values ($1, $2, $3)".to_owned(),
                param_types: vec![None, None, None],
            })
            .expect("statement parsed");
        collector.assert_receive_intermediate(Ok(QueryEvent::ParseComplete));

        engine
            .execute(Command::DescribeStatement { name: "".to_owned() })
            .expect("statement described");
        collector.assert_receive_intermediate(Ok(QueryEvent::StatementParameters(vec![
            PgType::SmallInt,
            PgType::SmallInt,
            PgType::SmallInt,
        ])));

        engine
            .execute(Command::Parse {
                statement_name: "".to_owned(),
                sql: "insert into schema_name.table_name values ($1, $2, $3)".to_owned(),
                param_types: vec![Some(PgType::SmallInt), Some(PgType::SmallInt), Some(PgType::SmallInt)],
            })
            .expect("statement parsed");
        collector.assert_receive_intermediate(Ok(QueryEvent::ParseComplete));

        engine
            .execute(Command::Bind {
                portal_name: "".to_owned(),
                statement_name: "".to_owned(),
                param_formats: vec![PgFormat::Binary, PgFormat::Binary, PgFormat::Binary],
                raw_params: vec![Some(vec![0, 0, 0, 1]), Some(vec![0, 0, 0, 2]), Some(vec![0, 0, 0, 3])],
                result_formats: vec![],
            })
            .expect("portal bound");
        collector.assert_receive_intermediate(Ok(QueryEvent::BindComplete));

        engine
            .execute(Command::Execute {
                portal_name: "".to_owned(),
                max_rows: 1,
            })
            .expect("portal executed");
        collector.assert_receive_intermediate(Ok(QueryEvent::RecordsInserted(1)));
    }

    #[rstest::rstest]
    fn update() {}
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
            ("col1".to_owned(), PgType::SmallInt),
            ("col2".to_owned(), PgType::SmallInt),
            ("col3".to_owned(), PgType::SmallInt),
        ])));
        collector.assert_receive_intermediate(Ok(QueryEvent::StatementParameters(vec![])));
    }

    #[rstest::rstest]
    fn statement_parameters(database_with_table: (InMemory, ResultCollector)) {
        let (mut engine, collector) = database_with_table;

        engine
            .execute(Command::Parse {
                statement_name: "statement_name".to_owned(),
                sql: "update schema_name.table_name set col1 = $1 where col2 = $2;".to_owned(),
                param_types: vec![Some(PgType::SmallInt), Some(PgType::SmallInt)],
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
            PgType::SmallInt,
            PgType::SmallInt,
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
                    param_types: vec![Some(PgType::SmallInt), Some(PgType::SmallInt)],
                })
                .expect("statement parsed");
            collector.assert_receive_intermediate(Ok(QueryEvent::ParseComplete));

            engine
                .execute(Command::Bind {
                    statement_name: "statement_name".to_owned(),
                    portal_name: "portal_name".to_owned(),
                    param_formats: vec![PgFormat::Binary, PgFormat::Text],
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
            collector.assert_receive_intermediate(Ok(QueryEvent::RecordsInserted(1)));
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
                    sql: "update schema_name.table_name set col1 = $1, col2 = $2".to_owned(),
                    param_types: vec![Some(PgType::Integer), Some(PgType::VarChar)],
                })
                .expect("query parsed");
            collector.assert_receive_intermediate(Ok(QueryEvent::ParseComplete));

            engine
                .execute(Command::Bind {
                    portal_name: "portal_name".to_owned(),
                    statement_name: "statement_name".to_owned(),
                    param_formats: vec![PgFormat::Binary, PgFormat::Text],
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
            collector.assert_receive_intermediate(Ok(QueryEvent::RecordsUpdated(1)));
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
            collector.assert_receive_intermediate(Ok(QueryEvent::RecordsSelected(1)));
        }
    }

    #[cfg(test)]
    mod assign_operation_queries {
        use super::*;

        #[rstest::rstest]
        fn insert_with_indeterminate_type(database_with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = database_with_table;

            engine
                .execute(Command::Parse {
                    statement_name: "statement_name".to_owned(),
                    sql: "insert into schema_name.table_name values (1, $9)".to_owned(),
                    param_types: vec![Some(PgType::SmallInt); 4],
                })
                .expect("statement parsed");
            collector.assert_receive_intermediate(Err(QueryError::indeterminate_parameter_data_type(4)));
        }

        #[rstest::rstest]
        fn insert_for_all_columns_analysis(database_with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = database_with_table;

            engine
                .execute(Command::Parse {
                    statement_name: "statement_name".to_owned(),
                    sql: "insert into schema_name.table_name values ($3, $2, $1)".to_owned(),
                    param_types: vec![None],
                })
                .expect("statement parsed");
            collector.assert_receive_intermediate(Ok(QueryEvent::ParseComplete));

            engine
                .execute(Command::Bind {
                    statement_name: "statement_name".to_owned(),
                    portal_name: "portal_name".to_owned(),
                    param_formats: vec![PgFormat::Text; 3],
                    raw_params: vec![Some(b"1".to_vec()); 3],
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
            collector.assert_receive_intermediate(Ok(QueryEvent::RecordsInserted(1)));
        }

        #[rstest::rstest]
        fn insert_for_specified_columns_analysis(database_with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = database_with_table;

            engine
                .execute(Command::Parse {
                    statement_name: "statement_name".to_owned(),
                    sql: "insert into schema_name.table_name (col3, COL2, COL1) values ($1, $2, $3)".to_owned(),
                    param_types: vec![None],
                })
                .expect("statement parsed");
            collector.assert_receive_intermediate(Ok(QueryEvent::ParseComplete));

            engine
                .execute(Command::Bind {
                    statement_name: "statement_name".to_owned(),
                    portal_name: "portal_name".to_owned(),
                    param_formats: vec![PgFormat::Text; 3],
                    raw_params: vec![Some(b"1".to_vec()); 3],
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
            collector.assert_receive_intermediate(Ok(QueryEvent::RecordsInserted(1)));
        }
    }

    #[cfg(test)]
    mod reassign_operation_queries {
        use super::*;

        #[rstest::rstest]
        fn update_with_indeterminate_type(database_with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = database_with_table;

            engine
                .execute(Command::Parse {
                    statement_name: "statement_name".to_owned(),
                    sql: "update schema_name.table_name set COL2 = $9".to_owned(),
                    param_types: vec![Some(PgType::SmallInt); 4],
                })
                .expect("statement parsed");
            collector.assert_receive_intermediate(Err(QueryError::indeterminate_parameter_data_type(4)));
        }

        #[rstest::rstest]
        fn update_for_all_rows(database_with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = database_with_table;

            engine
                .execute(Command::Query {
                    sql: "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6), (4, 8, 9)".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(3)));

            engine
                .execute(Command::Parse {
                    statement_name: "statement_name".to_owned(),
                    sql: "update schema_name.table_name set col3 = $1, COL1 = $2".to_owned(),
                    param_types: vec![None],
                })
                .expect("statement parsed");
            collector.assert_receive_intermediate(Ok(QueryEvent::ParseComplete));

            engine
                .execute(Command::Bind {
                    statement_name: "statement_name".to_owned(),
                    portal_name: "portal_name".to_owned(),
                    param_formats: vec![PgFormat::Text; 2],
                    raw_params: vec![Some(b"10".to_vec()); 2],
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
            collector.assert_receive_intermediate(Ok(QueryEvent::RecordsUpdated(3)));
        }

        #[rstest::rstest]
        fn update_for_specified_rows(database_with_table: (InMemory, ResultCollector)) {
            let (mut engine, collector) = database_with_table;

            engine
                .execute(Command::Query {
                    sql: "insert into schema_name.table_name values (1, 2, 3), (4, 5, 6), (4, 8, 9)".to_owned(),
                })
                .expect("query executed");
            collector.assert_receive_single(Ok(QueryEvent::RecordsInserted(3)));

            engine
                .execute(Command::Parse {
                    statement_name: "statement_name".to_owned(),
                    sql: "update schema_name.table_name set col2 = $1, col3 = $2 where COL1 = $3".to_owned(),
                    param_types: vec![None],
                })
                .expect("statement parsed");
            collector.assert_receive_intermediate(Ok(QueryEvent::ParseComplete));

            engine
                .execute(Command::Bind {
                    statement_name: "statement_name".to_owned(),
                    portal_name: "portal_name".to_owned(),
                    param_formats: vec![PgFormat::Text; 3],
                    raw_params: vec![Some(b"100".to_vec()), Some(b"200".to_vec()), Some(b"40".to_vec())],
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

            // TODO: `where` clause needs to be handled in `query_planner`.
            collector.assert_receive_intermediate(Ok(QueryEvent::RecordsUpdated(3)));
        }
    }
}
