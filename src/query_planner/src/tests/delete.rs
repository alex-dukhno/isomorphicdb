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
use crate::{
    plan::{Plan, TableDeletes},
    planner::QueryPlanner,
    tests::{ident, ResultCollector, TABLE},
};
use protocol::results::QueryError;
use sqlparser::ast::{ObjectName, Statement};

#[rstest::rstest]
fn delete_from_table_that_in_nonexistent_schema(planner_and_sender: (QueryPlanner, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender;
    assert_eq!(
        query_planner.plan(Statement::Delete {
            table_name: ObjectName(vec![ident("non_existent_schema"), ident(TABLE)]),
            selection: None
        }),
        Err(())
    );

    collector.assert_content(vec![Err(QueryError::schema_does_not_exist("non_existent_schema"))])
}

#[rstest::rstest]
fn delete_from_nonexistent_table(planner_and_sender_with_schema: (QueryPlanner, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender_with_schema;
    assert_eq!(
        query_planner.plan(Statement::Delete {
            table_name: ObjectName(vec![ident(SCHEMA), ident("non_existent_table")]),
            selection: None
        }),
        Err(())
    );

    collector.assert_content(vec![Err(QueryError::table_does_not_exist(format!(
        "{}.{}",
        SCHEMA, "non_existent_table"
    )))])
}

#[rstest::rstest]
fn delete_from_table_with_unqualified_name(planner_and_sender_with_schema: (QueryPlanner, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender_with_schema;
    assert_eq!(
        query_planner.plan(Statement::Delete {
            table_name: ObjectName(vec![ident("only_schema_in_the_name")]),
            selection: None
        }),
        Err(())
    );

    collector.assert_content(vec![Err(QueryError::syntax_error(
        "unsupported table name 'only_schema_in_the_name'. All table names must be qualified",
    ))])
}

#[rstest::rstest]
fn c_table_with_unsupported_name(planner_and_sender_with_schema: (QueryPlanner, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender_with_schema;
    assert_eq!(
        query_planner.plan(Statement::Delete {
            table_name: ObjectName(vec![
                ident("first_part"),
                ident("second_part"),
                ident("third_part"),
                ident("fourth_part")
            ]),
            selection: None
        }),
        Err(())
    );

    collector.assert_content(vec![Err(QueryError::syntax_error(
        "unable to process table name 'first_part.second_part.third_part.fourth_part'",
    ))])
}

#[rstest::rstest]
fn delete_from_table(planner_and_sender_with_table: (QueryPlanner, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender_with_table;
    assert_eq!(
        query_planner.plan(Statement::Delete {
            table_name: ObjectName(vec![ident(SCHEMA), ident(TABLE)]),
            selection: None
        }),
        Ok(Plan::Delete(TableDeletes {
            table_id: TableId((0, 0))
        }))
    );

    collector.assert_content(vec![])
}
