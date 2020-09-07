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
    plan::{Plan, TableInserts},
    planner::QueryPlanner,
    tests::{ident, ResultCollector, TABLE},
};
use protocol::results::QueryError;
use sqlparser::ast::{ObjectName, Query, SetExpr, Statement, Values};

#[rstest::rstest]
fn insert_into_table_that_in_nonexistent_schema(planner_and_sender: (QueryPlanner, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender;
    assert_eq!(
        query_planner.plan(Statement::Insert {
            table_name: ObjectName(vec![ident("non_existent_schema"), ident(TABLE)]),
            columns: vec![],
            source: Box::new(Query {
                ctes: vec![],
                body: SetExpr::Values(Values(vec![])),
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None
            })
        }),
        Err(())
    );

    collector.assert_content(vec![Err(QueryError::schema_does_not_exist("non_existent_schema"))])
}

#[rstest::rstest]
fn insert_into_nonexistent_table(planner_and_sender_with_schema: (QueryPlanner, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender_with_schema;
    assert_eq!(
        query_planner.plan(Statement::Insert {
            table_name: ObjectName(vec![ident(SCHEMA), ident("non_existent_table")]),
            columns: vec![],
            source: Box::new(Query {
                ctes: vec![],
                body: SetExpr::Values(Values(vec![])),
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None
            })
        }),
        Err(())
    );

    collector.assert_content(vec![Err(QueryError::table_does_not_exist(format!(
        "{}.{}",
        SCHEMA, "non_existent_table"
    )))])
}

#[rstest::rstest]
fn insert_into_table(planner_and_sender_with_table: (QueryPlanner, ResultCollector)) {
    let (query_planner, collector) = planner_and_sender_with_table;
    assert_eq!(
        query_planner.plan(Statement::Insert {
            table_name: ObjectName(vec![ident(SCHEMA), ident(TABLE)]),
            columns: vec![],
            source: Box::new(Query {
                ctes: vec![],
                body: SetExpr::Values(Values(vec![])),
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None
            })
        }),
        Ok(Plan::Insert(TableInserts {
            full_table_name: TableId(0, 0),
            column_indices: vec![],
            input: Box::new(Query {
                ctes: vec![],
                body: SetExpr::Values(Values(vec![])),
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None
            })
        }))
    );

    collector.assert_content(vec![])
}
