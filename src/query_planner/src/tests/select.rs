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
use plan::{FullTableId, SelectInput};
use sqlparser::ast::{ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins};

#[rstest::rstest]
fn select_from_table_that_in_nonexistent_schema(planner: QueryPlanner) {
    assert_eq!(
        planner.plan(&Statement::Query(Box::new(Query {
            ctes: vec![],
            body: SetExpr::Select(Box::new(Select {
                distinct: false,
                top: None,
                projection: vec![SelectItem::Wildcard],
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![ident("non_existent_schema"), ident(TABLE)]),
                        alias: None,
                        args: vec![],
                        with_hints: vec![]
                    },
                    joins: vec![],
                }],
                selection: None,
                group_by: vec![],
                having: None,
            })),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        }))),
        Err(vec![PlanError::schema_does_not_exist(&"non_existent_schema")])
    );
}

#[rstest::rstest]
fn select_from_nonexistent_table(planner_with_schema: QueryPlanner) {
    assert_eq!(
        planner_with_schema.plan(&Statement::Query(Box::new(Query {
            ctes: vec![],
            body: SetExpr::Select(Box::new(Select {
                distinct: false,
                top: None,
                projection: vec![SelectItem::Wildcard],
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![ident(SCHEMA), ident("non_existent_table")]),
                        alias: None,
                        args: vec![],
                        with_hints: vec![]
                    },
                    joins: vec![],
                }],
                selection: None,
                group_by: vec![],
                having: None,
            })),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        }))),
        Err(vec![PlanError::table_does_not_exist(&format!(
            "{}.{}",
            SCHEMA, "non_existent_table"
        ))])
    );
}

#[rstest::rstest]
fn select_from_table_with_unqualified_name(planner_with_schema: QueryPlanner) {
    assert_eq!(
        planner_with_schema.plan(&Statement::Query(Box::new(Query {
            ctes: vec![],
            body: SetExpr::Select(Box::new(Select {
                distinct: false,
                top: None,
                projection: vec![SelectItem::Wildcard],
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![ident("only_schema_in_the_name")]),
                        alias: None,
                        args: vec![],
                        with_hints: vec![]
                    },
                    joins: vec![],
                }],
                selection: None,
                group_by: vec![],
                having: None,
            })),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        }))),
        Err(vec![PlanError::syntax_error(
            &"unsupported table name 'only_schema_in_the_name'. All table names must be qualified",
        )])
    );
}

#[rstest::rstest]
fn select_from_table_with_unsupported_name(planner_with_schema: QueryPlanner) {
    assert_eq!(
        planner_with_schema.plan(&Statement::Query(Box::new(Query {
            ctes: vec![],
            body: SetExpr::Select(Box::new(Select {
                distinct: false,
                top: None,
                projection: vec![SelectItem::Wildcard],
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![
                            ident("first_part"),
                            ident("second_part"),
                            ident("third_part"),
                            ident("fourth_part")
                        ]),
                        alias: None,
                        args: vec![],
                        with_hints: vec![]
                    },
                    joins: vec![],
                }],
                selection: None,
                group_by: vec![],
                having: None,
            })),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        }))),
        Err(vec![PlanError::syntax_error(
            &"unable to process table name 'first_part.second_part.third_part.fourth_part'",
        )])
    );
}

#[rstest::rstest]
fn select_from_table(planner_with_no_column_table: QueryPlanner) {
    assert_eq!(
        planner_with_no_column_table.plan(&Statement::Query(Box::new(Query {
            ctes: vec![],
            body: SetExpr::Select(Box::new(Select {
                distinct: false,
                top: None,
                projection: vec![SelectItem::Wildcard],
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![ident(SCHEMA), ident(TABLE)]),
                        alias: None,
                        args: vec![],
                        with_hints: vec![]
                    },
                    joins: vec![],
                }],
                selection: None,
                group_by: vec![],
                having: None,
            })),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        }))),
        Ok(Plan::Select(SelectInput {
            table_id: FullTableId::from((0, 0)),
            selected_columns: vec![],
            predicate: None
        }))
    );
}
