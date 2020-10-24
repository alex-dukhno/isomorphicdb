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
use constraints::TypeConstraint;
use plan::{FullTableId, TableInserts};
use sql_model::sql_types::SqlType;
use sqlparser::ast::{ObjectName, Query, SetExpr, Statement, Values};

#[rstest::rstest]
fn insert_into_table_that_in_nonexistent_schema(planner: QueryPlanner) {
    assert_eq!(
        planner.plan(&Statement::Insert {
            table_name: ObjectName(vec![ident("non_existent_schema"), ident(TABLE)]),
            columns: vec![],
            source: Box::new(Query {
                with: None,
                body: SetExpr::Values(Values(vec![])),
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None
            })
        }),
        Err(vec![PlanError::schema_does_not_exist(&"non_existent_schema")])
    );
}

#[rstest::rstest]
fn insert_into_nonexistent_table(planner_with_schema: QueryPlanner) {
    assert_eq!(
        planner_with_schema.plan(&Statement::Insert {
            table_name: ObjectName(vec![ident(SCHEMA), ident("non_existent_table")]),
            columns: vec![],
            source: Box::new(Query {
                with: None,
                body: SetExpr::Values(Values(vec![])),
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None
            })
        }),
        Err(vec![PlanError::table_does_not_exist(&format!(
            "{}.{}",
            SCHEMA, "non_existent_table"
        ))])
    );
}

#[rstest::rstest]
fn insert_into_table_with_unqualified_name(planner_with_schema: QueryPlanner) {
    assert_eq!(
        planner_with_schema.plan(&Statement::Insert {
            table_name: ObjectName(vec![ident("only_schema_in_the_name")]),
            columns: vec![],
            source: Box::new(Query {
                with: None,
                body: SetExpr::Values(Values(vec![])),
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None
            })
        }),
        Err(vec![PlanError::syntax_error(
            &"unsupported table name 'only_schema_in_the_name'. All table names must be qualified",
        )])
    );
}

#[rstest::rstest]
fn insert_into_table_with_unsupported_name(planner_with_schema: QueryPlanner) {
    assert_eq!(
        planner_with_schema.plan(&Statement::Insert {
            table_name: ObjectName(vec![
                ident("first_part"),
                ident("second_part"),
                ident("third_part"),
                ident("fourth_part")
            ]),
            columns: vec![],
            source: Box::new(Query {
                with: None,
                body: SetExpr::Values(Values(vec![])),
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None
            })
        }),
        Err(vec![PlanError::syntax_error(
            &"unable to process table name 'first_part.second_part.third_part.fourth_part'",
        )])
    );
}

#[rstest::rstest]
fn insert_into_table(planner_with_table: QueryPlanner) {
    assert_eq!(
        planner_with_table.plan(&Statement::Insert {
            table_name: ObjectName(vec![ident(SCHEMA), ident(TABLE)]),
            columns: vec![ident("small_int"), ident("integer"), ident("big_int")],
            source: Box::new(Query {
                with: None,
                body: SetExpr::Values(Values(vec![])),
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None
            })
        }),
        Ok(Plan::Insert(TableInserts {
            table_id: FullTableId::from((0, 0)),
            column_indices: vec![
                (0, "small_int".to_owned(), SqlType::SmallInt, TypeConstraint::SmallInt),
                (1, "integer".to_owned(), SqlType::Integer, TypeConstraint::Integer),
                (2, "big_int".to_owned(), SqlType::BigInt, TypeConstraint::BigInt)
            ],
            input: vec![]
        }))
    );
}

#[rstest::rstest]
fn insert_into_table_without_columns(planner_with_table: QueryPlanner) {
    assert_eq!(
        planner_with_table.plan(&Statement::Insert {
            table_name: ObjectName(vec![ident(SCHEMA), ident(TABLE)]),
            columns: vec![],
            source: Box::new(Query {
                with: None,
                body: SetExpr::Values(Values(vec![])),
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None
            })
        }),
        Ok(Plan::Insert(TableInserts {
            table_id: FullTableId::from((0, 0)),
            column_indices: vec![
                (0, "small_int".to_owned(), SqlType::SmallInt, TypeConstraint::SmallInt),
                (1, "integer".to_owned(), SqlType::Integer, TypeConstraint::Integer),
                (2, "big_int".to_owned(), SqlType::BigInt, TypeConstraint::BigInt)
            ],
            input: vec![]
        }))
    );
}
