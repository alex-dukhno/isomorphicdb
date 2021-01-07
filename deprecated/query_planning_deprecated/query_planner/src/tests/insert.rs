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
use bigdecimal::BigDecimal;
use constraints::TypeConstraint;
use plan::{FullTableId, TableInserts};
use sql_ast::{
    Expr, ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, UnaryOperator, Value,
    Values,
};
use types::SqlType;

fn insert_into_with_columns(table_name: ObjectName, columns: Vec<Ident>, body: SetExpr) -> Statement {
    Statement::Insert {
        table_name,
        columns,
        source: Box::new(Query {
            with: None,
            body,
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        }),
    }
}

fn insert_into(table_name: ObjectName, values: Vec<Vec<Expr>>) -> Statement {
    insert_into_with_columns(table_name, vec![], SetExpr::Values(Values(values)))
}

fn insert_into_select(table_name: ObjectName, query: Query) -> Statement {
    insert_into_with_columns(table_name, vec![], SetExpr::Query(Box::new(query)))
}

/// ```sql
/// insert into non_existent_schema.table_name values ();
/// ```
#[rstest::rstest]
fn insert_into_table_that_in_nonexistent_schema(planner: QueryPlanner) {
    assert_eq!(
        planner.plan(&insert_into(
            ObjectName(vec![ident("non_existent_schema"), ident(TABLE)]),
            vec![]
        )),
        Err(PlanError::schema_does_not_exist(&"non_existent_schema"))
    );
}

/// ```sql
/// insert into schema_name.non_existent_table values ();
/// ```
#[rstest::rstest]
fn insert_into_nonexistent_table(planner_with_schema: QueryPlanner) {
    assert_eq!(
        planner_with_schema.plan(&insert_into(
            ObjectName(vec![ident(SCHEMA), ident("non_existent_table")]),
            vec![]
        )),
        Err(PlanError::table_does_not_exist(&format!(
            "{}.{}",
            SCHEMA, "non_existent_table"
        )))
    );
}

/// ```sql
/// insert into only_schema_in_the_name values ();
/// ```
#[rstest::rstest]
fn insert_into_table_with_unqualified_name(planner_with_schema: QueryPlanner) {
    assert_eq!(
        planner_with_schema.plan(&insert_into(ObjectName(vec![ident("only_schema_in_the_name")]), vec![])),
        Err(PlanError::syntax_error(
            &"unsupported table name 'only_schema_in_the_name'. All table names must be qualified",
        ))
    );
}

/// ```sql
/// insert into first_part.second_part.third_part.fourth_part values ();
/// ```
#[rstest::rstest]
fn insert_into_table_with_unsupported_name(planner_with_schema: QueryPlanner) {
    assert_eq!(
        planner_with_schema.plan(&insert_into(
            ObjectName(vec![
                ident("first_part"),
                ident("second_part"),
                ident("third_part"),
                ident("fourth_part")
            ]),
            vec![]
        )),
        Err(PlanError::syntax_error(
            &"unable to process table name 'first_part.second_part.third_part.fourth_part'",
        ))
    );
}

/// ```sql
/// insert into schema_name.table_name (small_int, integer, big_int) values ();
/// ```
#[rstest::rstest]
fn insert_into_table(planner_with_table: QueryPlanner) {
    assert_eq!(
        planner_with_table.plan(&insert_into_with_columns(
            ObjectName(vec![ident(SCHEMA), ident(TABLE)]),
            vec![ident("small_int"), ident("integer"), ident("big_int")],
            SetExpr::Values(Values(vec![]))
        )),
        Ok(Plan::Insert(TableInserts {
            table_id: FullTableId::from((0, 0)),
            column_indices: vec![
                (
                    0,
                    "small_int".to_owned(),
                    SqlType::small_int(),
                    TypeConstraint::SmallInt
                ),
                (1, "integer".to_owned(), SqlType::integer(), TypeConstraint::Integer),
                (2, "big_int".to_owned(), SqlType::big_int(), TypeConstraint::BigInt)
            ],
            input: vec![]
        }))
    );
}

/// ```sql
/// create table schema_name.table_name;
/// insert into schema_name.table_name values ();
/// ```
#[rstest::rstest]
fn insert_into_table_without_columns(planner_with_no_column_table: QueryPlanner) {
    assert_eq!(
        planner_with_no_column_table.plan(&insert_into(ObjectName(vec![ident(SCHEMA), ident(TABLE)]), vec![])),
        Ok(Plan::Insert(TableInserts {
            table_id: FullTableId::from((0, 0)),
            column_indices: vec![],
            input: vec![]
        }))
    );
}

/// ```sql
/// insert into schema_name.table_name values (not 1);
/// ```
#[rstest::rstest]
fn insert_with_syntax_error(planner_with_table: QueryPlanner) {
    assert_eq!(
        planner_with_table.plan(&insert_into(
            ObjectName(vec![ident(SCHEMA), ident(TABLE)]),
            vec![vec![Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(Expr::Value(Value::Number(BigDecimal::from(1))))
            }]]
        )),
        Err(PlanError::syntax_error(&"operation 'logical not' not supported"))
    );
}

/// ```sql
/// insert into schema_name.table_name values (not 1);
/// ```
#[rstest::rstest]
fn insert_with_not_supported_expression(planner_with_table: QueryPlanner) {
    assert_eq!(
        planner_with_table.plan(&insert_into(
            ObjectName(vec![ident(SCHEMA), ident(TABLE)]),
            vec![vec![Expr::UnaryOp {
                op: UnaryOperator::PGAbs,
                expr: Box::new(Expr::Value(Value::Number(BigDecimal::from(-1))))
            }]]
        )),
        Err(PlanError::feature_not_supported(&"not handled Expression [@ -1]"))
    );
}

/// ```sql
/// insert into schema_name.table_name values (not 1);
/// ```
#[rstest::rstest]
fn insert_with_not_supported_feature(planner_with_table: QueryPlanner) {
    assert_eq!(
        planner_with_table.plan(&insert_into_select(
            ObjectName(vec![ident(SCHEMA), ident(TABLE)]),
            Query {
                with: None,
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
            }
        )),
        Err(PlanError::feature_not_supported(
            &"(SELECT * FROM schema_name.table_name)"
        ))
    );
}
