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
use ast::predicates::{PredicateOp, PredicateValue};
use bigdecimal::BigDecimal;
use plan::{FullTableId, SelectInput};
use sql_ast::{
    BinaryOperator, Expr, ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value,
};
use std::convert::TryFrom;

#[rstest::rstest]
fn select_from_table(planner_with_table: QueryPlanner) {
    assert_eq!(
        planner_with_table.plan(&Statement::Query(Box::new(Query {
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
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(ident("small_int"))),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Value(Value::Number(BigDecimal::try_from(0).unwrap())))
                }),
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
            selected_columns: vec![0, 1, 2],
            predicate: Some((
                PredicateValue::Column(0),
                PredicateOp::Eq,
                PredicateValue::Number(BigDecimal::try_from(0).unwrap())
            ))
        }))
    );
}
