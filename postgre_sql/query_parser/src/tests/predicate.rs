// Copyright 2020 - 2021 Alex Dukhno
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

#[test]
fn select() {
    let statements = QUERY_PARSER.parse("select * from schema_name.table_name where col1 = 1;");

    assert_eq!(
        statements,
        Ok(Request::Statement(Statement::Query(Query::Select(SelectQuery {
            select_items: vec![SelectItem::Wildcard],
            schema_name: "schema_name".to_owned(),
            table_name: "table_name".to_owned(),
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("col1".to_owned())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::Int(1)))
            }),
        }))))
    );
}

#[test]
fn update() {
    let statements = QUERY_PARSER.parse("update schema_name.table_name set col1 = 123 where col2 = 2;");

    assert_eq!(
        statements,
        Ok(Request::Statement(Statement::Query(Query::Update(UpdateQuery {
            schema_name: "schema_name".to_owned(),
            table_name: "table_name".to_owned(),
            assignments: vec![Assignment {
                column: "col1".to_owned(),
                value: Expr::Value(Value::Int(123))
            }],
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("col2".to_owned())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::Int(2)))
            }),
        }))))
    );
}

#[test]
fn delete() {
    let statements = QUERY_PARSER.parse("delete from schema_name.table_name where col1 = 1;");

    assert_eq!(
        statements,
        Ok(Request::Statement(Statement::Query(Query::Delete(DeleteQuery {
            schema_name: "schema_name".to_owned(),
            table_name: "table_name".to_owned(),
            where_clause: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column("col1".to_owned())),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::Int(1)))
            }),
        }))))
    );
}
