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

#[cfg(test)]
mod delete;
#[cfg(test)]
mod extended;
#[cfg(test)]
mod index;
#[cfg(test)]
mod insert;
#[cfg(test)]
mod joins;
#[cfg(test)]
mod predicate;
#[cfg(test)]
mod schema;
#[cfg(test)]
mod select;
#[cfg(test)]
mod table;
#[cfg(test)]
mod update;

const QUERY_PARSER: QueryParser = QueryParser::new();

#[test]
fn set_variable() {
    let statements = QUERY_PARSER.parse("set variable=value;");

    assert_eq!(
        statements,
        Ok(vec![Some(Statement::Config(Set {
            variable: "variable".to_owned(),
            value: "value".to_owned()
        }))])
    );
}

#[test]
fn semi_colon() {
    assert_eq!(QUERY_PARSER.parse(";"), Ok(vec![None]));
}

#[test]
fn empty_string() {
    assert_eq!(QUERY_PARSER.parse(" "), Ok(vec![]));
}

#[test]
fn many_empty_queries() {
    assert_eq!(QUERY_PARSER.parse(";;"), Ok(vec![None, None]));
}

#[test]
fn many_statements() {
    assert_eq!(
        QUERY_PARSER.parse("select 1; select 2;"),
        Ok(vec![
            Some(Statement::Query(Query::Select(SelectStatement {
                projection_items: vec![SelectItem::UnnamedExpr(Expr::Value(Value::Int(1)))],
                relations: None,
                where_clause: None
            }))),
            Some(Statement::Query(Query::Select(SelectStatement {
                projection_items: vec![SelectItem::UnnamedExpr(Expr::Value(Value::Int(2)))],
                relations: None,
                where_clause: None
            })))
        ])
    )
}
