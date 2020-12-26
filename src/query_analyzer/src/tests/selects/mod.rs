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

#[cfg(test)]
mod expressions;
#[cfg(test)]
mod general_cases;

fn select_with_columns(name: Vec<&'static str>, projection: Vec<ast::SelectItem>) -> ast::Statement {
    ast::Statement::Query(Box::new(ast::Query {
        with: None,
        body: ast::SetExpr::Select(Box::new(ast::Select {
            distinct: false,
            top: None,
            projection,
            from: vec![ast::TableWithJoins {
                relation: ast::TableFactor::Table {
                    name: ast::ObjectName(name.into_iter().map(ident).collect()),
                    alias: None,
                    args: vec![],
                    with_hints: vec![],
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
    }))
}

fn select(name: Vec<&'static str>) -> ast::Statement {
    select_with_columns(name, vec![ast::SelectItem::Wildcard])
}
