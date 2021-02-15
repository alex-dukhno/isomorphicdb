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
mod expressions;
#[cfg(test)]
mod general_cases;

fn select_with_columns(name: Vec<&'static str>, projection: Vec<sql_ast::SelectItem>) -> sql_ast::Statement {
    sql_ast::Statement::Query(Box::new(sql_ast::Query {
        with: None,
        body: sql_ast::SetExpr::Select(Box::new(sql_ast::Select {
            distinct: false,
            top: None,
            projection,
            from: vec![sql_ast::TableWithJoins {
                relation: sql_ast::TableFactor::Table {
                    name: sql_ast::ObjectName(name.into_iter().map(ident).collect()),
                    alias: None,
                    args: vec![],
                    with_hints: vec![],
                },
                joins: vec![],
            }],
            lateral_views: vec![],
            selection: None,
            group_by: vec![],
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
        })),
        order_by: vec![],
        limit: None,
        offset: None,
        fetch: None,
    }))
}

fn select(name: Vec<&'static str>) -> sql_ast::Statement {
    select_with_columns(name, vec![sql_ast::SelectItem::Wildcard])
}
