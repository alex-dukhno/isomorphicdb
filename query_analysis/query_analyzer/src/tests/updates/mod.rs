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
#[cfg(test)]
mod parameters;

fn update_statement(
    table_name: Vec<&'static str>,
    assignments: Vec<(&'static str, sql_ast::Expr)>,
) -> sql_ast::Statement {
    sql_ast::Statement::Update {
        table_name: sql_ast::ObjectName(table_name.into_iter().map(ident).collect()),
        assignments: assignments
            .into_iter()
            .map(|(id, value)| sql_ast::Assignment { id: ident(id), value })
            .collect(),
        selection: None,
    }
}

fn update_stmt_with_parameters(table_name: Vec<&'static str>) -> sql_ast::Statement {
    sql_ast::Statement::Update {
        table_name: sql_ast::ObjectName(table_name.into_iter().map(ident).collect()),
        assignments: vec![sql_ast::Assignment {
            id: ident("col_2"),
            value: sql_ast::Expr::Identifier(ident("$1")),
        }],
        selection: None,
    }
}
