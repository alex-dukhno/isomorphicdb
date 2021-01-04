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
#[cfg(test)]
mod invalid_expressions;
#[cfg(test)]
mod parameters;

fn inner_insert(
    full_name: Vec<&'static str>,
    multiple_values: Vec<Vec<sql_ast::Expr>>,
    columns: Vec<&'static str>,
) -> sql_ast::Statement {
    sql_ast::Statement::Insert {
        table_name: sql_ast::ObjectName(full_name.into_iter().map(ident).collect()),
        columns: columns.into_iter().map(ident).collect(),
        source: Box::new(sql_ast::Query {
            with: None,
            body: sql_ast::SetExpr::Values(sql_ast::Values(multiple_values)),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        }),
    }
}

fn insert_with_values(full_name: Vec<&'static str>, values: Vec<Vec<sql_ast::Expr>>) -> sql_ast::Statement {
    inner_insert(full_name, values, vec![])
}
