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
mod invalid_expressions;
#[cfg(test)]
mod parameters;

fn inner_insert(
    full_name: Vec<&'static str>,
    multiple_values: Vec<Vec<ast::Expr>>,
    columns: Vec<&'static str>,
) -> ast::Statement {
    ast::Statement::Insert {
        table_name: ast::ObjectName(full_name.into_iter().map(ident).collect()),
        columns: columns.into_iter().map(ident).collect(),
        source: Box::new(ast::Query {
            with: None,
            body: ast::SetExpr::Values(ast::Values(multiple_values)),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        }),
    }
}

fn insert_with_values(full_name: Vec<&'static str>, values: Vec<Vec<ast::Expr>>) -> ast::Statement {
    inner_insert(full_name, values, vec![])
}
