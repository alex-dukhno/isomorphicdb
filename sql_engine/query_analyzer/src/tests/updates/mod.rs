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
use query_ast::Value;

#[cfg(test)]
mod expressions;
#[cfg(test)]
mod general_cases;

fn update_statement(schema_name: &str, table_name: &str, assignments: Vec<(&str, Expr)>) -> Query {
    Query::Update(UpdateStatement {
        schema_name: schema_name.to_owned(),
        table_name: table_name.to_owned(),
        assignments: assignments
            .into_iter()
            .map(|(column, value)| Assignment {
                column: column.to_owned(),
                value,
            })
            .collect(),
        where_clause: None,
    })
}

fn update_stmt_with_parameters(schema_name: &str, table_name: &str) -> Query {
    Query::Update(UpdateStatement {
        schema_name: schema_name.to_owned(),
        table_name: table_name.to_owned(),
        assignments: vec![Assignment {
            column: "col_2".to_owned(),
            value: Expr::Value(Value::Param(1)),
        }],
        where_clause: None,
    })
}
