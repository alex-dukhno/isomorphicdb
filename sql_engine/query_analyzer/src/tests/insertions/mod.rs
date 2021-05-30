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
mod expressions;
#[cfg(test)]
mod general_cases;

fn small_int(value: i16) -> Expr {
    Expr::Value(number(value))
}

fn inner_insert(schema_name: &str, table_name: &str, multiple_values: Vec<Vec<Expr>>, columns: Vec<&str>) -> Query {
    Query::Insert(InsertQuery {
        schema_name: schema_name.to_owned(),
        table_name: table_name.to_owned(),
        columns: columns.into_iter().map(ToOwned::to_owned).collect(),
        source: InsertSource::Values(Values(multiple_values)),
    })
}

fn insert_with_values(schema_name: &str, table_name: &str, values: Vec<Vec<Expr>>) -> Query {
    inner_insert(schema_name, table_name, values, vec![])
}
