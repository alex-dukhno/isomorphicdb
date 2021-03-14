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
use query_ast::Extended;

#[test]
fn prepare() {
    let statement =
        QUERY_PARSER.parse("prepare foo_plan (smallint) as insert into schema_name.table_name values ($1);");

    assert_eq!(
        statement,
        Ok(vec![Statement::Extended(Extended::Prepare {
            query: Query::Insert(InsertStatement {
                schema_name: "schema_name".to_owned(),
                table_name: "table_name".to_owned(),
                columns: vec![],
                source: InsertSource::Values(Values(vec![vec![Expr::Param(1)]]))
            }),
            name: "foo_plan".to_owned(),
            param_types: vec![DataType::SmallInt]
        })])
    );
}

#[test]
fn deallocate() {
    let statement = QUERY_PARSER.parse("deallocate foo_plan;");

    assert_eq!(
        statement,
        Ok(vec![Statement::Extended(Extended::Deallocate {
            name: "foo_plan".to_owned()
        })])
    );
}

#[test]
fn execute() {
    let statement = QUERY_PARSER.parse("execute foo_plan(123)");

    assert_eq!(
        statement,
        Ok(vec![Statement::Extended(Extended::Execute {
            name: "foo_plan".to_owned(),
            param_values: vec![Value::Int(123)]
        })])
    )
}
