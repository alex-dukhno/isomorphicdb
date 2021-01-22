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
use data_definition_operations::{Kind, Record, Step, SystemObject, SystemOperation};
use data_manipulation_untyped_tree::{ScalarValue, StaticEvaluationTree, StaticItem};

#[cfg(test)]
mod expressions;
#[cfg(test)]
mod general_cases;

fn small_int(value: i16) -> sql_ast::Expr {
    sql_ast::Expr::Value(number(value))
}

fn create_schema(schema_name: &str) -> SystemOperation {
    SystemOperation {
        kind: Kind::Create(SystemObject::Schema),
        skip_steps_if: None,
        steps: vec![vec![
            Step::CheckExistence {
                system_object: SystemObject::Schema,
                object_name: vec![schema_name.to_owned()],
            },
            Step::CreateFolder {
                name: schema_name.to_owned(),
            },
            Step::CreateRecord {
                record: Record::Schema {
                    schema_name: schema_name.to_owned(),
                },
            },
        ]],
    }
}

fn create_table(schema_name: &str, table_name: &str, columns: Vec<(&str, SqlType)>) -> SystemOperation {
    let columns_steps = columns
        .into_iter()
        .map(|(name, sql_type)| Step::CreateRecord {
            record: Record::Column {
                schema_name: schema_name.to_owned(),
                table_name: table_name.to_owned(),
                column_name: name.to_owned(),
                sql_type,
            },
        })
        .collect::<Vec<Step>>();
    let mut general_steps = vec![
        Step::CheckExistence {
            system_object: SystemObject::Schema,
            object_name: vec![schema_name.to_owned()],
        },
        Step::CheckExistence {
            system_object: SystemObject::Table,
            object_name: vec![schema_name.to_owned(), table_name.to_owned()],
        },
        Step::CreateFile {
            folder_name: schema_name.to_owned(),
            name: table_name.to_owned(),
        },
        Step::CreateRecord {
            record: Record::Table {
                schema_name: schema_name.to_owned(),
                table_name: table_name.to_owned(),
            },
        },
    ];
    general_steps.extend(columns_steps);
    SystemOperation {
        kind: Kind::Create(SystemObject::Table),
        skip_steps_if: None,
        steps: vec![general_steps],
    }
}

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
