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

use bigdecimal::BigDecimal;

use catalog::{Database, InMemoryDatabase};
use data_definition_operations::{Kind, Record, Step, SystemObject, SystemOperation};
use data_manipulation_operators::{Arithmetic, Bitwise, Comparison, Logical, Operation, PatternMatching, StringOp};
use data_manipulation_untyped_tree::Bool;

use super::*;

#[cfg(test)]
mod ddl;
#[cfg(test)]
mod delete;
#[cfg(test)]
mod insertions;
#[cfg(test)]
mod operation_mapping;
#[cfg(test)]
mod selects;
#[cfg(test)]
mod updates;

const SCHEMA: &str = "schema_name";
const TABLE: &str = "table_name";

fn ident<S: ToString>(name: S) -> sql_ast::Ident {
    sql_ast::Ident {
        value: name.to_string(),
        quote_style: None,
    }
}

fn string(value: &'static str) -> sql_ast::Expr {
    sql_ast::Expr::Value(sql_ast::Value::SingleQuotedString(value.to_owned()))
}

fn null() -> sql_ast::Expr {
    sql_ast::Expr::Value(sql_ast::Value::Null)
}

fn boolean(value: bool) -> sql_ast::Expr {
    sql_ast::Expr::Value(sql_ast::Value::Boolean(value))
}

fn number(value: i16) -> sql_ast::Value {
    sql_ast::Value::Number(BigDecimal::from(value))
}

fn create_schema_ops(schema_name: &str) -> SystemOperation {
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

fn create_table_ops(schema_name: &str, table_name: &str, columns: Vec<(&str, SqlType)>) -> SystemOperation {
    let column_steps: Vec<Step> = columns
        .into_iter()
        .map(|(column_name, column_type)| Step::CreateRecord {
            record: Record::Column {
                schema_name: schema_name.to_owned(),
                table_name: table_name.to_owned(),
                column_name: column_name.to_owned(),
                sql_type: column_type,
            },
        })
        .collect();
    let mut all_steps: Vec<Step> = vec![
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
    all_steps.extend(column_steps);
    SystemOperation {
        kind: Kind::Create(SystemObject::Table),
        skip_steps_if: None,
        steps: vec![all_steps],
    }
}
