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
use data_definition_execution_plan::{ColumnInfo, CreateSchemaQuery, CreateTableQuery, SchemaChange};
use data_manipulation_operators::{BiArithmetic, BiLogical, BiOperator, Bitwise, Comparison, Concat, Matching, UnOperator};
use definition::SchemaName;
use query_ast::{Assignment, BinaryOperator, DataType, Expr, Value};
use storage::Database;
use types_old::SqlTypeOld;

#[cfg(test)]
mod delete;
#[cfg(test)]
mod insertions;
#[cfg(test)]
mod selects;
#[cfg(test)]
mod updates;

const SCHEMA: &str = "schema_name";
const TABLE: &str = "table_name";

fn string(value: &'static str) -> Expr {
    Expr::Value(Value::String(value.to_owned()))
}

fn null() -> Expr {
    Expr::Value(Value::Null)
}

fn boolean(value: bool) -> Expr {
    Expr::Cast {
        expr: Box::new(Expr::Value(Value::String(value.to_string().chars().take(1).next().unwrap().to_string()))),
        data_type: DataType::Bool,
    }
}

fn number(value: i16) -> Value {
    Value::Int(value as i32)
}

fn create_schema_ops(schema_name: &str) -> SchemaChange {
    SchemaChange::CreateSchema(CreateSchemaQuery {
        schema_name: SchemaName::from(&schema_name),
        if_not_exists: false,
    })
}

fn create_table_ops(schema_name: &str, table_name: &str, columns: Vec<(&str, SqlTypeOld)>) -> SchemaChange {
    SchemaChange::CreateTable(CreateTableQuery {
        full_table_name: FullTableName::from((&schema_name, &table_name)),
        column_defs: columns
            .into_iter()
            .map(|(name, sql_type)| ColumnInfo {
                name: name.to_owned(),
                sql_type,
            })
            .collect(),
        if_not_exists: true,
    })
}
