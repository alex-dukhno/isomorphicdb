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
use data_manipulation_operators::{
    BiArithmetic, BiLogical, BiOperation, Bitwise, Comparison, PatternMatching, StringOp, UnArithmetic, UnBitwise,
    UnLogical, UnOperation,
};
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

fn create_schema_ops(schema_name: &str) -> SchemaChange {
    SchemaChange::CreateSchema(CreateSchemaQuery {
        schema_name: SchemaName::from(&schema_name),
        if_not_exists: false,
    })
}

fn create_table_ops(schema_name: &str, table_name: &str, columns: Vec<(&str, SqlType)>) -> SchemaChange {
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
