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

use super::*;
use analysis_tree::{StaticEvaluationTree, DynamicEvaluationTree};
use bigdecimal::BigDecimal;
use catalog::{Database, InMemoryDatabase};
use data_manager::DatabaseHandle;
use expr_operators::{
    Arithmetic, Bitwise, Bool, Comparison, StaticItem, Logical, DynamicItem, Operation, PatternMatching, ScalarValue,
    StringOp,
};
use meta_def::{ColumnDefinition, Id};

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

fn with_table(columns: &[ColumnDefinition]) -> (Arc<DatabaseHandle>, Id, Id) {
    let data_manager = Arc::new(DatabaseHandle::in_memory());
    let schema_id = data_manager.create_schema(SCHEMA).expect("schema created");
    let table_id = data_manager
        .create_table(schema_id, TABLE, columns)
        .expect("table created");
    (data_manager, schema_id, table_id)
}
