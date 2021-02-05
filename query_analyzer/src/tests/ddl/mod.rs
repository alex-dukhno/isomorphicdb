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
use data_definition_execution_plan::{ColumnInfo, CreateSchemaQuery, CreateTableQuery, DropSchemasQuery, SchemaChange};

#[cfg(test)]
mod create_index;
#[cfg(test)]
mod create_schema;
#[cfg(test)]
mod create_table;
#[cfg(test)]
mod drop_statements;

fn create_table_if_not_exists(
    name: Vec<&str>,
    columns: Vec<sql_ast::ColumnDef>,
    if_not_exists: bool,
) -> sql_ast::Statement {
    sql_ast::Statement::CreateTable {
        or_replace: false,
        name: sql_ast::ObjectName(name.into_iter().map(ident).collect()),
        columns,
        constraints: vec![],
        with_options: vec![],
        if_not_exists,
        external: false,
        file_format: None,
        location: None,
        query: None,
        without_rowid: false,
    }
}

fn create_table(name: Vec<&str>, columns: Vec<sql_ast::ColumnDef>) -> sql_ast::Statement {
    create_table_if_not_exists(name, columns, false)
}
