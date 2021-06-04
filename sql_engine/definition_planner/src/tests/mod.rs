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
use data_definition_execution_plan::{ColumnInfo, CreateSchemaQuery, CreateTableQuery, DropSchemasQuery, SchemaChange};
use storage::Database;

#[cfg(test)]
mod create_index;
#[cfg(test)]
mod create_schema;
#[cfg(test)]
mod create_table;
#[cfg(test)]
mod drop_schema;
#[cfg(test)]
mod drop_table;

const SCHEMA: &str = "schema_name";
const TABLE: &str = "table_name";

fn create_table_if_not_exists(schema_name: &str, table_name: &str, columns: Vec<ColumnDef>, if_not_exists: bool) -> Definition {
    Definition::CreateTable {
        schema_name: schema_name.to_owned(),
        table_name: table_name.to_owned(),
        columns,
        if_not_exists,
    }
}

fn create_table(schema_name: &str, table_name: &str, columns: Vec<ColumnDef>) -> Definition {
    create_table_if_not_exists(schema_name, table_name, columns, false)
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

fn create_schema_ops(schema_name: &str) -> SchemaChange {
    SchemaChange::CreateSchema(CreateSchemaQuery {
        schema_name: SchemaName::from(&schema_name),
        if_not_exists: false,
    })
}
