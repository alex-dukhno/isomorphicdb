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

use definition::{FullTableName, SchemaName};
use types::SqlType;

#[derive(Debug, PartialEq)]
pub struct CreateSchemaQuery {
    pub schema_name: SchemaName,
    pub if_not_exists: bool,
}

#[derive(Debug, PartialEq)]
pub struct DropSchemasQuery {
    pub schema_names: Vec<SchemaName>,
    pub cascade: bool,
    pub if_exists: bool,
}

#[derive(Debug, PartialEq)]
pub struct ColumnInfo {
    pub name: String,
    pub sql_type: SqlType,
}

#[derive(Debug, PartialEq)]
pub struct CreateTableQuery {
    pub full_table_name: FullTableName,
    pub column_defs: Vec<ColumnInfo>,
    pub if_not_exists: bool,
}

#[derive(Debug, PartialEq)]
pub struct DropTablesQuery {
    pub full_table_names: Vec<FullTableName>,
    pub cascade: bool,
    pub if_exists: bool,
}

#[derive(Debug, PartialEq)]
pub struct ColumnDesc {
    pub name: String,
    pub sql_type: SqlType,
    pub ord_num: usize,
}

impl From<(String, SqlType, usize)> for ColumnDesc {
    fn from(tuple: (String, SqlType, usize)) -> ColumnDesc {
        let (name, sql_type, ord_num) = tuple;
        ColumnDesc {
            name,
            sql_type,
            ord_num,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct CreateIndexQuery {
    pub name: String,
    pub full_table_name: FullTableName,
    pub column_names: Vec<String>,
}

#[derive(Debug, PartialEq)]
pub enum SchemaChange {
    CreateSchema(CreateSchemaQuery),
    DropSchemas(DropSchemasQuery),
    CreateTable(CreateTableQuery),
    DropTables(DropTablesQuery),
    CreateIndex(CreateIndexQuery),
}

#[derive(Debug, PartialEq)]
pub enum ExecutionOutcome {
    SchemaCreated,
    SchemaDropped,
    TableCreated,
    TableDropped,
    IndexCreated
}

#[derive(Debug, PartialEq)]
pub enum ExecutionError {
    SchemaAlreadyExists(String),
    SchemaDoesNotExist(String),
    TableAlreadyExists(String, String),
    TableDoesNotExist(String, String),
    SchemaHasDependentObjects(String),
    ColumnNotFound(String),
}
