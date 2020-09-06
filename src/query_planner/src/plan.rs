// Copyright 2020 Alex Dukhno
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

///! represents a plan to be executed by the engine.
use sqlparser::ast::{Assignment, Ident, Query, Statement};

use crate::{FullTableName, SchemaName};
use data_manager::ColumnDefinition;

///! represents a plan to be executed by the engine.
use crate::{SchemaId, TableId};
use sql_model::Id;

#[derive(Debug, Clone)]
pub struct TableCreationInfo {
    pub schema_id: Id,
    pub table_name: String,
    pub columns: Vec<ColumnDefinition>,
}

impl TableCreationInfo {
    pub(crate) fn new<S: ToString>(schema_id: Id, table_name: S, columns: Vec<ColumnDefinition>) -> TableCreationInfo {
        TableCreationInfo {
            schema_id,
            table_name: table_name.to_string(),
            columns,
        }
    }

    pub fn as_tuple(&self) -> (Id, &str, &[ColumnDefinition]) {
        (self.schema_id, self.table_name.as_str(), self.columns.as_slice())
    }
}

#[derive(Debug, Clone)]
pub struct SchemaCreationInfo {
    pub schema_name: String,
}

#[derive(Debug, Clone)]
pub struct TableInserts {
    pub full_table_name: TableId,
    pub column_indices: Vec<Ident>,
    pub input: Box<Query>,
}

#[derive(Debug, Clone)]
pub struct TableUpdates {
    pub full_table_name: TableId,
    pub assignments: Vec<Assignment>,
}

#[derive(Debug, Clone)]
pub struct TableDeletes {
    pub full_table_name: TableId,
}

#[derive(Debug, Clone)]
pub struct SelectInput {
    pub table_id: TableId,
    pub selected_columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum Plan {
    CreateTable(TableCreationInfo),
    CreateSchema(SchemaCreationInfo),
    DropTables(Vec<TableId>),
    DropSchemas(Vec<(SchemaId, bool)>),
    Select(SelectInput),
    Update(TableUpdates),
    Delete(TableDeletes),
    Insert(TableInserts),
    NotProcessed(Box<Statement>),
}