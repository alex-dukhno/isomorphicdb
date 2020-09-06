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

use sqlparser::ast::{Assignment, Ident, Query, Statement};

use data_manager::ColumnDefinition;

///! represents a plan to be executed by the engine.
use crate::{FullTableName, SchemaName};

#[derive(Debug, Clone)]
pub struct TableCreationInfo {
    pub schema_name: String,
    pub table_name: String,
    pub columns: Vec<ColumnDefinition>, // pub table_constraints: Vec<TableConstraints> ??
}

#[derive(Debug, Clone)]
pub struct SchemaCreationInfo {
    pub schema_name: String,
}

#[derive(Debug, Clone)]
pub struct TableInserts {
    pub table_id: FullTableName,
    pub column_indices: Vec<Ident>,
    pub input: Box<Query>,
}

#[derive(Debug, Clone)]
pub struct TableUpdates {
    pub table_id: FullTableName,
    pub assignments: Vec<Assignment>,
}

#[derive(Debug, Clone)]
pub struct TableDeletes {
    pub table_id: FullTableName,
}

#[derive(Debug, Clone)]
pub struct SelectInput {
    pub schema_name: String,
    pub table_name: String,
    pub selected_columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum Plan {
    CreateTable(TableCreationInfo),
    CreateSchema(SchemaCreationInfo),
    DropTables(Vec<FullTableName>),
    DropSchemas(Vec<(SchemaName, bool)>),
    Select(SelectInput),
    Update(TableUpdates),
    Delete(TableDeletes),
    Insert(TableInserts),
    NotProcessed(Box<Statement>),
}
