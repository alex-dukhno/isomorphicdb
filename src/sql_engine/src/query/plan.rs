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
use crate::query::{SchemaId, TableId};
use sqlparser::ast::{Ident, Query, Statement};
use storage::ColumnDefinition;

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
    pub table_id: TableId,
    pub column_indices: Vec<Ident>,
    pub input: Box<Query>,
}

#[derive(Debug, Clone)]
pub enum Plan {
    CreateTable(TableCreationInfo),
    CreateSchema(SchemaCreationInfo),
    DropTables(Vec<TableId>),
    DropSchemas(Vec<SchemaId>),
    Insert(TableInserts),
    NotProcessed(Box<Statement>),
}
