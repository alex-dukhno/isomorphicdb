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

mod in_memory;

use binary::Binary;
use data_definition_operations::{ExecutionError, ExecutionOutcome, SystemOperation};
use data_manipulation_typed_tree::{DynamicTypedTree, StaticTypedTree};
use definition::{ColumnDef, FullTableName, SchemaName, TableDef};
use repr::Datum;
use std::{
    fmt::{self, Debug, Formatter},
    iter::FromIterator,
};

pub use in_memory::InMemoryDatabase;

pub type Key = Binary;
pub type Value = Binary;

#[derive(Debug, PartialEq)]
pub struct StorageError;

pub struct Cursor {
    source: Box<dyn Iterator<Item = (Binary, Binary)>>,
}

impl Debug for Cursor {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Data Cursor")
    }
}

impl FromIterator<(Binary, Binary)> for Cursor {
    fn from_iter<T: IntoIterator<Item = (Binary, Binary)>>(iter: T) -> Self {
        Self {
            source: Box::new(iter.into_iter().collect::<Vec<(Binary, Binary)>>().into_iter()),
        }
    }
}

impl Iterator for Cursor {
    type Item = (Binary, Binary);

    fn next(&mut self) -> Option<Self::Item> {
        self.source.next()
    }
}

trait DataTable {
    fn select(&self) -> Cursor;
    fn insert(&self, data: Vec<Value>) -> usize;
    fn update(&self, data: Vec<(Key, Value)>) -> usize;
    fn delete(&self, data: Vec<Key>) -> usize;
    fn next_column_ord(&self) -> u64;
}

trait SchemaHandle {
    type Table: DataTable;
    fn create_table(&self, table_name: &str) -> bool;
    fn drop_table(&self, table_name: &str) -> bool;
    fn empty(&self) -> bool;
    fn all_tables(&self) -> Vec<String>;
    fn work_with<T, F: Fn(&Self::Table) -> T>(&self, table_name: &str, operation: F) -> Option<T>;
}

trait DataCatalog {
    type Schema: SchemaHandle;
    fn create_schema(&self, schema_name: &str) -> bool;
    fn drop_schema(&self, schema_name: &str) -> bool;
    fn work_with<T, F: Fn(&Self::Schema) -> T>(&self, schema_name: &str, operation: F) -> Option<T>;
}

pub trait CatalogDefinition {
    fn table_definition(&self, table_full_name: &FullTableName) -> Option<Option<TableDef>>;

    fn schema_exists(&self, schema_name: &SchemaName) -> bool;
}

const DEFINITION_SCHEMA: &str = "DEFINITION_SCHEMA";
const SCHEMATA_TABLE: &str = "SCHEMATA";
const TABLES_TABLE: &str = "TABLES";
const COLUMNS_TABLE: &str = "COLUMNS";

pub trait SqlTable {
    fn insert(&self, data: &[Vec<Option<StaticTypedTree>>]) -> usize;
    fn insert_with_columns(&self, column_names: Vec<String>, rows: Vec<Vec<Option<StaticTypedTree>>>) -> usize;

    fn select(&self) -> (Vec<ColumnDef>, Vec<Vec<Datum>>);
    fn select_with_columns(&self, column_names: Vec<String>) -> Result<(Vec<ColumnDef>, Vec<Vec<Datum>>), String>;

    fn delete_all(&self) -> usize;

    fn update(&self, column_names: Vec<String>, assignments: Vec<DynamicTypedTree>) -> usize;
}

pub trait Database {
    type Table: SqlTable;

    fn execute(&self, operation: SystemOperation) -> Result<ExecutionOutcome, ExecutionError>;

    fn work_with<R, F: Fn(&Self::Table) -> R>(&self, full_table_name: &FullTableName, operation: F) -> R;
}
