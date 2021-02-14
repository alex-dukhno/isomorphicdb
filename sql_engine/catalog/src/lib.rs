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

use data_binary::Binary;
use data_definition_execution_plan::{ExecutionError, ExecutionOutcome, SchemaChange};
use data_manipulation_query_result::QueryExecutionError;
use data_manipulation_typed_tree::{DynamicTypedTree, StaticTypedTree};
use data_scalar::ScalarValue;
use definition::{ColumnDef, FullIndexName, FullTableName, SchemaName, TableDef};
use std::{
    fmt::{self, Debug, Formatter},
    iter::FromIterator,
};

pub use in_memory::{InMemoryDatabase, InMemoryTable};
use types::{SqlType, SqlTypeFamily};

mod in_memory;

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
    fn insert(&self, data: Vec<Value>) -> Vec<Key>;
    fn update(&self, data: Vec<(Key, Value)>) -> usize;
    fn delete(&self, data: Vec<Key>) -> usize;
    fn next_column_ord(&self) -> u64;
    fn create_index(&self, index_name: &str, over_column: usize);
}

trait SchemaHandle {
    type Table: DataTable;
    fn create_table(&self, table_name: &str) -> bool;
    fn drop_table(&self, table_name: &str) -> bool;
    fn empty(&self) -> bool;
    fn all_tables(&self) -> Vec<String>;
    fn create_index(&self, table_name: &str, index_name: &str, column_index: usize) -> bool;
    fn work_with<T, F: Fn(&Self::Table) -> T>(&self, table_name: &str, operation: F) -> Option<T>;
}

trait DataCatalog {
    type Schema: SchemaHandle;
    fn create_schema(&self, schema_name: &str) -> bool;
    fn drop_schema(&self, schema_name: &str) -> bool;
    fn work_with<T, F: Fn(&Self::Schema) -> T>(&self, schema_name: &str, operation: F) -> Option<T>;
}

pub trait CatalogDefinition {
    fn table_definition(&self, table_full_name: FullTableName) -> Option<Option<TableDef>>;

    fn schema_exists(&self, schema_name: &SchemaName) -> bool;
}

const DEFINITION_SCHEMA: &str = "DEFINITION_SCHEMA";
const SCHEMATA_TABLE: &str = "SCHEMATA";
const TABLES_TABLE: &str = "TABLES";
const INDEXES_TABLE: &str = "TABLES";
const COLUMNS_TABLE: &str = "COLUMNS";

pub trait SqlTable {
    fn columns(&self) -> Vec<(String, SqlTypeFamily)>;
    fn columns_short(&self) -> Vec<(String, SqlType)>;
    fn write(&self, row: Binary);
    fn write_key(&self, key: Binary, row: Option<Binary>);
    fn scan(&self) -> Cursor;

    fn insert(&self, data: Vec<Vec<Option<StaticTypedTree>>>) -> Result<usize, QueryExecutionError>;

    fn select(&self, column_names: Vec<String>)
        -> Result<(Vec<ColumnDef>, Vec<Vec<ScalarValue>>), QueryExecutionError>;

    fn delete_all(&self) -> usize;

    fn update(&self, assignments: Vec<Option<DynamicTypedTree>>) -> Result<usize, QueryExecutionError>;
}

pub trait Database {
    type Table: SqlTable;
    type Index;

    fn execute(&self, schema_change: SchemaChange) -> Result<ExecutionOutcome, ExecutionError>;

    fn table(&self, full_table_name: &FullTableName) -> Box<dyn SqlTable>;
    fn work_with<R, F: Fn(&Self::Table) -> R>(&self, full_table_name: &FullTableName, operation: F) -> R;
    fn work_with_index<R, F: Fn(&Self::Index) -> R>(&self, full_index_name: FullIndexName, operation: F) -> R;
}
