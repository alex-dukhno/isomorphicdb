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

mod in_memory;
mod on_disk;
mod sql;

use binary::Binary;
use std::fmt::{self, Debug, Formatter};
use std::io;
use std::iter::FromIterator;

use definition_operations::{ExecutionError, ExecutionOutcome, SystemOperation};
pub use in_memory::InMemoryCatalogHandle;
pub use on_disk::OnDiskCatalogHandle;
pub use sql::in_memory::InMemoryDatabase;
pub use sql::on_disk::OnDiskDatabase;

pub type Key = Binary;
pub type Value = Binary;

const DEFINITION_SCHEMA: &str = "DEFINITION_SCHEMA";
const SCHEMATA_TABLE: &str = "SCHEMATA";
const TABLES_TABLE: &str = "TABLES";
const COLUMNS_TABLE: &str = "COLUMNS";

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

pub trait DataTable {
    fn read(&self) -> Cursor;
    fn insert(&self, data: Vec<Value>) -> usize;
    fn update(&self, data: Vec<(Key, Value)>) -> usize;
    fn delete(&self, data: Vec<Key>) -> usize;
}

pub trait SchemaHandle {
    type Table: DataTable;
    fn create_table(&self, table_name: &str) -> bool;
    fn drop_table(&self, table_name: &str) -> bool;
    fn work_with<T, F: Fn(&Self::Table) -> T>(&self, table_name: &str, operation: F) -> Option<T>;
}

pub trait DataCatalog {
    type Schema: SchemaHandle;
    fn create_schema(&self, schema_name: &str) -> bool;
    fn drop_schema(&self, schema_name: &str) -> bool;
    fn work_with<T, F: Fn(&Self::Schema) -> T>(&self, schema_name: &str, operation: F) -> Option<T>;
}

pub trait SqlTable {}

pub trait SqlSchema {}

pub trait Database {
    type Schema: SqlSchema;
    type Table: SqlTable;

    fn execute(&self, operation: SystemOperation) -> Result<ExecutionOutcome, ExecutionError>;
}
