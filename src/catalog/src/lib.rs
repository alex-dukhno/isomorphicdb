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

use binary::Binary;
use std::fmt::{self, Debug, Formatter};
use std::io;
use std::iter::FromIterator;

pub use in_memory::InMemoryCatalogHandle;
pub use on_disk::OnDiskCatalogHandle;
use std::ops::Deref;
use std::sync::Arc;

pub type Key = Binary;
pub type Value = Binary;

#[derive(Debug, PartialEq)]
pub struct StorageError;

pub struct Cursor {
    source: Box<dyn Iterator<Item = io::Result<Result<(Binary, Binary), StorageError>>>>,
}

impl Debug for Cursor {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Data Cursor")
    }
}

impl FromIterator<io::Result<Result<(Binary, Binary), StorageError>>> for Cursor {
    fn from_iter<T: IntoIterator<Item = io::Result<Result<(Binary, Binary), StorageError>>>>(iter: T) -> Self {
        Self {
            source: Box::new(
                iter.into_iter()
                    .collect::<Vec<io::Result<Result<(Binary, Binary), StorageError>>>>()
                    .into_iter(),
            ),
        }
    }
}

impl FromIterator<(Binary, Binary)> for Cursor {
    fn from_iter<T: IntoIterator<Item = (Binary, Binary)>>(iter: T) -> Self {
        Self {
            source: Box::new(
                iter.into_iter()
                    .map(|item| Ok(Ok(item)))
                    .collect::<Vec<io::Result<Result<(Binary, Binary), StorageError>>>>()
                    .into_iter(),
            ),
        }
    }
}

impl Iterator for Cursor {
    type Item = io::Result<Result<(Binary, Binary), StorageError>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.source.next()
    }
}

pub trait DataTable {
    fn scan(&self) -> Cursor;
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

pub trait Database {
    type Table;
    type Schema;

    fn new(name: &str) -> Arc<Self>;
}

pub struct OnDiskDatabase;

impl Database for OnDiskDatabase {
    type Table = ();
    type Schema = ();

    fn new(name: &str) -> Arc<Self> {
        Arc::new(OnDiskDatabase)
    }
}

pub struct InMemoryDatabase;

impl Database for InMemoryDatabase {
    type Table = ();
    type Schema = ();

    fn new(name: &str) -> Arc<Self> {
        Arc::new(InMemoryDatabase)
    }
}

#[derive(Clone)]
pub struct DatabaseHandleNew {
    inner: DatabaseHandleInner,
}

#[derive(Clone)]
enum DatabaseHandleInner {
    InMemory(Arc<InMemoryDatabase>),
    OnDisk(Arc<OnDiskDatabase>),
}

impl DatabaseHandleNew {
    pub fn in_memory() -> DatabaseHandleNew {
        DatabaseHandleNew {
            inner: DatabaseHandleInner::InMemory(InMemoryDatabase::new("in-memory")),
        }
    }

    pub fn persistent(path_to_database: &str) -> DatabaseHandleNew {
        DatabaseHandleNew {
            inner: DatabaseHandleInner::OnDisk(OnDiskDatabase::new(path_to_database)),
        }
    }
}
