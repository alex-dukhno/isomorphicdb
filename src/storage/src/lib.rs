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

extern crate kernel;
extern crate log;

use kernel::{SystemError, SystemResult};
use representation::Binary;

pub type Row = (Key, Values);
pub type Key = Binary;
pub type Values = Binary;
pub type ReadCursor = Box<dyn Iterator<Item = SystemResult<Row>>>;
pub type StorageResult<T> = std::result::Result<T, StorageError>;

mod in_memory;
mod persistent;

pub use crate::{in_memory::InMemoryDatabaseCatalog, persistent::PersistentDatabaseCatalog};

pub enum InitStatus {
    Created,
    Loaded,
}

#[derive(Debug, PartialEq)]
pub enum StorageError {
    RuntimeCheckError,
    SystemError(SystemError),
}

pub trait Storage {
    fn create_namespace(&self, namespace: &str) -> StorageResult<()>;

    fn drop_namespace(&self, namespace: &str) -> StorageResult<()>;

    fn create_tree(&self, namespace: &str, object_name: &str) -> StorageResult<()>;

    fn drop_tree(&self, namespace: &str, object_name: &str) -> StorageResult<()>;

    fn write(&self, namespace: &str, object_name: &str, values: Vec<Row>) -> StorageResult<usize>;

    fn read(&self, namespace: &str, object_name: &str) -> StorageResult<ReadCursor>;

    fn delete(&self, namespace: &str, object_name: &str, keys: Vec<Key>) -> StorageResult<usize>;
}

#[cfg(test)]
mod tests;
