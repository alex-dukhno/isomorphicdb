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

use kernel::SystemError;
use representation::Binary;
use std::io::{self};

pub type Row = (Key, Values);
pub type Key = Binary;
pub type Values = Binary;
pub type RowResult = io::Result<Result<Row, InnerStorageError>>;
pub type ReadCursor = Box<dyn Iterator<Item = RowResult>>;
pub type StorageResult<T> = Result<T, StorageError>;

mod in_memory;
mod persistent;

pub use crate::{in_memory::InMemoryDatabase, persistent::PersistentDatabase};

pub enum InitStatus {
    Created,
    Loaded,
}

#[derive(Debug, PartialEq)]
pub enum StorageError {
    RuntimeCheckError,
    SystemError(SystemError),
    SledError(sled::Error),
}

#[derive(Debug, PartialEq)]
pub enum InnerStorageError {
    Io,
    CascadeIo(Vec<String>),
    Storage,
    DefinitionChanged(Definition),
}

#[derive(Debug, PartialEq)]
pub enum DefinitionError {
    SchemaAlreadyExists,
    SchemaDoesNotExist,
    ObjectAlreadyExists,
    ObjectDoesNotExist,
}

#[derive(Debug, PartialEq)]
pub enum Definition {
    SchemaAlreadyExists,
    SchemaDoesNotExist,
    ObjectAlreadyExists,
    ObjectDoesNotExist,
}

pub trait Database {
    fn create_schema(&self, schema_name: &str) -> io::Result<Result<Result<(), DefinitionError>, InnerStorageError>>;

    fn drop_schema(&self, schema_name: &str) -> io::Result<Result<Result<(), DefinitionError>, InnerStorageError>>;

    fn create_object(
        &self,
        schema_name: &str,
        object_name: &str,
    ) -> io::Result<Result<Result<(), DefinitionError>, InnerStorageError>>;

    fn drop_object(
        &self,
        schema_name: &str,
        object_name: &str,
    ) -> io::Result<Result<Result<(), DefinitionError>, InnerStorageError>>;

    fn write(
        &self,
        schema_name: &str,
        object_name: &str,
        values: Vec<Row>,
    ) -> io::Result<Result<Result<usize, DefinitionError>, InnerStorageError>>;

    fn read(
        &self,
        schema_name: &str,
        object_name: &str,
    ) -> io::Result<Result<Result<ReadCursor, DefinitionError>, InnerStorageError>>;

    fn delete(
        &self,
        schema_name: &str,
        object_name: &str,
        keys: Vec<Key>,
    ) -> io::Result<Result<Result<usize, DefinitionError>, InnerStorageError>>;
}

#[cfg(test)]
mod tests;
