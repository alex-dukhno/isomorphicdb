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
extern crate sql_types;

use kernel::{SystemError, SystemResult};
use representation::Binary;
use serde::{Deserialize, Serialize};
use sled::{Db as Namespace, Error as SledError, Error};
use sql_types::{ConstraintError, SqlType};
use std::{collections::HashMap, path::PathBuf, sync::RwLock};

pub mod backend;
pub mod frontend;

pub type Projection = (Vec<ColumnDefinition>, Vec<Vec<String>>);
pub type Row = (Key, Values);
pub type Key = Binary;
pub type Values = Binary;
pub type ReadCursor = Box<dyn Iterator<Item = Result<Row, SystemError>>>;
pub type NewReadCursor = Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), StorageError>>>;

use std::fmt::{self, Display, Formatter};

pub type StorageResult<T> = std::result::Result<T, StorageError>;

#[derive(Debug)]
pub struct StorageError {
    message: String,
    backtrace: backtrace::Backtrace,
    kind: StorageErrorKind,
}

impl StorageError {
    pub fn bug(message: String) -> StorageError {
        StorageError {
            message,
            backtrace: backtrace::Backtrace::new(),
            kind: StorageErrorKind::Bug,
        }
    }

    pub fn unrecoverable(message: String) -> StorageError {
        StorageError {
            message,
            backtrace: backtrace::Backtrace::new(),
            kind: StorageErrorKind::Unrecoverable,
        }
    }

    pub fn io(io_error: std::io::Error) -> StorageError {
        StorageError {
            message: "IO error has happened".to_owned(),
            backtrace: backtrace::Backtrace::new(),
            kind: StorageErrorKind::Io(io_error),
        }
    }
}

impl PartialEq for StorageError {
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message && self.kind == other.kind
    }
}

#[derive(Debug)]
pub enum StorageErrorKind {
    Unrecoverable,
    Bug,
    Io(std::io::Error),
}

impl PartialEq for StorageErrorKind {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (StorageErrorKind::Io(_), StorageErrorKind::Io(_)) => true,
            (StorageErrorKind::Unrecoverable, StorageErrorKind::Unrecoverable) => true,
            (StorageErrorKind::Bug, StorageErrorKind::Bug) => true,
            _ => false,
        }
    }
}

pub trait KeyValueStorage {
    fn create_key_space(&self, key_space_name: &str) -> StorageResult<()>;

    fn scan_key_space(&self, key_space_name: &str) -> StorageResult<NewReadCursor>;

    fn write_to_key_space(&self, key_space_name: &str, key: &[u8], values: &[u8]) -> StorageResult<()>;

    fn delete_from_key_space(&self, key_space_name: &str, key: &[u8]) -> StorageResult<()>;
}

pub struct SledKeyValueStorage {
    namespace: Namespace,
}

unsafe impl Send for SledKeyValueStorage {}

unsafe impl Sync for SledKeyValueStorage {}

impl SledKeyValueStorage {
    pub fn init(path: PathBuf) -> StorageResult<(SledKeyValueStorage, bool)> {
        log::info!("initializing on-disk storage under [{:?}] folder", path);
        let namespace = match sled::open(path) {
            Ok(namespace) => namespace,
            Err(error) => return Err(error.into()),
        };
        let was_recovered = namespace.was_recovered();
        if was_recovered {
            log::info!("on-disk storage recovered DEFINITION_KEY_SPACE from previous start");
        } else {
            log::info!("on-disk storage initialize new DEFINITION_KEY_SPACE");
        }

        Ok((SledKeyValueStorage { namespace }, !was_recovered))
    }
}

impl KeyValueStorage for SledKeyValueStorage {
    fn create_key_space(&self, key_space_name: &str) -> StorageResult<()> {
        match self.namespace.open_tree(key_space_name) {
            Ok(_tree) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn scan_key_space(&self, key_space_name: &str) -> StorageResult<NewReadCursor> {
        match self.namespace.open_tree(key_space_name) {
            Ok(tree) => Ok(Box::new(tree.iter().map(|row| {
                row.map(|(key, values)| (key.to_vec(), values.to_vec()))
                    .map_err(|error| error.into())
            }))),
            Err(error) => Err(error.into()),
        }
    }

    fn write_to_key_space(&self, key_space_name: &str, key: &[u8], values: &[u8]) -> StorageResult<()> {
        match self.namespace.open_tree(key_space_name) {
            Ok(tree) => match tree.insert(key, values) {
                Ok(None) => Ok(()),
                Ok(Some(prev_value)) => Err(StorageError::bug(format!(
                    "Previous value {} already were present in {} while trying to insert key {:?} with {:?} value",
                    std::str::from_utf8(&prev_value).unwrap(),
                    key_space_name,
                    key,
                    values
                ))),
                Err(error) => Err(error.into()),
            },
            Err(error) => Err(error.into()),
        }
    }

    fn delete_from_key_space(&self, key_space_name: &str, key: &[u8]) -> StorageResult<()> {
        match self.namespace.open_tree(key_space_name) {
            Ok(tree) => match tree.remove(key) {
                Ok(Some(_)) => Ok(()),
                Ok(None) => Err(StorageError::bug(format!(
                    "there is no record about object with {:?} key in {:?}",
                    key, key_space_name
                ))),
                Err(error) => Err(error.into()),
            },
            Err(error) => Err(error.into()),
        }
    }
}

impl Into<StorageError> for SledError {
    fn into(self) -> StorageError {
        match self {
            SledError::CollectionNotFound(system_file) => StorageError::unrecoverable(format!(
                "System file [{}] can't be found",
                String::from_utf8(system_file.to_vec()).expect("name of system file")
            )),
            SledError::Unsupported(operation) => {
                StorageError::unrecoverable(format!("Unsupported operation [{}] was used on Sled", operation))
            }
            SledError::Corruption { at, bt: _bt } => {
                if let Some(at) = at {
                    StorageError::unrecoverable(format!("Sled encountered corruption at {}", at))
                } else {
                    StorageError::unrecoverable("Sled encountered corruption".to_owned())
                }
            }
            SledError::ReportableBug(description) => {
                StorageError::unrecoverable(format!("Sled encountered reportable BUG: {}", description))
            }
            SledError::Io(error) => StorageError::io(error),
        }
    }
}

#[cfg(test)]
mod sled_error_mapping {
    use super::*;
    use assert2::assert;
    use sled::DiskPtr;
    use std::io::{Error, ErrorKind};

    #[test]
    fn collection_not_found() {
        let error: StorageError = SledError::CollectionNotFound(sled::IVec::from("test")).into();
        assert!(error == StorageError::unrecoverable("System file [test] can't be found".to_owned()))
    }

    #[test]
    fn unsupported() {
        let error: StorageError = SledError::Unsupported("NOT_SUPPORTED".to_owned()).into();
        assert!(
            error == StorageError::unrecoverable("Unsupported operation [NOT_SUPPORTED] was used on Sled".to_owned())
        )
    }

    #[test]
    fn corruption_with_position() {
        let at = DiskPtr::Inline(900);
        let error: StorageError = SledError::Corruption { at: Some(at), bt: () }.into();
        assert!(error == StorageError::unrecoverable(format!("Sled encountered corruption at {}", at)))
    }

    #[test]
    fn corruption_without_position() {
        let error: StorageError = SledError::Corruption { at: None, bt: () }.into();
        assert!(error == StorageError::unrecoverable("Sled encountered corruption".to_owned()))
    }

    #[test]
    fn reportable_bug() {
        let description = "SOME_BUG_HERE";
        let error: StorageError = SledError::ReportableBug(description.to_owned()).into();
        assert!(error == StorageError::unrecoverable(format!("Sled encountered reportable BUG: {}", description)));
    }

    #[test]
    fn io() {
        let error: StorageError = SledError::Io(Error::new(ErrorKind::Other, "oh no!")).into();
        assert!(error == StorageError::io(Error::new(ErrorKind::Other, "oh no!")))
    }
}

#[derive(Debug, PartialEq)]
pub struct SchemaAlreadyExists;

#[derive(Debug, PartialEq)]
pub struct SchemaDoesNotExist;

#[derive(Debug, PartialEq)]
pub enum CreateTableError {
    SchemaDoesNotExist,
    TableAlreadyExists,
}

#[derive(Debug, PartialEq)]
pub enum DropTableError {
    SchemaDoesNotExist,
    TableDoesNotExist,
}

#[derive(Debug, PartialEq)]
pub enum OperationOnTableError {
    SchemaDoesNotExist,
    TableDoesNotExist,
    InsertTooManyExpressions,
    // Returns non existing columns.
    ColumnDoesNotExist(Vec<String>),
    // Returns vector of (error, column) and a row index.
    ConstraintViolations(Vec<(ConstraintError, ColumnDefinition)>, usize),
}

#[derive(Debug, Clone)]
pub struct TableDescription {
    schema_name: String,
    table_name: String,
    column_data: Vec<ColumnDefinition>,
}

impl TableDescription {
    pub fn new(schema_name: &str, table_name: &str, column_data: Vec<ColumnDefinition>) -> Self {
        Self {
            schema_name: schema_name.to_owned(),
            table_name: table_name.to_owned(),
            column_data,
        }
    }

    pub fn column_len(&self) -> usize {
        self.column_data.len()
    }

    pub fn column_type(&self, column_idx: usize) -> SqlType {
        if let Some(column) = self.column_data.get(column_idx) {
            column.sql_type
        } else {
            panic!("attempting to access type of invalid column index")
        }
    }

    pub fn column_type_by_name(&self, name: &str) -> Option<SqlType> {
        self.column_data
            .iter()
            .find(|column| column.name == name)
            .map(|column| column.sql_type)
    }

    pub fn column_data(&self) -> &[ColumnDefinition] {
        self.column_data.as_slice()
    }

    pub fn scheme(&self) -> &str {
        self.schema_name.as_str()
    }

    pub fn table(&self) -> &str {
        self.table_name.as_str()
    }

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.schema_name, self.table_name)
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    name: String,
    sql_type: SqlType,
}

impl ColumnDefinition {
    pub fn new(name: &str, sql_type: SqlType) -> Self {
        Self {
            name: name.to_string(),
            sql_type,
        }
    }

    pub fn sql_type(&self) -> SqlType {
        self.sql_type
    }

    pub fn has_name(&self, other_name: &str) -> bool {
        self.name == other_name
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }
}
