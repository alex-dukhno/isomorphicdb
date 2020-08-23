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

use crate::{catalog_manager::data_definition::DataDefinition, ColumnDefinition, TableDefinition};
use kernel::{Object, Operation, SystemError, SystemResult};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use storage::{InMemoryDatabaseCatalog, PersistentDatabaseCatalog, ReadCursor, Row, Storage, StorageError};

mod data_definition;

pub struct CatalogManager {
    key_id_generator: AtomicU64,
    data_storage: Box<dyn Storage>,
    data_definition: DataDefinition,
}

impl Default for CatalogManager {
    fn default() -> CatalogManager {
        Self::in_memory().expect("no errors")
    }
}

unsafe impl Send for CatalogManager {}
unsafe impl Sync for CatalogManager {}

const DEFAULT_CATALOG: &'_ str = "public";

impl CatalogManager {
    pub fn in_memory() -> SystemResult<CatalogManager> {
        let data_definition = DataDefinition::in_memory();
        data_definition.create_catalog(DEFAULT_CATALOG);
        Ok(Self {
            key_id_generator: AtomicU64::default(),
            data_storage: Box::new(InMemoryDatabaseCatalog::default()),
            data_definition,
        })
    }

    pub fn persistent(path: PathBuf) -> SystemResult<CatalogManager> {
        let data_definition = DataDefinition::persistent(&path)?;
        data_definition.create_catalog(DEFAULT_CATALOG);
        Ok(Self {
            key_id_generator: AtomicU64::default(),
            data_storage: Box::new(PersistentDatabaseCatalog::new(path)),
            data_definition,
        })
    }

    pub fn next_key_id(&self) -> u64 {
        self.key_id_generator.fetch_add(1, Ordering::SeqCst)
    }

    pub fn table_descriptor(&self, schema_name: &str, table_name: &str) -> SystemResult<TableDefinition> {
        if self.table_exists(schema_name, table_name) {
            // we know the table exists
            let columns_metadata = self.table_columns(schema_name, table_name)?;

            Ok(TableDefinition::new(schema_name, table_name, columns_metadata))
        } else {
            Err(SystemError::bug_in_sql_engine(
                Operation::Access,
                Object::Table(schema_name, table_name),
            ))
        }
    }

    pub fn create_schema(&self, schema_name: &str) -> SystemResult<()> {
        self.data_definition.create_schema(DEFAULT_CATALOG, schema_name);
        match self.data_storage.create_namespace(schema_name) {
            Ok(()) => Ok(()),
            Err(StorageError::SystemError(error)) => Err(error),
            Err(StorageError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Create,
                Object::Schema(schema_name),
            )),
        }
    }

    pub fn drop_schema(&self, schema_name: &str) -> SystemResult<()> {
        self.data_definition.drop_schema(DEFAULT_CATALOG, schema_name);
        match self.data_storage.drop_namespace(schema_name) {
            Ok(()) => Ok(()),
            Err(StorageError::SystemError(error)) => Err(error),
            Err(StorageError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Drop,
                Object::Schema(schema_name),
            )),
        }
    }

    pub fn create_table(
        &self,
        schema_name: &str,
        table_name: &str,
        column_definitions: &[ColumnDefinition],
    ) -> SystemResult<()> {
        self.data_definition
            .create_table(DEFAULT_CATALOG, schema_name, table_name, column_definitions);
        match self.data_storage.create_tree(schema_name, table_name) {
            Ok(()) => Ok(()),
            Err(StorageError::SystemError(error)) => Err(error),
            Err(StorageError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Create,
                Object::Table(schema_name, table_name),
            )),
        }
    }

    pub fn table_columns(&self, schema_name: &str, table_name: &str) -> SystemResult<Vec<ColumnDefinition>> {
        Ok(self
            .data_definition
            .table_columns(DEFAULT_CATALOG, schema_name, table_name))
    }

    pub fn drop_table(&self, schema_name: &str, table_name: &str) -> SystemResult<()> {
        self.data_definition
            .drop_table(DEFAULT_CATALOG, schema_name, table_name);
        match self.data_storage.drop_tree(schema_name, table_name) {
            Ok(()) => Ok(()),
            Err(StorageError::SystemError(error)) => Err(error),
            Err(StorageError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Drop,
                Object::Table(schema_name, table_name),
            )),
        }
    }

    pub fn insert_into(&self, schema_name: &str, table_name: &str, values: Vec<Row>) -> SystemResult<usize> {
        log::debug!("{:#?}", values);
        match self.data_storage.write(schema_name, table_name, values) {
            Ok(size) => Ok(size),
            Err(StorageError::SystemError(error)) => Err(error),
            Err(StorageError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Access,
                Object::Table(schema_name, table_name),
            )),
        }
    }

    pub fn table_scan(&self, schema_name: &str, table_name: &str) -> SystemResult<ReadCursor> {
        match self.data_storage.read(schema_name, table_name) {
            Ok(read) => Ok(read),
            Err(StorageError::SystemError(error)) => Err(error),
            Err(StorageError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Access,
                Object::Table(schema_name, table_name),
            )),
        }
    }

    pub fn update_all(&self, schema_name: &str, table_name: &str, rows: Vec<Row>) -> SystemResult<usize> {
        match self.data_storage.write(schema_name, table_name, rows) {
            Ok(size) => Ok(size),
            Err(StorageError::SystemError(error)) => Err(error),
            Err(StorageError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Access,
                Object::Table(schema_name, table_name),
            )),
        }
    }

    pub fn delete_all_from(&self, schema_name: &str, table_name: &str) -> SystemResult<usize> {
        match self.data_storage.read(schema_name, table_name) {
            Ok(reads) => {
                let keys = reads.map(Result::unwrap).map(|(key, _)| key).collect();
                match self.data_storage.delete(schema_name, table_name, keys) {
                    Ok(len) => Ok(len),
                    _ => unreachable!(
                        "all errors that make code fall in here should have been handled in read operation"
                    ),
                }
            }
            Err(StorageError::SystemError(error)) => Err(error),
            Err(StorageError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Access,
                Object::Table(schema_name, table_name),
            )),
        }
    }

    pub fn schema_exists(&self, schema_name: &str) -> bool {
        matches!(
            self.data_definition.schema_exists(DEFAULT_CATALOG, schema_name),
            Some((_, Some(_)))
        )
    }

    pub fn table_exists(&self, schema_name: &str, table_name: &str) -> bool {
        matches!(
            self.data_definition
                .table_exists(DEFAULT_CATALOG, schema_name, table_name),
            Some((_, Some((_, Some(_)))))
        )
    }
}

#[cfg(test)]
mod tests;
