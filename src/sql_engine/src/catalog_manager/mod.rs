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

use crate::{catalog_manager::metadata::DataDefinition, ColumnDefinition, TableDefinition};
use kernel::{Object, Operation, SystemError, SystemResult};
use storage::{DatabaseCatalog, ReadCursor, Row, SledDatabaseCatalog, StorageError};

mod metadata;

pub struct CatalogManager {
    key_id_generator: usize,
    persistent: Box<dyn DatabaseCatalog>,
    data_definition: DataDefinition,
}

impl CatalogManager {
    pub fn default() -> SystemResult<Self> {
        Self::new(Box::new(SledDatabaseCatalog::default()))
    }
}

unsafe impl Send for CatalogManager {}
unsafe impl Sync for CatalogManager {}

const DEFAULT_CATALOG: &'_ str = "public";

impl CatalogManager {
    pub fn new(persistent: Box<dyn DatabaseCatalog>) -> SystemResult<Self> {
        match persistent.create_namespace_with_objects("system", vec!["columns"]) {
            Ok(()) => {
                let definition = DataDefinition::in_memory();
                definition.create_catalog(DEFAULT_CATALOG);
                Ok(Self {
                    key_id_generator: 0,
                    persistent,
                    data_definition: definition,
                })
            }
            Err(StorageError::SystemError(e)) => Err(e),
            Err(StorageError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Create,
                Object::Schema("system"),
            )),
        }
    }

    pub fn next_key_id(&mut self) -> usize {
        let key_id = self.key_id_generator;
        self.key_id_generator += 1;
        key_id
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

    pub fn create_schema(&mut self, schema_name: &str) -> SystemResult<()> {
        self.data_definition.create_schema(DEFAULT_CATALOG, schema_name);
        match self.persistent.create_namespace(schema_name) {
            Ok(()) => Ok(()),
            Err(StorageError::SystemError(error)) => Err(error),
            Err(StorageError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Create,
                Object::Schema(schema_name),
            )),
        }
    }

    pub fn drop_schema(&mut self, schema_name: &str) -> SystemResult<()> {
        self.data_definition.drop_schema(DEFAULT_CATALOG, schema_name);
        match self.persistent.drop_namespace(schema_name) {
            Ok(()) => Ok(()),
            Err(StorageError::SystemError(error)) => Err(error),
            Err(StorageError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Drop,
                Object::Schema(schema_name),
            )),
        }
    }

    pub fn create_table(
        &mut self,
        schema_name: &str,
        table_name: &str,
        column_definitions: &[ColumnDefinition],
    ) -> SystemResult<()> {
        self.data_definition
            .create_table(DEFAULT_CATALOG, schema_name, table_name, column_definitions);
        match self.persistent.create_tree(schema_name, table_name) {
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

    pub fn drop_table(&mut self, schema_name: &str, table_name: &str) -> SystemResult<()> {
        self.data_definition
            .drop_table(DEFAULT_CATALOG, schema_name, table_name);
        match self.persistent.drop_tree(schema_name, table_name) {
            Ok(()) => Ok(()),
            Err(StorageError::SystemError(error)) => Err(error),
            Err(StorageError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Drop,
                Object::Table(schema_name, table_name),
            )),
        }
    }

    pub fn insert_into(&mut self, schema_name: &str, table_name: &str, values: Vec<Row>) -> SystemResult<usize> {
        log::debug!("{:#?}", values);
        match self.persistent.write(schema_name, table_name, values) {
            Ok(size) => Ok(size),
            Err(StorageError::SystemError(error)) => Err(error),
            Err(StorageError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Access,
                Object::Table(schema_name, table_name),
            )),
        }
    }

    pub fn table_scan(&mut self, schema_name: &str, table_name: &str) -> SystemResult<ReadCursor> {
        match self.persistent.read(schema_name, table_name) {
            Ok(read) => Ok(read),
            Err(StorageError::SystemError(error)) => Err(error),
            Err(StorageError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Access,
                Object::Table(schema_name, table_name),
            )),
        }
    }

    pub fn update_all(&mut self, schema_name: &str, table_name: &str, rows: Vec<Row>) -> SystemResult<usize> {
        match self.persistent.write(schema_name, table_name, rows) {
            Ok(size) => Ok(size),
            Err(StorageError::SystemError(error)) => Err(error),
            Err(StorageError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Access,
                Object::Table(schema_name, table_name),
            )),
        }
    }

    pub fn delete_all_from(&mut self, schema_name: &str, table_name: &str) -> SystemResult<usize> {
        match self.persistent.read(schema_name, table_name) {
            Ok(reads) => {
                let keys = reads.map(Result::unwrap).map(|(key, _)| key).collect();
                match self.persistent.delete(schema_name, table_name, keys) {
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
