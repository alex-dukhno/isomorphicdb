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

use crate::{catalog_manager::data_definition::DataDefinition, ColumnDefinition};
use kernel::{Object, Operation, SystemError, SystemResult};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use storage::{Database, InMemoryDatabase, InitStatus, Key, PersistentDatabase, ReadCursor, Row};

pub type FullSchemaId = Option<u64>;
pub type FullTableId = Option<(u64, Option<u64>)>;

mod data_definition;

pub enum DropStrategy {
    Restrict,
    Cascade,
}

#[derive(Debug, PartialEq)]
pub enum DropSchemaError {
    CatalogDoesNotExist,
    DoesNotExist,
    HasDependentObjects,
}

pub struct CatalogManager {
    key_id_generator: AtomicU64,
    data_storage: Box<dyn Database>,
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
            data_storage: Box::new(InMemoryDatabase::default()),
            data_definition,
        })
    }

    pub fn persistent(path: PathBuf) -> SystemResult<CatalogManager> {
        let data_definition = DataDefinition::persistent(&path)?;
        let catalog = PersistentDatabase::new(path.join(DEFAULT_CATALOG));
        match data_definition.catalog_exists(DEFAULT_CATALOG) {
            Some(_id) => {
                for schema in data_definition.schemas(DEFAULT_CATALOG) {
                    match catalog.init(schema.as_str()) {
                        Ok(Ok(InitStatus::Loaded)) => {
                            for table in data_definition.tables(DEFAULT_CATALOG, schema.as_str()) {
                                catalog.open_object(schema.as_str(), table.as_str());
                            }
                        }
                        Ok(Ok(InitStatus::Created)) => {
                            log::error!("Schema {:?} should have been already created", schema);
                            return Err(SystemError::bug_in_sql_engine(
                                Operation::Access,
                                Object::Schema(schema.as_str()),
                            ));
                        }
                        Ok(Err(error)) => {
                            log::error!("Error during schema {:?} initialization {:?}", schema, error);
                            return Err(SystemError::bug_in_sql_engine(
                                Operation::Access,
                                Object::Schema(schema.as_str()),
                            ));
                        }
                        Err(io_error) => return Err(SystemError::io(io_error)),
                    }
                }
            }
            None => {
                data_definition.create_catalog(DEFAULT_CATALOG);
            }
        }
        Ok(Self {
            key_id_generator: AtomicU64::default(),
            data_storage: Box::new(catalog),
            data_definition,
        })
    }

    pub fn next_key_id(&self) -> u64 {
        self.key_id_generator.fetch_add(1, Ordering::SeqCst)
    }

    pub fn create_schema(&self, schema_name: &str) -> SystemResult<()> {
        self.data_definition.create_schema(DEFAULT_CATALOG, schema_name);
        match self.data_storage.create_schema(schema_name) {
            Ok(Ok(Ok(()))) => Ok(()),
            _ => Err(SystemError::bug_in_sql_engine(
                Operation::Create,
                Object::Schema(schema_name),
            )),
        }
    }

    pub fn drop_schema(&self, schema_name: &str, strategy: DropStrategy) -> SystemResult<Result<(), DropSchemaError>> {
        match self.data_definition.drop_schema(DEFAULT_CATALOG, schema_name, strategy) {
            Ok(()) => match self.data_storage.drop_schema(schema_name) {
                Ok(Ok(Ok(()))) => Ok(Ok(())),
                _ => Err(SystemError::bug_in_sql_engine(
                    Operation::Drop,
                    Object::Schema(schema_name),
                )),
            },
            Err(error) => Ok(Err(error)),
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
        match self.data_storage.create_object(schema_name, table_name) {
            Ok(Ok(Ok(()))) => Ok(()),
            _ => Err(SystemError::bug_in_sql_engine(
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
        match self.data_storage.drop_object(schema_name, table_name) {
            Ok(Ok(Ok(()))) => Ok(()),
            _ => Err(SystemError::bug_in_sql_engine(
                Operation::Drop,
                Object::Table(schema_name, table_name),
            )),
        }
    }

    pub fn write_into(&self, schema_name: &str, table_name: &str, values: Vec<Row>) -> SystemResult<usize> {
        log::debug!("{:#?}", values);
        match self.data_storage.write(schema_name, table_name, values) {
            Ok(Ok(Ok(size))) => Ok(size),
            _ => Err(SystemError::bug_in_sql_engine(
                Operation::Access,
                Object::Table(schema_name, table_name),
            )),
        }
    }

    pub fn full_scan(&self, schema_name: &str, table_name: &str) -> SystemResult<ReadCursor> {
        match self.data_storage.read(schema_name, table_name) {
            Ok(Ok(Ok(read))) => Ok(read),
            _ => Err(SystemError::bug_in_sql_engine(
                Operation::Access,
                Object::Table(schema_name, table_name),
            )),
        }
    }

    pub fn delete_from(&self, schema_name: &str, table_name: &str, keys: Vec<Key>) -> SystemResult<usize> {
        match self.data_storage.delete(schema_name, table_name, keys) {
            Ok(Ok(Ok(len))) => Ok(len),
            _ => Err(SystemError::bug_in_sql_engine(
                Operation::Access,
                Object::Table(schema_name, table_name),
            )),
        }
    }

    pub fn schema_exists(&self, schema_name: &str) -> FullSchemaId {
        self.data_definition
            .schema_exists(DEFAULT_CATALOG, schema_name)
            .and_then(|(_catalog, schema)| schema)
    }

    pub fn table_exists(&self, schema_name: &str, table_name: &str) -> FullTableId {
        self.data_definition
            .table_exists(DEFAULT_CATALOG, schema_name, table_name)
            .and_then(|(_catalog, full_table)| full_table)
    }
}

#[cfg(test)]
mod tests;
