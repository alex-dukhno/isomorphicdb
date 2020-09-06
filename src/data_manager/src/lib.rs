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

use std::{
    collections::HashMap,
    io::{self},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        RwLock,
    },
};

use serde::{Deserialize, Serialize};

use kernel::{Object, Operation, SystemError, SystemResult};
use representation::Binary;
use sql_types::SqlType;

use crate::{data_definition::DataDefinition, in_memory::InMemoryDatabase, persistent::PersistentDatabase};

mod data_definition;
mod in_memory;
pub mod persistent;

pub type Row = (Key, Values);
pub type Key = Binary;
pub type Values = Binary;
pub type RowResult = io::Result<Result<Row, StorageError>>;
pub type ReadCursor = Box<dyn Iterator<Item = RowResult>>;

pub enum InitStatus {
    Created,
    Loaded,
}

#[derive(Debug, PartialEq)]
pub enum StorageError {
    Io,
    CascadeIo(Vec<String>),
    Storage,
}

#[derive(Debug, PartialEq)]
pub enum DefinitionError {
    SchemaAlreadyExists,
    SchemaDoesNotExist,
    ObjectAlreadyExists,
    ObjectDoesNotExist,
}

pub type SchemaId<'s> = &'s str;
pub type ObjectId<'o> = &'o str;

pub trait Database {
    fn create_schema(&self, schema_name: SchemaId) -> io::Result<Result<Result<(), DefinitionError>, StorageError>>;

    fn drop_schema(&self, schema_name: SchemaId) -> io::Result<Result<Result<(), DefinitionError>, StorageError>>;

    fn create_object(
        &self,
        schema_name: SchemaId,
        object_name: ObjectId,
    ) -> io::Result<Result<Result<(), DefinitionError>, StorageError>>;

    fn drop_object(
        &self,
        schema_name: SchemaId,
        object_name: ObjectId,
    ) -> io::Result<Result<Result<(), DefinitionError>, StorageError>>;

    fn write(
        &self,
        schema_name: SchemaId,
        object_name: ObjectId,
        values: Vec<(Key, Values)>,
    ) -> io::Result<Result<Result<usize, DefinitionError>, StorageError>>;

    fn read(
        &self,
        schema_name: SchemaId,
        object_name: ObjectId,
    ) -> io::Result<Result<Result<ReadCursor, DefinitionError>, StorageError>>;

    fn delete(
        &self,
        schema_name: SchemaId,
        object_name: ObjectId,
        keys: Vec<Key>,
    ) -> io::Result<Result<Result<usize, DefinitionError>, StorageError>>;
}

pub type RecordId = u64;
pub type FullSchemaId = Option<RecordId>;
pub type FullTableId = Option<(RecordId, Option<RecordId>)>;

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

pub struct DataManager {
    data_storage: Box<dyn Database>,
    data_definition: DataDefinition,
    schemas: RwLock<HashMap<RecordId, String>>,
    tables: RwLock<HashMap<(RecordId, RecordId), Vec<String>>>,
    record_id_generators: RwLock<HashMap<(RecordId, RecordId), AtomicU64>>,
}

impl Default for DataManager {
    fn default() -> DataManager {
        Self::in_memory().expect("no errors")
    }
}

unsafe impl Send for DataManager {}

unsafe impl Sync for DataManager {}

const DEFAULT_CATALOG: &'_ str = "public";

impl DataManager {
    pub fn in_memory() -> SystemResult<DataManager> {
        let data_definition = DataDefinition::in_memory();
        data_definition.create_catalog(DEFAULT_CATALOG);
        Ok(Self {
            data_storage: Box::new(InMemoryDatabase::default()),
            data_definition,
            schemas: RwLock::default(),
            tables: RwLock::default(),
            record_id_generators: RwLock::default(),
        })
    }

    pub fn persistent(path: PathBuf) -> SystemResult<DataManager> {
        let data_definition = DataDefinition::persistent(&path)?;
        let catalog = PersistentDatabase::new(path.join(DEFAULT_CATALOG));
        let schemas = RwLock::new(HashMap::new());
        let tables = RwLock::new(HashMap::new());
        match data_definition.catalog_exists(DEFAULT_CATALOG) {
            Some(_id) => {
                for (schema_id, schema_name) in data_definition.schemas(DEFAULT_CATALOG) {
                    schemas
                        .write()
                        .expect("to acquire write lock")
                        .insert(schema_id, schema_name.clone());
                    match catalog.init(schema_name.as_str()) {
                        Ok(Ok(InitStatus::Loaded)) => {
                            for (table_id, table_name) in data_definition.tables(DEFAULT_CATALOG, schema_name.as_str())
                            {
                                tables
                                    .write()
                                    .expect("to acquire write lock")
                                    .insert((schema_id, table_id), vec![schema_name.clone(), table_name.clone()]);
                                catalog.open_object(schema_name.as_str(), table_name.as_str());
                            }
                        }
                        Ok(Ok(InitStatus::Created)) => {
                            log::error!("Schema {:?} should have been already created", schema_name);
                            return Err(SystemError::bug_in_sql_engine(
                                Operation::Access,
                                Object::Schema(schema_name.as_str()),
                            ));
                        }
                        Ok(Err(error)) => {
                            log::error!("Error during schema {:?} initialization {:?}", schema_name, error);
                            return Err(SystemError::bug_in_sql_engine(
                                Operation::Access,
                                Object::Schema(schema_name.as_str()),
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
            data_storage: Box::new(catalog),
            data_definition,
            schemas,
            tables,
            record_id_generators: RwLock::default(),
        })
    }

    pub fn next_key_id(&self, schema_id: RecordId, table_id: RecordId) -> RecordId {
        match self
            .record_id_generators
            .read()
            .expect("to acquire read lock")
            .get(&(schema_id, table_id))
        {
            Some(id_generator) => id_generator.fetch_add(1, Ordering::SeqCst),
            None => panic!(),
        }
    }

    pub fn create_schema(&self, schema_name: &str) -> SystemResult<RecordId> {
        match self.data_definition.create_schema(DEFAULT_CATALOG, schema_name) {
            Some((_, Some(schema_id))) => {
                self.schemas
                    .write()
                    .expect("to acquire write lock")
                    .insert(schema_id, schema_name.to_owned());
                match self.data_storage.create_schema(schema_name) {
                    Ok(Ok(Ok(()))) => Ok(schema_id),
                    _ => Err(SystemError::bug_in_sql_engine(
                        Operation::Create,
                        Object::Schema(schema_name),
                    )),
                }
            }
            Some((_, None)) => Err(SystemError::bug_in_sql_engine(
                Operation::Create,
                Object::Schema(schema_name),
            )),
            None => Err(SystemError::bug_in_sql_engine(
                Operation::Create,
                Object::Schema(schema_name),
            )),
        }
    }

    pub fn drop_schema(
        &self,
        schema_id: RecordId,
        strategy: DropStrategy,
    ) -> SystemResult<Result<(), DropSchemaError>> {
        match self.schemas.write().expect("to acquire write lock").remove(&schema_id) {
            None => Ok(Err(DropSchemaError::DoesNotExist)),
            Some(schema_name) => {
                match self
                    .data_definition
                    .drop_schema(DEFAULT_CATALOG, schema_name.as_str(), strategy)
                {
                    Ok(()) => match self.data_storage.drop_schema(schema_name.as_str()) {
                        Ok(Ok(Ok(()))) => Ok(Ok(())),
                        _ => Err(SystemError::bug_in_sql_engine(
                            Operation::Drop,
                            Object::Schema(schema_name.as_str()),
                        )),
                    },
                    Err(error) => Ok(Err(error)),
                }
            }
        }
    }

    pub fn create_table(
        &self,
        schema_id: RecordId,
        table_name: &str,
        column_definitions: &[ColumnDefinition],
    ) -> SystemResult<RecordId> {
        match self.schemas.read().expect("to acquire read lock").get(&schema_id) {
            Some(schema_name) => {
                match self
                    .data_definition
                    .create_table(DEFAULT_CATALOG, schema_name, table_name, column_definitions)
                {
                    Some((_, Some((_, Some(table_id))))) => {
                        self.tables.write().expect("to acquire write lock").insert(
                            (schema_id, table_id),
                            vec![schema_name.to_owned(), table_name.to_owned()],
                        );
                        self.record_id_generators
                            .write()
                            .expect("to acquire write lock")
                            .insert((schema_id, table_id), AtomicU64::default());
                        match self.data_storage.create_object(schema_name, table_name) {
                            Ok(Ok(Ok(()))) => Ok(table_id),
                            _ => Err(SystemError::bug_in_sql_engine(
                                Operation::Create,
                                Object::Table(schema_name, table_name),
                            )),
                        }
                    }
                    _ => Err(SystemError::bug_in_sql_engine(
                        Operation::Create,
                        Object::Table(schema_id.to_string().as_str(), table_name),
                    )),
                }
            }
            None => Err(SystemError::bug_in_sql_engine(
                Operation::Create,
                Object::Table(schema_id.to_string().as_str(), table_name),
            )),
        }
    }

    pub fn table_columns(&self, schema_id: RecordId, table_id: RecordId) -> SystemResult<Vec<ColumnDefinition>> {
        match self
            .tables
            .read()
            .expect("to acquire read lock")
            .get(&(schema_id, table_id))
        {
            Some(full_name) => {
                Ok(self
                    .data_definition
                    .table_columns(DEFAULT_CATALOG, full_name[0].as_str(), full_name[1].as_str()))
            }
            _ => Err(SystemError::bug_in_sql_engine(
                Operation::Access,
                Object::Table(schema_id.to_string().as_str(), table_id.to_string().as_str()),
            )),
        }
    }

    pub fn drop_table(&self, schema_id: RecordId, table_id: RecordId) -> SystemResult<()> {
        match self
            .tables
            .write()
            .expect("to acquire write lock")
            .remove(&(schema_id, table_id))
        {
            None => Err(SystemError::bug_in_sql_engine(
                Operation::Drop,
                Object::Table(schema_id.to_string().as_str(), table_id.to_string().as_str()),
            )),
            Some(full_name) => {
                self.data_definition
                    .drop_table(DEFAULT_CATALOG, full_name[0].as_str(), full_name[1].as_str());
                match self
                    .data_storage
                    .drop_object(full_name[0].as_str(), full_name[1].as_str())
                {
                    Ok(Ok(Ok(()))) => Ok(()),
                    _ => Err(SystemError::bug_in_sql_engine(
                        Operation::Drop,
                        Object::Table(schema_id.to_string().as_str(), table_id.to_string().as_str()),
                    )),
                }
            }
        }
    }

    pub fn write_into(
        &self,
        schema_id: RecordId,
        table_id: RecordId,
        values: Vec<(Key, Values)>,
    ) -> SystemResult<usize> {
        match self
            .tables
            .read()
            .expect("to acquire read lock")
            .get(&(schema_id, table_id))
        {
            Some(full_name) => {
                log::debug!("{:#?}", values);
                match self
                    .data_storage
                    .write(full_name[0].as_str(), full_name[1].as_str(), values)
                {
                    Ok(Ok(Ok(size))) => Ok(size),
                    _ => Err(SystemError::bug_in_sql_engine(
                        Operation::Access,
                        Object::Table(schema_id.to_string().as_str(), table_id.to_string().as_str()),
                    )),
                }
            }
            None => Err(SystemError::bug_in_sql_engine(
                Operation::Access,
                Object::Table(schema_id.to_string().as_str(), table_id.to_string().as_str()),
            )),
        }
    }

    pub fn full_scan(&self, schema_id: RecordId, table_id: RecordId) -> SystemResult<ReadCursor> {
        match self
            .tables
            .read()
            .expect("to acquire read lock")
            .get(&(schema_id, table_id))
        {
            Some(full_name) => match self.data_storage.read(full_name[0].as_str(), full_name[1].as_str()) {
                Ok(Ok(Ok(read))) => Ok(read),
                _ => Err(SystemError::bug_in_sql_engine(
                    Operation::Access,
                    Object::Table(schema_id.to_string().as_str(), table_id.to_string().as_str()),
                )),
            },
            None => Err(SystemError::bug_in_sql_engine(
                Operation::Access,
                Object::Table(schema_id.to_string().as_str(), table_id.to_string().as_str()),
            )),
        }
    }

    pub fn delete_from(&self, schema_id: RecordId, table_id: RecordId, keys: Vec<Key>) -> SystemResult<usize> {
        match self
            .tables
            .read()
            .expect("to acquire read lock")
            .get(&(schema_id, table_id))
        {
            Some(full_name) => match self
                .data_storage
                .delete(full_name[0].as_str(), full_name[1].as_str(), keys)
            {
                Ok(Ok(Ok(len))) => Ok(len),
                _ => Err(SystemError::bug_in_sql_engine(
                    Operation::Access,
                    Object::Table(schema_id.to_string().as_str(), table_id.to_string().as_str()),
                )),
            },
            None => Err(SystemError::bug_in_sql_engine(
                Operation::Access,
                Object::Table(schema_id.to_string().as_str(), table_id.to_string().as_str()),
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
