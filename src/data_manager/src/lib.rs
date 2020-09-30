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

use crate::{data_definition::DataDefinition, in_memory::InMemoryDatabase, persistent::PersistentDatabase};
use binary::Binary;
use chashmap::CHashMap;
use kernel::{Object, Operation, SystemError, SystemResult};
use sql_model::sql_types::SqlType;
use sql_model::{sql_errors::DefinitionError, Id};
use std::collections::HashMap;
use std::{
    io::{self},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

mod data_definition;
mod in_memory;
pub mod persistent;

pub type Row = (Key, Values);
pub type Key = Binary;
pub type Values = Binary;
pub type RowResult = io::Result<Result<Row, StorageError>>;
pub type ReadCursor = Box<dyn Iterator<Item = RowResult>>;

pub type FullSchemaId = Option<Id>;
pub type FullTableId = Option<(Id, Option<Id>)>;
pub type SchemaName<'s> = &'s str;
pub type ObjectName<'o> = &'o str;

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

pub trait MetadataView {
    fn schema_exists<S: AsRef<str>>(&self, schema_name: &S) -> FullSchemaId;

    fn table_exists<S: AsRef<str>, T: AsRef<str>>(&self, schema_name: &S, table_name: &T) -> FullTableId;

    fn table_columns<I: AsRef<(Id, Id)>>(&self, table_id: &I) -> Result<Vec<ColumnDefinition>, ()>;

    fn column_ids<I: AsRef<(Id, Id)>, N: AsRef<str> + PartialEq<N>>(
        &self,
        table_id: &I,
        names: &[N],
    ) -> Result<(Vec<Id>, Vec<String>), ()>;

    fn column_defs<I: AsRef<(Id, Id)>>(&self, table_id: &I, ids: &[Id]) -> Vec<ColumnDefinition>;
}

pub trait Database {
    fn create_schema(&self, schema_name: SchemaName) -> io::Result<Result<Result<(), DefinitionError>, StorageError>>;

    fn drop_schema(&self, schema_name: SchemaName) -> io::Result<Result<Result<(), DefinitionError>, StorageError>>;

    fn create_object(
        &self,
        schema_name: SchemaName,
        object_name: ObjectName,
    ) -> io::Result<Result<Result<(), DefinitionError>, StorageError>>;

    fn drop_object(
        &self,
        schema_name: SchemaName,
        object_name: ObjectName,
    ) -> io::Result<Result<Result<(), DefinitionError>, StorageError>>;

    fn write(
        &self,
        schema_name: SchemaName,
        object_name: ObjectName,
        values: Vec<(Key, Values)>,
    ) -> io::Result<Result<Result<usize, DefinitionError>, StorageError>>;

    fn read(
        &self,
        schema_name: SchemaName,
        object_name: ObjectName,
    ) -> io::Result<Result<Result<ReadCursor, DefinitionError>, StorageError>>;

    fn delete(
        &self,
        schema_name: SchemaName,
        object_name: ObjectName,
        keys: Vec<Key>,
    ) -> io::Result<Result<Result<usize, DefinitionError>, StorageError>>;
}

#[derive(Debug, PartialEq, Clone)]
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
    schemas: CHashMap<Id, String>,
    tables: CHashMap<(Id, Id), Vec<String>>,
    record_id_generators: CHashMap<(Id, Id), AtomicU64>,
}

impl Default for DataManager {
    fn default() -> DataManager {
        DataManager::in_memory().expect("no errors")
    }
}

unsafe impl Send for DataManager {}

unsafe impl Sync for DataManager {}

const DEFAULT_CATALOG: &'_ str = "public";

impl DataManager {
    pub fn in_memory() -> SystemResult<DataManager> {
        let data_definition = DataDefinition::in_memory();
        data_definition.create_catalog(DEFAULT_CATALOG);
        Ok(DataManager {
            data_storage: Box::new(InMemoryDatabase::default()),
            data_definition,
            schemas: CHashMap::default(),
            tables: CHashMap::default(),
            record_id_generators: CHashMap::default(),
        })
    }

    pub fn persistent(path: PathBuf) -> SystemResult<DataManager> {
        let data_definition = DataDefinition::persistent(&path)?;
        let catalog = PersistentDatabase::new(path.join(DEFAULT_CATALOG));
        let schemas = CHashMap::new();
        let tables = CHashMap::new();
        match data_definition.catalog_exists(DEFAULT_CATALOG) {
            Some(_id) => {
                for (schema_id, schema_name) in data_definition.schemas(DEFAULT_CATALOG) {
                    schemas.insert(schema_id, schema_name.clone());
                    match catalog.init(schema_name.as_str()) {
                        Ok(Ok(InitStatus::Loaded)) => {
                            for (table_id, table_name) in data_definition.tables(DEFAULT_CATALOG, schema_name.as_str())
                            {
                                tables.insert((schema_id, table_id), vec![schema_name.clone(), table_name.clone()]);
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
        Ok(DataManager {
            data_storage: Box::new(catalog),
            data_definition,
            schemas,
            tables,
            record_id_generators: CHashMap::default(),
        })
    }

    pub fn next_key_id<I: AsRef<(Id, Id)>>(&self, table_id: &I) -> Id {
        match self.record_id_generators.get(table_id.as_ref()) {
            Some(id_generator) => id_generator.fetch_add(1, Ordering::SeqCst),
            None => panic!(),
        }
    }

    pub fn create_schema(&self, schema_name: &str) -> Result<Id, ()> {
        match self.data_definition.create_schema(DEFAULT_CATALOG, schema_name) {
            Some((_, Some(schema_id))) => {
                self.schemas.insert(schema_id, schema_name.to_owned());
                match self.data_storage.create_schema(schema_name) {
                    Ok(Ok(Ok(()))) => Ok(schema_id),
                    _ => {
                        log::error!(
                            "{:?}",
                            SystemError::bug_in_sql_engine(Operation::Create, Object::Schema(schema_name))
                        );
                        Err(())
                    }
                }
            }
            Some((_, None)) => {
                log::error!(
                    "{:?}",
                    SystemError::bug_in_sql_engine(Operation::Create, Object::Schema(schema_name))
                );
                Err(())
            }
            None => {
                log::error!(
                    "{:?}",
                    SystemError::bug_in_sql_engine(Operation::Create, Object::Schema(schema_name))
                );
                Err(())
            }
        }
    }

    pub fn drop_schema<I: AsRef<Id>>(
        &self,
        schema_id: &I,
        strategy: DropStrategy,
    ) -> Result<Result<(), DropSchemaError>, ()> {
        match self.schemas.remove(schema_id.as_ref()) {
            None => Ok(Err(DropSchemaError::DoesNotExist)),
            Some(schema_name) => {
                match self
                    .data_definition
                    .drop_schema(DEFAULT_CATALOG, schema_name.as_str(), strategy)
                {
                    Ok(()) => match self.data_storage.drop_schema(schema_name.as_str()) {
                        Ok(Ok(Ok(()))) => Ok(Ok(())),
                        _ => {
                            log::error!(
                                "{:?}",
                                SystemError::bug_in_sql_engine(Operation::Drop, Object::Schema(schema_name.as_str()),)
                            );
                            Err(())
                        }
                    },
                    Err(error) => Ok(Err(error)),
                }
            }
        }
    }

    pub fn create_table(
        &self,
        schema_id: Id,
        table_name: &str,
        column_definitions: &[ColumnDefinition],
    ) -> Result<Id, ()> {
        match self.schemas.get(&schema_id) {
            Some(schema_name) => {
                match self
                    .data_definition
                    .create_table(DEFAULT_CATALOG, &*schema_name, table_name, column_definitions)
                {
                    Some((_, Some((_, Some(table_id))))) => {
                        self.tables.insert(
                            (schema_id, table_id),
                            vec![(*schema_name).clone(), table_name.to_owned()],
                        );
                        self.record_id_generators
                            .insert((schema_id, table_id), AtomicU64::default());
                        match self.data_storage.create_object(&*schema_name, table_name) {
                            Ok(Ok(Ok(()))) => Ok(table_id),
                            _ => {
                                log::error!(
                                    "{:?}",
                                    SystemError::bug_in_sql_engine(
                                        Operation::Create,
                                        Object::Table(&*schema_name, table_name),
                                    )
                                );
                                Err(())
                            }
                        }
                    }
                    _ => {
                        log::error!(
                            "{:?}",
                            SystemError::bug_in_sql_engine(
                                Operation::Create,
                                Object::Table(schema_id.to_string().as_str(), table_name),
                            )
                        );
                        Err(())
                    }
                }
            }
            None => {
                log::error!(
                    "{:?}",
                    SystemError::bug_in_sql_engine(
                        Operation::Create,
                        Object::Table(schema_id.to_string().as_str(), table_name),
                    )
                );
                Err(())
            }
        }
    }

    pub fn drop_table<I: AsRef<(Id, Id)>>(&self, table_id: &I) -> Result<(), ()> {
        match self.tables.remove(table_id.as_ref()) {
            None => {
                let (schema_id, table_id) = table_id.as_ref();
                log::error!(
                    "{:?}",
                    SystemError::bug_in_sql_engine(
                        Operation::Drop,
                        Object::Table(schema_id.to_string().as_str(), table_id.to_string().as_str()),
                    )
                );
                Err(())
            }
            Some(full_name) => {
                self.data_definition
                    .drop_table(DEFAULT_CATALOG, full_name[0].as_str(), full_name[1].as_str());
                match self
                    .data_storage
                    .drop_object(full_name[0].as_str(), full_name[1].as_str())
                {
                    Ok(Ok(Ok(()))) => Ok(()),
                    _ => {
                        let (schema_id, table_id) = table_id.as_ref();
                        log::error!(
                            "{:?}",
                            SystemError::bug_in_sql_engine(
                                Operation::Drop,
                                Object::Table(schema_id.to_string().as_str(), table_id.to_string().as_str()),
                            )
                        );
                        Err(())
                    }
                }
            }
        }
    }

    pub fn write_into<I: AsRef<(Id, Id)>>(&self, table_id: &I, values: Vec<(Key, Values)>) -> Result<usize, ()> {
        match self.tables.get(table_id.as_ref()) {
            Some(full_name) => {
                log::trace!("values to write {:#?}", values);
                match self
                    .data_storage
                    .write(full_name[0].as_str(), full_name[1].as_str(), values)
                {
                    Ok(Ok(Ok(size))) => Ok(size),
                    _ => {
                        let (schema_id, table_id) = table_id.as_ref();
                        log::error!(
                            "{:?}",
                            SystemError::bug_in_sql_engine(
                                Operation::Access,
                                Object::Table(schema_id.to_string().as_str(), table_id.to_string().as_str()),
                            )
                        );
                        Err(())
                    }
                }
            }
            None => {
                let (schema_id, table_id) = table_id.as_ref();
                log::error!(
                    "{:?}",
                    SystemError::bug_in_sql_engine(
                        Operation::Access,
                        Object::Table(schema_id.to_string().as_str(), table_id.to_string().as_str()),
                    )
                );
                Err(())
            }
        }
    }

    pub fn full_scan<I: AsRef<(Id, Id)>>(&self, table_id: &I) -> Result<ReadCursor, ()> {
        match self.tables.get(table_id.as_ref()) {
            Some(full_name) => match self.data_storage.read(full_name[0].as_str(), full_name[1].as_str()) {
                Ok(Ok(Ok(read))) => Ok(read),
                _ => {
                    let (schema_id, table_id) = table_id.as_ref();
                    log::error!(
                        "{:?}",
                        SystemError::bug_in_sql_engine(
                            Operation::Access,
                            Object::Table(schema_id.to_string().as_str(), table_id.to_string().as_str()),
                        )
                    );
                    Err(())
                }
            },
            None => {
                let (schema_id, table_id) = table_id.as_ref();
                log::error!(
                    "{:?}",
                    SystemError::bug_in_sql_engine(
                        Operation::Access,
                        Object::Table(schema_id.to_string().as_str(), table_id.to_string().as_str()),
                    )
                );
                Err(())
            }
        }
    }

    pub fn delete_from<I: AsRef<(Id, Id)>>(&self, table_id: &I, keys: Vec<Key>) -> Result<usize, ()> {
        match self.tables.get(table_id.as_ref()) {
            Some(full_name) => match self
                .data_storage
                .delete(full_name[0].as_str(), full_name[1].as_str(), keys)
            {
                Ok(Ok(Ok(len))) => Ok(len),
                _ => {
                    let (schema_id, table_id) = table_id.as_ref();
                    log::error!(
                        "{:?}",
                        SystemError::bug_in_sql_engine(
                            Operation::Access,
                            Object::Table(schema_id.to_string().as_str(), table_id.to_string().as_str()),
                        )
                    );
                    Err(())
                }
            },
            None => {
                let (schema_id, table_id) = table_id.as_ref();
                log::error!(
                    "{:?}",
                    SystemError::bug_in_sql_engine(
                        Operation::Access,
                        Object::Table(schema_id.to_string().as_str(), table_id.to_string().as_str()),
                    )
                );
                Err(())
            }
        }
    }
}

impl MetadataView for DataManager {
    fn schema_exists<S: AsRef<str>>(&self, schema_name: &S) -> FullSchemaId {
        self.data_definition
            .schema_exists(DEFAULT_CATALOG, schema_name.as_ref())
            .and_then(|(_catalog, schema)| schema)
    }

    fn table_exists<S: AsRef<str>, T: AsRef<str>>(&self, schema_name: &S, table_name: &T) -> FullTableId {
        self.data_definition
            .table_exists(DEFAULT_CATALOG, schema_name.as_ref(), table_name.as_ref())
            .and_then(|(_catalog, full_table)| full_table)
    }

    fn table_columns<I: AsRef<(Id, Id)>>(&self, table_id: &I) -> Result<Vec<ColumnDefinition>, ()> {
        match self.tables.get(table_id.as_ref()) {
            Some(full_name) => {
                Ok(self
                    .data_definition
                    .table_columns(DEFAULT_CATALOG, full_name[0].as_str(), full_name[1].as_str()))
            }
            _ => {
                let (schema_id, table_id) = table_id.as_ref();
                log::error!(
                    "{:?}",
                    SystemError::bug_in_sql_engine(
                        Operation::Access,
                        Object::Table(schema_id.to_string().as_str(), table_id.to_string().as_str()),
                    )
                );
                Err(())
            }
        }
    }

    fn column_ids<I: AsRef<(Id, Id)>, N: AsRef<str> + PartialEq<N>>(
        &self,
        table_id: &I,
        names: &[N],
    ) -> Result<(Vec<Id>, Vec<String>), ()> {
        match self.tables.get(table_id.as_ref()) {
            Some(full_name) => {
                let columns = self
                    .data_definition
                    .table_column_names_ids(DEFAULT_CATALOG, full_name[0].as_str(), full_name[1].as_str())
                    .into_iter()
                    .collect::<HashMap<_, _>>();
                let mut ids = vec![];
                let mut not_found = vec![];
                for name in names {
                    match columns.get(name.as_ref()) {
                        Some(id) => ids.push(*id),
                        None => not_found.push(name.as_ref().to_owned()),
                    }
                }
                Ok((ids, not_found))
            }
            _ => {
                let (schema_id, table_id) = table_id.as_ref();
                log::error!(
                    "{:?}",
                    SystemError::bug_in_sql_engine(
                        Operation::Access,
                        Object::Table(schema_id.to_string().as_str(), table_id.to_string().as_str()),
                    )
                );
                Err(())
            }
        }
    }

    fn column_defs<I: AsRef<(Id, Id)>>(&self, table_id: &I, ids: &[Id]) -> Vec<ColumnDefinition> {
        match self.tables.get(table_id.as_ref()) {
            Some(full_name) => {
                let columns = self.data_definition.table_id_columns(
                    DEFAULT_CATALOG,
                    full_name[0].as_str(),
                    full_name[1].as_str(),
                );
                let mut ret = vec![];
                for id in ids {
                    for (i, column) in &columns {
                        if id == i {
                            ret.push(column.clone());
                        }
                    }
                }
                ret
            }
            _ => {
                let (schema_id, table_id) = table_id.as_ref();
                log::error!(
                    "{:?}",
                    SystemError::bug_in_sql_engine(
                        Operation::Access,
                        Object::Table(schema_id.to_string().as_str(), table_id.to_string().as_str()),
                    )
                );
                vec![]
            }
        }
    }
}

#[cfg(test)]
mod tests;
