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

use binary::{Key, ReadCursor, Values};
use dashmap::DashMap;
use meta_def::ColumnDefinition;
use metadata::{DataDefinition, MetadataView};
use sql_model::{DropSchemaError, DropStrategy, Id};
use std::fmt::{Display, Formatter};
use std::{
    fmt,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use storage::{Database, FullSchemaId, FullTableId, InMemoryDatabase, InitStatus, PersistentDatabase};

pub struct DataManager {
    databases: DashMap<String, Box<dyn Database>>,
    data_definition: Arc<DataDefinition>,
    schemas: DashMap<Id, String>,
    tables: DashMap<(Id, Id), Vec<String>>,
    record_id_generators: DashMap<(Id, Id), AtomicU64>,
}

impl Default for DataManager {
    fn default() -> DataManager {
        DataManager::in_memory(Arc::new(DataDefinition::in_memory()))
    }
}

unsafe impl Send for DataManager {}

unsafe impl Sync for DataManager {}

pub const DEFAULT_CATALOG: &'_ str = "public";

impl DataManager {
    pub fn in_memory(data_definition: Arc<DataDefinition>) -> DataManager {
        data_definition.create_catalog(DEFAULT_CATALOG);
        let databases: DashMap<String, Box<dyn Database>> = DashMap::default();
        databases.insert(DEFAULT_CATALOG.to_lowercase(), Box::new(InMemoryDatabase::default()));
        DataManager {
            databases,
            data_definition,
            schemas: DashMap::default(),
            tables: DashMap::default(),
            record_id_generators: DashMap::default(),
        }
    }

    pub fn persistent(data_definition: Arc<DataDefinition>, path: PathBuf) -> Result<DataManager, ()> {
        let catalog = PersistentDatabase::new(path.join(DEFAULT_CATALOG));
        let schemas = DashMap::new();
        let tables = DashMap::new();
        match data_definition.catalog_exists(DEFAULT_CATALOG) {
            Some(_id) => {
                for (schema_id, schema_name) in data_definition.schemas(DEFAULT_CATALOG) {
                    schemas.insert(schema_id, schema_name.clone());
                    match catalog.init(&schema_name) {
                        Ok(Ok(InitStatus::Loaded)) => {
                            for (table_id, table_name) in data_definition.tables(DEFAULT_CATALOG, schema_name.as_str())
                            {
                                tables.insert((schema_id, table_id), vec![schema_name.clone(), table_name.clone()]);
                                catalog.open_object(schema_name.as_str(), table_name.as_str());
                            }
                        }
                        Ok(Ok(InitStatus::Created)) => {
                            log::error!("Schema {:?} should have been already created", schema_name);
                            return Err(());
                        }
                        Ok(Err(error)) => {
                            log::error!("Error during schema {:?} initialization {:?}", schema_name, error);
                            return Err(());
                        }
                        Err(io_error) => {
                            log::error!("IO Error during schema {:?} initialization {:?}", schema_name, io_error);
                            return Err(());
                        }
                    }
                }
            }
            None => {
                data_definition.create_catalog(DEFAULT_CATALOG);
            }
        }
        let databases: DashMap<String, Box<dyn Database>> = DashMap::default();
        databases.insert(DEFAULT_CATALOG.to_lowercase(), Box::new(catalog));
        Ok(DataManager {
            databases,
            data_definition,
            schemas,
            tables,
            record_id_generators: DashMap::default(),
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
                match self.databases.get(DEFAULT_CATALOG).unwrap().create_schema(schema_name) {
                    Ok(Ok(Ok(()))) => Ok(schema_id),
                    _ => {
                        log::error!(
                            "SQL Engine does not check '{}' existence of SCHEMA before creating one",
                            schema_name
                        );
                        Err(())
                    }
                }
            }
            Some((_, None)) => {
                log::error!(
                    "SQL Engine does not check '{}' existence of SCHEMA before creating one",
                    schema_name
                );
                Err(())
            }
            None => {
                log::error!(
                    "SQL Engine does not check '{}' existence of SCHEMA before creating one",
                    schema_name
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
            Some((_, schema_name)) => {
                match self
                    .data_definition
                    .drop_schema(DEFAULT_CATALOG, schema_name.as_str(), strategy)
                {
                    Ok(()) => match self
                        .databases
                        .get(DEFAULT_CATALOG)
                        .unwrap()
                        .drop_schema(schema_name.as_str())
                    {
                        Ok(Ok(Ok(()))) => Ok(Ok(())),
                        _ => {
                            log::error!(
                                "SQL Engine does not check '{}' existence of SCHEMA before dropping one",
                                schema_name
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
                        match self
                            .databases
                            .get(DEFAULT_CATALOG)
                            .unwrap()
                            .create_object(&*schema_name, table_name)
                        {
                            Ok(Ok(Ok(()))) => Ok(table_id),
                            _ => {
                                log::error!(
                                    "SQL Engine does not check '{}.{}' existence of TABLE before creating one",
                                    &*schema_name,
                                    table_name
                                );
                                Err(())
                            }
                        }
                    }
                    _ => {
                        log::error!(
                            "SQL Engine does not check '{}.{}' existence of TABLE before creating one",
                            &*schema_name,
                            table_name
                        );
                        Err(())
                    }
                }
            }
            None => {
                engine_bug_reporter(Operation::Create, Object::Schema(schema_id));
                Err(())
            }
        }
    }

    pub fn drop_table<I: AsRef<(Id, Id)>>(&self, table_id: &I) -> Result<(), ()> {
        match self.tables.remove(table_id.as_ref()) {
            None => {
                let (schema_id, table_id) = table_id.as_ref();
                engine_bug_reporter(Operation::Drop, Object::Table(*schema_id, *table_id));
                Err(())
            }
            Some((_, full_name)) => {
                self.data_definition
                    .drop_table(DEFAULT_CATALOG, full_name[0].as_str(), full_name[1].as_str());
                match self
                    .databases
                    .get(DEFAULT_CATALOG)
                    .unwrap()
                    .drop_object(full_name[0].as_str(), full_name[1].as_str())
                {
                    Ok(Ok(Ok(()))) => Ok(()),
                    _ => {
                        let (schema_id, table_id) = table_id.as_ref();
                        engine_bug_reporter(Operation::Drop, Object::Table(*schema_id, *table_id));
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
                match self.databases.get(DEFAULT_CATALOG).unwrap().write(
                    full_name[0].as_str(),
                    full_name[1].as_str(),
                    values,
                ) {
                    Ok(Ok(Ok(size))) => Ok(size),
                    _ => {
                        let (schema_id, table_id) = table_id.as_ref();
                        engine_bug_reporter(Operation::Access, Object::Table(*schema_id, *table_id));
                        Err(())
                    }
                }
            }
            None => {
                let (schema_id, table_id) = table_id.as_ref();
                engine_bug_reporter(Operation::Access, Object::Table(*schema_id, *table_id));
                Err(())
            }
        }
    }

    pub fn full_scan<I: AsRef<(Id, Id)>>(&self, table_id: &I) -> Result<ReadCursor, ()> {
        match self.tables.get(table_id.as_ref()) {
            Some(full_name) => match self
                .databases
                .get(DEFAULT_CATALOG)
                .unwrap()
                .read(full_name[0].as_str(), full_name[1].as_str())
            {
                Ok(Ok(Ok(read))) => Ok(read),
                _ => {
                    let (schema_id, table_id) = table_id.as_ref();
                    engine_bug_reporter(Operation::Access, Object::Table(*schema_id, *table_id));
                    Err(())
                }
            },
            None => {
                let (schema_id, table_id) = table_id.as_ref();
                engine_bug_reporter(Operation::Access, Object::Table(*schema_id, *table_id));
                Err(())
            }
        }
    }

    pub fn delete_from<I: AsRef<(Id, Id)>>(&self, table_id: &I, keys: Vec<Key>) -> Result<usize, ()> {
        match self.tables.get(table_id.as_ref()) {
            Some(full_name) => match self.databases.get(DEFAULT_CATALOG).unwrap().delete(
                full_name[0].as_str(),
                full_name[1].as_str(),
                keys,
            ) {
                Ok(Ok(Ok(len))) => Ok(len),
                _ => {
                    let (schema_id, table_id) = table_id.as_ref();
                    engine_bug_reporter(Operation::Access, Object::Table(*schema_id, *table_id));
                    Err(())
                }
            },
            None => {
                let (schema_id, table_id) = table_id.as_ref();
                engine_bug_reporter(Operation::Access, Object::Table(*schema_id, *table_id));
                Err(())
            }
        }
    }
}

impl MetadataView for DataManager {
    fn schema_exists<S: AsRef<str>>(&self, schema_name: &S) -> FullSchemaId {
        self.data_definition.schema_exists(schema_name)
    }

    fn table_exists<S: AsRef<str>, T: AsRef<str>>(&self, schema_name: &S, table_name: &T) -> FullTableId {
        self.data_definition.table_exists(schema_name, table_name)
    }

    fn table_columns<I: AsRef<(Id, Id)>>(&self, table_id: &I) -> Result<Vec<(Id, ColumnDefinition)>, ()> {
        self.data_definition.table_columns(table_id)
    }

    fn column_ids<I: AsRef<(Id, Id)>, N: AsRef<str> + PartialEq<N>>(
        &self,
        table_id: &I,
        names: &[N],
    ) -> Result<(Vec<Id>, Vec<String>), ()> {
        self.data_definition.column_ids(table_id, names)
    }

    fn column_defs<I: AsRef<(Id, Id)>>(&self, table_id: &I, ids: &[Id]) -> Vec<ColumnDefinition> {
        self.data_definition.column_defs(table_id, ids)
    }
}

fn engine_bug_reporter(operation: Operation, object: Object) {
    log::error!(
        "This is most possibly a 🐛[BUG] in sql engine. It does not check existence of {} before {} one",
        object,
        operation
    )
}

enum Operation {
    Create,
    Drop,
    Access,
}

impl Display for Operation {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Operation::Create => write!(f, "creating"),
            Operation::Drop => write!(f, "dropping"),
            Operation::Access => write!(f, "accessing"),
        }
    }
}

enum Object {
    Table(Id, Id),
    Schema(Id),
}

impl Display for Object {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Object::Table(schema_id, table_id) => write!(f, "TABLE [{}.{}]", schema_id, table_id),
            Object::Schema(schema_id) => write!(f, "SCHEMA [{}]", schema_id),
        }
    }
}

#[cfg(test)]
mod tests;
