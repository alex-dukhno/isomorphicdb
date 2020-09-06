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
    io::{self, ErrorKind},
    path::PathBuf,
    sync::{Arc, RwLock},
};

use sled::{Db as Schema, DiskPtr, Error as SledError, IVec, Tree};

use representation::Binary;

use crate::{
    Database, DefinitionError, InitStatus, Key, ObjectName, ReadCursor, RowResult, SchemaName, StorageError, Values,
};

pub struct PersistentDatabase {
    path: PathBuf,
    schemas: RwLock<HashMap<String, Arc<Schema>>>,
}

impl PersistentDatabase {
    pub fn new(path: PathBuf) -> PersistentDatabase {
        PersistentDatabase {
            path,
            schemas: RwLock::default(),
        }
    }

    pub fn init(&self, schema_name: SchemaName) -> io::Result<Result<InitStatus, StorageError>> {
        let path_to_schema = PathBuf::from(&self.path).join(schema_name);
        log::info!("path to schema {:?}", path_to_schema);
        self.open_database(path_to_schema).map(|storage| {
            storage.map(|schema| {
                let recovered = schema.was_recovered();
                self.schemas
                    .write()
                    .expect("to acquire write lock")
                    .insert(schema_name.to_owned(), Arc::new(schema));
                log::debug!("schemas after initialization {:?}", self.schemas);
                if recovered {
                    InitStatus::Loaded
                } else {
                    InitStatus::Created
                }
            })
        })
    }

    pub fn open_object(&self, schema_name: SchemaName, object_name: ObjectName) {
        if let Some(schema) = self.schemas.read().expect("to acquire write lock").get(schema_name) {
            self.open_tree(schema.clone(), object_name)
                .expect("no io error")
                .expect("no platform error")
                .expect("no definition error");
        }
    }

    fn open_database(&self, path_to_schema: PathBuf) -> io::Result<Result<Schema, StorageError>> {
        match self.open_database_with_failpoint(path_to_schema) {
            Ok(schema) => Ok(Ok(schema)),
            Err(error) => match error {
                SledError::Io(io_error) => Err(io_error),
                SledError::Corruption { .. } => Ok(Err(StorageError::Storage)),
                SledError::ReportableBug(_) => Ok(Err(StorageError::Storage)),
                SledError::Unsupported(_) => Ok(Err(StorageError::Storage)),
                SledError::CollectionNotFound(_) => Ok(Err(StorageError::Storage)),
            },
        }
    }

    fn open_database_with_failpoint(&self, path_to_schema: PathBuf) -> Result<Schema, SledError> {
        fail::fail_point!("sled-fail-to-open-db", |kind| Err(sled_error(kind)));
        sled::open(path_to_schema)
    }

    fn open_tree(
        &self,
        schema: Arc<Schema>,
        object_name: ObjectName,
    ) -> io::Result<Result<Result<Tree, DefinitionError>, StorageError>> {
        match self.open_tree_with_failpoint(schema, object_name) {
            Ok(tree) => Ok(Ok(Ok(tree))),
            Err(error) => match error {
                SledError::Io(io_error) => Err(io_error),
                SledError::Corruption { .. } => Ok(Err(StorageError::Storage)),
                SledError::ReportableBug(_) => Ok(Err(StorageError::Storage)),
                SledError::Unsupported(_) => Ok(Err(StorageError::Storage)),
                SledError::CollectionNotFound(_) => Ok(Ok(Err(DefinitionError::ObjectDoesNotExist))),
            },
        }
    }

    fn open_tree_with_failpoint(&self, schema: Arc<Schema>, object_name: ObjectName) -> Result<Tree, SledError> {
        fail::fail_point!("sled-fail-to-open-tree", |kind| Err(sled_error(kind)));
        schema.open_tree(object_name)
    }

    fn drop_database(&self, schema: Arc<Schema>) -> io::Result<Result<Result<(), DefinitionError>, StorageError>> {
        let mut io_errors = vec![];
        for tree_name in schema.tree_names() {
            let name = tree_name.clone();
            match self.drop_database_cascade_with_failpoint(schema.clone(), tree_name) {
                Ok(true) => log::info!("{:?} was dropped", name),
                Ok(false) => log::info!("{:?} was not dropped", name),
                Err(SledError::Io(_)) => io_errors.push(String::from_utf8_lossy(&name).into()),
                Err(SledError::Corruption { .. }) => return Ok(Err(StorageError::Storage)),
                Err(SledError::CollectionNotFound(_)) => return Ok(Err(StorageError::Storage)),
                Err(SledError::Unsupported(message)) => {
                    if message != "cannot remove the core structures" {
                        return Ok(Err(StorageError::Storage));
                    }
                }
                Err(SledError::ReportableBug(_)) => return Ok(Err(StorageError::Storage)),
            }
        }
        if io_errors.is_empty() {
            Ok(Ok(Ok(())))
        } else {
            Ok(Err(StorageError::CascadeIo(io_errors)))
        }
    }

    fn drop_database_cascade_with_failpoint(&self, schema: Arc<Schema>, tree: IVec) -> Result<bool, SledError> {
        fail::fail_point!("sled-fail-to-drop-db", |kind| {
            if tree == b"__sled__default" {
                Err(SledError::Unsupported("cannot remove the core structures".into()))
            } else {
                Err(sled_error(kind))
            }
        });
        schema.drop_tree(tree)
    }

    fn drop_tree_with_failpoint(&self, schema: Arc<Schema>, tree: IVec) -> Result<bool, SledError> {
        fail::fail_point!("sled-fail-to-drop-tree", |kind| Err(sled_error(kind)));
        schema.drop_tree(tree)
    }

    fn insert_into_tree_with_failpoint(
        &self,
        tree: &Tree,
        key: &Binary,
        values: &Binary,
    ) -> Result<Option<IVec>, SledError> {
        fail::fail_point!("sled-fail-to-insert-into-tree", |kind| Err(sled_error(kind)));
        tree.insert(key.to_bytes(), values.to_bytes())
    }

    fn tree_flush(
        &self,
        tree: Tree,
        io_operations: usize,
    ) -> io::Result<Result<Result<usize, DefinitionError>, StorageError>> {
        match self.tree_flush_with_failpoint(tree) {
            Ok(flushed) => {
                log::debug!("| io operations {:?} | flushed {:?} |", io_operations, flushed);
                Ok(Ok(Ok(io_operations)))
            }
            Err(error) => match error {
                SledError::Io(io_error) => Err(io_error),
                SledError::Corruption { .. } => Ok(Err(StorageError::Storage)),
                SledError::ReportableBug(_) => Ok(Err(StorageError::Storage)),
                SledError::Unsupported(_) => Ok(Err(StorageError::Storage)),
                SledError::CollectionNotFound(_) => Ok(Ok(Err(DefinitionError::ObjectDoesNotExist))),
            },
        }
    }

    fn tree_flush_with_failpoint(&self, tree: Tree) -> Result<usize, SledError> {
        fail::fail_point!("sled-fail-to-flush-tree", |kind| Err(sled_error(kind)));
        tree.flush()
    }

    fn iterator_over_tree_with_failpoint(&self, object: Tree) -> Box<dyn Iterator<Item = sled::Result<(IVec, IVec)>>> {
        fail::fail_point!("sled-fail-iterate-over-tree", |kind| Box::new(
            vec![Err(sled_error(kind))].into_iter()
        ));
        Box::new(object.iter())
    }
    fn remove_fro_tree_with_failpoint(&self, object: &Tree, key: Binary) -> Result<Option<IVec>, SledError> {
        fail::fail_point!("sled-fail-to-remove-from-tree", |kind| Err(sled_error(kind)));
        object.remove(key.to_bytes())
    }

    fn empty_iterator(&self) -> Box<dyn Iterator<Item = RowResult>> {
        Box::new(std::iter::empty())
    }
}

impl Database for PersistentDatabase {
    fn create_schema(&self, schema_name: SchemaName) -> io::Result<Result<Result<(), DefinitionError>, StorageError>> {
        if self
            .schemas
            .read()
            .expect("to acquire read lock")
            .contains_key(schema_name)
        {
            Ok(Ok(Err(DefinitionError::SchemaAlreadyExists)))
        } else {
            let path_to_schema = PathBuf::from(&self.path).join(schema_name);
            log::info!("path to schema {:?}", path_to_schema);
            self.open_database(path_to_schema).map(|storage| {
                storage.map(|schema| {
                    self.schemas
                        .write()
                        .expect("to acquire write lock")
                        .insert(schema_name.to_owned(), Arc::new(schema));
                    Ok(())
                })
            })
        }
    }

    fn drop_schema(&self, schema_name: SchemaName) -> io::Result<Result<Result<(), DefinitionError>, StorageError>> {
        match self.schemas.write().expect("to acquire write lock").remove(schema_name) {
            Some(schema) => self.drop_database(schema),
            None => Ok(Ok(Err(DefinitionError::SchemaDoesNotExist))),
        }
    }

    fn create_object(
        &self,
        schema_name: SchemaName,
        object_name: ObjectName,
    ) -> io::Result<Result<Result<(), DefinitionError>, StorageError>> {
        match self.schemas.read().expect("to acquire read lock").get(schema_name) {
            Some(schema) => {
                if schema.tree_names().contains(&(object_name.into())) {
                    Ok(Ok(Err(DefinitionError::ObjectAlreadyExists)))
                } else {
                    self.open_tree(schema.clone(), object_name)
                        .map(|io| io.map(|storage| storage.map(|_object| ())))
                }
            }
            None => Ok(Ok(Err(DefinitionError::SchemaDoesNotExist))),
        }
    }

    fn drop_object(
        &self,
        schema_name: SchemaName,
        object_name: ObjectName,
    ) -> io::Result<Result<Result<(), DefinitionError>, StorageError>> {
        match self.schemas.read().expect("to acquire read lock").get(schema_name) {
            Some(schema) => match self.drop_tree_with_failpoint(schema.clone(), object_name.as_bytes().into()) {
                Ok(true) => Ok(Ok(Ok(()))),
                Ok(false) => Ok(Ok(Err(DefinitionError::ObjectDoesNotExist))),
                Err(error) => match error {
                    SledError::Io(io_error) => Err(io_error),
                    SledError::Corruption { .. } => Ok(Err(StorageError::Storage)),
                    SledError::ReportableBug(_) => Ok(Err(StorageError::Storage)),
                    SledError::Unsupported(_) => Ok(Err(StorageError::Storage)),
                    SledError::CollectionNotFound(_) => Ok(Ok(Err(DefinitionError::ObjectDoesNotExist))),
                },
            },
            None => Ok(Ok(Err(DefinitionError::SchemaDoesNotExist))),
        }
    }

    fn write(
        &self,
        schema_name: SchemaName,
        object_name: ObjectName,
        rows: Vec<(Key, Values)>,
    ) -> io::Result<Result<Result<usize, DefinitionError>, StorageError>> {
        match self.schemas.read().expect("to acquire read lock").get(schema_name) {
            Some(schema) => {
                if schema.tree_names().contains(&(object_name.into())) {
                    match self.open_tree(schema.clone(), object_name) {
                        Ok(Ok(Ok(object))) => {
                            let mut written_rows = 0;
                            for (key, values) in rows.iter() {
                                match self.insert_into_tree_with_failpoint(&object, key, values) {
                                    Ok(_) => written_rows += 1,
                                    Err(error) => match error {
                                        SledError::Io(io_error) => return Err(io_error),
                                        SledError::Corruption { .. } => return Ok(Err(StorageError::Storage)),
                                        SledError::ReportableBug(_) => return Ok(Err(StorageError::Storage)),
                                        SledError::Unsupported(_) => return Ok(Err(StorageError::Storage)),
                                        SledError::CollectionNotFound(_) => {
                                            return Ok(Ok(Err(DefinitionError::ObjectDoesNotExist)));
                                        }
                                    },
                                }
                            }
                            self.tree_flush(object, written_rows)
                        }
                        otherwise => otherwise.map(|io| io.map(|storage| storage.map(|_object| 0))),
                    }
                } else {
                    Ok(Ok(Err(DefinitionError::ObjectDoesNotExist)))
                }
            }
            None => Ok(Ok(Err(DefinitionError::SchemaDoesNotExist))),
        }
    }

    fn read(
        &self,
        schema_name: SchemaName,
        object_name: ObjectName,
    ) -> io::Result<Result<Result<ReadCursor, DefinitionError>, StorageError>> {
        match self.schemas.read().expect("to acquire read lock").get(schema_name) {
            Some(schema) => {
                if schema.tree_names().contains(&(object_name.into())) {
                    match self.open_tree(schema.clone(), object_name) {
                        Ok(Ok(Ok(object))) => Ok(Ok(Ok(Box::new(self.iterator_over_tree_with_failpoint(object).map(
                            |item| match item {
                                Ok((key, values)) => Ok(Ok((
                                    Binary::with_data(key.to_vec()),
                                    Binary::with_data(values.to_vec()),
                                ))),
                                Err(error) => match error {
                                    SledError::Io(io_error) => Err(io_error),
                                    SledError::Corruption { .. } => Ok(Err(StorageError::Storage)),
                                    SledError::ReportableBug(_) => Ok(Err(StorageError::Storage)),
                                    SledError::Unsupported(_) => Ok(Err(StorageError::Storage)),
                                    SledError::CollectionNotFound(_) => Ok(Err(StorageError::Storage)),
                                },
                            },
                        ))))),
                        otherwise => otherwise.map(|io| io.map(|storage| storage.map(|_object| self.empty_iterator()))),
                    }
                } else {
                    log::error!(
                        "No namespace with {:?} doesn't contain {:?} object",
                        schema_name,
                        object_name
                    );
                    Ok(Ok(Err(DefinitionError::ObjectDoesNotExist)))
                }
            }
            None => {
                log::error!("No schema with {:?} name found", schema_name);
                Ok(Ok(Err(DefinitionError::SchemaDoesNotExist)))
            }
        }
    }

    fn delete(
        &self,
        schema_name: SchemaName,
        object_name: ObjectName,
        keys: Vec<Key>,
    ) -> io::Result<Result<Result<usize, DefinitionError>, StorageError>> {
        match self.schemas.read().expect("to acquire read lock").get(schema_name) {
            Some(schema) => {
                if schema.tree_names().contains(&(object_name.into())) {
                    match self.open_tree(schema.clone(), object_name) {
                        Ok(Ok(Ok(object))) => {
                            let mut deleted = 0;
                            for key in keys {
                                match self.remove_fro_tree_with_failpoint(&object, key) {
                                    Ok(_) => deleted += 1,
                                    Err(error) => match error {
                                        SledError::Io(io_error) => return Err(io_error),
                                        SledError::Corruption { .. } => return Ok(Err(StorageError::Storage)),
                                        SledError::ReportableBug(_) => return Ok(Err(StorageError::Storage)),
                                        SledError::Unsupported(_) => return Ok(Err(StorageError::Storage)),
                                        SledError::CollectionNotFound(_) => {
                                            return Ok(Ok(Err(DefinitionError::ObjectDoesNotExist)));
                                        }
                                    },
                                }
                            }
                            self.tree_flush(object, deleted)
                        }
                        otherwise => otherwise.map(|io| io.map(|storage| storage.map(|_object| 0))),
                    }
                } else {
                    Ok(Ok(Err(DefinitionError::ObjectDoesNotExist)))
                }
            }
            None => Ok(Ok(Err(DefinitionError::SchemaDoesNotExist))),
        }
    }
}

fn sled_error(kind: Option<String>) -> SledError {
    match kind.as_deref() {
        Some("io") => SledError::Io(ErrorKind::Other.into()),
        Some("corruption") => SledError::Corruption {
            at: Some(DiskPtr::Inline(500)),
            bt: (),
        },
        Some("bug") => SledError::ReportableBug("BUG".to_owned()),
        Some("unsupported(core_structure)") => SledError::Unsupported("cannot remove the core structures".into()),
        Some("unsupported") => SledError::Unsupported("Unsupported Operation".to_owned()),
        Some("collection_not_found") => SledError::CollectionNotFound(vec![].into()),
        _ => panic!("wrong sled error kind {:?}", &kind),
    }
}
