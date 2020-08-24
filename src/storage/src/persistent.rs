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

use crate::{Database, InitStatus, Key, ReadCursor, Row, StorageError, StorageResult};
use kernel::SystemError;
use representation::Binary;
use sled::{Db as Schema, Error as SledError};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, RwLock},
};

pub struct SledErrorMapper;

impl SledErrorMapper {
    fn map(error: SledError) -> SystemError {
        match error {
            SledError::CollectionNotFound(system_file) => SystemError::unrecoverable(format!(
                "System file [{}] can't be found",
                String::from_utf8(system_file.to_vec()).expect("name of system file")
            )),
            SledError::Unsupported(operation) => {
                SystemError::unrecoverable(format!("Unsupported operation [{}] was used on Sled", operation))
            }
            SledError::Corruption { at, bt: _bt } => {
                if let Some(at) = at {
                    SystemError::unrecoverable(format!("Sled encountered corruption at {}", at))
                } else {
                    SystemError::unrecoverable("Sled encountered corruption".to_owned())
                }
            }
            SledError::ReportableBug(description) => {
                SystemError::unrecoverable(format!("Sled encountered reportable BUG: {}", description))
            }
            SledError::Io(error) => SystemError::io(error),
        }
    }
}

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

    pub fn init(&self, schema_name: &str) -> StorageResult<InitStatus> {
        let path_to_schema = PathBuf::from(&self.path).join(schema_name);
        log::info!("path to schema {:?}", path_to_schema);
        match sled::open(path_to_schema) {
            Ok(schema) => {
                let recovered = schema.was_recovered();
                self.schemas
                    .write()
                    .expect("to acquire write lock")
                    .insert(schema_name.to_owned(), Arc::new(schema));
                log::debug!("namespaces after initialization {:?}", self.schemas);
                if recovered {
                    Ok(InitStatus::Loaded)
                } else {
                    Ok(InitStatus::Created)
                }
            }
            Err(error) => Err(StorageError::SystemError(SledErrorMapper::map(error))),
        }
    }

    pub fn open_object(&self, schema_name: &str, object_name: &str) {
        if let Some(schema) = self.schemas.read().expect("to acquire write lock").get(schema_name) {
            schema
                .open_tree(object_name)
                .expect("to open tree")
                .flush()
                .expect("to flush");
        }
    }

    fn new_schema(&self, schema_name: &str) -> StorageResult<Arc<Schema>> {
        if self
            .schemas
            .read()
            .expect("to acquire read lock")
            .contains_key(schema_name)
        {
            Err(StorageError::RuntimeCheckError)
        } else {
            let path_to_schema = PathBuf::from(&self.path).join(schema_name);
            log::info!("path to schema {:?}", path_to_schema);
            match sled::open(path_to_schema) {
                Ok(schema) => {
                    let schema = Arc::new(schema);
                    self.schemas
                        .write()
                        .expect("to acquire write lock")
                        .insert(schema_name.to_owned(), schema.clone());
                    Ok(schema)
                }
                Err(error) => Err(StorageError::SystemError(SledErrorMapper::map(error))),
            }
        }
    }
}

impl Database for PersistentDatabase {
    fn create_schema(&self, schema_name: &str) -> StorageResult<()> {
        self.new_schema(schema_name).map(|_| ())
    }

    fn drop_schema(&self, schema_name: &str) -> StorageResult<()> {
        match self.schemas.write().expect("to acquire write lock").remove(schema_name) {
            Some(schema) => {
                for tree in schema.tree_names() {
                    let name = tree.clone();
                    match schema.drop_tree(tree) {
                        Ok(true) => log::info!("{:?} was dropped", name),
                        Ok(false) => log::info!("{:?} was not dropped", name),
                        Err(error) => log::error!("{:?} was not dropped due to {:?}", name, error),
                    }
                }
                drop(schema);
                Ok(())
            }
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn create_object(&self, schema_name: &str, object_name: &str) -> StorageResult<()> {
        match self.schemas.read().expect("to acquire read lock").get(schema_name) {
            Some(schema) => {
                if schema.tree_names().contains(&(object_name.into())) {
                    Err(StorageError::RuntimeCheckError)
                } else {
                    match schema.open_tree(object_name) {
                        Ok(object) => {
                            log::debug!("tree {:?}.{:?} was created as {:?}", schema_name, object_name, object);
                            object.flush().expect("Ok");
                            Ok(())
                        }
                        Err(error) => Err(StorageError::SystemError(SledErrorMapper::map(error))),
                    }
                }
            }
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn drop_object(&self, schema_name: &str, object_name: &str) -> StorageResult<()> {
        match self.schemas.read().expect("to acquire read lock").get(schema_name) {
            Some(schema) => match schema.drop_tree(object_name.as_bytes()) {
                Ok(true) => Ok(()),
                Ok(false) => Err(StorageError::RuntimeCheckError),
                Err(error) => Err(StorageError::SystemError(SledErrorMapper::map(error))),
            },
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn write(&self, schema_name: &str, object_name: &str, rows: Vec<Row>) -> StorageResult<usize> {
        match self.schemas.read().expect("to acquire read lock").get(schema_name) {
            Some(schema) => {
                if schema.tree_names().contains(&(object_name.into())) {
                    match schema.open_tree(object_name) {
                        Ok(object) => {
                            let mut written_rows = 0;
                            for (key, values) in rows.iter() {
                                match object
                                    .insert::<sled::IVec, sled::IVec>(key.to_bytes().into(), values.to_bytes().into())
                                {
                                    Ok(_) => written_rows += 1,
                                    Err(error) => return Err(StorageError::SystemError(SledErrorMapper::map(error))),
                                }
                            }
                            object.flush().expect("Ok");
                            log::info!("{:?} data is written to {:?}.{:?}", rows, schema_name, object_name);
                            Ok(written_rows)
                        }
                        Err(error) => Err(StorageError::SystemError(SledErrorMapper::map(error))),
                    }
                } else {
                    Err(StorageError::RuntimeCheckError)
                }
            }
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn read(&self, schema_name: &str, object_name: &str) -> StorageResult<ReadCursor> {
        match self.schemas.read().expect("to acquire read lock").get(schema_name) {
            Some(schema) => {
                if schema.tree_names().contains(&(object_name.into())) {
                    match schema.open_tree(object_name) {
                        Ok(object) => Ok(Box::new(object.iter().map(|item| match item {
                            Ok((key, values)) => {
                                Ok((Binary::with_data(key.to_vec()), Binary::with_data(values.to_vec())))
                            }
                            Err(error) => Err(SledErrorMapper::map(error)),
                        }))),
                        Err(error) => Err(StorageError::SystemError(SledErrorMapper::map(error))),
                    }
                } else {
                    log::error!(
                        "No namespace with {:?} doesn't contain {:?} object",
                        schema_name,
                        object_name
                    );
                    Err(StorageError::RuntimeCheckError)
                }
            }
            None => {
                log::error!("No namespace with {:?} name found", schema_name);
                Err(StorageError::RuntimeCheckError)
            }
        }
    }

    fn delete(&self, schema_name: &str, object_name: &str, keys: Vec<Key>) -> StorageResult<usize> {
        match self.schemas.read().expect("to acquire read lock").get(schema_name) {
            Some(schema) => {
                if schema.tree_names().contains(&(object_name.into())) {
                    let mut deleted = 0;
                    match schema.open_tree(object_name) {
                        Ok(object) => {
                            for key in keys {
                                match object.remove(key.to_bytes()) {
                                    Ok(_) => deleted += 1,
                                    Err(error) => return Err(StorageError::SystemError(SledErrorMapper::map(error))),
                                }
                            }
                            object.flush().expect("Ok");
                        }
                        Err(error) => return Err(StorageError::SystemError(SledErrorMapper::map(error))),
                    }
                    Ok(deleted)
                } else {
                    Err(StorageError::RuntimeCheckError)
                }
            }
            None => Err(StorageError::RuntimeCheckError),
        }
    }
}

#[cfg(test)]
mod sled_error_mapper {
    use super::*;
    use sled::DiskPtr;
    use std::io::{Error, ErrorKind};

    #[test]
    fn collection_not_found() {
        assert_eq!(
            SledErrorMapper::map(SledError::CollectionNotFound(sled::IVec::from("test"))),
            SystemError::unrecoverable("System file [test] can't be found".to_owned())
        )
    }

    #[test]
    fn unsupported() {
        assert_eq!(
            SledErrorMapper::map(SledError::Unsupported("NOT_SUPPORTED".to_owned())),
            SystemError::unrecoverable("Unsupported operation [NOT_SUPPORTED] was used on Sled".to_owned())
        )
    }

    #[test]
    fn corruption_with_position() {
        let at = DiskPtr::Inline(900);
        assert_eq!(
            SledErrorMapper::map(SledError::Corruption { at: Some(at), bt: () }),
            SystemError::unrecoverable(format!("Sled encountered corruption at {}", at))
        )
    }

    #[test]
    fn corruption_without_position() {
        assert_eq!(
            SledErrorMapper::map(SledError::Corruption { at: None, bt: () }),
            SystemError::unrecoverable("Sled encountered corruption".to_owned())
        )
    }

    #[test]
    fn reportable_bug() {
        let description = "SOME_BUG_HERE";
        assert_eq!(
            SledErrorMapper::map(SledError::ReportableBug(description.to_owned())),
            SystemError::unrecoverable(format!("Sled encountered reportable BUG: {}", description))
        );
    }

    #[test]
    fn io() {
        assert_eq!(
            SledErrorMapper::map(SledError::Io(Error::new(ErrorKind::Other, "oh no!"))),
            SystemError::io(Error::new(ErrorKind::Other, "oh no!"))
        )
    }
}
