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

use crate::{InitStatus, Key, ReadCursor, Row, Storage, StorageError, StorageResult};
use kernel::SystemError;
use representation::Binary;
use sled::{Db as NameSpace, Error as SledError};
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

pub struct PersistentDatabaseCatalog {
    path: PathBuf,
    namespaces: RwLock<HashMap<String, Arc<NameSpace>>>,
}

impl PersistentDatabaseCatalog {
    pub fn new(path: PathBuf) -> PersistentDatabaseCatalog {
        PersistentDatabaseCatalog {
            path,
            namespaces: RwLock::default(),
        }
    }

    pub fn init(&self, namespace_name: &str) -> StorageResult<InitStatus> {
        let path_to_namespace = PathBuf::from(&self.path).join(namespace_name);
        match sled::open(path_to_namespace) {
            Ok(namespace) => {
                let recovered = namespace.was_recovered();
                self.namespaces
                    .write()
                    .expect("to acquire write lock")
                    .insert(namespace_name.to_owned(), Arc::new(namespace));
                log::debug!("namespaces after initialization {:?}", self.namespaces);
                if recovered {
                    Ok(InitStatus::Loaded)
                } else {
                    Ok(InitStatus::Created)
                }
            }
            Err(error) => Err(StorageError::SystemError(SledErrorMapper::map(error))),
        }
    }

    fn new_namespace(&self, namespace: &str) -> StorageResult<Arc<NameSpace>> {
        if self
            .namespaces
            .read()
            .expect("to acquire read lock")
            .contains_key(namespace)
        {
            Err(StorageError::RuntimeCheckError)
        } else {
            match sled::Config::default().temporary(true).open() {
                Ok(database) => {
                    let database = Arc::new(database);
                    self.namespaces
                        .write()
                        .expect("to acquire write lock")
                        .insert(namespace.to_owned(), database.clone());
                    Ok(database)
                }
                Err(error) => Err(StorageError::SystemError(SledErrorMapper::map(error))),
            }
        }
    }
}

impl Storage for PersistentDatabaseCatalog {
    fn create_namespace(&self, namespace: &str) -> StorageResult<()> {
        self.new_namespace(namespace).map(|_| ())
    }

    fn drop_namespace(&self, namespace: &str) -> StorageResult<()> {
        match self
            .namespaces
            .write()
            .expect("to acquire write lock")
            .remove(namespace)
        {
            Some(namespace) => {
                drop(namespace);
                Ok(())
            }
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn create_tree(&self, namespace_name: &str, object_name: &str) -> StorageResult<()> {
        match self
            .namespaces
            .read()
            .expect("to acquire read lock")
            .get(namespace_name)
        {
            Some(namespace) => {
                if namespace.tree_names().contains(&(object_name.into())) {
                    Err(StorageError::RuntimeCheckError)
                } else {
                    match namespace.open_tree(object_name) {
                        Ok(object) => {
                            log::debug!(
                                "tree {:?}.{:?} was created as {:?}",
                                namespace_name,
                                object_name,
                                object
                            );
                            Ok(())
                        }
                        Err(error) => Err(StorageError::SystemError(SledErrorMapper::map(error))),
                    }
                }
            }
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn drop_tree(&self, namespace: &str, object_name: &str) -> StorageResult<()> {
        match self.namespaces.read().expect("to acquire read lock").get(namespace) {
            Some(namespace) => match namespace.drop_tree(object_name.as_bytes()) {
                Ok(true) => Ok(()),
                Ok(false) => Err(StorageError::RuntimeCheckError),
                Err(error) => Err(StorageError::SystemError(SledErrorMapper::map(error))),
            },
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn write(&self, namespace: &str, object_name: &str, rows: Vec<Row>) -> StorageResult<usize> {
        match self.namespaces.read().expect("to acquire read lock").get(namespace) {
            Some(namespace) => {
                if namespace.tree_names().contains(&(object_name.into())) {
                    match namespace.open_tree(object_name) {
                        Ok(object) => {
                            let mut written_rows = 0;
                            for (key, values) in rows {
                                match object
                                    .insert::<sled::IVec, sled::IVec>(key.to_bytes().into(), values.to_bytes().into())
                                {
                                    Ok(_) => written_rows += 1,
                                    Err(error) => return Err(StorageError::SystemError(SledErrorMapper::map(error))),
                                }
                            }
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

    fn read(&self, namespace: &str, object_name: &str) -> StorageResult<ReadCursor> {
        match self.namespaces.read().expect("to acquire read lock").get(namespace) {
            Some(namespace) => {
                if namespace.tree_names().contains(&(object_name.into())) {
                    match namespace.open_tree(object_name) {
                        Ok(object) => Ok(Box::new(object.iter().map(|item| match item {
                            Ok((key, values)) => {
                                Ok((Binary::with_data(key.to_vec()), Binary::with_data(values.to_vec())))
                            }
                            Err(error) => Err(SledErrorMapper::map(error)),
                        }))),
                        Err(error) => Err(StorageError::SystemError(SledErrorMapper::map(error))),
                    }
                } else {
                    Err(StorageError::RuntimeCheckError)
                }
            }
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn delete(&self, namespace: &str, object_name: &str, keys: Vec<Key>) -> StorageResult<usize> {
        match self.namespaces.read().expect("to acquire read lock").get(namespace) {
            Some(namespace) => {
                if namespace.tree_names().contains(&(object_name.into())) {
                    let mut deleted = 0;
                    match namespace.open_tree(object_name) {
                        Ok(object) => {
                            for key in keys {
                                match object.remove(key.to_bytes()) {
                                    Ok(_) => deleted += 1,
                                    Err(error) => return Err(StorageError::SystemError(SledErrorMapper::map(error))),
                                }
                            }
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

#[cfg(test)]
mod tests {}
