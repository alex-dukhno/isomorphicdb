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

use crate::{Key, ReadCursor, Row, Storage, StorageError, StorageResult, Values};
use kernel::SystemResult;
use std::{
    collections::{BTreeMap, HashMap},
    sync::RwLock,
};

#[derive(Default, Debug)]
struct StorageObject {
    records: BTreeMap<Key, Values>,
}

#[derive(Default, Debug)]
struct Namespace {
    pub objects: HashMap<String, StorageObject>,
}

#[derive(Default)]
pub struct InMemoryDatabaseCatalog {
    namespaces: RwLock<HashMap<String, Namespace>>,
}

impl Storage for InMemoryDatabaseCatalog {
    fn create_namespace(&self, namespace: &str) -> StorageResult<()> {
        if self
            .namespaces
            .read()
            .expect("to acquire read lock")
            .contains_key(namespace)
        {
            Err(StorageError::RuntimeCheckError)
        } else {
            self.namespaces
                .write()
                .expect("to acquire write lock")
                .insert(namespace.to_owned(), Namespace::default());
            Ok(())
        }
    }

    fn drop_namespace(&self, namespace: &str) -> StorageResult<()> {
        match self
            .namespaces
            .write()
            .expect("to acquire write lock")
            .remove(namespace)
        {
            Some(_namespace) => Ok(()),
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn create_tree(&self, namespace: &str, object_name: &str) -> StorageResult<()> {
        match self
            .namespaces
            .write()
            .expect("to acquire write lock")
            .get_mut(namespace)
        {
            Some(namespace) => {
                if namespace.objects.contains_key(object_name) {
                    Err(StorageError::RuntimeCheckError)
                } else {
                    namespace
                        .objects
                        .insert(object_name.to_owned(), StorageObject::default());
                    Ok(())
                }
            }
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn drop_tree(&self, namespace: &str, object_name: &str) -> StorageResult<()> {
        match self
            .namespaces
            .write()
            .expect("to acquire write lock")
            .get_mut(namespace)
        {
            Some(namespace) => match namespace.objects.remove(object_name) {
                Some(_) => Ok(()),
                None => Err(StorageError::RuntimeCheckError),
            },
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn write(&self, namespace: &str, object_name: &str, rows: Vec<(Key, Values)>) -> StorageResult<usize> {
        match self
            .namespaces
            .write()
            .expect("to acquire write lock")
            .get_mut(namespace)
        {
            Some(namespace) => match namespace.objects.get_mut(object_name) {
                Some(object) => {
                    let len = rows.len();
                    for (key, value) in rows {
                        object.records.insert(key, value);
                    }
                    Ok(len)
                }
                None => Err(StorageError::RuntimeCheckError),
            },
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn read(&self, namespace: &str, object_name: &str) -> StorageResult<ReadCursor> {
        match self.namespaces.read().expect("to acquire read lock").get(namespace) {
            Some(namespace) => match namespace.objects.get(object_name) {
                Some(object) => Ok(Box::new(
                    object
                        .records
                        .clone()
                        .into_iter()
                        .map(Ok)
                        .collect::<Vec<SystemResult<Row>>>()
                        .into_iter(),
                )),
                None => Err(StorageError::RuntimeCheckError),
            },
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn delete(&self, namespace: &str, object_name: &str, keys: Vec<Key>) -> StorageResult<usize> {
        match self
            .namespaces
            .write()
            .expect("to acquire write lock")
            .get_mut(namespace)
        {
            Some(namespace) => match namespace.objects.get_mut(object_name) {
                Some(object) => {
                    object.records = object
                        .records
                        .clone()
                        .into_iter()
                        .filter(|(key, _values)| !keys.contains(key))
                        .collect();
                    Ok(keys.len())
                }
                None => Err(StorageError::RuntimeCheckError),
            },
            None => Err(StorageError::RuntimeCheckError),
        }
    }
}
