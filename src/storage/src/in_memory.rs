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

use crate::{Database, Key, ReadCursor, Row, StorageError, StorageResult, Values};
use kernel::SystemResult;
use std::{
    collections::{BTreeMap, HashMap},
    sync::RwLock,
};

type Name = String;

#[derive(Default, Debug)]
struct StorageObject {
    records: BTreeMap<Key, Values>,
}

#[derive(Default, Debug)]
struct Schema {
    pub objects: HashMap<Name, StorageObject>,
}

#[derive(Default)]
pub struct InMemoryDatabase {
    schemas: RwLock<HashMap<Name, Schema>>,
}

impl Database for InMemoryDatabase {
    fn create_schema(&self, schema_name: &str) -> StorageResult<()> {
        if self
            .schemas
            .read()
            .expect("to acquire read lock")
            .contains_key(schema_name)
        {
            Err(StorageError::RuntimeCheckError)
        } else {
            self.schemas
                .write()
                .expect("to acquire write lock")
                .insert(schema_name.to_owned(), Schema::default());
            Ok(())
        }
    }

    fn drop_schema(&self, schema_name: &str) -> StorageResult<()> {
        match self.schemas.write().expect("to acquire write lock").remove(schema_name) {
            Some(_namespace) => Ok(()),
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn create_object(&self, schema_name: &str, object_name: &str) -> StorageResult<()> {
        match self
            .schemas
            .write()
            .expect("to acquire write lock")
            .get_mut(schema_name)
        {
            Some(schema) => {
                if schema.objects.contains_key(object_name) {
                    Err(StorageError::RuntimeCheckError)
                } else {
                    schema.objects.insert(object_name.to_owned(), StorageObject::default());
                    Ok(())
                }
            }
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn drop_object(&self, schema_name: &str, object_name: &str) -> StorageResult<()> {
        match self
            .schemas
            .write()
            .expect("to acquire write lock")
            .get_mut(schema_name)
        {
            Some(schema) => match schema.objects.remove(object_name) {
                Some(_) => Ok(()),
                None => Err(StorageError::RuntimeCheckError),
            },
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn write(&self, schema_name: &str, object_name: &str, rows: Vec<(Key, Values)>) -> StorageResult<usize> {
        match self
            .schemas
            .write()
            .expect("to acquire write lock")
            .get_mut(schema_name)
        {
            Some(schema) => match schema.objects.get_mut(object_name) {
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

    fn read(&self, schema_name: &str, object_name: &str) -> StorageResult<ReadCursor> {
        match self.schemas.read().expect("to acquire read lock").get(schema_name) {
            Some(schema) => match schema.objects.get(object_name) {
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

    fn delete(&self, schema_name: &str, object_name: &str, keys: Vec<Key>) -> StorageResult<usize> {
        match self
            .schemas
            .write()
            .expect("to acquire write lock")
            .get_mut(schema_name)
        {
            Some(schema) => match schema.objects.get_mut(object_name) {
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
