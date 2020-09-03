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

use crate::{Database, DefinitionError, Key, ObjectId, ReadCursor, RowResult, SchemaId, StorageError, Values};
use std::{
    collections::{BTreeMap, HashMap},
    io::{self},
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
    fn create_schema(&self, schema_name: SchemaId) -> io::Result<Result<Result<(), DefinitionError>, StorageError>> {
        if self
            .schemas
            .read()
            .expect("to acquire read lock")
            .contains_key(schema_name)
        {
            Ok(Ok(Err(DefinitionError::SchemaAlreadyExists)))
        } else {
            self.schemas
                .write()
                .expect("to acquire write lock")
                .insert(schema_name.to_owned(), Schema::default());
            Ok(Ok(Ok(())))
        }
    }

    fn drop_schema(&self, schema_name: SchemaId) -> io::Result<Result<Result<(), DefinitionError>, StorageError>> {
        match self.schemas.write().expect("to acquire write lock").remove(schema_name) {
            Some(_namespace) => Ok(Ok(Ok(()))),
            None => Ok(Ok(Err(DefinitionError::SchemaDoesNotExist))),
        }
    }

    fn create_object(
        &self,
        schema_name: SchemaId,
        object_name: ObjectId,
    ) -> io::Result<Result<Result<(), DefinitionError>, StorageError>> {
        match self
            .schemas
            .write()
            .expect("to acquire write lock")
            .get_mut(schema_name)
        {
            Some(schema) => {
                if schema.objects.contains_key(object_name) {
                    Ok(Ok(Err(DefinitionError::ObjectAlreadyExists)))
                } else {
                    schema.objects.insert(object_name.to_owned(), StorageObject::default());
                    Ok(Ok(Ok(())))
                }
            }
            None => Ok(Ok(Err(DefinitionError::SchemaDoesNotExist))),
        }
    }

    fn drop_object(
        &self,
        schema_name: SchemaId,
        object_name: ObjectId,
    ) -> io::Result<Result<Result<(), DefinitionError>, StorageError>> {
        match self
            .schemas
            .write()
            .expect("to acquire write lock")
            .get_mut(schema_name)
        {
            Some(schema) => match schema.objects.remove(object_name) {
                Some(_) => Ok(Ok(Ok(()))),
                None => Ok(Ok(Err(DefinitionError::ObjectDoesNotExist))),
            },
            None => Ok(Ok(Err(DefinitionError::SchemaDoesNotExist))),
        }
    }

    fn write(
        &self,
        schema_name: SchemaId,
        object_name: ObjectId,
        rows: Vec<(Key, Values)>,
    ) -> io::Result<Result<Result<usize, DefinitionError>, StorageError>> {
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
                    Ok(Ok(Ok(len)))
                }
                None => Ok(Ok(Err(DefinitionError::ObjectDoesNotExist))),
            },
            None => Ok(Ok(Err(DefinitionError::SchemaDoesNotExist))),
        }
    }

    fn read(
        &self,
        schema_name: SchemaId,
        object_name: ObjectId,
    ) -> io::Result<Result<Result<ReadCursor, DefinitionError>, StorageError>> {
        match self.schemas.read().expect("to acquire read lock").get(schema_name) {
            Some(schema) => match schema.objects.get(object_name) {
                Some(object) => Ok(Ok(Ok(Box::new(
                    object
                        .records
                        .clone()
                        .into_iter()
                        .map(|value| Ok(Ok(value)))
                        .collect::<Vec<RowResult>>()
                        .into_iter(),
                )))),
                None => Ok(Ok(Err(DefinitionError::ObjectDoesNotExist))),
            },
            None => Ok(Ok(Err(DefinitionError::SchemaDoesNotExist))),
        }
    }

    fn delete(
        &self,
        schema_name: SchemaId,
        object_name: ObjectId,
        keys: Vec<Key>,
    ) -> io::Result<Result<Result<usize, DefinitionError>, StorageError>> {
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
                    Ok(Ok(Ok(keys.len())))
                }
                None => Ok(Ok(Err(DefinitionError::ObjectDoesNotExist))),
            },
            None => Ok(Ok(Err(DefinitionError::SchemaDoesNotExist))),
        }
    }
}
