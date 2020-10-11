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

use crate::{Database, Key, ObjectName, ReadCursor, SchemaName, StorageError, Values};
use binary::RowResult;
use dashmap::DashMap;
use sql_model::sql_errors::DefinitionError;
use std::{
    collections::BTreeMap,
    io::{self},
};

type Name = String;

#[derive(Default, Debug)]
struct StorageObject {
    records: BTreeMap<Key, Values>,
}

#[derive(Default, Debug)]
struct Schema {
    pub objects: DashMap<Name, StorageObject>,
}

#[derive(Default)]
pub struct InMemoryDatabase {
    schemas: DashMap<Name, Schema>,
}

impl Database for InMemoryDatabase {
    fn create_schema(&self, schema_name: SchemaName) -> io::Result<Result<Result<(), DefinitionError>, StorageError>> {
        if self.schemas.contains_key(schema_name) {
            Ok(Ok(Err(DefinitionError::SchemaAlreadyExists)))
        } else {
            self.schemas.insert(schema_name.to_owned(), Schema::default());
            Ok(Ok(Ok(())))
        }
    }

    fn drop_schema(&self, schema_name: SchemaName) -> io::Result<Result<Result<(), DefinitionError>, StorageError>> {
        match self.schemas.remove(schema_name) {
            Some(_namespace) => Ok(Ok(Ok(()))),
            None => Ok(Ok(Err(DefinitionError::SchemaDoesNotExist))),
        }
    }

    fn create_object(
        &self,
        schema_name: SchemaName,
        object_name: ObjectName,
    ) -> io::Result<Result<Result<(), DefinitionError>, StorageError>> {
        match self.schemas.get(schema_name) {
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
        schema_name: SchemaName,
        object_name: ObjectName,
    ) -> io::Result<Result<Result<(), DefinitionError>, StorageError>> {
        match self.schemas.get(schema_name) {
            Some(schema) => match schema.objects.remove(object_name) {
                Some(_) => Ok(Ok(Ok(()))),
                None => Ok(Ok(Err(DefinitionError::ObjectDoesNotExist))),
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
        match self.schemas.get(schema_name) {
            Some(schema) => match schema.objects.get_mut(object_name) {
                Some(mut object) => {
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
        schema_name: SchemaName,
        object_name: ObjectName,
    ) -> io::Result<Result<Result<ReadCursor, DefinitionError>, StorageError>> {
        match self.schemas.get(schema_name) {
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
        schema_name: SchemaName,
        object_name: ObjectName,
        keys: Vec<Key>,
    ) -> io::Result<Result<Result<usize, DefinitionError>, StorageError>> {
        match self.schemas.get_mut(schema_name) {
            Some(schema) => match schema.objects.get_mut(object_name) {
                Some(mut object) => {
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
