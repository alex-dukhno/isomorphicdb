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

use crate::backend::{
    self, BackendStorage, CreateObjectError, DropObjectError, NamespaceAlreadyExists, NamespaceDoesNotExist,
    OperationOnObjectError, SledBackendStorage,
};
use crate::{
    CreateTableError, DropTableError, OperationOnTableError, Projection, SchemaAlreadyExists, SchemaDoesNotExist,
};
use kernel::{SystemError, SystemResult};

pub struct FrontendStorage<P: BackendStorage> {
    key_id_generator: usize,
    persistent: P,
}

impl FrontendStorage<SledBackendStorage> {
    pub fn default() -> SystemResult<Self> {
        Self::new(SledBackendStorage::default())
    }
}

impl<P: BackendStorage> FrontendStorage<P> {
    pub fn new(mut persistent: P) -> SystemResult<Self> {
        match persistent.create_namespace("system")? {
            Ok(()) => match persistent.create_object("system", "columns")? {
                Ok(()) => Ok(Self {
                    key_id_generator: 0,
                    persistent,
                }),
                Err(CreateObjectError::NamespaceDoesNotExist) => {
                    // something that can't be possible
                    Err(SystemError::unrecoverable("system namespace does not exist".to_owned()))
                }
                Err(CreateObjectError::ObjectAlreadyExists) => Err(SystemError::unrecoverable(
                    // something that can't be possible
                    "system table 'columns' already exists".to_owned(),
                )),
            },
            Err(NamespaceAlreadyExists) => {
                Err(SystemError::unrecoverable("system namespace already exists".to_owned()))
            }
        }
    }

    #[allow(clippy::match_wild_err_arm, clippy::map_entry)]
    pub fn create_schema(&mut self, schema_name: &str) -> SystemResult<Result<(), SchemaAlreadyExists>> {
        match self.persistent.create_namespace(schema_name)? {
            Ok(()) => Ok(Ok(())),
            Err(NamespaceAlreadyExists) => Ok(Err(SchemaAlreadyExists)),
        }
    }

    pub fn drop_schema(&mut self, schema_name: &str) -> SystemResult<Result<(), SchemaDoesNotExist>> {
        match self.persistent.drop_namespace(schema_name)? {
            Ok(()) => Ok(Ok(())),
            Err(NamespaceDoesNotExist) => Ok(Err(SchemaDoesNotExist)),
        }
    }

    pub fn create_table(
        &mut self,
        schema_name: &str,
        table_name: &str,
        column_names: Vec<String>,
    ) -> SystemResult<Result<(), CreateTableError>> {
        match self.persistent.create_object(schema_name, table_name)? {
            Ok(()) => {
                match self.persistent.write(
                    "system",
                    "columns",
                    vec![(
                        table_name.as_bytes().to_vec(),
                        column_names.into_iter().map(|s| s.as_bytes().to_vec()).collect(),
                    )],
                )? {
                    Ok(_) => {
                        log::info!("column data is recorded");
                    }
                    Err(e) => {
                        let message = format!("something went wrong {:?}", e);
                        log::error!("{}", message);
                        return Err(SystemError::unrecoverable(message));
                    }
                }
                Ok(Ok(()))
            }
            Err(CreateObjectError::ObjectAlreadyExists) => Ok(Err(CreateTableError::TableAlreadyExists)),
            Err(CreateObjectError::NamespaceDoesNotExist) => Ok(Err(CreateTableError::SchemaDoesNotExist)),
        }
    }

    pub fn table_columns(
        &mut self,
        _schema_name: &str,
        table_name: &str,
    ) -> SystemResult<Result<Vec<String>, OperationOnTableError>> {
        let reads = self.persistent.read("system", "columns")?;
        match reads {
            Ok(reads) => Ok(Ok(reads
                .map(backend::Result::unwrap)
                .filter(|(table, _columns)| *table == table_name.as_bytes().to_vec())
                .map(|(_id, columns)| columns.iter().map(|c| String::from_utf8(c.to_vec()).unwrap()).collect())
                .next()
                .unwrap_or_default())),
            Err(OperationOnObjectError::ObjectDoesNotExist) => Ok(Err(OperationOnTableError::TableDoesNotExist)),
            Err(OperationOnObjectError::NamespaceDoesNotExist) => Ok(Err(OperationOnTableError::SchemaDoesNotExist)),
        }
    }

    pub fn drop_table(&mut self, schema_name: &str, table_name: &str) -> SystemResult<Result<(), DropTableError>> {
        match self.persistent.drop_object(schema_name, table_name)? {
            Ok(()) => Ok(Ok(())),
            Err(DropObjectError::ObjectDoesNotExist) => Ok(Err(DropTableError::TableDoesNotExist)),
            Err(DropObjectError::NamespaceDoesNotExist) => Ok(Err(DropTableError::SchemaDoesNotExist)),
        }
    }

    pub fn insert_into(
        &mut self,
        schema_name: &str,
        table_name: &str,
        values: Vec<Vec<String>>,
    ) -> SystemResult<Result<(), OperationOnTableError>> {
        let mut to_write = vec![];
        for value in values {
            let key = self.key_id_generator.to_be_bytes().to_vec();
            to_write.push((key, value.iter().map(|s| s.clone().into_bytes()).collect()));
            self.key_id_generator += 1;
        }
        match self.persistent.write(schema_name, table_name, to_write)? {
            Ok(_size) => Ok(Ok(())),
            Err(OperationOnObjectError::ObjectDoesNotExist) => Ok(Err(OperationOnTableError::TableDoesNotExist)),
            Err(OperationOnObjectError::NamespaceDoesNotExist) => Ok(Err(OperationOnTableError::SchemaDoesNotExist)),
        }
    }

    pub fn select_all_from(
        &mut self,
        schema_name: &str,
        table_name: &str,
        columns: Vec<String>,
    ) -> SystemResult<Result<Projection, OperationOnTableError>> {
        match self.table_columns(schema_name, table_name)? {
            Ok(all_columns) => {
                let mut column_indexes = vec![];
                let mut non_existing_columns = vec![];
                for (i, column) in columns.iter().enumerate() {
                    let mut found = None;
                    for (index, name) in all_columns.iter().enumerate() {
                        if name == column {
                            found = Some((index, i));
                            break;
                        }
                    }

                    if let Some(index_pair) = found {
                        column_indexes.push(index_pair);
                    } else {
                        non_existing_columns.push(column.clone());
                    }
                }

                if !non_existing_columns.is_empty() {
                    return Ok(Err(OperationOnTableError::ColumnDoesNotExist(non_existing_columns)));
                }

                let data = match self.persistent.read(schema_name, table_name)? {
                    Ok(read) => read
                        .map(backend::Result::unwrap)
                        .map(|(_key, values)| values)
                        .map(|bytes| {
                            let all_values = bytes
                                .iter()
                                .map(|b| String::from_utf8(b.to_vec()).unwrap())
                                .collect::<Vec<String>>();
                            let mut values = vec![];
                            for (origin, ord) in &column_indexes {
                                for (index, value) in all_values.iter().enumerate() {
                                    if index == *origin {
                                        values.push((ord, value.clone()))
                                    }
                                }
                            }
                            values.into_iter().map(|(_, value)| value).collect()
                        })
                        .collect(),
                    Err(OperationOnObjectError::ObjectDoesNotExist) => {
                        return Ok(Err(OperationOnTableError::TableDoesNotExist))
                    }
                    Err(OperationOnObjectError::NamespaceDoesNotExist) => {
                        unimplemented!()
                        // return Ok(Err(OperationOnTableError::SchemaDoesNotExist))
                    }
                };
                Ok(Ok((columns, data)))
            }
            Err(e) => Ok(Err(e)),
        }
    }

    pub fn update_all(
        &mut self,
        schema_name: &str,
        table_name: &str,
        value: String,
    ) -> SystemResult<Result<usize, OperationOnTableError>> {
        let reads = self.persistent.read(schema_name, table_name)?;
        match reads {
            Ok(reads) => {
                let to_update: Vec<(Vec<u8>, Vec<Vec<u8>>)> = reads
                    .map(backend::Result::unwrap)
                    .map(|(key, _)| (key, vec![value.clone().into_bytes()]))
                    .collect();

                let len = to_update.len();
                self.persistent.write(schema_name, table_name, to_update)?.unwrap();
                Ok(Ok(len))
            }
            Err(OperationOnObjectError::ObjectDoesNotExist) => Ok(Err(OperationOnTableError::TableDoesNotExist)),
            Err(OperationOnObjectError::NamespaceDoesNotExist) => Ok(Err(OperationOnTableError::SchemaDoesNotExist)),
        }
    }

    pub fn delete_all_from(
        &mut self,
        schema_name: &str,
        table_name: &str,
    ) -> SystemResult<Result<usize, OperationOnTableError>> {
        let reads = self.persistent.read(schema_name, table_name)?;

        let to_delete: Vec<Vec<u8>> = match reads {
            Ok(reads) => reads.map(backend::Result::unwrap).map(|(key, _)| key).collect(),
            Err(OperationOnObjectError::ObjectDoesNotExist) => {
                return Ok(Err(OperationOnTableError::TableDoesNotExist))
            }
            Err(OperationOnObjectError::NamespaceDoesNotExist) => {
                return Ok(Err(OperationOnTableError::SchemaDoesNotExist))
            }
        };

        match self.persistent.delete(schema_name, table_name, to_delete)? {
            Ok(len) => Ok(Ok(len)),
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests;
