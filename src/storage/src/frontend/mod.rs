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

use std::collections::BTreeSet;

use crate::{
    backend::{
        self, BackendStorage, CreateObjectError, DropObjectError, NamespaceAlreadyExists, NamespaceDoesNotExist,
        OperationOnObjectError, Row, SledBackendStorage,
    },
    ColumnDefinition, CreateTableError, DropTableError, OperationOnTableError, Projection, SchemaAlreadyExists,
    SchemaDoesNotExist, TableDescription,
};
use kernel::{SystemError, SystemResult};
use std::hint::unreachable_unchecked;

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
        match persistent.create_namespace_with_objects("system", vec!["columns"])? {
            Ok(()) => Ok(Self {
                key_id_generator: 0,
                persistent,
            }),
            Err(NamespaceAlreadyExists) => {
                Err(SystemError::unrecoverable("system namespace already exists".to_owned()))
            }
        }
    }

    pub fn table_descriptor(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> SystemResult<Result<TableDescription, OperationOnTableError>> {
        match self.persistent.check_for_table(schema_name, table_name)? {
            Ok(()) => {}
            Err(OperationOnObjectError::NamespaceDoesNotExist) => {
                return Ok(Err(OperationOnTableError::SchemaDoesNotExist))
            }
            Err(OperationOnObjectError::ObjectDoesNotExist) => {
                return Ok(Err(OperationOnTableError::TableDoesNotExist))
            }
        }

        // we know the table exists
        let columns_metadata = self.table_columns(schema_name, table_name)?;

        Ok(Ok(TableDescription::new(schema_name, table_name, columns_metadata)))
    }

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
        column_names: &[ColumnDefinition],
    ) -> SystemResult<Result<(), CreateTableError>> {
        match self.persistent.create_object(schema_name, table_name)? {
            Ok(()) => self
                .persistent
                .write(
                    "system",
                    "columns",
                    vec![(
                        (schema_name.to_owned() + table_name).as_bytes().to_vec(),
                        column_names
                            .iter()
                            .map(|column_defs| bincode::serialize(&column_defs).unwrap())
                            .collect::<Vec<Vec<u8>>>()
                            .join(&b'|')
                            .to_vec(),
                    )],
                )?
                .map(|_| {
                    log::info!("column data is recorded");
                    Ok(())
                })
                .map_err(|error| {
                    let message = format!(
                        "Can't access \"system.columns\" table to read columns metadata because of {:?}",
                        error
                    );
                    log::error!("{}", message);
                    SystemError::unrecoverable(message)
                }),
            Err(CreateObjectError::ObjectAlreadyExists) => Ok(Err(CreateTableError::TableAlreadyExists)),
            Err(CreateObjectError::NamespaceDoesNotExist) => Ok(Err(CreateTableError::SchemaDoesNotExist)),
        }
    }

    pub fn table_columns(&self, schema_name: &str, table_name: &str) -> SystemResult<Vec<ColumnDefinition>> {
        self.persistent
            .read("system", "columns")?
            .map(|reads| {
                reads
                    .map(backend::Result::unwrap)
                    .filter(|(table, _columns)| *table == (schema_name.to_owned() + table_name).as_bytes().to_vec())
                    .map(|(_id, columns)| {
                        columns
                            .split(|b| *b == b'|')
                            .filter(|v| !v.is_empty())
                            .map(|c| bincode::deserialize(c).unwrap())
                            .collect::<Vec<_>>()
                    })
                    .next()
                    .unwrap_or_default()
            })
            .map_err(|error| {
                let message = format!(
                    "Can't access \"system.columns\" table to read columns metadata because of {:?}",
                    error
                );
                log::error!("{}", message);
                SystemError::unrecoverable(message)
            })
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
        rows: Vec<Vec<u8>>,
    ) -> SystemResult<Result<(), OperationOnTableError>> {
        let keyed_rows = rows.into_iter().map(|row| {
            // this is bad
            let key = self.key_id_generator.to_be_bytes().to_vec();
            self.key_id_generator += 1;
            (key, row)
        }).collect::<Vec<(Vec<u8>, Vec<u8>)>>();

        match self.persistent.write(schema_name, table_name, keyed_rows) {
            Ok(_size) => Ok(Ok(())),
            Err(_) => unreachable!("QueryProcessor Error")
        }
    }

    pub fn select_all_from(
        &mut self,
        schema_name: &str,
        table_name: &str,
        column_names: Vec<String>,
    ) -> SystemResult<Result<Projection, OperationOnTableError>> {
        let all_columns = self.table_columns(schema_name, table_name)?;
        let mut description = vec![];
        let mut column_indexes = vec![];
        let mut non_existing_columns = vec![];
        for (i, column_name) in column_names.iter().enumerate() {
            let mut found = None;
            for (index, column_definition) in all_columns.iter().enumerate() {
                if column_definition.has_name(column_name) {
                    found = Some(((index, i), column_definition.clone()));
                    break;
                }
            }

            if let Some((index_pair, column_definition)) = found {
                column_indexes.push(index_pair);
                description.push(column_definition);
            } else {
                non_existing_columns.push(column_name.clone());
            }
        }

        let data = match self.persistent.read(schema_name, table_name)? {
            Ok(read) => {
                if !non_existing_columns.is_empty() {
                    return Ok(Err(OperationOnTableError::ColumnDoesNotExist(non_existing_columns)));
                }
                read.map(backend::Result::unwrap)
                    .map(|(_key, values)| values)
                    .collect()
            }
            Err(OperationOnObjectError::ObjectDoesNotExist) => {
                return Ok(Err(OperationOnTableError::TableDoesNotExist))
            }
            Err(OperationOnObjectError::NamespaceDoesNotExist) => {
                return Ok(Err(OperationOnTableError::SchemaDoesNotExist))
            }
        };
        Ok(Ok((description, data)))
    }

    pub fn update_all(
        &mut self,
        schema_name: &str,
        table_name: &str,
        rows: Vec<(String, String)>,
    ) -> SystemResult<Result<usize, OperationOnTableError>> {
        let all_columns = self.table_columns(schema_name, table_name)?;
        let mut errors = Vec::new();
        let mut index_value_pairs = Vec::new();
        let mut non_existing_columns = BTreeSet::new();
        let mut column_exists = false;

        // only process the rows if the table and schema exist.
        if self.persistent.is_table_exists(schema_name, table_name) {
            for (column_name, value) in rows {
                for (index, column_definition) in all_columns.iter().enumerate() {
                    if column_definition.has_name(&column_name) {
                        match column_definition.sql_type().validate_and_serialize(value.as_str()) {
                            Ok(bytes) => {
                                index_value_pairs.push((index, bytes));
                            }
                            Err(e) => {
                                errors.push((e, column_definition.clone()));
                            }
                        }

                        column_exists = true;

                        break;
                    }
                }

                if !column_exists {
                    non_existing_columns.insert(column_name.clone());
                }
            }
        }

        match self.persistent.read(schema_name, table_name)? {
            Ok(reads) => {
                if !non_existing_columns.is_empty() {
                    return Ok(Err(OperationOnTableError::ColumnDoesNotExist(
                        non_existing_columns.into_iter().collect(),
                    )));
                }
                if !errors.is_empty() {
                    // Index will always be 1.
                    return Ok(Err(OperationOnTableError::ConstraintViolations(errors, 1)));
                }
                let to_update: Vec<Row> = reads
                    .map(backend::Result::unwrap)
                    .map(|(key, values)| {
                        let mut values: Vec<&[u8]> = values.split(|b| *b == b'|').collect();
                        for (index, updated_value) in &index_value_pairs {
                            values[*index] = updated_value;
                        }

                        (key, values.join(&b'|'))
                    })
                    .collect();

                let len = to_update.len();
                match self.persistent.write(schema_name, table_name, to_update)? {
                    Ok(_size) => Ok(Ok(len)),
                    _ => unreachable!(
                        "all errors that make code fall in here should have been handled in read operation"
                    ),
                }
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
        match self.persistent.read(schema_name, table_name)? {
            Ok(reads) => {
                let keys = reads.map(backend::Result::unwrap).map(|(key, _)| key).collect();
                match self.persistent.delete(schema_name, table_name, keys)? {
                    Ok(len) => Ok(Ok(len)),
                    _ => unreachable!(
                        "all errors that make code fall in here should have been handled in read operation"
                    ),
                }
            }
            Err(OperationOnObjectError::ObjectDoesNotExist) => Ok(Err(OperationOnTableError::TableDoesNotExist)),
            Err(OperationOnObjectError::NamespaceDoesNotExist) => Ok(Err(OperationOnTableError::SchemaDoesNotExist)),
        }
    }

    pub fn schema_exists(&self, schema_name: &str) -> bool {
        self.persistent.is_schema_exists(schema_name)
    }

    pub fn table_exists(&self, schema_name: &str, table_name: &str) -> bool {
        self.persistent.is_table_exists(schema_name, table_name)
    }
}

// #[cfg(test)]
// mod tests;
