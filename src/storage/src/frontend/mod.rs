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

use crate::{
    backend::{BackendError, BackendStorage, SledBackendStorage},
    ColumnDefinition, ReadCursor, Row, TableDescription,
};
use kernel::{Object, Operation, SystemError, SystemResult};
use representation::Binary;

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
        match persistent.create_namespace_with_objects("system", vec!["columns"]) {
            Ok(()) => Ok(Self {
                key_id_generator: 0,
                persistent,
            }),
            Err(BackendError::SystemError(e)) => Err(e),
            Err(BackendError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Create,
                Object::Schema("system"),
            )),
        }
    }

    pub fn next_key_id(&mut self) -> usize {
        let key_id = self.key_id_generator;
        self.key_id_generator += 1;
        key_id
    }

    pub fn table_descriptor(&self, schema_name: &str, table_name: &str) -> SystemResult<TableDescription> {
        match self.persistent.check_for_object(schema_name, table_name) {
            Ok(()) => {}
            Err(BackendError::SystemError(error)) => return Err(error),
            Err(BackendError::RuntimeCheckError) => {
                return Err(SystemError::bug_in_sql_engine(
                    Operation::Access,
                    Object::Table(schema_name, table_name),
                ));
            }
        }

        // we know the table exists
        let columns_metadata = self.table_columns(schema_name, table_name)?;

        Ok(TableDescription::new(schema_name, table_name, columns_metadata))
    }

    pub fn create_schema(&mut self, schema_name: &str) -> SystemResult<()> {
        match self.persistent.create_namespace(schema_name) {
            Ok(()) => Ok(()),
            Err(BackendError::SystemError(error)) => Err(error),
            Err(BackendError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Create,
                Object::Schema(schema_name),
            )),
        }
    }

    pub fn drop_schema(&mut self, schema_name: &str) -> SystemResult<()> {
        match self.persistent.drop_namespace(schema_name) {
            Ok(()) => Ok(()),
            Err(BackendError::SystemError(error)) => Err(error),
            Err(BackendError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Drop,
                Object::Schema(schema_name),
            )),
        }
    }

    pub fn create_table(
        &mut self,
        schema_name: &str,
        table_name: &str,
        column_names: &[ColumnDefinition],
    ) -> SystemResult<()> {
        match self.persistent.create_object(schema_name, table_name) {
            Ok(()) => match self.persistent.write(
                "system",
                "columns",
                vec![(
                    Binary::with_data((schema_name.to_owned() + table_name).as_bytes().to_vec()),
                    Binary::with_data(
                        column_names
                            .iter()
                            .map(|column_defs| bincode::serialize(&column_defs).unwrap())
                            .collect::<Vec<Vec<u8>>>()
                            .join(&b'|')
                            .to_vec(),
                    ),
                )],
            ) {
                Ok(_size) => {
                    log::info!("column data is recorded");
                    Ok(())
                }
                Err(BackendError::SystemError(error)) => Err(error),
                Err(BackendError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                    Operation::Access,
                    Object::Table("system", "columns"),
                )),
            },
            Err(BackendError::SystemError(error)) => Err(error),
            Err(BackendError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Create,
                Object::Table(schema_name, table_name),
            )),
        }
    }

    pub fn table_columns(&self, schema_name: &str, table_name: &str) -> SystemResult<Vec<ColumnDefinition>> {
        match self.persistent.read("system", "columns") {
            Ok(reads) => Ok(reads
                .map(Result::unwrap)
                .filter(|(table, _columns)| {
                    *table == Binary::with_data((schema_name.to_owned() + table_name).as_bytes().to_vec())
                })
                .map(|(_id, columns)| {
                    columns
                        .to_bytes()
                        .split(|b| *b == b'|')
                        .filter(|v| !v.is_empty())
                        .map(|c| bincode::deserialize(c).unwrap())
                        .collect::<Vec<_>>()
                })
                .next()
                .unwrap_or_default()),
            Err(BackendError::SystemError(error)) => Err(error),
            Err(BackendError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Access,
                Object::Table(schema_name, table_name),
            )),
        }
    }

    pub fn drop_table(&mut self, schema_name: &str, table_name: &str) -> SystemResult<()> {
        match self.persistent.drop_object(schema_name, table_name) {
            Ok(()) => Ok(()),
            Err(BackendError::SystemError(error)) => Err(error),
            Err(BackendError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Drop,
                Object::Table(schema_name, table_name),
            )),
        }
    }

    pub fn insert_into(&mut self, schema_name: &str, table_name: &str, values: Vec<Row>) -> SystemResult<usize> {
        log::debug!("{:#?}", values);
        match self.persistent.write(schema_name, table_name, values) {
            Ok(size) => Ok(size),
            Err(BackendError::SystemError(error)) => Err(error),
            Err(BackendError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Access,
                Object::Table(schema_name, table_name),
            )),
        }
    }

    pub fn table_scan(&mut self, schema_name: &str, table_name: &str) -> SystemResult<ReadCursor> {
        match self.persistent.read(schema_name, table_name) {
            Ok(read) => Ok(read),
            Err(BackendError::SystemError(error)) => Err(error),
            Err(BackendError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Access,
                Object::Table(schema_name, table_name),
            )),
        }
    }

    pub fn update_all(&mut self, schema_name: &str, table_name: &str, rows: Vec<Row>) -> SystemResult<usize> {
        match self.persistent.write(schema_name, table_name, rows) {
            Ok(size) => Ok(size),
            Err(BackendError::SystemError(error)) => Err(error),
            Err(BackendError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Access,
                Object::Table(schema_name, table_name),
            )),
        }
    }

    pub fn delete_all_from(&mut self, schema_name: &str, table_name: &str) -> SystemResult<usize> {
        match self.persistent.read(schema_name, table_name) {
            Ok(reads) => {
                let keys = reads.map(Result::unwrap).map(|(key, _)| key).collect();
                match self.persistent.delete(schema_name, table_name, keys) {
                    Ok(len) => Ok(len),
                    _ => unreachable!(
                        "all errors that make code fall in here should have been handled in read operation"
                    ),
                }
            }
            Err(BackendError::SystemError(error)) => Err(error),
            Err(BackendError::RuntimeCheckError) => Err(SystemError::bug_in_sql_engine(
                Operation::Access,
                Object::Table(schema_name, table_name),
            )),
        }
    }

    pub fn schema_exists(&self, schema_name: &str) -> bool {
        self.persistent.is_namespace_exists(schema_name)
    }

    pub fn table_exists(&self, schema_name: &str, table_name: &str) -> bool {
        self.persistent.is_object_exists(schema_name, table_name)
    }
}

#[cfg(test)]
mod tests;
