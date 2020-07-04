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
    backend::{
        self, BackendStorage, CreateObjectError, DropObjectError, NamespaceAlreadyExists, NamespaceDoesNotExist,
        OperationOnObjectError, Row, SledBackendStorage,
    },
    CreateTableError, DropTableError, OperationOnTableError, Projection, SchemaAlreadyExists, SchemaDoesNotExist,
};
use kernel::{SystemError, SystemResult};
use serde::{Deserialize, Serialize};
use sql_types::{ConstraintError, SqlType};
use std::collections::HashMap;

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
        column_names: Vec<(String, SqlType)>,
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
                            .into_iter()
                            .map(|(name, sql_type)| bincode::serialize(&ColumnMetadata { name, sql_type }).unwrap())
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

    pub fn table_columns(&mut self, schema_name: &str, table_name: &str) -> SystemResult<Vec<(String, SqlType)>> {
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
                            .map(|c| {
                                let ColumnMetadata { name, sql_type } = bincode::deserialize(c).unwrap();
                                (name, sql_type)
                            })
                            .collect::<Vec<(String, SqlType)>>()
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
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    ) -> SystemResult<Result<(), OperationOnTableError>> {
        let all_columns = self.table_columns(schema_name, table_name)?;
        let index_columns = if columns.is_empty() {
            let mut index_cols = vec![];
            for (index, (name, sql_type)) in all_columns.iter().enumerate() {
                index_cols.push((index, name.clone(), *sql_type));
            }

            index_cols
        } else {
            let mut index_cols = vec![];
            let mut non_existing_cols = vec![];
            for col in columns {
                let mut found = None;
                for (index, (name, sql_type)) in all_columns.iter().enumerate() {
                    if *name == col {
                        found = Some((index, name.clone(), *sql_type));
                        break;
                    }
                }

                match found {
                    Some(index_col) => {
                        index_cols.push(index_col);
                    }
                    None => non_existing_cols.push(col),
                }
            }

            if !non_existing_cols.is_empty() {
                return Ok(Err(OperationOnTableError::ColumnDoesNotExist(non_existing_cols)));
            }

            index_cols
        };

        let mut to_write: Vec<Row> = vec![];
        let mut errors = HashMap::new();
        let mut operation_error = None;
        for row in rows {
            if row.len() > all_columns.len() {
                operation_error = Some(OperationOnTableError::InsertTooManyExpressions);
                break;
            }

            let key = self.key_id_generator.to_be_bytes().to_vec();

            // TODO: The default value or NULL should be initialized for SQL types of all columns.
            let mut record = vec![vec![0, 0]; all_columns.len()];
            let mut out_of_range = vec![];
            let mut not_an_int = vec![];
            let mut value_too_long = vec![];
            for (item, (index, name, sql_type)) in row.iter().zip(index_columns.iter()) {
                match sql_type.constraint().validate(item.as_str()) {
                    Ok(()) => {
                        record[*index] = sql_type.serializer().ser(item.as_str());
                    }
                    Err(ConstraintError::OutOfRange) => {
                        out_of_range.push((name.clone(), *sql_type));
                    }
                    Err(ConstraintError::NotAnInt) => {
                        not_an_int.push((name.clone(), *sql_type));
                    }
                    Err(ConstraintError::ValueTooLong) => {
                        value_too_long.push((name.clone(), *sql_type));
                    }
                }
            }
            if !out_of_range.is_empty() {
                errors
                    .entry(ConstraintError::OutOfRange)
                    .or_insert_with(Vec::new)
                    .push(out_of_range);
            }
            if !not_an_int.is_empty() {
                errors
                    .entry(ConstraintError::NotAnInt)
                    .or_insert_with(Vec::new)
                    .push(not_an_int);
            }
            if !value_too_long.is_empty() {
                errors
                    .entry(ConstraintError::ValueTooLong)
                    .or_insert_with(Vec::new)
                    .push(value_too_long);
            }
            to_write.push((key, record.join(&b'|')));
            self.key_id_generator += 1;
        }
        if !errors.is_empty() {
            return Ok(Err(OperationOnTableError::ConstraintViolation(errors)));
        }
        match self.persistent.write(schema_name, table_name, to_write)? {
            Ok(_size) => {
                if let Some(err) = operation_error {
                    Ok(Err(err))
                } else {
                    Ok(Ok(()))
                }
            }
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
        let all_columns = self.table_columns(schema_name, table_name)?;
        let mut description = vec![];
        let mut column_indexes = vec![];
        let mut non_existing_columns = vec![];
        for (i, column) in columns.iter().enumerate() {
            let mut found = None;
            for (index, (name, sql_type)) in all_columns.iter().enumerate() {
                if name == column {
                    found = Some(((index, i), (name.clone(), *sql_type)));
                    break;
                }
            }

            if let Some((index_pair, name_type_pair)) = found {
                column_indexes.push(index_pair);
                description.push(name_type_pair);
            } else {
                non_existing_columns.push(column.clone());
            }
        }

        let data = match self.persistent.read(schema_name, table_name)? {
            Ok(read) => {
                if !non_existing_columns.is_empty() {
                    return Ok(Err(OperationOnTableError::ColumnDoesNotExist(non_existing_columns)));
                }
                read.map(backend::Result::unwrap)
                    .map(|(_key, values)| values)
                    .map(|bytes| {
                        let mut values = vec![];
                        for (i, (origin, ord)) in column_indexes.iter().enumerate() {
                            for (index, value) in bytes.split(|b| *b == b'|').enumerate() {
                                if index == *origin {
                                    values.push((ord, description[i].1.serializer().des(value)))
                                }
                            }
                        }
                        values.into_iter().map(|(_, value)| value).collect()
                    })
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
        let mut errors = HashMap::new();
        let mut out_of_range = vec![];
        let mut not_an_int = vec![];
        let mut value_too_long = vec![];
        let mut index_value_pairs = vec![];
        let mut non_existing_columns = vec![];
        for (column_name, value) in rows {
            let mut found = None;
            for (index, (name, sql_type)) in all_columns.iter().enumerate() {
                if *name == column_name {
                    match sql_type.constraint().validate(value.as_str()) {
                        Ok(()) => {
                            found = Some((index, sql_type.serializer().ser(value.as_str())));
                        }
                        Err(ConstraintError::OutOfRange) => {
                            out_of_range.push((name.clone(), *sql_type));
                        }
                        Err(ConstraintError::NotAnInt) => {
                            not_an_int.push((name.clone(), *sql_type));
                        }
                        Err(ConstraintError::ValueTooLong) => {
                            value_too_long.push((name.clone(), *sql_type));
                        }
                    }
                    break;
                }
            }
            if let Some(pair) = found {
                index_value_pairs.push(pair);
            } else if out_of_range.is_empty() && not_an_int.is_empty() && value_too_long.is_empty() {
                non_existing_columns.push(column_name.clone());
            }
        }

        if !out_of_range.is_empty() {
            errors
                .entry(ConstraintError::OutOfRange)
                .or_insert_with(Vec::new)
                .push(out_of_range);
        }
        if !not_an_int.is_empty() {
            errors
                .entry(ConstraintError::NotAnInt)
                .or_insert_with(Vec::new)
                .push(not_an_int);
        }
        if !value_too_long.is_empty() {
            errors
                .entry(ConstraintError::ValueTooLong)
                .or_insert_with(Vec::new)
                .push(value_too_long);
        }

        match self.persistent.read(schema_name, table_name)? {
            Ok(reads) => {
                if !non_existing_columns.is_empty() {
                    return Ok(Err(OperationOnTableError::ColumnDoesNotExist(non_existing_columns)));
                }
                if !errors.is_empty() {
                    return Ok(Err(OperationOnTableError::ConstraintViolation(errors)));
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
}

#[derive(Serialize, Deserialize)]
struct ColumnMetadata {
    name: String,
    sql_type: SqlType,
}

#[cfg(test)]
mod tests;
