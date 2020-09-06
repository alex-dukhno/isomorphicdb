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

///! Module for transforming the input Query AST into representation the engine can process.
use crate::query::plan::{Plan, SchemaCreationInfo, TableCreationInfo, TableInserts};
use crate::query::{SchemaId, SchemaNamingError, TableId, TableNamingError};
use data_manager::{ColumnDefinition, DataManager};
use protocol::{results::QueryError, Sender};
use sql_types::SqlType;
use sqlparser::ast::{ColumnDef, DataType, ObjectName, ObjectType, Statement};
use std::{convert::TryFrom, sync::Arc};
use itertools::Itertools;
use std::sync::mpsc::sync_channel;

type Result<T> = std::result::Result<T, ()>;

pub(crate) struct QueryProcessor {
    storage: Arc<DataManager>,
    sender: Arc<dyn Sender>,
}

impl<'qp> QueryProcessor {
    pub fn new(storage: Arc<DataManager>, sender: Arc<dyn Sender>) -> Self {
        Self { storage, sender }
    }

    fn resolve_table_name(&self, name: &ObjectName) -> Result<(TableId, String, String)> {
        let schema_name = name.0.first().unwrap().value.clone();
        let table_name = name.0.iter().skip(1).join(".");
        match self.storage.table_exists(&schema_name, &table_name) {
            None => {
                self.sender
                    .send(Err(QueryError::schema_does_not_exist(schema_name.to_owned())))
                    .expect("To Send Query Result to Client");
                return Err(())
            }
            Some((_, None)) => {
                self.sender
                    .send(Err(QueryError::table_does_not_exist(format!(
                        "{}.{}",
                        schema_name, table_name
                    ))))
                    .expect("To Send Query Result to Client");
                return Err(());
            }
            Some((schema_id, Some(table_id))) => Ok((TableId(schema_id, table_id), schema_name, table_name))
        }
    }

    fn resolve_schema_name(&self, name: &ObjectName) -> Result<(SchemaId, String)> {
        let schema_name = name.0.iter().join(".");

        match self.storage.schema_exists(&schema_name) {
            None => {
                self.sender
                    .send(Err(QueryError::schema_does_not_exist(schema_name)))
                    .expect("To Send Query Result to Client");
                return Err(());
            }
            Some(schema_id) => Ok((SchemaId(schema_id), schema_name))
        }
    }

    pub fn process(&self, stmt: Statement) -> Result<Plan> {
        match stmt {
            Statement::CreateTable { name, columns, .. } => self.handle_create_table(name, &columns),
            Statement::CreateSchema { schema_name, .. } => {
                let schema_name = schema_name.0.iter().join(".");
                match self.storage.schema_exists(&schema_name) {
                    Some(_) => {
                        self.sender
                            .send(Err(QueryError::schema_already_exists(schema_name)))
                            .expect("To Send Query Result to Client");
                        Err(())
                    }
                    None => Ok(Plan::CreateSchema(SchemaCreationInfo {
                        schema_name
                    })),
                }
            }
            Statement::Drop {
                object_type,
                names,
                cascade,
                ..
            } => self.handle_drop(&object_type, &names, cascade),
            Statement::Insert {
                table_name,
                columns,
                source,
            } => {
                let (table_id, schema_name, table_name) = self.resolve_table_name(&table_name)?;
                Ok(Plan::Insert(TableInserts {
                    table_id,
                    column_indices: columns,
                    input: source
                }))
            }
            _ => Ok(Plan::NotProcessed(Box::new(stmt.clone()))),
        }
    }

    fn sql_type_from_datatype(&self, datatype: &DataType) -> Result<SqlType> {
        match datatype {
            DataType::SmallInt => Ok(SqlType::SmallInt(i16::min_value())),
            DataType::Int => Ok(SqlType::Integer(i32::min_value())),
            DataType::BigInt => Ok(SqlType::BigInt(i64::min_value())),
            DataType::Char(len) => Ok(SqlType::Char(len.unwrap_or(255))),
            DataType::Varchar(len) => Ok(SqlType::VarChar(len.unwrap_or(255))),
            DataType::Boolean => Ok(SqlType::Bool),
            DataType::Custom(name) => {
                let name = name.to_string();
                match name.as_str() {
                    "serial" => Ok(SqlType::Integer(1)),
                    "smallserial" => Ok(SqlType::SmallInt(1)),
                    "bigserial" => Ok(SqlType::BigInt(1)),
                    other_type => {
                        self.sender
                            .send(Err(QueryError::feature_not_supported(format!(
                                "{} type is not supported",
                                other_type
                            ))))
                            .expect("To Send Query Result to Client");
                        Err(())
                    }
                }
            }
            other_type => {
                self.sender
                    .send(Err(QueryError::feature_not_supported(format!(
                        "{} type is not supported",
                        other_type
                    ))))
                    .expect("To Send Query Result to Client");
                Err(())
            }
        }
    }

    fn resolve_column_definitions(&self, columns: &[ColumnDef]) -> Result<Vec<ColumnDefinition>> {
        let mut column_defs = Vec::new();
        for column in columns {
            let sql_type = self.sql_type_from_datatype(&column.data_type)?;
            // maybe a different type should be used to represent this instead of the storage's representation.
            let column_definition = ColumnDefinition::new(column.name.value.as_str(), sql_type);
            column_defs.push(column_definition);
        }
        Ok(column_defs)
    }

    fn handle_create_table(&self, name: ObjectName, columns: &[ColumnDef]) -> Result<Plan> {
        let schema_name = name.0.first().unwrap().value.clone();
        let table_name = name.0.iter().skip(1).join(".");

        match self.storage.table_exists(&schema_name, &table_name) {
            None => {
                self.sender
                    .send(Err(QueryError::schema_does_not_exist(schema_name)))
                    .expect("To Send Query Result to Client");
                Err(())
            }
            Some((_, Some(_))) => {
                self.sender
                    .send(Err(QueryError::table_already_exists(format!(
                        "{}.{}",
                        schema_name, table_name
                    ))))
                    .expect("To Send Query Result to Client");
                Err(())
            }
            Some((schema_id, None)) => {
                let columns = self.resolve_column_definitions(columns)?;
                let table_info = TableCreationInfo {
                    schema_id: SchemaId(schema_id),
                    schema_name,
                    table_name,
                    columns,
                };
                Ok(Plan::CreateTable(table_info))
            }
        }
    }

    fn handle_drop(&self, object_type: &ObjectType, names: &[ObjectName], cascade: bool) -> Result<Plan> {
        match object_type {
            ObjectType::Table => {
                let mut table_names = Vec::with_capacity(names.len());
                for name in names {
                    let (table_id, _, _) = self.resolve_table_name(name)?;
                    table_names.push(table_id);
                }
                Ok(Plan::DropTables(table_names))
            }
            ObjectType::Schema => {
                let mut schema_names = Vec::with_capacity(names.len());
                for name in names {
                    log::debug!("handling drop: {}", name.to_string());
                    let (schema_id, _) = self.resolve_schema_name(name)?;
                    schema_names.push((schema_id, cascade));
                }
                Ok(Plan::DropSchemas(schema_names))
            }
            _ => unimplemented!(),
        }
    }
}
