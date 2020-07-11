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

use kernel::SystemResult;
use protocol::results::{QueryErrorBuilder, QueryEvent, QueryResult};
use sql_types::SqlType;
use sqlparser::ast::{ColumnDef, DataType, ObjectName};
use std::sync::{Arc, Mutex};
use storage::{backend::BackendStorage, frontend::FrontendStorage, CreateTableError};

pub(crate) struct CreateTableCommand<P: BackendStorage> {
    name: ObjectName,
    columns: Vec<ColumnDef>,
    storage: Arc<Mutex<FrontendStorage<P>>>,
}

impl<P: BackendStorage> CreateTableCommand<P> {
    pub(crate) fn new(
        name: ObjectName,
        columns: Vec<ColumnDef>,
        storage: Arc<Mutex<FrontendStorage<P>>>,
    ) -> CreateTableCommand<P> {
        CreateTableCommand { name, columns, storage }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<QueryResult> {
        let table_name = self.name.0.pop().unwrap().to_string();
        let schema_name = self.name.0.pop().unwrap().to_string();
        let mut column_definitions = vec![];
        for column in self.columns.iter() {
            let name = column.name.to_string();
            let sql_type = match c.data_type {
                DataType::SmallInt => SqlType::SmallInt(i16::min_value()),
                DataType::Int => SqlType::Integer(i32::min_value()),
                DataType::BigInt => SqlType::BigInt(i64::min_value()),
                DataType::Char(len) => SqlType::Char(len.unwrap_or(255)),
                DataType::Varchar(len) => SqlType::VarChar(len.unwrap_or(255)),
                DataType::Boolean => SqlType::Bool,
                DataType::Custom(name) => {
                    let name = name.to_string();
                    match name.as_str() {
                        "serial" => SqlType::Integer(1),
                        "smallserial" => SqlType::SmallInt(1),
                        "bigserial" => SqlType::BigInt(1),
                        other_type => {
                            return Ok(Err(QueryErrorBuilder::new()
                                .feature_not_supported(format!("{} type is not supported", other_type))
                                .build()))
                        }
                    }
                }
                other_type => {
                    return Ok(Err(QueryErrorBuilder::new()
                        .feature_not_supported(format!("{} type is not supported", other_type))
                        .build()))
                }
            };
            column_definitions.push((name, sql_type))
        }
        match (self.storage.lock().unwrap()).create_table(&schema_name, &table_name, column_definitions)? {
            Ok(()) => Ok(Ok(QueryEvent::TableCreated)),
            Err(CreateTableError::SchemaDoesNotExist) => {
                Ok(Err(QueryErrorBuilder::new().schema_does_not_exist(schema_name).build()))
            }
            Err(CreateTableError::TableAlreadyExists) => {
                // this is what the test expected. Also, there should maybe this name should already be generated somewhere.
                Ok(Err(QueryErrorBuilder::new()
                    .table_already_exists(format!("{}.{}", schema_name, table_name))
                    .build()))
            }
        }
    }
}
