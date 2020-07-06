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
use protocol::results::{QueryError, QueryEvent, QueryResult};
use sql_types::SqlType;
use sqlparser::ast::{ColumnDef, ObjectName};
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
        match (self.storage.lock().unwrap()).create_table(
            &schema_name,
            &table_name,
            self.columns
                .iter()
                .cloned()
                .map(|c| {
                    let name = c.name.to_string();
                    let sql_type = match c.data_type {
                        sqlparser::ast::DataType::SmallInt => SqlType::SmallInt,
                        sqlparser::ast::DataType::Int => SqlType::Integer,
                        sqlparser::ast::DataType::BigInt => SqlType::BigInt,
                        sqlparser::ast::DataType::Char(len) => SqlType::Char(len.unwrap_or(255)),
                        sqlparser::ast::DataType::Varchar(len) => SqlType::VarChar(len.unwrap_or(255)),
                        sqlparser::ast::DataType::Boolean => SqlType::Bool,
                        _ => unimplemented!(),
                    };
                    (name, sql_type)
                })
                .collect(),
        )? {
            Ok(()) => Ok(Ok(QueryEvent::TableCreated)),
            Err(CreateTableError::SchemaDoesNotExist) => Ok(Err(QueryError::schema_does_not_exist(schema_name))),
            Err(CreateTableError::TableAlreadyExists) => Ok(Err(QueryError::table_already_exists(table_name))),
        }
    }
}
