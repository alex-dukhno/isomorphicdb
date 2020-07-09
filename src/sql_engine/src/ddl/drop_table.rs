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
use sqlparser::ast::ObjectName;
use std::sync::{Arc, Mutex};
use storage::{backend::BackendStorage, frontend::FrontendStorage, DropTableError};

pub(crate) struct DropTableCommand<P: BackendStorage> {
    name: ObjectName,
    storage: Arc<Mutex<FrontendStorage<P>>>,
}

impl<P: BackendStorage> DropTableCommand<P> {
    pub(crate) fn new(name: ObjectName, storage: Arc<Mutex<FrontendStorage<P>>>) -> DropTableCommand<P> {
        DropTableCommand { name, storage }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<QueryResult> {
        let mut builder = QueryErrorBuilder::new();
        let table_name = self.name.0[1].to_string();
        let schema_name = self.name.0[0].to_string();
        match (self.storage.lock().unwrap()).drop_table(&schema_name, &table_name)? {
            Ok(()) => Ok(Ok(QueryEvent::TableDropped)),
            Err(DropTableError::TableDoesNotExist) => {
                builder.table_does_not_exist(schema_name + "." + table_name.as_str());
                Ok(Err(builder.build()))
            }
            Err(DropTableError::SchemaDoesNotExist) => {
                builder.schema_does_not_exist(schema_name);
                Ok(Err(builder.build()))
            }
        }
    }
}
