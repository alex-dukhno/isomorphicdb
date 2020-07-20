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

use crate::query::TableId;
use kernel::SystemResult;
use protocol::results::{QueryErrorBuilder, QueryEvent, QueryResult};
use std::sync::{Arc, Mutex};
use storage::{backend::BackendStorage, frontend::FrontendStorage, DropTableError};

pub(crate) struct DropTableCommand<P: BackendStorage> {
    name: TableId,
    storage: Arc<Mutex<FrontendStorage<P>>>,
}

impl<P: BackendStorage> DropTableCommand<P> {
    pub(crate) fn new(name: TableId, storage: Arc<Mutex<FrontendStorage<P>>>) -> DropTableCommand<P> {
        DropTableCommand { name, storage }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<QueryResult> {
        let table_name = self.name.name();
        let schema_name = self.name.schema_name();
        match (self.storage.lock().unwrap()).drop_table(schema_name, table_name)? {
            Ok(()) => Ok(Ok(QueryEvent::TableDropped)),
            Err(DropTableError::TableDoesNotExist) => Ok(Err(QueryErrorBuilder::new()
                .table_does_not_exist(format!("{}.{}", schema_name, table_name))
                .build())),
            Err(DropTableError::SchemaDoesNotExist) => Ok(Err(QueryErrorBuilder::new()
                .schema_does_not_exist(schema_name.to_string())
                .build())),
        }
    }
}
