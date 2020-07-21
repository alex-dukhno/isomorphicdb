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
use protocol::{
    results::{QueryErrorBuilder, QueryEvent},
    Sender,
};
use std::sync::{Arc, Mutex};
use storage::{backend::BackendStorage, frontend::FrontendStorage, DropTableError};

pub(crate) struct DropTableCommand<P: BackendStorage> {
    name: TableId,
    storage: Arc<Mutex<FrontendStorage<P>>>,
    session: Arc<dyn Sender>,
}

impl<P: BackendStorage> DropTableCommand<P> {
    pub(crate) fn new(
        name: TableId,
        storage: Arc<Mutex<FrontendStorage<P>>>,
        session: Arc<dyn Sender>,
    ) -> DropTableCommand<P> {
        DropTableCommand { name, storage, session }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<()> {
        let table_name = self.name.name();
        let schema_name = self.name.schema_name();
        match (self.storage.lock().unwrap()).drop_table(schema_name, table_name)? {
            Ok(()) => {
                self.session
                    .send(Ok(QueryEvent::TableDropped))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
            Err(DropTableError::TableDoesNotExist) => {
                self.session
                    .send(Err(QueryErrorBuilder::new()
                        .table_does_not_exist(schema_name.to_owned() + "." + table_name)
                        .build()))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
            Err(DropTableError::SchemaDoesNotExist) => {
                self.session
                    .send(Err(QueryErrorBuilder::new()
                        .schema_does_not_exist(schema_name.to_owned())
                        .build()))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
        }
    }
}
