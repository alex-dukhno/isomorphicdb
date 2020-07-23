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

use crate::query::TableCreationInfo;
use kernel::SystemResult;
use protocol::{
    results::{QueryErrorBuilder, QueryEvent},
    Sender,
};
use std::sync::{Arc, Mutex};
use storage::{backend::BackendStorage, frontend::FrontendStorage, CreateTableError};

pub(crate) struct CreateTableCommand<P: BackendStorage> {
    table_info: TableCreationInfo,
    storage: Arc<Mutex<FrontendStorage<P>>>,
    session: Arc<dyn Sender>,
}

impl<P: BackendStorage> CreateTableCommand<P> {
    pub(crate) fn new(
        table_info: TableCreationInfo,
        storage: Arc<Mutex<FrontendStorage<P>>>,
        session: Arc<dyn Sender>,
    ) -> CreateTableCommand<P> {
        CreateTableCommand {
            table_info,
            storage,
            session,
        }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<()> {
        let table_name = self.table_info.table_name.as_str();
        let schema_name = self.table_info.schema_name.as_str();

        match (self.storage.lock().unwrap()).create_table(
            schema_name,
            table_name,
            self.table_info.columns.as_slice(),
        )? {
            Ok(()) => {
                self.session
                    .send(Ok(QueryEvent::TableCreated))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
            Err(CreateTableError::SchemaDoesNotExist) => {
                self.session
                    .send(Err(QueryErrorBuilder::new()
                        .schema_does_not_exist(schema_name.to_string())
                        .build()))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
            Err(CreateTableError::TableAlreadyExists) => {
                // this is what the test expected. Also, there should maybe this name should already be generated somewhere.
                self.session
                    .send(Err(QueryErrorBuilder::new()
                        .table_already_exists(format!("{}.{}", schema_name, table_name))
                        .build()))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
        }
    }
}
