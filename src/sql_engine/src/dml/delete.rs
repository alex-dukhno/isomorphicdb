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
use protocol::{
    results::{QueryErrorBuilder, QueryEvent},
    Sender,
};
use sqlparser::ast::ObjectName;
use std::sync::{Arc, Mutex};
use storage::{backend::BackendStorage, frontend::FrontendStorage, OperationOnTableError};

pub(crate) struct DeleteCommand<'dc, P: BackendStorage> {
    raw_sql_query: &'dc str,
    name: ObjectName,
    storage: Arc<Mutex<FrontendStorage<P>>>,
    session: Arc<dyn Sender>,
}

impl<'dc, P: BackendStorage> DeleteCommand<'dc, P> {
    pub(crate) fn new(
        raw_sql_query: &'dc str,
        name: ObjectName,
        storage: Arc<Mutex<FrontendStorage<P>>>,
        session: Arc<dyn Sender>,
    ) -> DeleteCommand<'dc, P> {
        DeleteCommand {
            raw_sql_query,
            name,
            storage,
            session,
        }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<()> {
        let schema_name = self.name.0[0].to_string();
        let table_name = self.name.0[1].to_string();
        match (self.storage.lock().unwrap()).delete_all_from(&schema_name, &table_name)? {
            Ok(records_number) => {
                self.session
                    .send(Ok(QueryEvent::RecordsDeleted(records_number)))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
            Err(OperationOnTableError::SchemaDoesNotExist) => {
                self.session
                    .send(Err(QueryErrorBuilder::new().schema_does_not_exist(schema_name).build()))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
            Err(OperationOnTableError::TableDoesNotExist) => {
                self.session
                    .send(Err(QueryErrorBuilder::new()
                        .table_does_not_exist(schema_name + "." + table_name.as_str())
                        .build()))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
            Err(OperationOnTableError::ColumnDoesNotExist(non_existing_columns)) => {
                self.session
                    .send(Err(QueryErrorBuilder::new()
                        .column_does_not_exist(non_existing_columns)
                        .build()))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
            _ => {
                self.session
                    .send(Err(QueryErrorBuilder::new()
                        .feature_not_supported(self.raw_sql_query.to_owned())
                        .build()))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
        }
    }
}
