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

use crate::{catalog_manager::CatalogManager, query::plan::TableCreationInfo};
use kernel::SystemResult;
use protocol::{
    results::{QueryError, QueryEvent},
    Sender,
};
use std::sync::Arc;

pub(crate) struct CreateTableCommand {
    table_info: TableCreationInfo,
    storage: Arc<CatalogManager>,
    session: Arc<dyn Sender>,
}

impl CreateTableCommand {
    pub(crate) fn new(
        table_info: TableCreationInfo,
        storage: Arc<CatalogManager>,
        session: Arc<dyn Sender>,
    ) -> CreateTableCommand {
        CreateTableCommand {
            table_info,
            storage,
            session,
        }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<()> {
        let table_name = self.table_info.table_name.as_str();
        let schema_name = self.table_info.schema_name.as_str();

        match self.storage.table_exists(schema_name, table_name) {
            None => self
                .session
                .send(Err(QueryError::schema_does_not_exist(schema_name.to_owned())))
                .expect("To Send Query Result to Client"),
            Some((_, Some(_))) => self
                .session
                .send(Err(QueryError::table_already_exists(table_name.to_owned())))
                .expect("To Send Query Result to Client"),
            Some((schema_id, None)) => {
                match self
                    .storage
                    .create_table(schema_id, table_name, self.table_info.columns.as_slice())
                {
                    Err(error) => return Err(error),
                    Ok(()) => self
                        .session
                        .send(Ok(QueryEvent::TableCreated))
                        .expect("To Send Query Result to Client"),
                }
            }
        }
        Ok(())
    }
}
