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

use std::sync::Arc;

use data_manager::DataManager;
use kernel::SystemResult;
use protocol::{
    results::{QueryError, QueryEvent},
    Sender,
};
use query_planner::FullTableName;

pub(crate) struct DropTableCommand {
    name: FullTableName,
    storage: Arc<DataManager>,
    sender: Arc<dyn Sender>,
}

impl DropTableCommand {
    pub(crate) fn new(name: FullTableName, storage: Arc<DataManager>, sender: Arc<dyn Sender>) -> DropTableCommand {
        DropTableCommand { name, storage, sender }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<()> {
        let table_name = self.name.name();
        let schema_name = self.name.schema_name();
        match self.storage.table_exists(schema_name, table_name) {
            None => self
                .sender
                .send(Err(QueryError::schema_does_not_exist(schema_name.to_owned())))
                .expect("To Send Query Result to Client"),
            Some((_, None)) => self
                .sender
                .send(Err(QueryError::table_does_not_exist(
                    schema_name.to_owned() + "." + table_name,
                )))
                .expect("To Send Query Result to Client"),
            Some((schema_id, Some(table_id))) => match self.storage.drop_table(schema_id, table_id) {
                Err(error) => return Err(error),
                Ok(()) => self
                    .sender
                    .send(Ok(QueryEvent::TableDropped))
                    .expect("To Send Query Result to Client"),
            },
        }
        Ok(())
    }
}
