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
use data_manager::DataManager;
use kernel::SystemResult;
use protocol::{results::QueryEvent, Sender};
use std::sync::Arc;

pub(crate) struct DropTableCommand {
    name: TableId,
    storage: Arc<DataManager>,
    sender: Arc<dyn Sender>,
}

impl DropTableCommand {
    pub(crate) fn new(name: TableId, storage: Arc<DataManager>, sender: Arc<dyn Sender>) -> DropTableCommand {
        DropTableCommand { name, storage, sender }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<()> {
        let schema_id = self.name.schema().name();
        let table_id = self.name.name();
        match self.storage.drop_table(schema_id, table_id) {
            Err(error) => Err(error),
            Ok(()) => {
                self.sender
                    .send(Ok(QueryEvent::TableDropped))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
        }
    }
}
