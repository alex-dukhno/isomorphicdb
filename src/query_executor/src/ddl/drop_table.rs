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

use data_manager::DataManager;
use plan::TableId;
use protocol::{results::QueryEvent, Sender};
use std::sync::Arc;
use storage::Database;

pub(crate) struct DropTableCommand<D: Database> {
    table_id: TableId,
    data_manager: Arc<DataManager<D>>,
    sender: Arc<dyn Sender>,
}

impl<D: Database> DropTableCommand<D> {
    pub(crate) fn new(
        table_id: TableId,
        data_manager: Arc<DataManager<D>>,
        sender: Arc<dyn Sender>,
    ) -> DropTableCommand<D> {
        DropTableCommand {
            table_id,
            data_manager,
            sender,
        }
    }

    pub(crate) fn execute(&mut self) {
        if let Err(()) = self.data_manager.drop_table(&self.table_id) {
            log::error!("Error while dropping table {:?}", self.table_id);
        }
        self.sender
            .send(Ok(QueryEvent::TableDropped))
            .expect("To Send Query Result to Client");
    }
}
