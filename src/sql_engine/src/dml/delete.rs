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

use crate::query::plan::TableDeletes;
use data_manager::DataManager;
use kernel::SystemResult;
use protocol::{
    results::{QueryError, QueryEvent},
    Sender,
};

pub(crate) struct DeleteCommand {
    table_deletes: TableDeletes,
    storage: Arc<DataManager>,
    sender: Arc<dyn Sender>,
}

impl DeleteCommand {
    pub(crate) fn new(
        table_deletes: TableDeletes,
        storage: Arc<DataManager>,
        sender: Arc<dyn Sender>,
    ) -> DeleteCommand {
        DeleteCommand {
            table_deletes,
            storage,
            sender,
        }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<()> {
        match self.storage.table_exists(
            self.table_deletes.table_id.schema_name(),
            self.table_deletes.table_id.name(),
        ) {
            None => self
                .sender
                .send(Err(QueryError::schema_does_not_exist(
                    self.table_deletes.table_id.schema_name().to_owned(),
                )))
                .expect("To Send Result to Client"),
            Some((_, None)) => self
                .sender
                .send(Err(QueryError::table_does_not_exist(
                    self.table_deletes.table_id.schema_name().to_owned() + "." + self.table_deletes.table_id.name(),
                )))
                .expect("To Send Result to Client"),
            Some((schema_id, Some(table_id))) => {
                match self.storage.full_scan(schema_id, table_id) {
                    Err(e) => return Err(e),
                    Ok(reads) => {
                        let keys = reads
                            .map(Result::unwrap)
                            .map(Result::unwrap)
                            .map(|(key, _)| key)
                            .collect();

                        match self.storage.delete_from(schema_id, table_id, keys) {
                            Err(e) => return Err(e),
                            Ok(records_number) => self
                                .sender
                                .send(Ok(QueryEvent::RecordsDeleted(records_number)))
                                .expect("To Send Query Result to Client"),
                        }
                    }
                };
            }
        }
        Ok(())
    }
}
