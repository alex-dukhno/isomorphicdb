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
use kernel::SystemResult;
use protocol::{
    results::{QueryError, QueryEvent},
    Sender,
};
use sqlparser::ast::ObjectName;
use std::sync::Arc;

pub(crate) struct DeleteCommand {
    name: ObjectName,
    storage: Arc<DataManager>,
    sender: Arc<dyn Sender>,
}

impl DeleteCommand {
    pub(crate) fn new(name: ObjectName, storage: Arc<DataManager>, sender: Arc<dyn Sender>) -> DeleteCommand {
        DeleteCommand { name, storage, sender }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<()> {
        let schema_name = self.name.0[0].to_string();
        let table_name = self.name.0[1].to_string();

        match self.storage.table_exists(&schema_name, &table_name) {
            None => self
                .sender
                .send(Err(QueryError::schema_does_not_exist(schema_name)))
                .expect("To Send Result to Client"),
            Some((_, None)) => self
                .sender
                .send(Err(QueryError::table_does_not_exist(
                    schema_name + "." + table_name.as_str(),
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
