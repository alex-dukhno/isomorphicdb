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

use data_manager::{DataManager, MetadataView};
use plan::SelectInput;
use protocol::{messages::ColumnMetadata, results::QueryEvent, Sender};

pub(crate) struct SelectCommand {
    select_input: SelectInput,
    data_manager: Arc<DataManager>,
    sender: Arc<dyn Sender>,
}

impl SelectCommand {
    pub(crate) fn new(
        select_input: SelectInput,
        data_manager: Arc<DataManager>,
        sender: Arc<dyn Sender>,
    ) -> SelectCommand {
        SelectCommand {
            select_input,
            data_manager,
            sender,
        }
    }

    pub(crate) fn execute(&mut self) {
        let records = match self.data_manager.full_scan(&self.select_input.table_id) {
            Err(()) => {
                log::error!("Error while scanning table {:?}", self.select_input.table_id);
                return;
            }
            Ok(records) => records,
        };
        self.sender
            .send(Ok(QueryEvent::RowDescription(
                self.data_manager
                    .column_defs(&self.select_input.table_id, &self.select_input.selected_columns)
                    .into_iter()
                    .map(|column| ColumnMetadata::new(column.name(), (&column.sql_type()).into()))
                    .collect(),
            )))
            .expect("To Send Query Result to Client");

        let mut index = 0;

        records
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(_key, values)| {
                let row: Vec<String> = values.unpack().into_iter().map(|datum| datum.to_string()).collect();

                let mut values = vec![];
                for origin in self.select_input.selected_columns.iter() {
                    for (index, value) in row.iter().enumerate() {
                        if (index as u64) == *origin {
                            values.push(value.clone())
                        }
                    }
                }
                values
            })
            .for_each(|value| {
                self.sender
                    .send(Ok(QueryEvent::DataRow(value)))
                    .expect("To Send Query Result to Client");
                index += 1;
            });

        self.sender
            .send(Ok(QueryEvent::RecordsSelected(index)))
            .expect("To Send Query Result to Client");
    }
}
