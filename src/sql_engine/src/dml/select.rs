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
use kernel::{SystemError, SystemResult};
use protocol::{
    results::{Description, QueryError, QueryEvent},
    Sender,
};
use query_planner::plan::SelectInput;

pub(crate) struct SelectCommand {
    select_input: SelectInput,
    storage: Arc<DataManager>,
    sender: Arc<dyn Sender>,
}

impl SelectCommand {
    pub(crate) fn new(select_input: SelectInput, storage: Arc<DataManager>, sender: Arc<dyn Sender>) -> SelectCommand {
        SelectCommand {
            select_input,
            storage,
            sender,
        }
    }

    pub(crate) fn describe(&mut self) -> SystemResult<Description> {
        let schema_id = self.select_input.table_id.schema().name();
        let table_id = self.select_input.table_id.name();
        let all_columns = self.storage.table_columns(schema_id, table_id)?;
        let mut column_definitions = vec![];
        let mut has_error = false;
        for column_name in &self.select_input.selected_columns {
            let mut found = None;
            for column_definition in &all_columns {
                if column_definition.has_name(&column_name) {
                    found = Some(column_definition);
                    break;
                }
            }

            if let Some(column_definition) = found {
                column_definitions.push(column_definition);
            } else {
                self.sender
                    .send(Err(QueryError::column_does_not_exist(column_name.to_owned())))
                    .expect("To Send Result to Client");
                has_error = true;
            }
        }

        if has_error {
            return Err(SystemError::runtime_check_failure("Column Does Not Exist".to_string()));
        }

        let description = column_definitions
            .into_iter()
            .map(|column_definition| (column_definition.name(), (&column_definition.sql_type()).into()))
            .collect();

        Ok(description)
    }

    pub(crate) fn execute(&mut self) -> SystemResult<()> {
        let schema_id = self.select_input.table_id.schema().name();
        let table_id = self.select_input.table_id.name();
        match self.storage.full_scan(schema_id, table_id) {
            Err(error) => Err(error),
            Ok(records) => {
                let all_columns = self.storage.table_columns(schema_id, table_id)?;
                let mut description = vec![];
                let mut column_indexes = vec![];
                let mut has_error = false;
                for column_name in self.select_input.selected_columns.iter() {
                    let mut found = None;
                    for (index, column_definition) in all_columns.iter().enumerate() {
                        if column_definition.has_name(column_name) {
                            found = Some((index, column_definition.clone()));
                            break;
                        }
                    }

                    if let Some((index, column_definition)) = found {
                        column_indexes.push(index);
                        description.push(column_definition);
                    } else {
                        self.sender
                            .send(Err(QueryError::column_does_not_exist(column_name.to_owned())))
                            .expect("To Send Result to Client");
                        has_error = true;
                    }
                }

                if has_error {
                    return Ok(());
                }

                let values: Vec<Vec<String>> = records
                    .map(Result::unwrap)
                    .map(Result::unwrap)
                    .map(|(_key, values)| {
                        let row: Vec<String> = values.unpack().into_iter().map(|datum| datum.to_string()).collect();

                        let mut values = vec![];
                        for origin in column_indexes.iter() {
                            for (index, value) in row.iter().enumerate() {
                                if index == *origin {
                                    values.push(value.clone())
                                }
                            }
                        }
                        values
                    })
                    .collect();

                let projection = (
                    description
                        .into_iter()
                        .map(|column| (column.name(), (&column.sql_type()).into()))
                        .collect(),
                    values,
                );
                self.sender
                    .send(Ok(QueryEvent::RecordsSelected(projection)))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
        }
    }
}
