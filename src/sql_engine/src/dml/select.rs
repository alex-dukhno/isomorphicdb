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

use ast::Datum;
use data_manager::DataManager;
use kernel::{SystemError, SystemResult};
use protocol::{
    messages::ColumnMetadata,
    results::{Description, QueryError, QueryEvent},
    Sender,
};
use query_planner::plan::SelectInput;
use rxrust::prelude::*;

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

    pub(crate) fn describe(&self) -> SystemResult<Description> {
        let all_columns = self.data_manager.table_columns(&self.select_input.table_id)?;
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
                    .send(Err(QueryError::column_does_not_exist(column_name)))
                    .expect("To Send Result to Client");
                has_error = true;
            }
        }

        if has_error {
            return Err(SystemError::runtime_check_failure(&"Column Does Not Exist"));
        }

        let description = column_definitions
            .into_iter()
            .map(|column_definition| (column_definition.name(), (&column_definition.sql_type()).into()))
            .collect();

        Ok(description)
    }

    pub(crate) fn execute(&mut self) -> SystemResult<()> {
        let all_columns = self.data_manager.table_columns(&self.select_input.table_id)?;
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
                    .send(Err(QueryError::column_does_not_exist(column_name)))
                    .expect("To Send Result to Client");
                has_error = true;
            }
        }

        if has_error {
            return Ok(());
        }

        match self.data_manager.full_scan(&self.select_input.table_id) {
            Err(error) => Err(error),
            Ok(records) => {
                self.sender
                    .send(Ok(QueryEvent::RowDescription(
                        description
                            .into_iter()
                            .map(|column| ColumnMetadata::new(column.name(), (&column.sql_type()).into()))
                            .collect(),
                    )))
                    .expect("To Send Query Result to Client");

                let mut index = 0;

                // currently only `select col1, col2, col3 from table_name` is supported
                // those this is how we create `source` vertex in query result streaming
                observable::from_iter(records)
                    // then we have to handle possible layers of errors IO -> storage -> model definition
                    // should it be different operations? -  ¯\_(ツ)_/¯
                    // yes, for now - calling `map` is represents it
                    .map(Result::unwrap)
                    .map(Result::unwrap)
                    // now we do projection operator it is hidden by calling `map` but what we do is to
                    // try find out under what column index we have to create tuple and send it to a client
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
                    // we send data in subscribe operation
                    .subscribe(|value| {
                        self.sender
                            .send(Ok(QueryEvent::DataRow(value)))
                            .expect("To Send Query Result to Client");
                        index += 1;
                    });

                // records
                //     .map(Result::unwrap)
                //     .map(Result::unwrap)
                //     .map(|(_key, values)| {
                //         let row: Vec<String> = values.unpack().into_iter().map(|datum| datum.to_string()).collect();
                //
                //         let mut values = vec![];
                //         for origin in column_indexes.iter() {
                //             for (index, value) in row.iter().enumerate() {
                //                 if index == *origin {
                //                     values.push(value.clone())
                //                 }
                //             }
                //         }
                //         values
                //     })
                //     .for_each(|value| {
                //         self.sender
                //             .send(Ok(QueryEvent::DataRow(value)))
                //             .expect("To Send Query Result to Client");
                //         index += 1;
                //     });

                self.sender
                    .send(Ok(QueryEvent::RecordsSelected(index)))
                    .expect("To Send Query Result to Client");

                Ok(())
            }
        }
    }
}
