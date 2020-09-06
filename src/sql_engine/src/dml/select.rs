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

use crate::query::plan::SelectInput;
use data_manager::DataManager;
use kernel::{Object, Operation, SystemError, SystemResult};
use protocol::{
    results::{Description, QueryError, QueryEvent},
    Sender,
};

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
        match self
            .storage
            .table_exists(&self.select_input.schema_name, &self.select_input.table_name)
        {
            None => {
                self.sender
                    .send(Err(QueryError::schema_does_not_exist(
                        self.select_input.schema_name.clone(),
                    )))
                    .expect("To Send Result to Client");
                Err(SystemError::bug_in_sql_engine(
                    Operation::Access,
                    Object::Schema(&self.select_input.schema_name),
                ))
            }
            Some((_, None)) => {
                self.sender
                    .send(Err(QueryError::table_does_not_exist(
                        self.select_input.schema_name.clone() + "." + self.select_input.table_name.as_str(),
                    )))
                    .expect("To Send Result to Client");
                Err(SystemError::bug_in_sql_engine(
                    Operation::Access,
                    Object::Table(&self.select_input.schema_name, &self.select_input.table_name),
                ))
            }
            Some((schema_id, Some(table_id))) => {
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
        }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<()> {
        match self
            .storage
            .table_exists(&self.select_input.schema_name, &self.select_input.table_name)
        {
            None => {
                self.sender
                    .send(Err(QueryError::schema_does_not_exist(
                        self.select_input.schema_name.clone(),
                    )))
                    .expect("To Send Result to Client");
                Err(SystemError::runtime_check_failure("Schema Does Not Exist".to_owned()))
            }
            Some((_, None)) => {
                self.sender
                    .send(Err(QueryError::table_does_not_exist(
                        self.select_input.schema_name.clone() + "." + self.select_input.table_name.as_str(),
                    )))
                    .expect("To Send Result to Client");
                Err(SystemError::runtime_check_failure("Table Does Not Exist".to_owned()))
            }
            Some((schema_id, Some(table_id))) => match self.storage.full_scan(schema_id, table_id) {
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
            },
        }
    }
}
