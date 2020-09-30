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

use binary::Binary;
use data_manager::{DataManager, MetadataView, ReadCursor};
use plan::{SelectInput, TableId};
use protocol::{messages::ColumnMetadata, results::QueryEvent, Sender};
use sql_model::Id;
use std::sync::Arc;

struct Source {
    table_id: TableId,
    cursor: Option<ReadCursor>,
    data_manager: Arc<DataManager>,
}

impl Source {
    fn new(table_id: TableId, data_manager: Arc<DataManager>) -> Source {
        Source {
            table_id,
            cursor: None,
            data_manager,
        }
    }
}

impl Iterator for Source {
    type Item = Binary;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor.is_none() {
            self.cursor = Some(self.data_manager.full_scan(&self.table_id).expect("no errors"));
        }
        if let Some(cursor) = self.cursor.as_mut() {
            cursor
                .next()
                .map(Result::unwrap)
                .map(Result::unwrap)
                .map(|(_key, values)| values)
        } else {
            None
        }
    }
}

struct Projection<I: Iterator<Item = Binary>> {
    selected_columns: Vec<Id>,
    input: Option<I>,
    consumed: usize,
}

impl<I: Iterator<Item = Binary>> Projection<I> {
    fn new(selected_columns: Vec<Id>) -> Projection<I> {
        Projection {
            selected_columns,
            input: None,
            consumed: 0,
        }
    }

    fn connect(&mut self, input: I) {
        self.input = Some(input)
    }
}

impl<'p, I: Iterator<Item = Binary>> Iterator for &'p mut Projection<I> {
    type Item = Vec<String>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(input) = self.input.as_mut() {
            if let Some(values) = input.next() {
                let data = values.unpack();
                let mut values = vec![];
                for origin in self.selected_columns.iter() {
                    values.push(data[*origin as usize].to_string());
                }
                self.consumed += 1;
                Some(values)
            } else {
                None
            }
        } else {
            None
        }
    }
}

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

    pub(crate) fn execute(self) {
        self.sender
            .send(Ok(QueryEvent::RowDescription(
                self.data_manager
                    .column_defs(&self.select_input.table_id, &self.select_input.selected_columns)
                    .into_iter()
                    .map(|column| ColumnMetadata::new(column.name(), (&column.sql_type()).into()))
                    .collect(),
            )))
            .expect("To Send Query Result to Client");

        let source = Source::new(self.select_input.table_id, self.data_manager.clone());
        let mut projection = Projection::new(self.select_input.selected_columns);
        projection.connect(source);

        for tuple in &mut projection {
            self.sender
                .send(Ok(QueryEvent::DataRow(tuple)))
                .expect("To Send Query Result to Client");
        }

        self.sender
            .send(Ok(QueryEvent::RecordsSelected(projection.consumed)))
            .expect("To Send Query Result to Client");
    }
}
