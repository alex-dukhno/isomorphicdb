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

use ast::{
    predicates::{PredicateOp, PredicateValue},
    values::ScalarValue,
};
use binary::ReadCursor;
use data_manager::DataManager;
use metadata::MetadataView;
use plan::{FullTableId, SelectInput};
use protocol::{messages::ColumnMetadata, results::QueryEvent, Sender};
use sql_model::Id;
use std::{convert::TryInto, sync::Arc};

struct Source {
    table_id: FullTableId,
    cursor: Option<ReadCursor>,
    data_manager: Arc<DataManager>,
}

impl Source {
    fn new(table_id: FullTableId, data_manager: Arc<DataManager>) -> Source {
        Source {
            table_id,
            cursor: None,
            data_manager,
        }
    }
}

impl Iterator for Source {
    type Item = Vec<ScalarValue>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor.is_none() {
            self.cursor = self.data_manager.full_scan(&self.table_id).ok();
        }
        if let Some(cursor) = self.cursor.as_mut() {
            if let Some((_key, value)) = cursor.next().map(Result::unwrap).map(Result::unwrap) {
                Some(
                    value
                        .unpack()
                        .iter()
                        .map(|d| d.try_into().unwrap())
                        .collect::<Vec<ScalarValue>>(),
                )
            } else {
                None
            }
        } else {
            None
        }
    }
}

struct Projection<'p> {
    selected_columns: Vec<Id>,
    input: Box<dyn Iterator<Item = Vec<ScalarValue>> + 'p>,
    consumed: usize,
}

impl<'p> Projection<'p> {
    fn new(selected_columns: Vec<Id>, input: Box<dyn Iterator<Item = Vec<ScalarValue>> + 'p>) -> Projection<'p> {
        Projection {
            selected_columns,
            input,
            consumed: 0,
        }
    }
}

impl<'p, 'i> Iterator for &'i mut Projection<'p> {
    type Item = Vec<String>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(data) = self.input.next() {
            let mut values = vec![];
            for origin in self.selected_columns.iter() {
                values.push(data[*origin as usize].to_string());
            }
            self.consumed += 1;
            log::info!("TUPLE: {:?}", values);
            Some(values)
        } else {
            None
        }
    }
}

struct Filter<'f> {
    iter: Box<dyn Iterator<Item = Vec<ScalarValue>> + 'f>,
    predicate: (PredicateValue, PredicateOp, PredicateValue),
}

impl<'f> Filter<'f> {
    fn new(
        iter: Box<dyn Iterator<Item = Vec<ScalarValue>> + 'f>,
        predicate: (PredicateValue, PredicateOp, PredicateValue),
    ) -> Filter {
        Filter { iter, predicate }
    }
}

impl<'f> Iterator for Filter<'f> {
    type Item = Vec<ScalarValue>;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.predicate {
            (PredicateValue::Column(col_index), PredicateOp::Eq, PredicateValue::Number(num)) => {
                while let Some(tuple) = self.iter.next() {
                    if ScalarValue::Number(num.clone()) == tuple[*col_index as usize] {
                        return Some(tuple);
                    }
                }
                None
            }
            _ => panic!(),
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
        let mut projection = match self.select_input.predicate {
            None => Projection::new(self.select_input.selected_columns, Box::new(source)),
            Some(predicate) => {
                let predicate = Filter::new(Box::new(source), predicate);
                Projection::new(self.select_input.selected_columns, Box::new(predicate))
            }
        };

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
