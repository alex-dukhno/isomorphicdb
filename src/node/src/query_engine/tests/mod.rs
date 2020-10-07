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

use super::*;
use protocol::results::{QueryEvent, QueryResult};
use std::{
    io,
    ops::DerefMut,
    sync::{Arc, Mutex},
};

#[cfg(test)]
mod delete;
#[cfg(test)]
mod extended_query_flow;
#[cfg(test)]
mod insert;
#[cfg(test)]
mod schema;
#[cfg(test)]
mod select;
#[cfg(test)]
mod table;
#[cfg(test)]
mod type_constraints;
#[cfg(test)]
mod update;
#[cfg(test)]
mod where_clause;

type InMemory = QueryEngine;
type ResultCollector = Arc<Collector>;

pub struct Collector(Mutex<Vec<QueryResult>>);

impl Sender for Collector {
    fn flush(&self) -> io::Result<()> {
        Ok(())
    }

    fn send(&self, query_result: QueryResult) -> io::Result<()> {
        self.0.lock().expect("locked").push(query_result);
        Ok(())
    }
}

impl Collector {
    fn new() -> ResultCollector {
        Arc::new(Collector(Mutex::new(vec![])))
    }

    fn assert_receive_till_this_moment(&self, expected: Vec<QueryResult>) {
        let result = self.0.lock().expect("locked").drain(0..).collect::<Vec<_>>();
        assert_eq!(result, expected)
    }

    fn assert_receive_intermediate(&self, expected: QueryResult) {
        let mut actual = self.0.lock().expect("locked");
        assert_eq!(actual.deref_mut().pop(), Some(expected));
    }

    fn assert_receive_single(&self, expected: QueryResult) {
        self.assert_query_complete();
        let mut actual = self.0.lock().expect("locked");
        assert_eq!(actual.deref_mut().pop(), Some(expected));
    }

    fn assert_receive_many(&self, expected: Vec<QueryResult>) {
        let actual = self
            .0
            .lock()
            .expect("locked")
            .drain(0..expected.len())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        self.assert_query_complete();
    }

    fn assert_query_complete(&self) {
        let mut actual = self.0.lock().expect("locked");
        assert_eq!(actual.deref_mut().pop(), Some(Ok(QueryEvent::QueryComplete)));
    }
}

#[rstest::fixture]
fn empty_database() -> (InMemory, ResultCollector) {
    let collector = Collector::new();
    let metadata = Arc::new(DataDefinition::in_memory());
    (
        InMemory::new(
            collector.clone(),
            metadata.clone(),
            Arc::new(DataManager::in_memory(metadata)),
        ),
        collector,
    )
}

#[rstest::fixture]
fn database_with_schema(empty_database: (InMemory, ResultCollector)) -> (InMemory, ResultCollector) {
    let (mut engine, collector) = empty_database;
    engine
        .execute(Command::Query {
            sql: "create schema schema_name;".to_string(),
        })
        .expect("query expected");
    collector.assert_receive_single(Ok(QueryEvent::SchemaCreated));

    (engine, collector)
}
