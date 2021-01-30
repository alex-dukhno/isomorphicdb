// Copyright 2020 - present Alex Dukhno
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
use catalog::InMemoryDatabase;
use pg_model::{
    results::{QueryEvent, QueryResult},
    Command,
};
use pg_wire::ColumnMetadata;
use std::{
    io,
    ops::DerefMut,
    sync::{Arc, Mutex},
};

// TODO: new engine does not handle deletes
#[cfg(test)]
mod delete;
// TODO: new engine does not handle extended query flow
// #[cfg(test)]
// mod extended_query_flow;
#[cfg(test)]
mod insert;
#[cfg(test)]
mod schema;
#[cfg(test)]
mod select;
// TODO: new engine does not handle extended query flow
// #[cfg(test)]
// mod simple_prepared_statement;
#[cfg(test)]
mod table;
// TODO: new engine does not handle type constraints
// #[cfg(test)]
// mod type_constraints;
// TODO: new engine does not handle updates
// #[cfg(test)]
// mod update;
// TODO: new engine does not support sophisticated selection plans
// #[cfg(test)]
// mod where_clause;

type InMemory = QueryEngine<InMemoryDatabase>;
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

    #[allow(dead_code)]
    fn assert_receive_till_this_moment(&self, expected: Vec<QueryResult>) {
        let result = self.0.lock().expect("locked").drain(0..).collect::<Vec<_>>();
        assert_eq!(result, expected)
    }

    #[allow(dead_code)]
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
    (
        InMemory::new(
            collector.clone(),
            Arc::new(DatabaseHandle::in_memory()),
            InMemoryDatabase::new(),
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

#[rstest::fixture]
fn database_with_table(database_with_schema: (InMemory, ResultCollector)) -> (InMemory, ResultCollector) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(Command::Query {
            sql: "create table schema_name.table_name (col1 smallint, col2 smallint, col3 smallint);".to_string(),
        })
        .expect("query expected");
    collector.assert_receive_single(Ok(QueryEvent::TableCreated));

    (engine, collector)
}
