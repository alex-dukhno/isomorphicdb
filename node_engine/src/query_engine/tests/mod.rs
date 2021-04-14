// Copyright 2020 - 2021 Alex Dukhno
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
use postgres::query_response::QueryEvent;
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
mod predicate;
#[cfg(test)]
mod schema;
#[cfg(test)]
mod select;
#[cfg(test)]
mod simple_prepared_statement;
#[cfg(test)]
mod table;
#[cfg(test)]
mod type_constraints;
#[cfg(test)]
mod update;

type InMemory = QueryEngine;
type ResultCollector = Arc<Mutex<Collector>>;

#[derive(Clone)]
pub struct Collector(Arc<Mutex<Vec<Vec<u8>>>>);

impl Sender for Collector {
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn send(&mut self, message: &[u8]) -> io::Result<()> {
        self.0.lock().unwrap().push(message.to_vec());
        Ok(())
    }
}

impl Collector {
    fn new() -> ResultCollector {
        Arc::new(Mutex::new(Collector(Arc::new(Mutex::new(vec![])))))
    }

    #[allow(dead_code)]
    fn assert_receive_till_this_moment(&self, expected: Vec<Result<QueryEvent, QueryError>>) {
        let result = self.0.lock().expect("locked").drain(0..).collect::<Vec<_>>();
        assert_eq!(
            result,
            expected
                .into_iter()
                .map(|r| match r {
                    Ok(ok) => ok.into(),
                    Err(err) => err.into(),
                })
                .collect::<Vec<Vec<u8>>>()
        )
    }

    #[allow(dead_code)]
    fn assert_receive_intermediate(&self, expected: Result<QueryEvent, QueryError>) {
        let mut actual = self.0.lock().expect("locked");
        assert_eq!(
            actual.deref_mut().pop(),
            Some(expected).map(|r| match r {
                Ok(ok) => ok.into(),
                Err(err) => err.into(),
            })
        );
    }

    fn assert_receive_single(&self, expected: Result<QueryEvent, QueryError>) {
        self.assert_query_complete();
        let mut actual = self.0.lock().expect("locked");
        assert_eq!(
            actual.deref_mut().pop(),
            Some(expected).map(|r| match r {
                Ok(ok) => ok.into(),
                Err(err) => err.into(),
            })
        );
    }

    fn assert_receive_many(&self, expected: Vec<Result<QueryEvent, QueryError>>) {
        let actual = self
            .0
            .lock()
            .expect("locked")
            .drain(0..expected.len())
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            expected
                .into_iter()
                .map(|r| match r {
                    Ok(ok) => ok.into(),
                    Err(err) => err.into(),
                })
                .collect::<Vec<Vec<u8>>>()
        );
        self.assert_query_complete();
    }

    fn assert_query_complete(&self) {
        let mut actual = self.0.lock().expect("locked");
        assert_eq!(actual.deref_mut().pop(), Some(QueryEvent::QueryComplete.into()));
    }
}

#[rstest::fixture]
fn empty_database() -> (InMemory, ResultCollector) {
    setup_logger();
    let collector = Collector::new();
    (InMemory::new(collector.clone(), Database::in_memory("")), collector)
}

#[rstest::fixture]
fn database_with_schema(empty_database: (InMemory, ResultCollector)) -> (InMemory, ResultCollector) {
    let (mut engine, collector) = empty_database;
    engine
        .execute(Request::Query {
            sql: "create schema schema_name;".to_string(),
        })
        .expect("query expected");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::SchemaCreated));

    (engine, collector)
}

#[rstest::fixture]
fn database_with_table(database_with_schema: (InMemory, ResultCollector)) -> (InMemory, ResultCollector) {
    let (mut engine, collector) = database_with_schema;
    engine
        .execute(Request::Query {
            sql: "create table schema_name.table_name (col1 smallint, col2 smallint, col3 smallint);".to_string(),
        })
        .expect("query expected");
    collector
        .lock()
        .unwrap()
        .assert_receive_single(Ok(QueryEvent::TableCreated));

    (engine, collector)
}

fn setup_logger() {
    if let Ok(()) = simple_logger::SimpleLogger::new().init() {};
}
