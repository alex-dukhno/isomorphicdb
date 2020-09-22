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

use std::{
    io,
    sync::{Arc, Mutex},
};

use protocol::results::{QueryEvent, QueryResult};

use data_manager::DataManager;
use parser::QueryParser;
use protocol::Sender;
use sql_engine::QueryExecutor;
use std::ops::DerefMut;

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
    pub fn assert_receive_till_this_moment(&self, expected: Vec<QueryResult>) {
        let result = self.0.lock().expect("locked").drain(0..).collect::<Vec<_>>();
        assert_eq!(result, expected)
    }

    pub fn assert_receive_intermediate(&self, expected: QueryResult) {
        let mut actual = self.0.lock().expect("locked");
        assert_eq!(actual.deref_mut().pop(), Some(expected));
    }

    pub fn assert_receive_single(&self, expected: QueryResult) {
        self.assert_query_complete();
        let mut actual = self.0.lock().expect("locked");
        assert_eq!(actual.deref_mut().pop(), Some(expected));
    }

    pub fn assert_receive_many(&self, expected: Vec<QueryResult>) {
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

pub type ResultCollector = Arc<Collector>;

#[rstest::fixture]
pub fn sender() -> ResultCollector {
    Arc::new(Collector(Mutex::new(vec![])))
}

#[rstest::fixture]
pub fn empty_database() -> (QueryExecutor, QueryParser, ResultCollector) {
    let collector = Arc::new(Collector(Mutex::new(vec![])));
    let data_manager = Arc::new(DataManager::in_memory().expect("to create data manager"));
    (
        QueryExecutor::new(data_manager.clone(), collector.clone()),
        QueryParser::new(collector.clone(), data_manager),
        collector,
    )
}

#[rstest::fixture]
pub fn database_with_schema(
    empty_database: (QueryExecutor, QueryParser, ResultCollector),
) -> (QueryExecutor, QueryParser, ResultCollector) {
    let (engine, parser, collector) = empty_database;
    engine.execute(&parser.parse("create schema schema_name;").expect("query parsed"));
    collector.assert_receive_single(Ok(QueryEvent::SchemaCreated));

    (engine, parser, collector)
}
