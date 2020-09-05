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

#[cfg(test)]
mod bind;
#[cfg(test)]
mod bind_prepared_statement_to_portal;
#[cfg(test)]
mod delete;
#[cfg(test)]
mod describe_prepared_statement;
#[cfg(test)]
mod execute_portal;
#[cfg(test)]
mod insert;
#[cfg(test)]
mod parse_prepared_statement;
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

use super::*;
use crate::{catalog_manager::CatalogManager, QueryExecutor};
use protocol::results::{QueryError, QueryResult};
use std::{
    io,
    ops::Deref,
    sync::{Arc, Mutex},
};

fn in_memory_catalog_manager() -> Arc<CatalogManager> {
    Arc::new(CatalogManager::default())
}

struct Collector(Mutex<Vec<QueryResult>>);

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
    fn assert_content(&self, expected: Vec<QueryResult>) {
        let result = self.0.lock().expect("locked");
        assert_eq!(result.deref(), &expected)
    }

    fn assert_content_for_single_queries(&self, expected: Vec<QueryResult>) {
        let actual = self.0.lock().expect("locked");
        assert_eq!(actual.deref(), &expected)
    }
}

type ResultCollector = Arc<Collector>;

#[rstest::fixture]
fn sender() -> ResultCollector {
    Arc::new(Collector(Mutex::new(vec![])))
}

#[rstest::fixture]
fn sql_engine() -> (QueryExecutor, ResultCollector) {
    let collector = Arc::new(Collector(Mutex::new(vec![])));
    (
        QueryExecutor::new(in_memory_catalog_manager(), collector.clone()),
        collector,
    )
}

#[rstest::fixture]
fn sql_engine_with_schema(sql_engine: (QueryExecutor, ResultCollector)) -> (QueryExecutor, ResultCollector) {
    let (mut engine, collector) = sql_engine;
    engine.execute("create schema schema_name;").expect("no system errors");

    (engine, collector)
}
