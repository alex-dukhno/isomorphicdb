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
mod delete;
#[cfg(test)]
mod describe_prepared_statement;
#[cfg(test)]
mod in_memory_backend_storage;
#[cfg(test)]
mod insert;
#[cfg(test)]
mod parse;
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
use in_memory_backend_storage::InMemoryStorage;
use protocol::results::QueryResult;
use std::{
    io,
    ops::Deref,
    sync::{Arc, Mutex},
};

fn in_memory_storage() -> Arc<CatalogManager> {
    Arc::new(CatalogManager::new(Box::new(InMemoryStorage::default())).unwrap())
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
        let expected: Vec<QueryResult> = expected
            .iter()
            .flat_map(|result| vec![result.clone(), Ok(QueryEvent::QueryComplete)])
            .collect();
        assert_eq!(actual.deref(), &expected)
    }
}

#[rstest::fixture]
fn sql_engine() -> (QueryExecutor, Arc<Collector>) {
    let collector = Arc::new(Collector(Mutex::new(vec![])));
    (QueryExecutor::new(in_memory_storage(), collector.clone()), collector)
}

#[rstest::fixture]
fn sql_engine_with_schema(sql_engine: (QueryExecutor, Arc<Collector>)) -> (QueryExecutor, Arc<Collector>) {
    let (mut engine, collector) = sql_engine;
    engine.execute("create schema schema_name;").expect("no system errors");

    (engine, collector)
}
