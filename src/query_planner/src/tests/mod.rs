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
use crate::planner::QueryPlanner;
use data_manager::DataManager;
use protocol::{results::QueryResult, Sender};
use std::{
    io,
    ops::Deref,
    sync::{Arc, Mutex},
};

#[cfg(test)]
mod create_table;

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
}

type ResultCollector = Arc<Collector>;

#[rstest::fixture]
fn sender() -> ResultCollector {
    Arc::new(Collector(Mutex::new(vec![])))
}

#[rstest::fixture]
fn planner_and_sender() -> (QueryPlanner, ResultCollector) {
    let collector = Arc::new(Collector(Mutex::new(vec![])));
    let manager = DataManager::in_memory().expect("to create data manager");
    manager.create_schema("schema_name").expect("schema created");
    (QueryPlanner::new(Arc::new(manager), collector.clone()), collector)
}