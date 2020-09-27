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
use data_manager::{ColumnDefinition, DataManager};
use protocol::{results::QueryResult, Sender};
use sql_model::sql_types::SqlType;
use sqlparser::ast::Ident;
use std::{
    io,
    ops::Deref,
    sync::{Arc, Mutex},
};

#[cfg(test)]
mod create_schema;
#[cfg(test)]
mod create_table;
#[cfg(test)]
mod delete;
#[cfg(test)]
mod drop_schema;
#[cfg(test)]
mod drop_table;
#[cfg(test)]
mod insert;
#[cfg(test)]
mod select;
#[cfg(test)]
mod update;

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

const SCHEMA: &str = "schema_name";
const TABLE: &str = "table_name";

#[rstest::fixture]
fn sender() -> ResultCollector {
    Arc::new(Collector(Mutex::new(vec![])))
}

#[rstest::fixture]
fn planner_and_sender() -> (QueryPlanner, ResultCollector) {
    let collector = Arc::new(Collector(Mutex::new(vec![])));
    let manager = DataManager::in_memory().expect("to create data manager");
    (QueryPlanner::new(Arc::new(manager), collector.clone()), collector)
}

#[rstest::fixture]
fn planner_and_sender_with_schema() -> (QueryPlanner, ResultCollector) {
    let collector = Arc::new(Collector(Mutex::new(vec![])));
    let manager = DataManager::in_memory().expect("to create data manager");
    manager.create_schema(SCHEMA).expect("schema created");
    (QueryPlanner::new(Arc::new(manager), collector.clone()), collector)
}

#[rstest::fixture]
fn planner_and_sender_with_table() -> (QueryPlanner, ResultCollector) {
    let collector = Arc::new(Collector(Mutex::new(vec![])));
    let manager = DataManager::in_memory().expect("to create data manager");
    let schema_id = manager.create_schema(SCHEMA).expect("schema created");
    manager
        .create_table(
            schema_id,
            TABLE,
            &[
                ColumnDefinition::new("small_int", SqlType::SmallInt(0)),
                ColumnDefinition::new("integer", SqlType::Integer(0)),
                ColumnDefinition::new("big_int", SqlType::BigInt(0)),
            ],
        )
        .expect("table created");
    (QueryPlanner::new(Arc::new(manager), collector.clone()), collector)
}

#[rstest::fixture]
fn planner_and_sender_with_no_column_table() -> (QueryPlanner, ResultCollector) {
    let collector = Arc::new(Collector(Mutex::new(vec![])));
    let manager = DataManager::in_memory().expect("to create data manager");
    let schema_id = manager.create_schema(SCHEMA).expect("schema created");
    manager.create_table(schema_id, TABLE, &[]).expect("table created");
    (QueryPlanner::new(Arc::new(manager), collector.clone()), collector)
}

fn ident<S: ToString>(name: S) -> Ident {
    Ident {
        value: name.to_string(),
        quote_style: None,
    }
}
