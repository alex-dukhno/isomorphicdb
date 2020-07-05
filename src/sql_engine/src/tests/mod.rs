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
mod insert;
#[cfg(test)]
mod schema;
#[cfg(test)]
mod select;
#[cfg(test)]
mod table;
#[cfg(test)]
mod update;

use super::*;
use crate::Handler;
use std::sync::{Arc, Mutex};
use storage::frontend::FrontendStorage;
use test_helpers::in_memory_backend_storage::InMemoryStorage;

fn in_memory_storage() -> Arc<Mutex<FrontendStorage<InMemoryStorage>>> {
    Arc::new(Mutex::new(FrontendStorage::new(InMemoryStorage::default()).unwrap()))
}

type InMemorySqlEngine = Handler<InMemoryStorage>;

#[rstest::fixture]
fn sql_engine() -> InMemorySqlEngine {
    Handler::new(in_memory_storage())
}

#[rstest::fixture]
fn sql_engine_with_schema(mut sql_engine: InMemorySqlEngine) -> InMemorySqlEngine {
    sql_engine
        .execute("create schema schema_name;")
        .expect("no system errors")
        .expect("schema created");

    sql_engine
}
