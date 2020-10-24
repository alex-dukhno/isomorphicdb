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
use meta_def::ColumnDefinition;
use sql_model::{sql_types::SqlType, DEFAULT_CATALOG};
use sqlparser::ast::Ident;
use std::sync::Arc;

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
#[cfg(test)]
mod where_clause;

const SCHEMA: &str = "schema_name";
const TABLE: &str = "table_name";

#[rstest::fixture]
fn planner() -> QueryPlanner {
    let manager = DataDefinition::in_memory();
    manager.create_catalog(DEFAULT_CATALOG);
    QueryPlanner::new(Arc::new(manager))
}

#[rstest::fixture]
fn planner_with_schema() -> QueryPlanner {
    let manager = DataDefinition::in_memory();
    manager.create_catalog(DEFAULT_CATALOG);
    manager.create_schema(DEFAULT_CATALOG, SCHEMA).expect("schema created");
    QueryPlanner::new(Arc::new(manager))
}

#[rstest::fixture]
fn planner_with_table() -> QueryPlanner {
    let manager = DataDefinition::in_memory();
    manager.create_catalog(DEFAULT_CATALOG);
    let _schema_id = manager.create_schema(DEFAULT_CATALOG, SCHEMA).expect("schema created");
    manager
        .create_table(
            DEFAULT_CATALOG,
            SCHEMA,
            TABLE,
            &[
                ColumnDefinition::new("small_int", SqlType::SmallInt),
                ColumnDefinition::new("integer", SqlType::Integer),
                ColumnDefinition::new("big_int", SqlType::BigInt),
            ],
        )
        .expect("table created");
    QueryPlanner::new(Arc::new(manager))
}

#[rstest::fixture]
fn planner_with_no_column_table() -> QueryPlanner {
    let manager = DataDefinition::in_memory();
    manager.create_catalog(DEFAULT_CATALOG);
    let _schema_id = manager.create_schema(DEFAULT_CATALOG, SCHEMA).expect("schema created");
    manager
        .create_table(DEFAULT_CATALOG, SCHEMA, TABLE, &[])
        .expect("table created");
    QueryPlanner::new(Arc::new(manager))
}

fn ident<S: ToString>(name: S) -> Ident {
    Ident {
        value: name.to_string(),
        quote_style: None,
    }
}
