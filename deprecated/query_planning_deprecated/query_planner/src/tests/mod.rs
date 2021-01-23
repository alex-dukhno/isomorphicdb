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
use data_manager::DatabaseHandle;
use meta_def::DeprecatedColumnDefinition;
use sql_ast::Ident;
use std::sync::Arc;
use types::SqlType;

#[cfg(test)]
mod delete;
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
fn planner() -> OldDeprecatedQueryPlanner {
    let manager = DatabaseHandle::in_memory();
    OldDeprecatedQueryPlanner::new(Arc::new(manager))
}

#[rstest::fixture]
fn planner_with_schema() -> OldDeprecatedQueryPlanner {
    let manager = DatabaseHandle::in_memory();
    manager.create_schema(SCHEMA).expect("schema created");
    OldDeprecatedQueryPlanner::new(Arc::new(manager))
}

#[rstest::fixture]
fn planner_with_table() -> OldDeprecatedQueryPlanner {
    let manager = DatabaseHandle::in_memory();
    let schema_id = manager.create_schema(SCHEMA).expect("schema created");
    manager
        .create_table(
            schema_id,
            TABLE,
            &[
                DeprecatedColumnDefinition::new("small_int", SqlType::small_int()),
                DeprecatedColumnDefinition::new("integer", SqlType::integer()),
                DeprecatedColumnDefinition::new("big_int", SqlType::big_int()),
            ],
        )
        .expect("table created");
    OldDeprecatedQueryPlanner::new(Arc::new(manager))
}

#[rstest::fixture]
fn planner_with_no_column_table() -> OldDeprecatedQueryPlanner {
    let manager = DatabaseHandle::in_memory();
    let schema_id = manager.create_schema(SCHEMA).expect("schema created");
    manager.create_table(schema_id, TABLE, &[]).expect("table created");
    OldDeprecatedQueryPlanner::new(Arc::new(manager))
}

fn ident<S: ToString>(name: S) -> Ident {
    Ident {
        value: name.to_string(),
        quote_style: None,
    }
}
