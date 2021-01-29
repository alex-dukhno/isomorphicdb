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

use catalog::Database;
use data_manipulation_typed_queries::TypedSelectQuery;
use data_manipulation_typed_tree::{DynamicTypedItem, DynamicTypedTree};
use read_query_plan::SelectPlan;
use std::sync::Arc;

pub struct ReadQueryPlanner<D: Database> {
    #[allow(dead_code)]
    database: Arc<D>,
}

impl<D: Database> ReadQueryPlanner<D> {
    pub fn new(database: Arc<D>) -> ReadQueryPlanner<D> {
        ReadQueryPlanner { database }
    }

    pub fn plan(&self, select: TypedSelectQuery) -> SelectPlan {
        SelectPlan {
            table: select.full_table_name,
            columns: select
                .projection_items
                .into_iter()
                .map(|item| match item {
                    DynamicTypedTree::Item(DynamicTypedItem::Column(name)) => name,
                    _ => unimplemented!(),
                })
                .collect(),
        }
    }
}
