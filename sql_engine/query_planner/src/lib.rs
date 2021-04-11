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

use catalog::CatalogHandler;
use data_manipulation_query_plan::{
    ConstraintValidator, DeleteQueryPlan, DynamicValues, Filter, FullTableScan, InsertQueryPlan, Projection, QueryPlan,
    Repeater, SelectQueryPlan, StaticExpressionEval, StaticValues, TableRecordKeys, UpdateQueryPlan,
};
use data_manipulation_typed_queries::TypedQuery;
use data_manipulation_typed_tree::{DynamicTypedItem, DynamicTypedTree};
use storage::TransactionalDatabase;

pub struct QueryPlanner<'p> {
    database: TransactionalDatabase<'p>,
    catalog: CatalogHandler<'p>,
}

impl<'p> From<TransactionalDatabase<'p>> for QueryPlanner<'p> {
    fn from(database: TransactionalDatabase<'p>) -> QueryPlanner<'p> {
        QueryPlanner {
            database: database.clone(),
            catalog: CatalogHandler::from(database),
        }
    }
}

impl<'p> QueryPlanner<'p> {
    pub fn plan(&self, query: TypedQuery) -> QueryPlan {
        match query {
            TypedQuery::Insert(insert) => {
                let table = self.database.table(&insert.full_table_name);
                QueryPlan::Insert(InsertQueryPlan::new(
                    ConstraintValidator::new(
                        StaticExpressionEval::new(StaticValues::new(insert.values)),
                        self.catalog.columns(&insert.full_table_name),
                    ),
                    table,
                ))
            }
            TypedQuery::Delete(delete) => {
                let table = self.database.table(&delete.full_table_name);
                QueryPlan::Delete(DeleteQueryPlan::new(
                    TableRecordKeys::new(FullTableScan::new(&table)),
                    table,
                ))
            }
            TypedQuery::Update(update) => {
                let table = self.database.table(&update.full_table_name);
                QueryPlan::Update(UpdateQueryPlan::new(
                    ConstraintValidator::new(
                        DynamicValues::new(Repeater::new(update.assignments), FullTableScan::new(&table)),
                        self.catalog.columns(&update.full_table_name),
                    ),
                    FullTableScan::new(&table),
                    table,
                ))
            }
            TypedQuery::Select(select) => {
                let table = self.database.table(&select.full_table_name);
                QueryPlan::Select(SelectQueryPlan::new(
                    Filter::new(Projection::new(FullTableScan::new(&table)), select.filter),
                    select
                        .projection_items
                        .into_iter()
                        .map(|item| match item {
                            DynamicTypedTree::Item(DynamicTypedItem::Column { name, .. }) => name,
                            _ => unimplemented!(),
                        })
                        .collect(),
                    self.catalog.columns_short(&select.full_table_name),
                ))
            }
        }
    }
}
