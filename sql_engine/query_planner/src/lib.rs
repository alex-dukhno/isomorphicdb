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
use data_manipulation_typed_tree::{TypedItem, TypedTree};
use storage::Transaction;

pub struct QueryPlanner<'p> {
    transaction: Transaction<'p>,
    catalog: CatalogHandler<'p>,
}

impl<'p> From<Transaction<'p>> for QueryPlanner<'p> {
    fn from(transaction: Transaction<'p>) -> QueryPlanner<'p> {
        QueryPlanner {
            transaction: transaction.clone(),
            catalog: CatalogHandler::from(transaction),
        }
    }
}

impl<'p> QueryPlanner<'p> {
    pub fn plan(&self, query: TypedQuery) -> QueryPlan {
        match query {
            TypedQuery::Insert(insert) => {
                let table = self.transaction.lookup_table_ref(&insert.full_table_name);
                QueryPlan::Insert(InsertQueryPlan::new(
                    ConstraintValidator::new(
                        StaticExpressionEval::new(StaticValues::new(insert.values)),
                        self.catalog.columns(&insert.full_table_name),
                    ),
                    table,
                ))
            }
            TypedQuery::Delete(delete) => {
                let table = self.transaction.lookup_table_ref(&delete.full_table_name);
                QueryPlan::Delete(DeleteQueryPlan::new(
                    TableRecordKeys::new(Filter::new(Projection::new(FullTableScan::new(&table)), delete.filter)),
                    table,
                ))
            }
            TypedQuery::Update(update) => {
                let table = self.transaction.lookup_table_ref(&update.full_table_name);
                QueryPlan::Update(UpdateQueryPlan::new(
                    ConstraintValidator::new(
                        DynamicValues::new(
                            Repeater::new(update.assignments),
                            Filter::new(Projection::new(FullTableScan::new(&table)), update.filter),
                        ),
                        self.catalog.columns(&update.full_table_name),
                    ),
                    FullTableScan::new(&table),
                    table,
                ))
            }
            TypedQuery::Select(select) => {
                let table = self.transaction.lookup_table_ref(&select.full_table_name);
                QueryPlan::Select(SelectQueryPlan::new(
                    Filter::new(Projection::new(FullTableScan::new(&table)), select.filter),
                    select
                        .projection_items
                        .into_iter()
                        .map(|item| match item {
                            TypedTree::Item(TypedItem::Column { name, .. }) => name,
                            _ => unimplemented!(),
                        })
                        .collect(),
                    self.catalog.columns_short(&select.full_table_name),
                ))
            }
        }
    }
}
