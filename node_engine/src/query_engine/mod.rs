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
use data_manipulation::{
    DynamicTypedTree, QueryPlan, StaticTypedTree, TypedDeleteQuery, TypedInsertQuery, TypedQuery, TypedSelectQuery,
    TypedUpdateQuery, UntypedQuery,
};
use definition_planner::DefinitionPlanner;
use postgre_sql::{
    query_ast::{Definition, Query, Statement},
    query_parser::QueryParser,
    query_response::{QueryError, QueryEvent},
};
use query_analyzer::QueryAnalyzer;
use query_planner::QueryPlanner;
use query_processing::{TypeChecker, TypeCoercion, TypeInference};
use storage::{Database, Transaction};

#[allow(dead_code)]
struct TransactionContext<'t> {
    parser: QueryParser,
    definition_planner: DefinitionPlanner<'t>,
    catalog: CatalogHandler<'t>,
    query_analyzer: QueryAnalyzer<'t>,
    type_inference: TypeInference,
    type_checker: TypeChecker,
    type_coercion: TypeCoercion,
    query_planner: QueryPlanner<'t>,
}

#[allow(dead_code)]
impl<'t> TransactionContext<'t> {
    fn new(transaction: Transaction<'t>) -> TransactionContext<'t> {
        TransactionContext {
            parser: QueryParser,
            definition_planner: DefinitionPlanner::from(transaction.clone()),
            catalog: CatalogHandler::from(transaction.clone()),
            query_analyzer: QueryAnalyzer::from(transaction.clone()),
            type_inference: TypeInference::default(),
            type_checker: TypeChecker,
            type_coercion: TypeCoercion,
            query_planner: QueryPlanner::from(transaction.clone()),
        }
    }

    fn commit(self) {}

    fn parse(&self, sql: &str) -> Result<Vec<Statement>, QueryError> {
        Ok(self.parser.parse(sql)?)
    }

    fn execute_ddl(&self, definition: Definition) -> Result<QueryEvent, QueryError> {
        let schema_change = self.definition_planner.plan(definition)?;
        Ok(self.catalog.apply(schema_change)?.into())
    }

    fn process(&self, query: Query) -> Result<TypedQuery, QueryError> {
        match self.query_analyzer.analyze(query)? {
            UntypedQuery::Insert(insert) => {
                let type_checked = insert
                    .values
                    .into_iter()
                    .map(|values| {
                        values
                            .into_iter()
                            .map(|value| value.map(|v| self.type_inference.infer_static(v, &[])))
                            .collect::<Vec<Option<StaticTypedTree>>>()
                    })
                    .map(|values| {
                        values
                            .into_iter()
                            .map(|value| value.map(|v| self.type_checker.check_static(v)))
                            .collect()
                    })
                    .collect::<Vec<Vec<Option<StaticTypedTree>>>>();
                let table_info = self
                    .catalog
                    .table_definition(insert.full_table_name.clone())
                    .unwrap()
                    .unwrap();
                let table_columns = table_info.columns();
                let mut type_coerced = vec![];
                for checked in type_checked {
                    let mut row = vec![];
                    for (index, c) in checked.into_iter().enumerate() {
                        row.push(c.map(|c| self.type_coercion.coerce_static(c, table_columns[index].sql_type())));
                    }
                    type_coerced.push(row);
                }
                Ok(TypedQuery::Insert(TypedInsertQuery {
                    full_table_name: insert.full_table_name,
                    values: type_coerced,
                }))
            }
            UntypedQuery::Select(select) => {
                let typed_values = select
                    .projection_items
                    .into_iter()
                    .map(|value| self.type_inference.infer_dynamic(value, &[]));
                let type_checked_values = typed_values
                    .into_iter()
                    .map(|value| self.type_checker.check_dynamic(value));
                let type_coerced_values = type_checked_values
                    .into_iter()
                    .map(|value| self.type_coercion.coerce_dynamic(value))
                    .collect::<Vec<DynamicTypedTree>>();

                let typed_filter = select.filter.map(|value| self.type_inference.infer_dynamic(value, &[]));
                let type_checked_filter = typed_filter.map(|value| self.type_checker.check_dynamic(value));
                let type_coerced_filter = type_checked_filter.map(|value| self.type_coercion.coerce_dynamic(value));

                Ok(TypedQuery::Select(TypedSelectQuery {
                    projection_items: type_coerced_values,
                    full_table_name: select.full_table_name,
                    filter: type_coerced_filter,
                }))
            }
            UntypedQuery::Update(update) => {
                let typed_values = update
                    .assignments
                    .into_iter()
                    .map(|value| value.map(|value| self.type_inference.infer_dynamic(value, &[])));
                let type_checked = typed_values
                    .into_iter()
                    .map(|value| value.map(|value| self.type_checker.check_dynamic(value)));
                let type_coerced = type_checked
                    .into_iter()
                    .map(|value| value.map(|value| self.type_coercion.coerce_dynamic(value)))
                    .collect::<Vec<Option<DynamicTypedTree>>>();

                let typed_filter = update.filter.map(|value| self.type_inference.infer_dynamic(value, &[]));
                let type_checked_filter = typed_filter.map(|value| self.type_checker.check_dynamic(value));
                let type_coerced_filter = type_checked_filter.map(|value| self.type_coercion.coerce_dynamic(value));

                Ok(TypedQuery::Update(TypedUpdateQuery {
                    full_table_name: update.full_table_name,
                    assignments: type_coerced,
                    filter: type_coerced_filter,
                }))
            }
            UntypedQuery::Delete(delete) => {
                let typed_filter = delete.filter.map(|value| self.type_inference.infer_dynamic(value, &[]));
                let type_checked_filter = typed_filter.map(|value| self.type_checker.check_dynamic(value));
                let type_coerced_filter = type_checked_filter.map(|value| self.type_coercion.coerce_dynamic(value));

                Ok(TypedQuery::Delete(TypedDeleteQuery {
                    full_table_name: delete.full_table_name,
                    filter: type_coerced_filter,
                }))
            }
        }
    }

    fn plan(&self, typed_query: TypedQuery) -> QueryPlan {
        self.query_planner.plan(typed_query)
    }
}

#[allow(dead_code)]
struct QueryEngine {
    database: Database,
}

#[allow(dead_code)]
impl QueryEngine {
    fn new(database: Database) -> QueryEngine {
        QueryEngine { database }
    }

    fn start_transaction(&self) -> TransactionContext {
        TransactionContext::new(self.database.transaction())
    }
}

#[cfg(test)]
mod tests;
