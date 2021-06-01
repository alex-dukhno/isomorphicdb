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
use definition::ColumnDef;
use definition_planner::DefinitionPlanner;
use postgre_sql::{
    query_ast::{Definition, Query},
    query_response::{QueryError, QueryEvent},
};
use query_analyzer::QueryAnalyzer;
use query_plan::QueryPlan;
use query_planner::QueryPlanner;
use query_processing::{TypeCheckerOld, TypeCoercionOld, TypeInferenceOld};
use std::fmt::{self, Debug, Formatter};
use storage::{Database, Transaction};
use typed_queries::{TypedDeleteQuery, TypedInsertQuery, TypedQuery, TypedSelectQuery, TypedUpdateQuery};
use typed_tree::TypedTreeOld;
use types::SqlTypeFamilyOld;
use untyped_queries::{UntypedInsertQuery, UntypedQuery, UntypedUpdateQuery};

pub struct TransactionManager {
    database: Database,
}

impl TransactionManager {
    pub fn new(database: Database) -> TransactionManager {
        TransactionManager { database }
    }

    pub fn start_transaction(&self) -> TransactionContext {
        TransactionContext::new(self.database.transaction())
    }
}

pub struct TransactionContext<'t> {
    definition_planner: DefinitionPlanner<'t>,
    catalog: CatalogHandler<'t>,
    query_analyzer: QueryAnalyzer<'t>,
    type_inference: TypeInferenceOld,
    type_checker: TypeCheckerOld,
    type_coercion: TypeCoercionOld,
    query_planner: QueryPlanner<'t>,
}

impl<'t> Debug for TransactionContext<'t> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "txn")
    }
}

impl<'t> TransactionContext<'t> {
    pub fn new(transaction: Transaction<'t>) -> TransactionContext<'t> {
        TransactionContext {
            definition_planner: DefinitionPlanner::from(transaction.clone()),
            catalog: CatalogHandler::from(transaction.clone()),
            query_analyzer: QueryAnalyzer::from(transaction.clone()),
            type_inference: TypeInferenceOld::default(),
            type_checker: TypeCheckerOld,
            type_coercion: TypeCoercionOld,
            query_planner: QueryPlanner::from(transaction.clone()),
        }
    }

    pub fn describe_insert(&self, insert: &UntypedInsertQuery) -> Vec<u32> {
        let table_definition = self.catalog.table_definition(insert.full_table_name.clone()).unwrap().unwrap();
        table_definition
            .columns()
            .iter()
            .map(ColumnDef::sql_type)
            .map(|sql_type| (&sql_type).into())
            .collect::<Vec<u32>>()
    }

    pub fn describe_update(&self, update: &UntypedUpdateQuery) -> Vec<u32> {
        let table_definition = self.catalog.table_definition(update.full_table_name.clone()).unwrap().unwrap();
        table_definition
            .columns()
            .iter()
            .map(ColumnDef::sql_type)
            .map(|sql_type| (&sql_type).into())
            .collect::<Vec<u32>>()
    }

    pub fn analyze(&self, query: Query) -> Result<UntypedQuery, QueryError> {
        Ok(self.query_analyzer.analyze(query)?)
    }

    pub fn process_untyped_query(&self, untyped_query: UntypedQuery, param_types: Vec<SqlTypeFamilyOld>) -> Result<TypedQuery, QueryError> {
        match untyped_query {
            UntypedQuery::Insert(insert) => {
                let type_coerced = insert
                    .values
                    .into_iter()
                    .map(|values| {
                        values
                            .into_iter()
                            .map(|value| value.map(|v| self.type_inference.infer_type(v, &param_types)))
                            .collect::<Vec<Option<TypedTreeOld>>>()
                    })
                    .map(|values| {
                        values
                            .into_iter()
                            .map(|value| value.map(|v| self.type_checker.type_check(v)))
                            .collect::<Vec<Option<TypedTreeOld>>>()
                    })
                    .map(|values| values.into_iter().map(|value| value.map(|c| self.type_coercion.coerce(c))).collect())
                    .collect::<Vec<Vec<Option<TypedTreeOld>>>>();
                Ok(TypedQuery::Insert(TypedInsertQuery {
                    full_table_name: insert.full_table_name,
                    values: type_coerced,
                }))
            }
            UntypedQuery::Select(select) => {
                let typed_values = select
                    .projection_items
                    .into_iter()
                    .map(|value| self.type_inference.infer_type(value, &param_types));
                let type_checked_values = typed_values.into_iter().map(|value| self.type_checker.type_check(value));
                let type_coerced_values = type_checked_values
                    .into_iter()
                    .map(|value| self.type_coercion.coerce(value))
                    .collect::<Vec<TypedTreeOld>>();

                let typed_filter = select.filter.map(|value| self.type_inference.infer_type(value, &param_types));
                let type_checked_filter = typed_filter.map(|value| self.type_checker.type_check(value));
                let type_coerced_filter = type_checked_filter.map(|value| self.type_coercion.coerce(value));

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
                    .map(|value| value.map(|value| self.type_inference.infer_type(value, &param_types)));
                let type_checked = typed_values
                    .into_iter()
                    .map(|value| value.map(|value| self.type_checker.type_check(value)));
                let type_coerced = type_checked
                    .into_iter()
                    .map(|value| value.map(|value| self.type_coercion.coerce(value)))
                    .collect::<Vec<Option<TypedTreeOld>>>();

                let typed_filter = update.filter.map(|value| self.type_inference.infer_type(value, &param_types));
                let type_checked_filter = typed_filter.map(|value| self.type_checker.type_check(value));
                let type_coerced_filter = type_checked_filter.map(|value| self.type_coercion.coerce(value));

                Ok(TypedQuery::Update(TypedUpdateQuery {
                    full_table_name: update.full_table_name,
                    assignments: type_coerced,
                    filter: type_coerced_filter,
                }))
            }
            UntypedQuery::Delete(delete) => {
                let typed_filter = delete.filter.map(|value| self.type_inference.infer_type(value, &param_types));
                let type_checked_filter = typed_filter.map(|value| self.type_checker.type_check(value));
                let type_coerced_filter = type_checked_filter.map(|value| self.type_coercion.coerce(value));

                Ok(TypedQuery::Delete(TypedDeleteQuery {
                    full_table_name: delete.full_table_name,
                    filter: type_coerced_filter,
                }))
            }
        }
    }

    pub fn commit(self) {}

    pub fn apply_schema_change(&self, definition: Definition) -> Result<QueryEvent, QueryError> {
        let schema_change = self.definition_planner.plan(definition)?;
        Ok(self.catalog.apply(schema_change)?.into())
    }

    pub fn process(&self, query: Query, param_types: Vec<SqlTypeFamilyOld>) -> Result<TypedQuery, QueryError> {
        match self.query_analyzer.analyze(query)? {
            UntypedQuery::Insert(insert) => {
                let type_coerced = insert
                    .values
                    .into_iter()
                    .map(|values| {
                        values
                            .into_iter()
                            .map(|value| value.map(|v| self.type_inference.infer_type(v, &param_types)))
                            .collect::<Vec<Option<TypedTreeOld>>>()
                    })
                    .map(|values| {
                        values
                            .into_iter()
                            .map(|value| value.map(|v| self.type_checker.type_check(v)))
                            .collect::<Vec<Option<TypedTreeOld>>>()
                    })
                    .map(|values| values.into_iter().map(|value| value.map(|v| self.type_coercion.coerce(v))).collect())
                    .collect::<Vec<Vec<Option<TypedTreeOld>>>>();
                Ok(TypedQuery::Insert(TypedInsertQuery {
                    full_table_name: insert.full_table_name,
                    values: type_coerced,
                }))
            }
            UntypedQuery::Select(select) => {
                let typed_values = select
                    .projection_items
                    .into_iter()
                    .map(|value| self.type_inference.infer_type(value, &[]));
                let type_checked_values = typed_values.into_iter().map(|value| self.type_checker.type_check(value));
                let type_coerced_values = type_checked_values
                    .into_iter()
                    .map(|value| self.type_coercion.coerce(value))
                    .collect::<Vec<TypedTreeOld>>();

                let typed_filter = select.filter.map(|value| self.type_inference.infer_type(value, &[]));
                let type_checked_filter = typed_filter.map(|value| self.type_checker.type_check(value));
                let type_coerced_filter = type_checked_filter.map(|value| self.type_coercion.coerce(value));

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
                    .map(|value| value.map(|value| self.type_inference.infer_type(value, &[])));
                let type_checked = typed_values
                    .into_iter()
                    .map(|value| value.map(|value| self.type_checker.type_check(value)));
                let type_coerced = type_checked
                    .into_iter()
                    .map(|value| value.map(|value| self.type_coercion.coerce(value)))
                    .collect::<Vec<Option<TypedTreeOld>>>();

                let typed_filter = update.filter.map(|value| self.type_inference.infer_type(value, &[]));
                let type_checked_filter = typed_filter.map(|value| self.type_checker.type_check(value));
                let type_coerced_filter = type_checked_filter.map(|value| self.type_coercion.coerce(value));

                Ok(TypedQuery::Update(TypedUpdateQuery {
                    full_table_name: update.full_table_name,
                    assignments: type_coerced,
                    filter: type_coerced_filter,
                }))
            }
            UntypedQuery::Delete(delete) => {
                let typed_filter = delete.filter.map(|value| self.type_inference.infer_type(value, &[]));
                let type_checked_filter = typed_filter.map(|value| self.type_checker.type_check(value));
                let type_coerced_filter = type_checked_filter.map(|value| self.type_coercion.coerce(value));

                Ok(TypedQuery::Delete(TypedDeleteQuery {
                    full_table_name: delete.full_table_name,
                    filter: type_coerced_filter,
                }))
            }
        }
    }

    pub fn plan(&self, typed_query: TypedQuery) -> QueryPlan {
        self.query_planner.plan(typed_query)
    }
}
