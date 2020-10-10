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

use crate::{PlanError, Planner, Result};
use ast::predicates::{PredicateOp, PredicateValue};
use metadata::DataDefinition;
use plan::{FullTableName, Plan, SelectInput, TableId};
use sql_model::DEFAULT_CATALOG;
use sqlparser::ast::{
    BinaryOperator, Expr, Ident, Query, Select, SelectItem, SetExpr, TableFactor, TableWithJoins, Value,
};
use std::{collections::HashMap, convert::TryFrom, ops::Deref, sync::Arc};

pub(crate) struct SelectPlanner {
    query: Box<Query>,
}

impl SelectPlanner {
    pub(crate) fn new(query: Box<Query>) -> SelectPlanner {
        SelectPlanner { query }
    }
}

impl Planner for SelectPlanner {
    fn plan(self, metadata: Arc<DataDefinition>) -> Result<Plan> {
        let Query { body, .. } = &*self.query;
        let result = if let SetExpr::Select(query) = body {
            let Select {
                projection,
                from,
                selection,
                ..
            } = query.deref();
            let TableWithJoins { relation, .. } = &from[0];
            let name = match relation {
                TableFactor::Table { name, .. } => name,
                _ => {
                    return Err(vec![PlanError::feature_not_supported(&*self.query)]);
                }
            };

            match FullTableName::try_from(name) {
                Ok(full_table_name) => {
                    let (schema_name, table_name) = full_table_name.as_tuple();
                    match metadata.table_exists(DEFAULT_CATALOG, &schema_name, &table_name) {
                        None => return Err(vec![]), // TODO catalog does not exists
                        Some((_, None)) => {
                            return Err(vec![PlanError::schema_does_not_exist(&schema_name)]);
                        }
                        Some((_, Some((_, None)))) => {
                            return Err(vec![PlanError::table_does_not_exist(&full_table_name)]);
                        }
                        Some((_, Some((schema_id, Some(table_id))))) => {
                            let selected_columns = {
                                let mut names: Vec<String> = vec![];
                                for item in projection {
                                    match item {
                                        SelectItem::Wildcard => {
                                            let all_columns =
                                                metadata.table_columns(DEFAULT_CATALOG, schema_name, table_name);
                                            names.extend(
                                                all_columns
                                                    .into_iter()
                                                    .map(|column_definition| column_definition.name())
                                                    .collect::<Vec<String>>(),
                                            )
                                        }
                                        SelectItem::UnnamedExpr(Expr::Identifier(Ident { value, .. })) => {
                                            names.push(value.to_lowercase())
                                        }
                                        _ => {
                                            return Err(vec![PlanError::feature_not_supported(&*self.query)]);
                                        }
                                    }
                                }
                                let columns = metadata
                                    .table_column_names_ids(DEFAULT_CATALOG, schema_name, table_name)
                                    .into_iter()
                                    .collect::<HashMap<_, _>>();
                                let mut ids = vec![];
                                let mut not_found = vec![];
                                for name in names {
                                    match columns.get(&name) {
                                        Some(id) => ids.push(*id),
                                        None => not_found.push(PlanError::column_does_not_exist(&name)),
                                    }
                                }

                                if !not_found.is_empty() {
                                    return Err(not_found);
                                }
                                ids
                            };

                            let predicate = match selection {
                                Some(Expr::BinaryOp { left, op, right }) => {
                                    let l = match left.deref() {
                                        Expr::Identifier(ident) => {
                                            let columns = metadata
                                                .table_column_names_ids(DEFAULT_CATALOG, schema_name, table_name)
                                                .into_iter()
                                                .collect::<HashMap<_, _>>();
                                            let mut ids = vec![];
                                            let mut not_found = vec![];
                                            match columns.get(&ident.to_string()) {
                                                Some(id) => ids.push(*id),
                                                None => not_found.push(ident.to_string()),
                                            }
                                            PredicateValue::Column(ids[0])
                                        }
                                        _ => panic!(),
                                    };
                                    let o = match op {
                                        BinaryOperator::Eq => PredicateOp::Eq,
                                        _ => panic!(),
                                    };
                                    let r = match right.deref() {
                                        Expr::Value(Value::Number(num)) => PredicateValue::Number(num.clone()),
                                        _ => panic!(),
                                    };
                                    Some((l, o, r))
                                }
                                _ => None,
                            };

                            SelectInput {
                                table_id: TableId::from((schema_id, table_id)),
                                selected_columns,
                                predicate,
                            }
                        }
                    }
                }
                Err(error) => {
                    return Err(vec![PlanError::syntax_error(&error)]);
                }
            }
        } else {
            return Err(vec![PlanError::feature_not_supported(&*self.query)]);
        };
        Ok(Plan::Select(result))
    }
}
