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
use data_definition::DataDefReader;
use plan::{FullTableId, FullTableName, Plan, SelectInput};
use sqlparser::ast::{
    BinaryOperator, Expr, Ident, Query, Select, SelectItem, SetExpr, TableFactor, TableWithJoins, Value,
};
use std::{convert::TryFrom, ops::Deref, sync::Arc};

pub(crate) struct SelectPlanner {
    query: Box<Query>,
}

impl SelectPlanner {
    pub(crate) fn new(query: Box<Query>) -> SelectPlanner {
        SelectPlanner { query }
    }
}

impl Planner for SelectPlanner {
    fn plan(self, metadata: Arc<dyn DataDefReader>) -> Result<Plan> {
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
                    return Err(PlanError::feature_not_supported(&*self.query));
                }
            };

            match FullTableName::try_from(name) {
                Ok(full_table_name) => {
                    let (schema_name, table_name) = full_table_name.as_tuple();
                    match metadata.table_exists(&schema_name, &table_name) {
                        None => {
                            return Err(PlanError::schema_does_not_exist(&schema_name));
                        }
                        Some((_, None)) => {
                            return Err(PlanError::table_does_not_exist(&full_table_name));
                        }
                        Some((schema_id, Some(table_id))) => {
                            let full_table_id = FullTableId::from((schema_id, table_id));
                            let selected_columns = {
                                let mut names: Vec<String> = vec![];
                                for item in projection {
                                    match item {
                                        SelectItem::Wildcard => {
                                            let all_columns =
                                                metadata.table_columns(&full_table_id).expect("table exists");
                                            names.extend(
                                                all_columns
                                                    .into_iter()
                                                    .map(|(_col_id, column_definition)| column_definition.name())
                                                    .collect::<Vec<String>>(),
                                            )
                                        }
                                        SelectItem::UnnamedExpr(Expr::Identifier(Ident { value, .. })) => {
                                            names.push(value.to_lowercase())
                                        }
                                        _ => {
                                            return Err(PlanError::feature_not_supported(&*self.query));
                                        }
                                    }
                                }
                                let (ids, not_found) =
                                    metadata.column_ids(&full_table_id, &names).expect("table exists");

                                if !not_found.is_empty() {
                                    return Err(PlanError::column_does_not_exist(&not_found[0]));
                                }
                                ids
                            };

                            let predicate = match selection {
                                Some(Expr::BinaryOp { left, op, right }) => {
                                    let l = match left.deref() {
                                        Expr::Identifier(ident) => {
                                            let (ids, _not_found) = metadata
                                                .column_ids(&full_table_id, &[ident.to_string()])
                                                .expect("table exists");
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
                                        Expr::Identifier(Ident { value, .. }) if value.starts_with('$') => {
                                            PredicateValue::Parameter(value[1..].to_string())
                                        }
                                        _ => panic!(),
                                    };
                                    Some((l, o, r))
                                }
                                _ => None,
                            };

                            SelectInput {
                                table_id: FullTableId::from((schema_id, table_id)),
                                selected_columns,
                                predicate,
                            }
                        }
                    }
                }
                Err(error) => {
                    return Err(PlanError::syntax_error(&error));
                }
            }
        } else {
            return Err(PlanError::feature_not_supported(&*self.query));
        };
        Ok(Plan::Select(result))
    }
}
