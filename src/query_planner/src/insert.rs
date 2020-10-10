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
use ast::operations::ScalarOp;
use constraints::TypeConstraint;
use metadata::DataDefinition;
use plan::{FullTableName, Plan, TableId, TableInserts};
use sql_model::DEFAULT_CATALOG;
use sqlparser::ast::{Ident, ObjectName, Query, SetExpr};
use std::{collections::HashSet, convert::TryFrom, sync::Arc};

pub(crate) struct InsertPlanner<'ip> {
    table_name: &'ip ObjectName,
    columns: &'ip [Ident],
    source: &'ip Query,
}

impl<'ip> InsertPlanner<'ip> {
    pub(crate) fn new(table_name: &'ip ObjectName, columns: &'ip [Ident], source: &'ip Query) -> InsertPlanner<'ip> {
        InsertPlanner {
            table_name,
            columns,
            source,
        }
    }
}

impl Planner for InsertPlanner<'_> {
    fn plan(self, metadata: Arc<DataDefinition>) -> Result<Plan> {
        match FullTableName::try_from(self.table_name) {
            Ok(full_table_name) => {
                let (schema_name, table_name) = full_table_name.as_tuple();
                match metadata.table_exists(DEFAULT_CATALOG, &schema_name, &table_name) {
                    None => Err(vec![]), // TODO catalog does not exists
                    Some((_, None)) => Err(vec![PlanError::schema_does_not_exist(&schema_name)]),
                    Some((_, Some((_, None)))) => Err(vec![PlanError::table_does_not_exist(&full_table_name)]),
                    Some((_, Some((schema_id, Some(table_id))))) => {
                        let Query { body, .. } = &self.source;
                        match body {
                            SetExpr::Values(values) => {
                                let table_id = TableId::from((schema_id, table_id));
                                let mut input = vec![];
                                for row in values.0.iter() {
                                    let mut scalar_values = vec![];
                                    for value in row {
                                        match ScalarOp::transform(&value) {
                                            Ok(Ok(value)) => scalar_values.push(value),
                                            Ok(Err(error)) => {
                                                return Err(vec![PlanError::syntax_error(&error)]);
                                            }
                                            Err(error) => {
                                                return Err(vec![PlanError::feature_not_supported(&error)]);
                                            }
                                        }
                                    }
                                    input.push(scalar_values);
                                }
                                let all_columns = metadata.table_columns(DEFAULT_CATALOG, schema_name, table_name);
                                let column_indices = if self.columns.is_empty() {
                                    all_columns
                                        .iter()
                                        .cloned()
                                        .enumerate()
                                        .map(|(index, col_def)| {
                                            (
                                                index,
                                                col_def.name(),
                                                col_def.sql_type(),
                                                TypeConstraint::from(&col_def.sql_type()),
                                            )
                                        })
                                        .collect::<Vec<_>>()
                                } else {
                                    let mut columns = HashSet::new();
                                    let mut index_cols = vec![];
                                    let mut errors = vec![];
                                    for col_name in self.columns.iter().map(|id| id.value.as_str()) {
                                        let column_name = col_name.to_lowercase();
                                        let mut found = None;
                                        for (index, column_definition) in all_columns.iter().enumerate() {
                                            if column_definition.has_name(&column_name) {
                                                if columns.contains(&column_name) {
                                                    errors.push(PlanError::duplicate_column(&column_name));
                                                }
                                                columns.insert(column_name.clone());
                                                found = Some((
                                                    index,
                                                    column_name.clone(),
                                                    column_definition.sql_type(),
                                                    TypeConstraint::from(&column_definition.sql_type()),
                                                ));
                                                break;
                                            }
                                        }

                                        match found {
                                            Some(index_col) => index_cols.push(index_col),
                                            None => {
                                                errors.push(PlanError::column_does_not_exist(&column_name));
                                            }
                                        }
                                    }

                                    if !errors.is_empty() {
                                        return Err(errors);
                                    }

                                    index_cols
                                };
                                Ok(Plan::Insert(TableInserts {
                                    table_id,
                                    column_indices,
                                    input,
                                }))
                            }
                            set_expr => Err(vec![PlanError::syntax_error(&set_expr)]),
                        }
                    }
                }
            }
            Err(error) => Err(vec![PlanError::syntax_error(&error)]),
        }
    }
}
