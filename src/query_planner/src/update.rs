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
use metadata::{DataDefinition, MetadataView};
use plan::{FullTableId, FullTableName, Plan, TableUpdates};
use sqlparser::ast::{Assignment, ObjectName};
use std::{collections::HashSet, convert::TryFrom, sync::Arc};

pub(crate) struct UpdatePlanner<'up> {
    table_name: &'up ObjectName,
    assignments: &'up [Assignment],
}

impl<'up> UpdatePlanner<'up> {
    pub(crate) fn new(table_name: &'up ObjectName, assignments: &'up [Assignment]) -> UpdatePlanner<'up> {
        UpdatePlanner {
            table_name,
            assignments,
        }
    }
}

impl Planner for UpdatePlanner<'_> {
    fn plan(self, metadata: Arc<DataDefinition>) -> Result<Plan> {
        match FullTableName::try_from(self.table_name) {
            Ok(full_table_name) => {
                let (schema_name, table_name) = full_table_name.as_tuple();
                match metadata.table_exists(&schema_name, &table_name) {
                    None => Err(vec![PlanError::schema_does_not_exist(&schema_name)]),
                    Some((_, None)) => Err(vec![PlanError::table_does_not_exist(&full_table_name)]),
                    Some((schema_id, Some(table_id))) => {
                        let full_table_id = FullTableId::from((schema_id, table_id));
                        let all_columns = metadata.table_columns(&full_table_id).expect("table exists");
                        let mut column_indices = vec![];
                        let mut input = vec![];
                        let mut columns = HashSet::new();
                        let mut errors = vec![];
                        for Assignment { id, value } in self.assignments.iter() {
                            let mut found = None;
                            let column_name = id.to_string().to_lowercase();
                            for (index, column_definition) in all_columns.iter().enumerate() {
                                if column_definition.has_name(&column_name) {
                                    match ScalarOp::transform(&value) {
                                        Ok(Ok(value)) => input.push(value),
                                        Ok(Err(error)) => {
                                            errors.push(PlanError::syntax_error(&error));
                                        }
                                        Err(error) => {
                                            errors.push(PlanError::feature_not_supported(&error));
                                        }
                                    }
                                    if columns.contains(&column_name) {
                                        errors.push(PlanError::syntax_error(&format!(
                                            "multiple assignments to same column \"{}\"",
                                            column_name
                                        )));
                                    }
                                    columns.insert(column_name.clone());
                                    found = Some((
                                        index,
                                        column_definition.name(),
                                        column_definition.sql_type(),
                                        TypeConstraint::from(&column_definition.sql_type()),
                                    ));
                                    break;
                                }
                            }

                            match found {
                                Some(index_col) => column_indices.push(index_col),
                                None => {
                                    errors.push(PlanError::column_does_not_exist(&column_name));
                                }
                            }
                        }

                        if !errors.is_empty() {
                            return Err(errors);
                        }

                        Ok(Plan::Update(TableUpdates {
                            table_id: full_table_id,
                            column_indices,
                            input,
                        }))
                    }
                }
            }
            Err(error) => Err(vec![PlanError::syntax_error(&error)]),
        }
    }
}
