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
use metadata::DataDefinition;
use plan::{FullTableName, Plan, TableId};
use sql_model::DEFAULT_CATALOG;
use sqlparser::ast::ObjectName;
use std::{convert::TryFrom, sync::Arc};

pub(crate) struct DropTablesPlanner<'dtp> {
    names: &'dtp [ObjectName],
    if_exists: bool,
}

impl DropTablesPlanner<'_> {
    pub(crate) fn new(names: &[ObjectName], if_exists: bool) -> DropTablesPlanner {
        DropTablesPlanner { names, if_exists }
    }
}

impl Planner for DropTablesPlanner<'_> {
    fn plan(self, metadata: Arc<DataDefinition>) -> Result<Plan> {
        let mut table_names = Vec::with_capacity(self.names.len());
        for name in self.names {
            match FullTableName::try_from(name) {
                Ok(full_table_name) => {
                    let (schema_name, table_name) = full_table_name.as_tuple();
                    match metadata.table_exists(DEFAULT_CATALOG, &schema_name, &table_name) {
                        None => return Err(vec![]), // TODO catalog does not exists
                        Some((_, None)) => {
                            return Err(vec![PlanError::schema_does_not_exist(&schema_name)]);
                        }
                        Some((_, Some((_, None)))) => {
                            return if self.if_exists {
                                Ok(Plan::NothingToExecute)
                            } else {
                                Err(vec![PlanError::table_does_not_exist(&full_table_name)])
                            }
                        }
                        Some((_, Some((schema_id, Some(table_id))))) => {
                            table_names.push(TableId::from((schema_id, table_id)))
                        }
                    }
                }
                Err(error) => {
                    return Err(vec![PlanError::syntax_error(&error)]);
                }
            }
        }
        Ok(Plan::DropTables(table_names))
    }
}
