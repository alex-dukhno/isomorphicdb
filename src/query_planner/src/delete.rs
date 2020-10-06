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

use crate::{Planner, Result};
use metadata::DataDefinition;
use plan::{FullTableName, Plan, TableDeletes, TableId};
use protocol::{results::QueryError, Sender};
use sql_model::DEFAULT_CATALOG;
use sqlparser::ast::ObjectName;
use std::{convert::TryFrom, sync::Arc};

pub(crate) struct DeletePlanner<'dp> {
    table_name: &'dp ObjectName,
}

impl DeletePlanner<'_> {
    pub(crate) fn new(table_name: &ObjectName) -> DeletePlanner {
        DeletePlanner { table_name }
    }
}

impl Planner for DeletePlanner<'_> {
    fn plan(self, metadata: Arc<DataDefinition>, sender: Arc<dyn Sender>) -> Result<Plan> {
        match FullTableName::try_from(self.table_name) {
            Ok(full_table_name) => {
                let (schema_name, table_name) = full_table_name.as_tuple();
                match metadata.table_exists(DEFAULT_CATALOG, &schema_name, &table_name) {
                    None => Err(()), // TODO catalog does not exists
                    Some((_, None)) => {
                        sender
                            .send(Err(QueryError::schema_does_not_exist(schema_name.to_owned())))
                            .expect("To Send Query Result to Client");
                        Err(())
                    }
                    Some((_, Some((_, None)))) => {
                        sender
                            .send(Err(QueryError::table_does_not_exist(format!(
                                "{}.{}",
                                schema_name, table_name
                            ))))
                            .expect("To Send Query Result to Client");
                        Err(())
                    }
                    Some((_catalog_id, Some((schema_id, Some(table_id))))) => Ok(Plan::Delete(TableDeletes {
                        table_id: TableId::from((schema_id, table_id)),
                    })),
                }
            }
            Err(error) => {
                sender
                    .send(Err(QueryError::syntax_error(error)))
                    .expect("To Send Query Result to Client");
                Err(())
            }
        }
    }
}
