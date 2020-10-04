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
use data_manager::{DataManager, Database, MetadataView};
use plan::{FullTableName, Plan, TableId};
use protocol::results::QueryEvent;
use protocol::{results::QueryError, Sender};
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

impl<D: Database> Planner<D> for DropTablesPlanner<'_> {
    fn plan(self, data_manager: Arc<DataManager<D>>, sender: Arc<dyn Sender>) -> Result<Plan> {
        let mut table_names = Vec::with_capacity(self.names.len());
        for name in self.names {
            match FullTableName::try_from(name) {
                Ok(full_table_name) => {
                    let (schema_name, table_name) = full_table_name.as_tuple();
                    match data_manager.table_exists(&schema_name, &table_name) {
                        None => {
                            sender
                                .send(Err(QueryError::schema_does_not_exist(schema_name)))
                                .expect("To Send Query Result to Client");
                            return Err(());
                        }
                        Some((_, None)) => {
                            if self.if_exists {
                                sender
                                    .send(Ok(QueryEvent::QueryComplete))
                                    .expect("To Send Query Result to Client");
                            } else {
                                sender
                                    .send(Err(QueryError::table_does_not_exist(full_table_name)))
                                    .expect("To Send Query Result to Client");
                            }
                            return Err(());
                        }
                        Some((schema_id, Some(table_id))) => table_names.push(TableId::from((schema_id, table_id))),
                    }
                }
                Err(error) => {
                    sender
                        .send(Err(QueryError::syntax_error(error)))
                        .expect("To Send Query Result to Client");
                    return Err(());
                }
            }
        }
        Ok(Plan::DropTables(table_names))
    }
}
