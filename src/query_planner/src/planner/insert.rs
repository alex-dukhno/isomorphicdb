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

use crate::{
    plan::{Plan, TableInserts},
    planner::{Planner, Result},
    FullTableName, TableId,
};
use data_manager::DataManager;
use protocol::{results::QueryError, Sender};
use sqlparser::ast::{Ident, ObjectName, Query, SetExpr};
use std::{convert::TryFrom, sync::Arc};

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
    fn plan(self, data_manager: Arc<DataManager>, sender: Arc<dyn Sender>) -> Result<Plan> {
        match FullTableName::try_from(self.table_name) {
            Ok(full_table_name) => {
                let (schema_name, table_name) = full_table_name.as_tuple();
                match data_manager.table_exists(&schema_name, &table_name) {
                    None => {
                        sender
                            .send(Err(QueryError::schema_does_not_exist(schema_name.to_owned())))
                            .expect("To Send Query Result to Client");
                        Err(())
                    }
                    Some((_, None)) => {
                        sender
                            .send(Err(QueryError::table_does_not_exist(format!(
                                "{}.{}",
                                schema_name, table_name
                            ))))
                            .expect("To Send Query Result to Client");
                        Err(())
                    }
                    Some((schema_id, Some(table_id))) => {
                        let Query { body, .. } = &self.source;
                        match body {
                            SetExpr::Values(values) => {
                                let table_id = TableId((schema_id, table_id));
                                let values = values.0.to_vec();
                                let all_columns = data_manager.table_columns(&table_id).expect("Ok");
                                let index_cols = if self.columns.is_empty() {
                                    let mut index_cols = vec![];
                                    for (index, column_definition) in all_columns.iter().cloned().enumerate() {
                                        index_cols.push((index, column_definition));
                                    }

                                    index_cols
                                } else {
                                    let column_names = self.columns.iter().map(|id| {
                                        let Ident { value, .. } = id;
                                        value
                                    });
                                    let mut index_cols = vec![];
                                    let mut has_error = false;
                                    for column_name in column_names {
                                        let mut found = None;
                                        for (index, column_definition) in all_columns.iter().enumerate() {
                                            if column_definition.has_name(&column_name) {
                                                found = Some((index, column_definition.clone()));
                                                break;
                                            }
                                        }

                                        match found {
                                            Some(index_col) => index_cols.push(index_col),
                                            None => {
                                                sender
                                                    .send(Err(QueryError::column_does_not_exist(column_name)))
                                                    .expect("To Send Result to Client");
                                                has_error = true;
                                            }
                                        }
                                    }

                                    if has_error {
                                        return Err(());
                                    }

                                    index_cols
                                };
                                Ok(Plan::Insert(TableInserts {
                                    table_id,
                                    column_indices: index_cols,
                                    input: values,
                                }))
                            }
                            set_expr => {
                                sender
                                    .send(Err(QueryError::syntax_error(format!("{} is not supported", set_expr))))
                                    .expect("To Send Query Result to Client");
                                Err(())
                            }
                        }
                    }
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
