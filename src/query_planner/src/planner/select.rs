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
    plan::{Plan, SelectInput},
    planner::{Planner, Result},
    TableId,
};
use data_manager::DataManager;
use kernel::{SystemError, SystemResult};
use protocol::{results::QueryError, Sender};
use sqlparser::ast::{Expr, Ident, Query, Select, SelectItem, SetExpr, TableFactor, TableWithJoins};
use std::{ops::Deref, sync::Arc};

pub(crate) struct SelectPlanner {
    query: Box<Query>,
}

impl SelectPlanner {
    pub(crate) fn new(query: Box<Query>) -> SelectPlanner {
        SelectPlanner { query }
    }

    fn parse_select_input(
        &self,
        query: Box<Query>,
        data_manager: Arc<DataManager>,
        sender: Arc<dyn Sender>,
    ) -> SystemResult<SelectInput> {
        let Query { body, .. } = &*query;
        if let SetExpr::Select(select) = body {
            let Select { projection, from, .. } = select.deref();
            let TableWithJoins { relation, .. } = &from[0];
            let (schema_name, table_name) = match relation {
                TableFactor::Table { name, .. } => {
                    let table_name = name.0[1].to_string();
                    let schema_name = name.0[0].to_string();
                    (schema_name, table_name)
                }
                _ => {
                    sender
                        .send(Err(QueryError::feature_not_supported(query)))
                        .expect("To Send Query Result to Client");
                    return Err(SystemError::runtime_check_failure("Feature Not Supported".to_owned()));
                }
            };

            match data_manager.table_exists(&schema_name, &table_name) {
                None => {
                    sender
                        .send(Err(QueryError::schema_does_not_exist(schema_name)))
                        .expect("To Send Result to Client");
                    Err(SystemError::runtime_check_failure("Schema Does Not Exist".to_owned()))
                }
                Some((_, None)) => {
                    sender
                        .send(Err(QueryError::table_does_not_exist(
                            schema_name + "." + table_name.as_str(),
                        )))
                        .expect("To Send Result to Client");
                    Err(SystemError::runtime_check_failure("Table Does Not Exist".to_owned()))
                }
                Some((schema_id, Some(table_id))) => {
                    let selected_columns = {
                        let projection = projection.clone();
                        let mut columns: Vec<String> = vec![];
                        for item in projection {
                            match item {
                                SelectItem::Wildcard => {
                                    let all_columns = data_manager.table_columns(schema_id, table_id)?;
                                    columns.extend(
                                        all_columns
                                            .into_iter()
                                            .map(|column_definition| column_definition.name())
                                            .collect::<Vec<String>>(),
                                    )
                                }
                                SelectItem::UnnamedExpr(Expr::Identifier(Ident { value, .. })) => {
                                    columns.push(value.clone())
                                }
                                _ => {
                                    sender
                                        .send(Err(QueryError::feature_not_supported(query)))
                                        .expect("To Send Query Result to Client");
                                    return Err(SystemError::runtime_check_failure("Feature Not Supported".to_owned()));
                                }
                            }
                        }
                        columns
                    };

                    Ok(SelectInput {
                        table_id: TableId(schema_id, table_id),
                        selected_columns,
                    })
                }
            }
        } else {
            sender
                .send(Err(QueryError::feature_not_supported(query)))
                .expect("To Send Query Result to Client");
            Err(SystemError::runtime_check_failure("Feature Not Supported".to_owned()))
        }
    }
}

impl Planner for SelectPlanner {
    fn plan(self, data_manager: Arc<DataManager>, sender: Arc<dyn Sender>) -> Result<Plan> {
        let result = self.parse_select_input(self.query.clone(), data_manager, sender);
        Ok(Plan::Select(result.map_err(|_| ())?))
    }
}
