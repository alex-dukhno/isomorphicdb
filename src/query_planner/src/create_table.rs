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
use data_manager::{ColumnDefinition, DataManager};
use plan::{FullTableName, Plan, TableCreationInfo};
use protocol::{results::QueryError, Sender};
use sql_model::sql_types::SqlType;
use sqlparser::ast::{ColumnDef, ObjectName};
use std::{convert::TryFrom, sync::Arc};

pub(crate) struct CreateTablePlanner<'ctp> {
    full_table_name: &'ctp ObjectName,
    columns: &'ctp [ColumnDef],
}

impl<'ctp> CreateTablePlanner<'ctp> {
    pub(crate) fn new(full_table_name: &'ctp ObjectName, columns: &'ctp [ColumnDef]) -> CreateTablePlanner<'ctp> {
        CreateTablePlanner {
            full_table_name,
            columns,
        }
    }
}

impl Planner for CreateTablePlanner<'_> {
    fn plan(self, data_manager: Arc<DataManager>, sender: Arc<dyn Sender>) -> Result<Plan> {
        match FullTableName::try_from(self.full_table_name) {
            Ok(full_table_name) => {
                let (schema_name, table_name) = full_table_name.as_tuple();
                match data_manager.table_exists(&schema_name, &table_name) {
                    None => {
                        sender
                            .send(Err(QueryError::schema_does_not_exist(schema_name)))
                            .expect("To Send Query Result to Client");
                        Err(())
                    }
                    Some((_, Some(_))) => {
                        sender
                            .send(Err(QueryError::table_already_exists(full_table_name)))
                            .expect("To Send Query Result to Client");
                        Err(())
                    }
                    Some((schema_id, None)) => {
                        let mut column_defs = Vec::new();
                        for column in self.columns {
                            match SqlType::try_from(&column.data_type) {
                                Ok(sql_type) => {
                                    column_defs.push(ColumnDefinition::new(column.name.value.as_str(), sql_type))
                                }
                                Err(error) => {
                                    sender
                                        .send(Err(QueryError::feature_not_supported(error)))
                                        .expect("To Send Result to Client");
                                    return Err(());
                                }
                            }
                        }
                        Ok(Plan::CreateTable(TableCreationInfo::new(
                            schema_id,
                            table_name,
                            column_defs,
                        )))
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
