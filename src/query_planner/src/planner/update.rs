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
    plan::{Plan, TableUpdates},
    planner::{Planner, Result},
    TableId,
};
use data_manager::DataManager;
use itertools::Itertools;
use protocol::{results::QueryError, Sender};
use sqlparser::ast::{Assignment, ObjectName};
use std::sync::Arc;

pub(crate) struct UpdatePlanner {
    table_name: ObjectName,
    assignments: Vec<Assignment>,
}

impl UpdatePlanner {
    pub(crate) fn new(table_name: ObjectName, assignments: Vec<Assignment>) -> UpdatePlanner {
        UpdatePlanner {
            table_name,
            assignments,
        }
    }
}

impl Planner for UpdatePlanner {
    fn plan(self, data_manager: Arc<DataManager>, sender: Arc<dyn Sender>) -> Result<Plan> {
        let schema_name = self.table_name.0.first().unwrap().value.clone();
        let table_name = self.table_name.0.iter().skip(1).join(".");
        let (table_id, _, _) = match data_manager.table_exists(&schema_name, &table_name) {
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
            Some((schema_id, Some(table_id))) => Ok((TableId(schema_id, table_id), schema_name, table_name)),
        }?;
        Ok(Plan::Update(TableUpdates {
            full_table_name: table_id,
            assignments: self.assignments,
        }))
    }
}
