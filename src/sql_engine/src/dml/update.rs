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

use std::sync::Arc;

use data_manager::{DataManager, Row};
use kernel::SystemResult;
use protocol::Sender;
use representation::{unpack_raw, Binary};

use crate::query::expr::{EvalScalarOp, ExpressionEvaluation};
use protocol::results::QueryEvent;
use query_planner::plan::TableUpdates;

pub(crate) struct UpdateCommand {
    table_update: TableUpdates,
    data_manager: Arc<DataManager>,
    sender: Arc<dyn Sender>,
}

impl UpdateCommand {
    pub(crate) fn new(
        table_update: TableUpdates,
        data_manager: Arc<DataManager>,
        sender: Arc<dyn Sender>,
    ) -> UpdateCommand {
        UpdateCommand {
            table_update,
            data_manager,
            sender,
        }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<()> {
        let table_definition = self.data_manager.table_columns(&self.table_update.table_id)?;
        let all_columns = table_definition.clone();

        let evaluation = ExpressionEvaluation::new(self.sender.clone(), table_definition);

        let mut to_update = vec![];
        let mut has_error = false;
        for item in self.table_update.assignments.iter() {
            match evaluation.eval_assignment(item) {
                Ok(assign) => to_update.push(assign),
                Err(()) => has_error = true,
            }
        }

        if has_error {
            return Ok(());
        }

        let to_update: Vec<Row> = match self.data_manager.full_scan(&self.table_update.table_id) {
            Err(error) => return Err(error),
            Ok(reads) => {
                let expr_eval = EvalScalarOp::new(self.sender.as_ref(), all_columns.to_vec());
                let mut res = Vec::new();
                for (row_idx, (key, values)) in reads.map(Result::unwrap).map(Result::unwrap).enumerate() {
                    let mut datums = unpack_raw(values.to_bytes());

                    let mut has_err = false;
                    for update in to_update.as_slice() {
                        has_err = expr_eval
                            .eval_on_row(&mut datums.as_mut_slice(), update, row_idx)
                            .is_err()
                            || has_err;
                    }

                    if has_err {
                        return Ok(());
                    }

                    res.push((key, Binary::pack(&datums)));
                }
                res
            }
        };

        match self.data_manager.write_into(&self.table_update.table_id, to_update) {
            Err(error) => return Err(error),
            Ok(records_number) => {
                self.sender
                    .send(Ok(QueryEvent::RecordsUpdated(records_number)))
                    .expect("To Send Query Result to Client");
            }
        }
        Ok(())
    }
}
