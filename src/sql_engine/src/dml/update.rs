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

use ast::scalar::Assign;
use expr_eval::{dynamic_expr::DynamicExpressionEvaluation, static_expr::StaticExpressionEvaluation};
use protocol::results::{QueryError, QueryEvent};
use query_planner::plan::TableUpdates;
use sql_model::sql_types::ConstraintError;
use std::collections::HashMap;

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
        let all_columns = table_definition
            .iter()
            .enumerate()
            .map(|(index, col_def)| (col_def.name(), (index, col_def.sql_type())))
            .collect::<HashMap<_, _>>();

        let mut evaluation = StaticExpressionEvaluation::new(self.sender.clone());

        let mut to_update = vec![];
        let mut has_error = false;
        for ((index, column_name, scalar_type), item) in self
            .table_update
            .column_indices
            .iter()
            .zip(self.table_update.input.iter())
        {
            match evaluation.eval(item) {
                Ok(value) => {
                    to_update.push(Assign {
                        column_name: column_name.clone(),
                        destination: *index,
                        value: Box::new(value),
                        scalar_type: *scalar_type,
                    });
                }
                Err(()) => {
                    has_error = true;
                }
            }
        }

        if has_error {
            return Ok(());
        }

        let to_update: Vec<Row> = match self.data_manager.full_scan(&self.table_update.table_id) {
            Err(error) => return Err(error),
            Ok(reads) => {
                let expr_eval = DynamicExpressionEvaluation::new(self.sender.as_ref(), all_columns);
                let mut res = Vec::new();
                for (row_idx, (key, values)) in reads.map(Result::unwrap).map(Result::unwrap).enumerate() {
                    let mut datums = unpack_raw(values.to_bytes());

                    let mut has_err = false;
                    for update in to_update.as_slice() {
                        let Assign {
                            column_name,
                            destination,
                            value,
                            scalar_type: _ty,
                        } = update;
                        let value = match expr_eval.eval(datums.as_mut_slice(), value.as_ref()) {
                            Ok(value) => value,
                            Err(()) => return Ok(()),
                        };
                        let (_index, sql_type) = &expr_eval.columns()[column_name];
                        match sql_type.constraint().validate(value.to_string().as_str()) {
                            Ok(()) => datums[*destination] = value,
                            Err(ConstraintError::OutOfRange) => {
                                self.sender
                                    .send(Err(QueryError::out_of_range(sql_type.into(), column_name, row_idx + 1)))
                                    .expect("To Send Query Result to client");
                                has_err = true;
                            }
                            Err(ConstraintError::TypeMismatch(value)) => {
                                self.sender
                                    .send(Err(QueryError::type_mismatch(
                                        &value,
                                        sql_type.into(),
                                        column_name,
                                        row_idx + 1,
                                    )))
                                    .expect("To Send Query Result to client");
                                has_err = true;
                            }
                            Err(ConstraintError::ValueTooLong(len)) => {
                                self.sender
                                    .send(Err(QueryError::string_length_mismatch(
                                        sql_type.into(),
                                        len,
                                        column_name,
                                        row_idx + 1,
                                    )))
                                    .expect("To Send Query Result to client");
                                has_err = true;
                            }
                        }
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
