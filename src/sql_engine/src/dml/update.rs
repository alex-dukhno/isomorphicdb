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

use binary::Binary;
use data_manager::DataManager;
use kernel::SystemResult;
use protocol::Sender;

use ast::{operations::ScalarOp, Datum};
use expr_eval::{dynamic_expr::DynamicExpressionEvaluation, static_expr::StaticExpressionEvaluation};
use protocol::results::{QueryError, QueryEvent};
use query_planner::plan::TableUpdates;
use sql_model::sql_types::ConstraintError;
use std::{collections::HashMap, convert::TryFrom};

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

    pub(crate) fn execute(&self) -> SystemResult<()> {
        let table_definition = self.data_manager.table_columns(&self.table_update.table_id)?;
        let all_columns = table_definition
            .iter()
            .enumerate()
            .map(|(index, col_def)| (col_def.name(), index))
            .collect::<HashMap<_, _>>();

        let evaluation = StaticExpressionEvaluation::new(self.sender.clone());

        let mut assignments = vec![];
        let mut has_error = false;
        for ((index, column_name, scalar_type), item) in self
            .table_update
            .column_indices
            .iter()
            .zip(self.table_update.input.iter())
        {
            match evaluation.eval(item) {
                Ok(value) => {
                    assignments.push((column_name.clone(), *index, Box::new(value), *scalar_type));
                }
                Err(()) => {
                    has_error = true;
                }
            }
        }

        if has_error {
            return Ok(());
        }

        match self.data_manager.full_scan(&self.table_update.table_id) {
            Err(error) => {
                self.sender
                    .send(Err(QueryError::syntax_error(
                        "something went wrong when read data from table",
                    )))
                    .expect("To Send Result to Client");
                return Err(error);
            }
            Ok(reads) => {
                let expr_eval = DynamicExpressionEvaluation::new(self.sender.clone(), all_columns);
                let mut to_update = Vec::new();
                for (row_idx, (key, values)) in reads.map(Result::unwrap).map(Result::unwrap).enumerate() {
                    let data = values.unpack();
                    let mut updated = values.unpack();

                    let mut has_err = false;
                    for update in assignments.as_slice() {
                        let (column_name, destination, value, sql_type) = update;
                        let value = match expr_eval.eval(data.as_slice(), value.as_ref()) {
                            Ok(ScalarOp::Value(value)) => value,
                            Ok(_) => return Ok(()),
                            Err(()) => return Ok(()),
                        };
                        match sql_type.constraint().validate(value.to_string().as_str()) {
                            Ok(()) => updated[*destination] = Datum::try_from(&value).expect("ok"),
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

                    to_update.push((key, Binary::pack(&updated)));
                }
                match self.data_manager.write_into(&self.table_update.table_id, to_update) {
                    Err(error) => {
                        self.sender
                            .send(Err(QueryError::syntax_error(
                                "something went wrong when write data to table",
                            )))
                            .expect("To Send Result to Client");
                        return Err(error);
                    }
                    Ok(records_number) => {
                        self.sender
                            .send(Ok(QueryEvent::RecordsUpdated(records_number)))
                            .expect("To Send Query Result to Client");
                    }
                }
            }
        };
        Ok(())
    }
}
