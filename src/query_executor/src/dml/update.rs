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

use ast::operations::ScalarOp;
use binary::Binary;
use constraints::{Constraint, ConstraintError};
use data_manager::DataManager;
use expr_eval::{dynamic_expr::DynamicExpressionEvaluation, static_expr::StaticExpressionEvaluation};
use metadata::MetadataView;
use plan::TableUpdates;
use protocol::{
    results::{QueryError, QueryEvent},
    Sender,
};
use std::{collections::HashMap, sync::Arc};

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

    pub(crate) fn execute(&self) {
        let table_definition = match self.data_manager.table_columns(&self.table_update.table_id) {
            Err(()) => {
                log::error!(
                    "Error while accessing table columns with id {:?}",
                    self.table_update.table_id
                );
                return;
            }
            Ok(table_definition) => table_definition,
        };
        let all_columns = table_definition
            .iter()
            .enumerate()
            .map(|(index, col_def)| (col_def.name(), index))
            .collect::<HashMap<_, _>>();

        let evaluation = StaticExpressionEvaluation::new(self.sender.clone());

        let mut assignments = vec![];
        for ((index, column_name, sql_type, type_constraint), item) in self
            .table_update
            .column_indices
            .iter()
            .zip(self.table_update.input.iter())
        {
            match evaluation.eval(item) {
                Ok(value) => {
                    assignments.push((
                        column_name.clone(),
                        *index,
                        Box::new(value),
                        *sql_type,
                        *type_constraint,
                    ));
                }
                Err(()) => return,
            }
        }

        let reads = match self.data_manager.full_scan(&self.table_update.table_id) {
            Err(()) => {
                log::error!("Error while scanning {:?}", self.table_update.table_id);
                return;
            }
            Ok(reads) => reads,
        };
        let expr_eval = DynamicExpressionEvaluation::new(self.sender.clone(), all_columns);
        let mut to_update = Vec::new();
        for (row_idx, (key, values)) in reads.map(Result::unwrap).map(Result::unwrap).enumerate() {
            let data = values.unpack();
            let mut updated = values.unpack();

            let mut has_err = false;
            for update in assignments.as_slice() {
                let (column_name, destination, value, sql_type, type_constraint) = update;
                let value = match expr_eval.eval(data.as_slice(), value.as_ref()) {
                    Ok(ScalarOp::Value(value)) => value,
                    Ok(_) => return,
                    Err(()) => return,
                };
                let value = match value.cast(&sql_type) {
                    Ok(value) => value,
                    Err(_err) => {
                        self.sender
                            .send(Err(QueryError::invalid_text_representation(sql_type.into(), value)))
                            .expect("To Send Result to User");
                        return;
                    }
                };
                match type_constraint.validate(value) {
                    Ok(datum) => updated[*destination] = datum,
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
                return;
            }

            to_update.push((key, Binary::pack(&updated)));
        }
        let size = match self.data_manager.write_into(&self.table_update.table_id, to_update) {
            Err(()) => {
                log::error!("Error while writing into {:?}", self.table_update.table_id);
                return;
            }
            Ok(size) => size,
        };
        self.sender
            .send(Ok(QueryEvent::RecordsUpdated(size)))
            .expect("To Send Query Result to Client");
    }
}
