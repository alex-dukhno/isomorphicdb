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
use binary::{Binary, Row};
use constraints::{Constraint, ConstraintError};
use data_manager::DataManager;
use expr_eval::static_expr::StaticExpressionEvaluation;
use expr_eval::EvalError;
use kernel::SystemError;
use meta_def::ColumnDefinition;
use plan::TableInserts;
use protocol::{
    results::{QueryError, QueryEvent},
    Sender,
};
use repr::Datum;
use std::sync::Arc;

pub(crate) struct InsertCommand {
    table_inserts: TableInserts,
    data_manager: Arc<DataManager>,
    sender: Arc<dyn Sender>,
}

impl InsertCommand {
    pub(crate) fn new(
        table_inserts: TableInserts,
        data_manager: Arc<DataManager>,
        sender: Arc<dyn Sender>,
    ) -> InsertCommand {
        InsertCommand {
            table_inserts,
            data_manager,
            sender,
        }
    }

    pub(crate) fn execute(&self) {
        let evaluation = StaticExpressionEvaluation::new();
        let mut rows = vec![];
        for line in &self.table_inserts.input {
            let mut row = vec![];
            for expression in line {
                let value = match evaluation.eval(expression) {
                    Ok(ScalarOp::Value(value)) => value,
                    Ok(ScalarOp::Column(column_identifier)) => {
                        log::error!(
                            "{:?}",
                            SystemError::runtime_check_failure(&format!(
                                "column name '{}' can't be used in insert statement",
                                column_identifier
                            ))
                        );
                        return;
                    }
                    Ok(operation) => {
                        log::error!(
                            "{:?}",
                            SystemError::runtime_check_failure(&format!(
                                "Operation '{:?}' can't be performed in insert statement",
                                operation
                            ))
                        );
                        return;
                    }
                    Err(EvalError::UndefinedFunction(op, left_type, right_type)) => {
                        self.sender
                            .send(Err(QueryError::undefined_function(op, left_type, right_type)))
                            .expect("To Send Query Result to Client");
                        return;
                    }
                    Err(EvalError::NonValue(not_a_value)) => {
                        log::error!("not a value {} was accessed during expression evaluation", not_a_value);
                        return;
                    }
                };
                row.push(value);
            }
            rows.push(row);
        }

        let mut to_write: Vec<Row> = vec![];
        for (row_index, row) in rows.iter().enumerate() {
            if row.len() > self.table_inserts.column_indices.len() {
                self.sender
                    .send(Err(QueryError::too_many_insert_expressions()))
                    .expect("To Send Result to Client");
                return;
            }

            let key = self
                .data_manager
                .next_key_id(&self.table_inserts.table_id)
                .to_be_bytes()
                .to_vec();

            // TODO: The default value or NULL should be initialized for SQL types of all columns.
            let mut record = vec![Datum::from_null(); self.table_inserts.column_indices.len()];
            let mut errors = vec![];
            for (item, (index, name, sql_type, type_constraint)) in
                row.iter().zip(self.table_inserts.column_indices.iter())
            {
                match item.cast(sql_type) {
                    Ok(item) => match type_constraint.validate(item) {
                        Ok(datum) => {
                            record[*index] = datum;
                        }
                        Err(error) => {
                            errors.push((error, ColumnDefinition::new(name, *sql_type)));
                        }
                    },
                    Err(_err) => {
                        self.sender
                            .send(Err(QueryError::invalid_text_representation(sql_type.into(), item)))
                            .expect("To Send Result to User");
                        return;
                    }
                }
            }
            if !errors.is_empty() {
                for (error, column_definition) in errors {
                    let error_to_send = match error {
                        ConstraintError::OutOfRange => QueryError::out_of_range(
                            (&column_definition.sql_type()).into(),
                            column_definition.name(),
                            row_index + 1,
                        ),
                        ConstraintError::TypeMismatch(value) => QueryError::type_mismatch(
                            &value,
                            (&column_definition.sql_type()).into(),
                            &column_definition.name(),
                            row_index + 1,
                        ),
                        ConstraintError::ValueTooLong(len) => QueryError::string_length_mismatch(
                            (&column_definition.sql_type()).into(),
                            len,
                            &column_definition.name(),
                            row_index + 1,
                        ),
                    };
                    self.sender
                        .send(Err(error_to_send))
                        .expect("To Send Query Result to Client");
                }
                return;
            }
            to_write.push((Binary::with_data(key), Binary::pack(&record)));
        }

        let size = match self.data_manager.write_into(&self.table_inserts.table_id, to_write) {
            Ok(size) => size,
            Err(()) => {
                log::error!("Error while writing into {:?}", self.table_inserts.table_id);
                return;
            }
        };
        self.sender
            .send(Ok(QueryEvent::RecordsInserted(size)))
            .expect("To Send Result to Client");
    }
}
