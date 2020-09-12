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
use data_manager::{ColumnDefinition, DataManager, Row};
use kernel::{SystemError, SystemResult};
use protocol::{
    results::{QueryError, QueryEvent},
    Sender,
};
use sql_model::sql_types::ConstraintError;

use ast::{operations::ScalarOp, Datum, EvalError};
use expr_eval::static_expr::StaticExpressionEvaluation;
use query_planner::plan::TableInserts;
use std::convert::TryFrom;

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

    pub(crate) fn execute(&self) -> SystemResult<()> {
        let evaluation = StaticExpressionEvaluation::new(self.sender.clone());
        let mut rows = vec![];
        for line in &self.table_inserts.input {
            let mut row = vec![];
            for expression in line {
                let value = match evaluation.eval(expression) {
                    Ok(ScalarOp::Value(value)) => value,
                    Ok(ScalarOp::Column(column_identifier)) => {
                        return Err(SystemError::runtime_check_failure(&format!(
                            "column name '{}' can't be used in insert statement",
                            column_identifier
                        )))
                    }
                    Ok(operation) => {
                        return Err(SystemError::runtime_check_failure(&format!(
                            "Operation '{:?}' can't be performed in insert statement",
                            operation
                        )))
                    }
                    Err(()) => return Ok(()),
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
                return Ok(());
            }

            let key = self
                .data_manager
                .next_key_id(&self.table_inserts.table_id)
                .to_be_bytes()
                .to_vec();

            // TODO: The default value or NULL should be initialized for SQL types of all columns.
            let mut record = vec![Datum::from_null(); self.table_inserts.column_indices.len()];
            let mut errors = vec![];
            for (item, (index, name, sql_type)) in row.iter().zip(self.table_inserts.column_indices.iter()) {
                match Datum::try_from(item) {
                    Ok(datum) => match sql_type.constraint().validate(datum.to_string().as_str()) {
                        Ok(()) => {
                            record[*index] = datum;
                        }
                        Err(error) => {
                            errors.push((error, ColumnDefinition::new(name, *sql_type)));
                        }
                    },
                    Err(EvalError::OutOfRangeNumeric(_)) => {
                        errors.push((ConstraintError::OutOfRange, ColumnDefinition::new(name, *sql_type)))
                    }
                    Err(_) => panic!(),
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
                return Ok(());
            }
            to_write.push((Binary::with_data(key), Binary::pack(&record)));
        }

        match self.data_manager.write_into(&self.table_inserts.table_id, to_write) {
            Err(error) => return Err(error),
            Ok(size) => self
                .sender
                .send(Ok(QueryEvent::RecordsInserted(size)))
                .expect("To Send Result to Client"),
        }

        Ok(())
    }
}
