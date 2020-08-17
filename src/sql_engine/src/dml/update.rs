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

use crate::dml::ExpressionEvaluation;
use crate::frontend::FrontendStorage;
use crate::ColumnDefinition;
use kernel::SystemResult;
use protocol::{
    results::{QueryErrorBuilder, QueryEvent},
    Sender,
};
use representation::{unpack_raw, Binary, Datum};
use sql_types::ConstraintError;
use sqlparser::ast::{Assignment, Expr, Ident, ObjectName, UnaryOperator, Value};
use std::{
    collections::BTreeSet,
    convert::TryFrom,
    sync::{Arc, Mutex},
};
use storage::{BackendStorage, Row};

pub(crate) struct UpdateCommand<P: BackendStorage> {
    name: ObjectName,
    assignments: Vec<Assignment>,
    storage: Arc<Mutex<FrontendStorage<P>>>,
    session: Arc<dyn Sender>,
}

impl<P: BackendStorage> UpdateCommand<P> {
    pub(crate) fn new(
        name: ObjectName,
        assignments: Vec<Assignment>,
        storage: Arc<Mutex<FrontendStorage<P>>>,
        session: Arc<dyn Sender>,
    ) -> UpdateCommand<P> {
        UpdateCommand {
            name,
            assignments,
            storage,
            session,
        }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<()> {
        let schema_name = self.name.0[0].to_string();
        let table_name = self.name.0[1].to_string();
        let mut to_update = vec![];

        let evaluation = ExpressionEvaluation::new(self.session.clone());

        for item in self.assignments.iter() {
            let Assignment { id, value } = &item;
            let Ident { value: column, .. } = id;
            let value = match value {
                Expr::Value(value) => value.clone(),
                Expr::UnaryOp { op, expr } => match (op, &**expr) {
                    (UnaryOperator::Minus, Expr::Value(Value::Number(v))) => Value::Number(-v),
                    (op, expr) => {
                        self.session
                            .send(Err(QueryErrorBuilder::new()
                                .syntax_error(op.to_string() + expr.to_string().as_str())
                                .build()))
                            .expect("To Send Query Result to Client");
                        return Ok(());
                    }
                },
                expr @ Expr::BinaryOp { .. } => match evaluation.eval(expr) {
                    Ok(expr_result) => expr_result,
                    Err(()) => return Ok(()),
                },
                expr => {
                    self.session
                        .send(Err(QueryErrorBuilder::new().syntax_error(expr.to_string()).build()))
                        .expect("To Send Query Result to Client");
                    return Ok(());
                }
            };

            to_update.push((column.to_owned(), value))
        }

        if !(self.storage.lock().unwrap()).schema_exists(&schema_name) {
            self.session
                .send(Err(QueryErrorBuilder::new().schema_does_not_exist(schema_name).build()))
                .expect("To Send Result to Client");
            return Ok(());
        }

        let all_columns = (self.storage.lock().unwrap()).table_columns(&schema_name, &table_name)?;
        let mut errors = Vec::new();
        let mut index_value_pairs = Vec::new();
        let mut non_existing_columns = BTreeSet::new();
        let mut column_exists = false;

        // only process the rows if the table and schema exist.
        if (self.storage.lock().unwrap()).table_exists(&schema_name, &table_name) {
            for (column_name, value) in to_update {
                for (index, column_definition) in all_columns.iter().enumerate() {
                    if column_definition.has_name(&column_name) {
                        let v = match value.clone() {
                            Value::Number(v) => v.to_string(),
                            Value::SingleQuotedString(v) => v.to_string(),
                            Value::Boolean(v) => v.to_string(),
                            _ => unimplemented!("other types not implemented"),
                        };
                        match column_definition.sql_type().constraint().validate(v.as_str()) {
                            Ok(()) => {
                                index_value_pairs.push((index, Datum::try_from(&value).unwrap()));
                            }
                            Err(e) => {
                                errors.push((e, column_definition.clone()));
                            }
                        }

                        column_exists = true;

                        break;
                    }
                }

                if !column_exists {
                    non_existing_columns.insert(column_name.clone());
                }
            }
        } else {
            self.session
                .send(Err(QueryErrorBuilder::new()
                    .table_does_not_exist(schema_name + "." + table_name.as_str())
                    .build()))
                .expect("To Send Result to Client");
            return Ok(());
        }

        if !non_existing_columns.is_empty() {
            self.session
                .send(Err(QueryErrorBuilder::new()
                    .column_does_not_exist(non_existing_columns.into_iter().collect())
                    .build()))
                .expect("To Send Result to Client");
            return Ok(());
        }
        if !errors.is_empty() {
            let row_index = 1;
            let mut builder = QueryErrorBuilder::new();
            let constraint_error_mapper = |(err, column_definition): &(ConstraintError, ColumnDefinition)| match err {
                ConstraintError::OutOfRange => {
                    builder.out_of_range(
                        (&column_definition.sql_type()).into(),
                        column_definition.name(),
                        row_index,
                    );
                }
                ConstraintError::TypeMismatch(value) => {
                    builder.type_mismatch(
                        value,
                        (&column_definition.sql_type()).into(),
                        column_definition.name(),
                        row_index,
                    );
                }
                ConstraintError::ValueTooLong(len) => {
                    builder.string_length_mismatch(
                        (&column_definition.sql_type()).into(),
                        *len,
                        column_definition.name(),
                        row_index,
                    );
                }
            };

            errors.iter().for_each(constraint_error_mapper);
            self.session
                .send(Err(builder.build()))
                .expect("To Send Query Result to Client");
            return Ok(());
        }

        let to_update: Vec<Row> = match (self.storage.lock().unwrap()).table_scan(&schema_name, &table_name) {
            Err(error) => return Err(error),
            Ok(reads) => reads
                .map(Result::unwrap)
                .map(|(key, values)| {
                    let mut datums = unpack_raw(values.to_bytes());
                    for (idx, data) in index_value_pairs.as_slice() {
                        datums[*idx] = data.clone();
                    }
                    (key, Binary::pack(&datums))
                })
                .collect(),
        };

        match (self.storage.lock().unwrap()).update_all(&schema_name, &table_name, to_update) {
            Err(error) => Err(error),
            Ok(records_number) => {
                self.session
                    .send(Ok(QueryEvent::RecordsUpdated(records_number)))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
        }
    }
}
