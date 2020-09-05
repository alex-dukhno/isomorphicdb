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

use crate::{catalog_manager::CatalogManager, dml::ExpressionEvaluation};
use kernel::SystemResult;
use protocol::{
    results::{QueryError, QueryEvent},
    Sender,
};
use representation::{unpack_raw, Binary, Datum};
use sql_types::ConstraintError;
use sqlparser::ast::{Assignment, Expr, Ident, ObjectName, UnaryOperator, Value};
use std::{collections::BTreeSet, convert::TryFrom, sync::Arc};
use storage::Row;

pub(crate) struct UpdateCommand {
    name: ObjectName,
    assignments: Vec<Assignment>,
    storage: Arc<CatalogManager>,
    sender: Arc<dyn Sender>,
}

impl UpdateCommand {
    pub(crate) fn new(
        name: ObjectName,
        assignments: Vec<Assignment>,
        storage: Arc<CatalogManager>,
        sender: Arc<dyn Sender>,
    ) -> UpdateCommand {
        UpdateCommand {
            name,
            assignments,
            storage,
            sender,
        }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<()> {
        let schema_name = self.name.0[0].to_string();
        let table_name = self.name.0[1].to_string();
        let mut to_update = vec![];

        let mut evaluation = ExpressionEvaluation::new(self.sender.clone());

        for item in self.assignments.iter() {
            let Assignment { id, value } = &item;
            let Ident { value: column, .. } = id;
            let value = match value {
                Expr::Value(value) => value.clone(),
                Expr::UnaryOp { op, expr } => match (op, &**expr) {
                    (UnaryOperator::Minus, Expr::Value(Value::Number(v))) => Value::Number(-v),
                    (op, expr) => {
                        self.sender
                            .send(Err(QueryError::syntax_error(
                                op.to_string() + expr.to_string().as_str(),
                            )))
                            .expect("To Send Query Result to Client");
                        return Ok(());
                    }
                },
                expr @ Expr::BinaryOp { .. } => match evaluation.eval(expr) {
                    Ok(expr_result) => expr_result,
                    Err(()) => return Ok(()),
                },
                expr => {
                    self.sender
                        .send(Err(QueryError::syntax_error(expr.to_string())))
                        .expect("To Send Query Result to Client");
                    return Ok(());
                }
            };

            to_update.push((column.to_owned(), value))
        }

        match self.storage.table_exists(&schema_name, &table_name) {
            None => self
                .sender
                .send(Err(QueryError::schema_does_not_exist(schema_name)))
                .expect("To Send Result to Client"),
            Some((_, None)) => self
                .sender
                .send(Err(QueryError::table_does_not_exist(
                    schema_name + "." + table_name.as_str(),
                )))
                .expect("To Send Result to Client"),
            Some((schema_id, Some(table_id))) => {
                let all_columns = self.storage.table_columns(schema_id, table_id)?;
                let mut errors = Vec::new();
                let mut index_value_pairs = Vec::new();
                let mut non_existing_columns = BTreeSet::new();
                let mut column_exists = false;

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

                if !non_existing_columns.is_empty() {
                    self.sender
                        .send(Err(QueryError::column_does_not_exist(
                            non_existing_columns.into_iter().collect(),
                        )))
                        .expect("To Send Result to Client");
                    return Ok(());
                }
                if !errors.is_empty() {
                    for (error, column_definition) in errors {
                        let error_to_send = match error {
                            ConstraintError::OutOfRange => QueryError::out_of_range(
                                (&column_definition.sql_type()).into(),
                                column_definition.name(),
                                1,
                            ),
                            ConstraintError::TypeMismatch(value) => QueryError::type_mismatch(
                                &value,
                                (&column_definition.sql_type()).into(),
                                column_definition.name(),
                                1,
                            ),
                            ConstraintError::ValueTooLong(len) => QueryError::string_length_mismatch(
                                (&column_definition.sql_type()).into(),
                                len,
                                column_definition.name(),
                                1,
                            ),
                        };
                        self.sender
                            .send(Err(error_to_send))
                            .expect("To Send Query Result to Client");
                    }
                    return Ok(());
                }

                let to_update: Vec<Row> = match self.storage.full_scan(schema_id, table_id) {
                    Err(error) => return Err(error),
                    Ok(reads) => reads
                        .map(Result::unwrap)
                        .map(Result::unwrap)
                        .map(|(key, values)| {
                            let mut values = unpack_raw(values.to_bytes());
                            for (idx, data) in index_value_pairs.as_slice() {
                                values[*idx] = data.clone();
                            }
                            (key, Binary::pack(&values))
                        })
                        .collect(),
                };

                match self.storage.write_into(schema_id, table_id, to_update) {
                    Err(error) => return Err(error),
                    Ok(records_number) => {
                        self.sender
                            .send(Ok(QueryEvent::RecordsUpdated(records_number)))
                            .expect("To Send Query Result to Client");
                    }
                }
            }
        }
        Ok(())
    }
}
