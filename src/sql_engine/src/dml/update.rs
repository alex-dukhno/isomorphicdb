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
use crate::query::expr::resolve_static_expr;
use kernel::SystemResult;
use protocol::{
    results::{QueryErrorBuilder, QueryEvent},
    Sender,
};
use sql_types::ConstraintError;
use sqlparser::ast::{Assignment, Expr, Ident, ObjectName, UnaryOperator, Value};
use std::sync::{Arc, Mutex};
use storage::{backend::BackendStorage, frontend::FrontendStorage, ColumnDefinition, OperationOnTableError};
use crate::query::{Row, unpack_raw};
use std::collections::BTreeSet;

pub(crate) struct UpdateCommand<'uc, P: BackendStorage> {
    raw_sql_query: &'uc str,
    name: ObjectName,
    assignments: Vec<Assignment>,
    storage: Arc<Mutex<FrontendStorage<P>>>,
    session: Arc<dyn Sender>,
}

impl<'uc, P: BackendStorage> UpdateCommand<'uc, P> {
    pub(crate) fn new(
        raw_sql_query: &'uc str,
        name: ObjectName,
        assignments: Vec<Assignment>,
        storage: Arc<Mutex<FrontendStorage<P>>>,
        session: Arc<dyn Sender>,
    ) -> UpdateCommand<'uc, P> {
        UpdateCommand {
            raw_sql_query,
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
        let mut non_existing_columns = BTreeSet::new();

        // only process the rows if the table and schema exist.
        let table_desciption = match self.storage.lock().unwrap().table_descriptor(schema_name.as_str(), table_name.as_str())? {
            Ok(desc) => desc,
            Err(OperationOnTableError::SchemaDoesNotExist) => {
                self.session
                    .send(Err(QueryErrorBuilder::new().schema_does_not_exist(schema_name.to_string()).build()))
                    .expect("To Send Query Result to Client");
                return Ok(())
            }
            Err(OperationOnTableError::TableDoesNotExist) => {
                self.session
                    .send(Err(QueryErrorBuilder::new()
                        .table_does_not_exist(format!("{}.{}", schema_name, table_name))
                        .build()))
                    .expect("To Send Query Result to Client");
                return Ok(())
            }
            _ => unreachable!()
        };

        for item in self.assignments.iter() {
            let Assignment { id, value } = &item;
            let Ident { value: column, .. } = id;

            let value = match resolve_static_expr(value) {
                Ok(datum) => datum,
                Err(e) => {
                    self.session
                        .send(Err(QueryErrorBuilder::new()
                            .feature_not_supported(format!("{:?}", e))
                            .build()))
                        .expect("To Send Result to Client");
                    return Ok(());
                }
            };
            // let value = match value {
            //     Expr::Value(Value::Number(val)) => val.to_string(),
            //     Expr::Value(Value::SingleQuotedString(v)) => v.to_string(),
            //     Expr::UnaryOp { op, expr } => match (op, &**expr) {
            //         (UnaryOperator::Minus, Expr::Value(Value::Number(v))) => "-".to_owned() + v.to_string().as_str(),
            //         (op, expr) => {
            //             self.session
            //                 .send(Err(QueryErrorBuilder::new()
            //                     .syntax_error(op.to_string() + expr.to_string().as_str())
            //                     .build()))
            //                 .expect("To Send Query Result to Client");
            //             return Ok(());
            //         }
            //     },
            //     expr @ Expr::BinaryOp { .. } => match ExpressionEvaluation::new(self.session.clone()).eval(expr) {
            //         Ok(expr_result) => expr_result.value(),
            //         Err(()) => return Ok(()),
            //     },
            //     expr => {
            //         self.session
            //             .send(Err(QueryErrorBuilder::new().syntax_error(expr.to_string()).build()))
            //             .expect("To Send Query Result to Client");
            //         return Ok(());
            //     }
            // };
            if let Some((idx, _)) = table_desciption.find_column(column.as_str()) {
                // type compatibility needs to be checked
                to_update.push((idx, value));
            }
            else {
                non_existing_columns.insert(column.clone());
            }
        }

        if !non_existing_columns.is_empty() {
            self.session
                .send(Err(QueryErrorBuilder::new()
                    .column_does_not_exist(non_existing_columns.into_iter().collect())
                    .build()))
                .expect("To Send Query Result to Client");
            return Ok(());
        }

        let rows: Vec<(Vec<u8>, Vec<u8>)> = self.storage.lock()
            .unwrap()
            .read_unchecked(schema_name.as_str(), table_name.as_str())?
            .map(Result::unwrap)
            .map(|(key, value)| {
                let mut datums = unpack_raw(value.as_slice()); // let mut datums = Row::with_data(value.clone()).unpack();
                // for (idx, column_def, value) in &to_update {
                //     datums[*idx] = value.clone();
                // }
                // (key, Row::pack(datums.clone().as_slice()).to_bytes())
                for (idx, data) in to_update.as_slice() {
                    datums[*idx] = data.clone();
                }
                (key, crate::query::Row::pack(&datums).to_bytes())
            }).collect();


        match (self.storage.lock().unwrap()).update_all(&schema_name, &table_name, rows)? {
            Ok(records_number) => {
                self.session
                    .send(Ok(QueryEvent::RecordsUpdated(records_number)))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
            Err(OperationOnTableError::SchemaDoesNotExist) => {
                self.session
                    .send(Err(QueryErrorBuilder::new().schema_does_not_exist(schema_name.to_string()).build()))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
            Err(OperationOnTableError::TableDoesNotExist) => {
                self.session
                    .send(Err(QueryErrorBuilder::new()
                        .table_does_not_exist(format!("{}.{}", schema_name, table_name))
                        .build()))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
            Err(OperationOnTableError::ColumnDoesNotExist(non_existing_columns)) => {
                self.session
                    .send(Err(QueryErrorBuilder::new()
                        .column_does_not_exist(non_existing_columns)
                        .build()))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
            Err(OperationOnTableError::ConstraintViolations(constraint_errors, row_index)) => {
                let mut builder = QueryErrorBuilder::new();
                let constraint_error_mapper = |(err, column_definition): &(ConstraintError, ColumnDefinition)| match err
                {
                    ConstraintError::OutOfRange => {
                        builder.out_of_range(
                            column_definition.sql_type().to_pg_types(),
                            column_definition.name(),
                            row_index,
                        );
                    }
                    ConstraintError::TypeMismatch(value) => {
                        builder.type_mismatch(
                            value,
                            column_definition.sql_type().to_pg_types(),
                            column_definition.name(),
                            row_index,
                        );
                    }
                    ConstraintError::ValueTooLong(len) => {
                        builder.string_length_mismatch(
                            column_definition.sql_type().to_pg_types(),
                            *len,
                            column_definition.name(),
                            row_index,
                        );
                    }
                };

                constraint_errors.iter().for_each(constraint_error_mapper);
                self.session
                    .send(Err(builder.build()))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
            _ => {
                self.session
                    .send(Err(QueryErrorBuilder::new()
                        .feature_not_supported(self.raw_sql_query.to_owned())
                        .build()))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
        }
    }
}
