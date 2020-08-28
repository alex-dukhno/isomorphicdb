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

use crate::query::expr::{ExpressionEvaluation, EvalScalarOp};
use crate::{catalog_manager::CatalogManager, ColumnDefinition};
use kernel::SystemResult;
use protocol::{
    results::{QueryErrorBuilder, QueryEvent},
    Sender,
};
use representation::{unpack_raw, Binary, Datum};
use sql_types::ConstraintError;
use sqlparser::ast::{Assignment, Expr, Ident, ObjectName, UnaryOperator, Value};
use std::{collections::BTreeSet, convert::TryFrom, sync::Arc};
use storage::Row;
use crate::query::scalar::ScalarOp;

pub(crate) struct UpdateCommand {
    name: ObjectName,
    assignments: Vec<Assignment>,
    storage: Arc<CatalogManager>,
    session: Arc<dyn Sender>,
}

impl UpdateCommand {
    pub(crate) fn new(
        name: ObjectName,
        assignments: Vec<Assignment>,
        storage: Arc<CatalogManager>,
        session: Arc<dyn Sender>,
    ) -> UpdateCommand {
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



        if !self.storage.schema_exists(&schema_name) {
            self.session
                .send(Err(QueryErrorBuilder::new().schema_does_not_exist(schema_name).build()))
                .expect("To Send Result to Client");
            return Ok(());
        }


        let all_columns;
        // let mut errors = Vec::new();
        let mut non_existing_columns = BTreeSet::new();
        let mut column_exists = false;


        // only process the rows if the table and schema exist.
        if !self.storage.table_exists(&schema_name, &table_name) {
            // for (column_name, value) in to_update {
            //     for (index, column_definition) in all_columns.iter().enumerate() {
            //         if column_definition.has_name(&column_name) {
            //             let datum = value.as_datum().unwrap();
            //             let v = datum.to_string();
            //             match column_definition.sql_type().constraint().validate(v.as_str()) {
            //                 Ok(()) => {
            //                     index_value_pairs.push((index, datum));
            //                 }
            //                 Err(e) => {
            //                     errors.push((e, column_definition.clone()));
            //                 }
            //             }
            //             column_exists = true;
            //             break;
            //         }
            //     }
            //
            //     if !column_exists {
            //         non_existing_columns.insert(column_name.clone());
            //     }
            // }
            self.session
                .send(Err(QueryErrorBuilder::new()
                    .table_does_not_exist(schema_name + "." + table_name.as_str())
                    .build()))
                .expect("To Send Result to Client");
            return Ok(());
        }

        let table_definition = self.storage.table_descriptor(&schema_name, &table_name)?;
        all_columns = table_definition.column_data();

        let evaluation = ExpressionEvaluation::new(self.session.clone(), vec![table_definition.clone()]);

        for item in self.assignments.iter() {
            match evaluation.eval_assignment(item) {
                Ok(assign) => to_update.push(assign),
                Err(()) => return Ok(()),
            }
        }

        if !non_existing_columns.is_empty() {
            self.session
                .send(Err(QueryErrorBuilder::new()
                    .column_does_not_exist(non_existing_columns.into_iter().collect())
                    .build()))
                .expect("To Send Result to Client");
            return Ok(());
        }
        // if !errors.is_empty() {
        //     let row_index = 1;
        //     let mut builder = QueryErrorBuilder::new();
        //     let constraint_error_mapper = |(err, column_definition): &(ConstraintError, ColumnDefinition)| match err {
        //         ConstraintError::OutOfRange => {
        //             builder.out_of_range(
        //                 (&column_definition.sql_type()).into(),
        //                 column_definition.name(),
        //                 row_index,
        //             );
        //         }
        //         ConstraintError::TypeMismatch(value) => {
        //             builder.type_mismatch(
        //                 value,
        //                 (&column_definition.sql_type()).into(),
        //                 column_definition.name(),
        //                 row_index,
        //             );
        //         }
        //         ConstraintError::ValueTooLong(len) => {
        //             builder.string_length_mismatch(
        //                 (&column_definition.sql_type()).into(),
        //                 *len,
        //                 column_definition.name(),
        //                 row_index,
        //             );
        //         }
        //     };
        //
        //     errors.iter().for_each(constraint_error_mapper);
        //     self.session
        //         .send(Err(builder.build()))
        //         .expect("To Send Query Result to Client");
        //     return Ok(());
        // }

        let to_update: Vec<Row> = match self.storage.table_scan(&schema_name, &table_name) {
            Err(error) => return Err(error),
            Ok(reads) => reads
                .map(Result::unwrap)
                .map(|(key, values)| {
                    let mut datums = unpack_raw(values.to_bytes());

                    for update in to_update.as_slice() {
                        EvalScalarOp::eval_on_row(self.session.as_ref(), &mut datums.as_mut_slice(), update).expect("failed to eval assignment expression");
                    }

                    (key, Binary::pack(&datums))
                })
                .collect(),
        };

        match self.storage.update_all(&schema_name, &table_name, to_update) {
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
