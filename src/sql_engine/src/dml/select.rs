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

use kernel::SystemResult;
use protocol::results::{QueryError, QueryEvent, QueryResult};
use sqlparser::ast::{Expr, Ident, Query, SelectItem};
use std::{
    ops::Deref,
    sync::{Arc, Mutex},
};
use storage::{backend::BackendStorage, frontend::FrontendStorage, OperationOnTableError};

pub(crate) struct SelectCommand<'q, P: BackendStorage> {
    raw_sql_query: &'q str,
    query: Box<Query>,
    storage: Arc<Mutex<FrontendStorage<P>>>,
}

impl<P: BackendStorage> SelectCommand<'_, P> {
    pub(crate) fn new(
        raw_sql_query: &'_ str,
        query: Box<Query>,
        storage: Arc<Mutex<FrontendStorage<P>>>,
    ) -> SelectCommand<P> {
        SelectCommand {
            raw_sql_query,
            query,
            storage,
        }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<QueryResult> {
        let sqlparser::ast::Query { body, .. } = &*self.query;
        if let sqlparser::ast::SetExpr::Select(select) = body {
            let sqlparser::ast::Select { projection, from, .. } = select.deref();
            let sqlparser::ast::TableWithJoins { relation, .. } = &from[0];
            let (schema_name, table_name) = match relation {
                sqlparser::ast::TableFactor::Table { name, .. } => {
                    let table_name = name.0[1].to_string();
                    let schema_name = name.0[0].to_string();
                    (schema_name, table_name)
                }
                _ => return Ok(Err(QueryError::not_supported_operation(self.raw_sql_query.to_owned()))),
            };
            let table_columns = {
                let projection = projection.clone();
                let mut columns: Vec<String> = vec![];
                for item in projection {
                    match item {
                        SelectItem::Wildcard => {
                            let all_columns =
                                (self.storage.lock().unwrap()).table_columns(&schema_name, &table_name)?;
                            columns.extend(
                                all_columns
                                    .into_iter()
                                    .map(|column_definition| column_definition.name())
                                    .collect::<Vec<_>>(),
                            )
                        }
                        SelectItem::UnnamedExpr(Expr::Identifier(Ident { value, .. })) => columns.push(value.clone()),
                        _ => return Ok(Err(QueryError::not_supported_operation(self.raw_sql_query.to_owned()))),
                    }
                }
                columns
            };
            match (self.storage.lock().unwrap()).select_all_from(&schema_name, &table_name, table_columns)? {
                Ok(records) => Ok(Ok(QueryEvent::RecordsSelected((
                    records
                        .0
                        .into_iter()
                        .map(|column_definition| (column_definition.name(), column_definition.sql_type().to_pg_types()))
                        .collect(),
                    records.1,
                )))),
                Err(OperationOnTableError::ColumnDoesNotExist(non_existing_columns)) => {
                    Ok(Err(QueryError::column_does_not_exist(non_existing_columns)))
                }
                Err(OperationOnTableError::SchemaDoesNotExist) => {
                    Ok(Err(QueryError::schema_does_not_exist(schema_name.to_owned())))
                }
                Err(OperationOnTableError::TableDoesNotExist) => Ok(Err(QueryError::table_does_not_exist(
                    schema_name.to_owned() + "." + table_name.as_str(),
                ))),
                _ => Ok(Err(QueryError::not_supported_operation(self.raw_sql_query.to_owned()))),
            }
        } else {
            Ok(Err(QueryError::not_supported_operation(self.raw_sql_query.to_owned())))
        }
    }
}
