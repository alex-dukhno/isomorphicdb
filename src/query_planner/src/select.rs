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

use crate::{Planner, Result};
use ast::predicates::{PredicateOp, PredicateValue};
use data_manager::{DataManager, MetadataView};
use plan::{FullTableName, Plan, SelectInput, TableId};
use protocol::{results::QueryError, Sender};
use sqlparser::ast::{
    BinaryOperator, Expr, Ident, Query, Select, SelectItem, SetExpr, TableFactor, TableWithJoins, Value,
};
use std::{convert::TryFrom, ops::Deref, sync::Arc};
use storage::Database;

pub(crate) struct SelectPlanner {
    query: Box<Query>,
}

impl SelectPlanner {
    pub(crate) fn new(query: Box<Query>) -> SelectPlanner {
        SelectPlanner { query }
    }
}

impl<D: Database> Planner<D> for SelectPlanner {
    fn plan(self, data_manager: Arc<DataManager<D>>, sender: Arc<dyn Sender>) -> Result<Plan> {
        let Query { body, .. } = &*self.query;
        let result = if let SetExpr::Select(query) = body {
            let Select {
                projection,
                from,
                selection,
                ..
            } = query.deref();
            let TableWithJoins { relation, .. } = &from[0];
            let name = match relation {
                TableFactor::Table { name, .. } => name,
                _ => {
                    sender
                        .send(Err(QueryError::feature_not_supported(&*self.query)))
                        .expect("To Send Query Result to Client");
                    return Err(());
                }
            };

            match FullTableName::try_from(name) {
                Ok(full_table_name) => {
                    let (schema_name, table_name) = full_table_name.as_tuple();
                    match data_manager.table_exists(&schema_name, &table_name) {
                        None => {
                            sender
                                .send(Err(QueryError::schema_does_not_exist(schema_name)))
                                .expect("To Send Result to Client");
                            return Err(());
                        }
                        Some((_, None)) => {
                            sender
                                .send(Err(QueryError::table_does_not_exist(
                                    schema_name.to_owned() + "." + table_name,
                                )))
                                .expect("To Send Result to Client");
                            return Err(());
                        }
                        Some((schema_id, Some(table_id))) => {
                            let selected_columns = {
                                let mut columns: Vec<String> = vec![];
                                for item in projection {
                                    match item {
                                        SelectItem::Wildcard => {
                                            let all_columns = data_manager
                                                .table_columns(&Box::new((schema_id, table_id)))
                                                .map_err(|_| ())?;
                                            columns.extend(
                                                all_columns
                                                    .into_iter()
                                                    .map(|column_definition| column_definition.name())
                                                    .collect::<Vec<String>>(),
                                            )
                                        }
                                        SelectItem::UnnamedExpr(Expr::Identifier(Ident { value, .. })) => {
                                            columns.push(value.to_lowercase())
                                        }
                                        _ => {
                                            sender
                                                .send(Err(QueryError::feature_not_supported(&*self.query)))
                                                .expect("To Send Query Result to Client");
                                            return Err(());
                                        }
                                    }
                                }
                                let (columns, not_found) = data_manager
                                    .column_ids(&Box::new((schema_id, table_id)), &columns)
                                    .expect("all needed column found");
                                for column_name in not_found {
                                    sender
                                        .send(Err(QueryError::column_does_not_exist(column_name)))
                                        .expect("To Send Result to Client");
                                }
                                columns
                            };

                            let predicate = match selection {
                                Some(Expr::BinaryOp { left, op, right }) => {
                                    let l = match left.deref() {
                                        Expr::Identifier(ident) => {
                                            let (columns, _not_found) = data_manager
                                                .column_ids(&Box::new((schema_id, table_id)), &[ident.to_string()])
                                                .expect("all needed column found");
                                            PredicateValue::Column(columns[0])
                                        }
                                        _ => panic!(),
                                    };
                                    let o = match op {
                                        BinaryOperator::Eq => PredicateOp::Eq,
                                        _ => panic!(),
                                    };
                                    let r = match right.deref() {
                                        Expr::Value(Value::Number(num)) => PredicateValue::Number(num.clone()),
                                        _ => panic!(),
                                    };
                                    Some((l, o, r))
                                }
                                _ => None,
                            };

                            SelectInput {
                                table_id: TableId::from((schema_id, table_id)),
                                selected_columns,
                                predicate,
                            }
                        }
                    }
                }
                Err(error) => {
                    sender
                        .send(Err(QueryError::syntax_error(error)))
                        .expect("To Send Query Result to Client");
                    return Err(());
                }
            }
        } else {
            sender
                .send(Err(QueryError::feature_not_supported(&*self.query)))
                .expect("To Send Query Result to Client");
            return Err(());
        };
        Ok(Plan::Select(result))
    }
}
