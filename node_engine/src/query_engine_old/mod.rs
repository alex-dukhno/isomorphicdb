// Copyright 2020 - 2021 Alex Dukhno
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

use crate::session::{
    statement::{Portal, PreparedStatement},
    Session,
};
use catalog::CatalogHandlerOld;
use data_definition::ExecutionOutcome;
use data_manipulation::{
    DynamicTypedTree, QueryExecutionResult, StaticTypedTree, TypedDeleteQuery, TypedInsertQuery, TypedQuery,
    TypedSelectQuery, TypedUpdateQuery, UntypedQuery,
};
use data_repr::scalar::ScalarValue;
use definition::ColumnDef;
use definition_planner::DefinitionPlannerOld;
use postgre_sql::{
    query_ast::{Extended, Statement},
    query_parser::QueryParser,
    query_response::{QueryError, QueryEvent},
    wire_protocol::{payload::*, Request, Sender},
};
use query_analyzer::QueryAnalyzerOld;
use query_planner::QueryPlannerOld;
use query_processing::{TypeChecker, TypeCoercion, TypeInference};
use std::{
    rc::Rc,
    str,
    sync::{Arc, Mutex},
};
use storage::{ConflictableTransactionError, Database, TransactionResult};
use types::{SqlType, SqlTypeFamily};

pub(crate) struct QueryEngineOld {
    session: Arc<Mutex<Session>>,
    sender: Arc<Mutex<dyn Sender>>,
    type_inference: TypeInference,
    type_checker: TypeChecker,
    type_coercion: TypeCoercion,
    database: Database,
}

pub fn decode(ty: u32, format: i16, raw: &[u8]) -> Result<Value, ()> {
    match format {
        1 => decode_binary(ty, raw),
        0 => decode_text(ty, raw),
        _ => unimplemented!(),
    }
}

fn decode_binary(ty: u32, raw: &[u8]) -> Result<Value, ()> {
    match ty {
        BOOL => {
            if raw.is_empty() {
                Err(())
            } else {
                Ok(Value::Bool(raw[0] != 0))
            }
        }
        CHAR | VARCHAR => str::from_utf8(raw)
            .map(|s| Value::String(s.into()))
            .map_err(|_cause| ()),
        SMALLINT => {
            if raw.len() < 4 {
                Err(())
            } else {
                Ok(Value::Int16(i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as i16))
            }
        }
        INT => {
            if raw.len() < 4 {
                Err(())
            } else {
                Ok(Value::Int32(i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]])))
            }
        }
        BIGINT => {
            if raw.len() < 8 {
                Err(())
            } else {
                Ok(Value::Int64(i64::from_be_bytes([
                    raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
                ])))
            }
        }
        _ => unimplemented!(),
    }
}

const BOOL_TRUE: &[&str] = &["t", "tr", "tru", "true", "y", "ye", "yes", "on", "1"];
const BOOL_FALSE: &[&str] = &["f", "fa", "fal", "fals", "false", "n", "no", "of", "off", "0"];

fn decode_text(ty: u32, raw: &[u8]) -> Result<Value, ()> {
    let s = match str::from_utf8(raw) {
        Ok(s) => s,
        Err(_cause) => return Err(()),
    };

    match ty {
        BOOL => {
            let v = s.trim().to_lowercase();
            if BOOL_TRUE.contains(&v.as_str()) {
                Ok(Value::Bool(true))
            } else if BOOL_FALSE.contains(&v.as_str()) {
                Ok(Value::Bool(false))
            } else {
                Err(())
            }
        }
        CHAR => Ok(Value::String(s.into())),
        VARCHAR => Ok(Value::String(s.into())),
        SMALLINT => s.trim().parse().map(Value::Int16).map_err(|_cause| ()),
        INT => s.trim().parse().map(Value::Int32).map_err(|_cause| ()),
        BIGINT => s.trim().parse().map(Value::Int64).map_err(|_cause| ()),
        _ => unimplemented!(),
    }
}

impl QueryEngineOld {
    pub(crate) fn new(sender: Arc<Mutex<dyn Sender>>, database: Database) -> QueryEngineOld {
        QueryEngineOld {
            session: Arc::default(),
            sender,
            type_inference: TypeInference::default(),
            type_checker: TypeChecker,
            type_coercion: TypeCoercion,
            database,
        }
    }

    pub(crate) fn execute(&mut self, request: Request) -> TransactionResult<()> {
        let inner = Rc::new(request);
        let mut session = self.session.lock().unwrap();
        self.database.old_transaction(|db| {
            log::trace!("TRANSACTION START");
            log::trace!("{:?}", db.lookup_table_ref("DEFINITION_SCHEMA.TABLES"));
            let query_analyzer = QueryAnalyzerOld::from(db.clone());
            let definition_planner = DefinitionPlannerOld::from(db.clone());
            let query_planner = QueryPlannerOld::from(db.clone());
            let catalog = CatalogHandlerOld::from(db.clone());
            let query_parser = QueryParser;
            let result = match &*inner {
                Request::Query { sql } => {
                    match query_parser.parse(&sql) {
                        Ok(mut statements) => match statements.pop().expect("single query") {
                            Statement::Extended(extended_query) => match extended_query {
                                Extended::Prepare {
                                    query,
                                    name,
                                    param_types,
                                } => {
                                    session.set_portal(
                                        name.clone(),
                                        Portal::new(
                                            name,
                                            query_analyzer.analyze(query).unwrap(),
                                            vec![],
                                            vec![],
                                            param_types
                                                .into_iter()
                                                .map(|data_type| SqlType::from(data_type).family())
                                                .collect(),
                                        ),
                                    );
                                    let x46: Vec<u8> = QueryEvent::StatementPrepared.into();
                                    self.sender.lock().unwrap()
                                        .send(&x46)
                                        .expect("To Send Result");
                                }
                                Extended::Execute { name, param_values } => {
                                    let param_values = param_values.into_iter().map(From::from).collect();
                                    match session.get_portal(&name) {
                                        Some(portal) => match portal.stmt() {
                                            UntypedQuery::Insert(insert) => {
                                                log::debug!("INSERT UNTYPED VALUES {:?}", insert.values);
                                                let typed_values = insert
                                                    .values
                                                    .into_iter()
                                                    .map(|values| {
                                                        values
                                                            .into_iter()
                                                            .map(|value| {
                                                                value.map(|v| {
                                                                    self.type_inference
                                                                        .infer_static(v, portal.param_types())
                                                                })
                                                            })
                                                            .collect()
                                                    })
                                                    .collect::<Vec<Vec<Option<StaticTypedTree>>>>();
                                                log::debug!("INSERT TYPED VALUES {:?}", typed_values);
                                                let type_checked = typed_values
                                                    .into_iter()
                                                    .map(|values| {
                                                        values
                                                            .into_iter()
                                                            .map(|value| value.map(|v| self.type_checker.check_static(v)))
                                                            .collect()
                                                    })
                                                    .collect::<Vec<Vec<Option<StaticTypedTree>>>>();
                                                log::debug!("INSERT TYPE CHECKED VALUES {:?}", type_checked);
                                                let table_info = catalog
                                                    .table_definition(insert.full_table_name.clone())
                                                    .unwrap()
                                                    .unwrap();
                                                let table_columns = table_info.columns();
                                                let mut type_coerced = vec![];
                                                for checked in type_checked {
                                                    let mut row = vec![];
                                                    for (index, c) in checked.into_iter().enumerate() {
                                                        row.push(c.map(|c| {
                                                            self.type_coercion
                                                                .coerce_static(c, table_columns[index].sql_type())
                                                        }));
                                                    }
                                                    type_coerced.push(row);
                                                }
                                                log::debug!("INSERT TYPE COERCED VALUES {:?}", type_coerced);
                                                let query_result = match query_planner
                                                    .plan(TypedQuery::Insert(TypedInsertQuery {
                                                        full_table_name: insert.full_table_name,
                                                        values: type_coerced,
                                                    }))
                                                    .execute(param_values)
                                                    .map(|r| { let r: QueryEvent = r.into(); r })
                                                    .map(|r| { let r: Vec<u8> = r.into(); r })
                                                    .map_err(|e| { let e: QueryError = e.into(); e })
                                                    .map_err(|e| { let e: Vec<u8> = e.into(); e }) {
                                                    Ok(ok) => ok,
                                                    Err(err) => err,
                                                };
                                                self.sender.lock().unwrap().send(&query_result).expect("To Send to client");
                                            }
                                            UntypedQuery::Update(update) => {
                                                let typed_values = update
                                                    .assignments
                                                    .into_iter()
                                                    .map(|value| {
                                                        value.map(|value| {
                                                            self.type_inference.infer_dynamic(value, portal.param_types())
                                                        })
                                                    })
                                                    .collect::<Vec<Option<DynamicTypedTree>>>();
                                                log::debug!("UPDATE TYPED VALUES - {:?}", typed_values);
                                                let type_checked = typed_values
                                                    .into_iter()
                                                    .map(|value| value.map(|value| self.type_checker.check_dynamic(value)))
                                                    .collect::<Vec<Option<DynamicTypedTree>>>();
                                                log::debug!("UPDATE TYPE CHECKED VALUES - {:?}", type_checked);
                                                let type_coerced = type_checked
                                                    .into_iter()
                                                    .map(|value| {
                                                        value.map(|value| self.type_coercion.coerce_dynamic(value))
                                                    })
                                                    .collect::<Vec<Option<DynamicTypedTree>>>();
                                                log::debug!("UPDATE TYPE COERCED VALUES - {:?}", type_coerced);

                                                log::debug!("UPDATE UNTYPED FILTER - {:?}", update.filter);
                                                let typed_filter = update
                                                    .filter
                                                    .map(|value| self.type_inference.infer_dynamic(value, &[]));
                                                log::debug!("UPDATE TYPED FILTER - {:?}", typed_filter);
                                                let type_checked_filter = typed_filter
                                                    .map(|value| self.type_checker.check_dynamic(value));
                                                log::debug!("UPDATE TYPE CHECKED FILTER - {:?}", type_checked_filter);
                                                let type_coerced_filter = type_checked_filter
                                                    .map(|value| self.type_coercion.coerce_dynamic(value));
                                                log::debug!("UPDATE TYPE COERCED FILTER - {:?}", type_coerced_filter);

                                                let query_result = match query_planner
                                                    .plan(TypedQuery::Update(TypedUpdateQuery {
                                                        full_table_name: update.full_table_name,
                                                        assignments: type_coerced,
                                                        filter: type_coerced_filter
                                                    }))
                                                    .execute(param_values)
                                                    .map(|r| { let r: QueryEvent = r.into(); r })
                                                    .map(|r| { let r: Vec<u8> = r.into(); r })
                                                    .map_err(|e| { let e: QueryError = e.into(); e })
                                                    .map_err(|e| { let e: Vec<u8> = e.into(); e }) {
                                                    Ok(ok) => ok,
                                                    Err(err) => err,
                                                };
                                                self.sender.lock().unwrap().send(&query_result).expect("To Send to client");
                                            }
                                            UntypedQuery::Select(select) => {
                                                log::debug!("SELECT UNTYPED VALUES - {:?}", select.projection_items);
                                                let typed_values = select
                                                    .projection_items
                                                    .into_iter()
                                                    .map(|value| self.type_inference.infer_dynamic(value, &[]))
                                                    .collect::<Vec<DynamicTypedTree>>();
                                                log::debug!("SELECT TYPED VALUES - {:?}", typed_values);
                                                let type_checked = typed_values
                                                    .into_iter()
                                                    .map(|value| self.type_checker.check_dynamic(value))
                                                    .collect::<Vec<DynamicTypedTree>>();
                                                log::debug!("SELECT TYPE CHECKED VALUES - {:?}", type_checked);
                                                let type_coerced = type_checked
                                                    .into_iter()
                                                    .map(|value| self.type_coercion.coerce_dynamic(value))
                                                    .collect::<Vec<DynamicTypedTree>>();
                                                log::debug!("SELECT TYPE COERCED VALUES - {:?}", type_coerced);

                                                log::debug!("SELECT UNTYPED FILTER - {:?}", select.filter);
                                                let typed_filter = select
                                                    .filter
                                                    .map(|value| self.type_inference.infer_dynamic(value, &[]));
                                                log::debug!("SELECT TYPED FILTER - {:?}", typed_filter);
                                                let type_checked_filter = typed_filter
                                                    .map(|value| self.type_checker.check_dynamic(value));
                                                log::debug!("SELECT TYPE CHECKED FILTER - {:?}", type_checked_filter);
                                                let type_coerced_filter = type_checked_filter
                                                    .map(|value| self.type_coercion.coerce_dynamic(value));
                                                log::debug!("SELECT TYPE COERCED FILTER - {:?}", type_coerced_filter);

                                                let query_result = query_planner
                                                    .plan(TypedQuery::Select(TypedSelectQuery {
                                                        projection_items: type_coerced,
                                                        full_table_name: select.full_table_name,
                                                        filter: type_coerced_filter
                                                    }))
                                                    .execute(param_values)
                                                    .map_err(|e| { let e: QueryError = e.into(); e })
                                                    .map_err(|e| { let e: Vec<u8> = e.into(); e });
                                                match query_result {
                                                    Ok(QueryExecutionResult::Selected((desc, data))) => {
                                                        let x45: Vec<u8> = QueryEvent::RowDescription(desc).into();
                                                        self.sender.lock().unwrap()
                                                            .send(&x45)
                                                            .expect("To Send to client");
                                                        let len = data.len();
                                                        for row in data {
                                                            let x44: Vec<u8> = QueryEvent::DataRow(
                                                                row.into_iter()
                                                                    .map(|scalar| scalar.as_text())
                                                                    .collect(),
                                                            ).into();
                                                            self.sender.lock().unwrap()
                                                                .send(&x44)
                                                                .expect("To Send to client");
                                                        }
                                                        let x43: Vec<u8> = QueryEvent::RecordsSelected(len).into();
                                                        self.sender.lock().unwrap()
                                                            .send(&x43)
                                                            .expect("To Send to client");
                                                    }
                                                    Ok(_) => unreachable!(),
                                                    Err(error) => {
                                                        self.sender.lock().unwrap().send(&error).expect("To Send to client");
                                                    }
                                                }
                                            }
                                            UntypedQuery::Delete(delete) => {

                                                log::debug!("DELETE UNTYPED FILTER - {:?}", delete.filter);
                                                let typed_filter = delete
                                                    .filter
                                                    .map(|value| self.type_inference.infer_dynamic(value, &[]));
                                                log::debug!("DELETE TYPED FILTER - {:?}", typed_filter);
                                                let type_checked_filter = typed_filter
                                                    .map(|value| self.type_checker.check_dynamic(value));
                                                log::debug!("DELETE TYPE CHECKED FILTER - {:?}", type_checked_filter);
                                                let type_coerced_filter = type_checked_filter
                                                    .map(|value| self.type_coercion.coerce_dynamic(value));
                                                log::debug!("DELETE TYPE COERCED FILTER - {:?}", type_coerced_filter);

                                                let query_result = match query_planner
                                                    .plan(TypedQuery::Delete(TypedDeleteQuery {
                                                        full_table_name: delete.full_table_name,
                                                        filter: type_coerced_filter
                                                    }))
                                                    .execute(param_values)
                                                    .map(|r| { let r: QueryEvent = r.into(); r })
                                                    .map(|r| { let r: Vec<u8> = r.into(); r })
                                                    .map_err(|e| { let e: QueryError = e.into(); e })
                                                    .map_err(|e| { let e: Vec<u8> = e.into(); e }) {
                                                    Ok(ok) => ok,
                                                    Err(err) => err,
                                                };
                                                self.sender.lock().unwrap().send(&query_result).expect("To Send to client");
                                            }
                                        },
                                        None => {
                                            let x42: Vec<u8> = QueryError::prepared_statement_does_not_exist(name).into();
                                            self.sender.lock().unwrap()
                                                .send(&x42)
                                                .expect("To Send Error to Client");
                                        }
                                    }
                                }
                                Extended::Deallocate { name } => {
                                    session.remove_portal(&name);
                                    let x41: Vec<u8> = QueryEvent::StatementDeallocated.into();
                                    self.sender.lock().unwrap()
                                        .send(&x41)
                                        .expect("To Send Statement Deallocated Event");
                                }
                            },
                            Statement::Definition(definition) => match definition_planner.plan(definition) {
                                Ok(schema_change) => {
                                    log::debug!("SCHEMA CHANGE - {:?}", schema_change);
                                    let query_result: Vec<u8> = match catalog.apply(schema_change) {
                                        Ok(ExecutionOutcome::SchemaCreated) => QueryEvent::SchemaCreated.into(),
                                        Ok(ExecutionOutcome::SchemaDropped) => QueryEvent::SchemaDropped.into(),
                                        Ok(ExecutionOutcome::TableCreated) => QueryEvent::TableCreated.into(),
                                        Ok(ExecutionOutcome::TableDropped) => QueryEvent::TableDropped.into(),
                                        Ok(ExecutionOutcome::IndexCreated) => QueryEvent::IndexCreated.into(),
                                        Err(error) => {
                                            let error: QueryError = error.into();
                                            error.into()
                                        },
                                    };
                                    self.sender.lock().unwrap().send(&query_result).expect("To Send Result to Client");
                                }
                                Err(error) => {
                                    let error: QueryError = error.into();
                                    let x40: Vec<u8> = error.into();
                                    self.sender.lock().unwrap().send(&x40).expect("To Send Result to Client")
                                },
                            },
                            Statement::Query(query) => match query_analyzer.analyze(query) {
                                Ok(UntypedQuery::Delete(delete)) => {

                                    log::debug!("DELETE UNTYPED FILTER - {:?}", delete.filter);
                                    let typed_filter = delete
                                        .filter
                                        .map(|value| self.type_inference.infer_dynamic(value, &[]));
                                    log::debug!("DELETE TYPED FILTER - {:?}", typed_filter);
                                    let type_checked_filter = typed_filter
                                        .map(|value| self.type_checker.check_dynamic(value));
                                    log::debug!("DELETE TYPE CHECKED FILTER - {:?}", type_checked_filter);
                                    let type_coerced_filter = type_checked_filter
                                        .map(|value| self.type_coercion.coerce_dynamic(value));
                                    log::debug!("DELETE TYPE COERCED FILTER - {:?}", type_coerced_filter);

                                    let query_result = match query_planner
                                        .plan(TypedQuery::Delete(TypedDeleteQuery {
                                            full_table_name: delete.full_table_name,
                                            filter: type_coerced_filter
                                        }))
                                        .execute(vec![])
                                        .map(|r| { let r: QueryEvent = r.into(); r })
                                        .map(|r| { let r: Vec<u8> = r.into(); r })
                                        .map_err(|e| { let e: QueryError = e.into(); e })
                                        .map_err(|e| { let e: Vec<u8> = e.into(); e }) {
                                        Ok(ok) => ok,
                                        Err(err) => err,
                                    };
                                    self.sender.lock().unwrap().send(&query_result).expect("To Send to client");
                                }
                                Ok(UntypedQuery::Update(update)) => {
                                    let typed_values = update
                                        .assignments
                                        .into_iter()
                                        .map(|value| value.map(|value| self.type_inference.infer_dynamic(value, &[])))
                                        .collect::<Vec<Option<DynamicTypedTree>>>();
                                    log::debug!("UPDATE TYPED VALUES - {:?}", typed_values);
                                    let type_checked = typed_values
                                        .into_iter()
                                        .map(|value| value.map(|value| self.type_checker.check_dynamic(value)))
                                        .collect::<Vec<Option<DynamicTypedTree>>>();
                                    log::debug!("UPDATE TYPE CHECKED VALUES - {:?}", type_checked);
                                    let type_coerced = type_checked
                                        .into_iter()
                                        .map(|value| value.map(|value| self.type_coercion.coerce_dynamic(value)))
                                        .collect::<Vec<Option<DynamicTypedTree>>>();
                                    log::debug!("UPDATE TYPE COERCED VALUES - {:?}", type_coerced);

                                    log::debug!("UPDATE UNTYPED FILTER - {:?}", update.filter);
                                    let typed_filter = update
                                        .filter
                                        .map(|value| self.type_inference.infer_dynamic(value, &[]));
                                    log::debug!("UPDATE TYPED FILTER - {:?}", typed_filter);
                                    let type_checked_filter = typed_filter
                                        .map(|value| self.type_checker.check_dynamic(value));
                                    log::debug!("UPDATE TYPE CHECKED FILTER - {:?}", type_checked_filter);
                                    let type_coerced_filter = type_checked_filter
                                        .map(|value| self.type_coercion.coerce_dynamic(value));
                                    log::debug!("UPDATE TYPE COERCED FILTER - {:?}", type_coerced_filter);

                                    let query_result = match query_planner
                                        .plan(TypedQuery::Update(TypedUpdateQuery {
                                            full_table_name: update.full_table_name,
                                            assignments: type_coerced,
                                            filter: type_coerced_filter
                                        }))
                                        .execute(vec![])
                                        .map(|r| { let r: QueryEvent = r.into(); r })
                                        .map(|r| { let r: Vec<u8> = r.into(); r })
                                        .map_err(|e| { let e: QueryError = e.into(); e })
                                        .map_err(|e| { let e: Vec<u8> = e.into(); e }) {
                                        Ok(ok) => ok,
                                        Err(err) => err,
                                    };
                                    self.sender.lock().unwrap().send(&query_result).expect("To Send to client");
                                }
                                Ok(UntypedQuery::Insert(insert)) => {
                                    log::debug!("INSERT UNTYPED VALUES {:?}", insert.values);
                                    let typed_values = insert
                                        .values
                                        .into_iter()
                                        .map(|values| {
                                            values
                                                .into_iter()
                                                .map(|value| value.map(|v| self.type_inference.infer_static(v, &[])))
                                                .collect()
                                        })
                                        .collect::<Vec<Vec<Option<StaticTypedTree>>>>();
                                    log::debug!("INSERT TYPED VALUES {:?}", typed_values);
                                    let type_checked = typed_values
                                        .into_iter()
                                        .map(|values| {
                                            values
                                                .into_iter()
                                                .map(|value| value.map(|v| self.type_checker.check_static(v)))
                                                .collect()
                                        })
                                        .collect::<Vec<Vec<Option<StaticTypedTree>>>>();
                                    log::debug!("INSERT TYPE CHECKED VALUES {:?}", type_checked);
                                    let table_info = catalog
                                        .table_definition(insert.full_table_name.clone())
                                        .unwrap()
                                        .unwrap();
                                    let table_columns = table_info.columns();
                                    let mut type_coerced = vec![];
                                    for checked in type_checked {
                                        let mut row = vec![];
                                        for (index, c) in checked.into_iter().enumerate() {
                                            row.push(c.map(|c| {
                                                self.type_coercion.coerce_static(c, table_columns[index].sql_type())
                                            }));
                                        }
                                        type_coerced.push(row);
                                    }
                                    log::debug!("INSERT TYPE COERCED VALUES {:?}", type_coerced);
                                    let query_result = match query_planner
                                        .plan(TypedQuery::Insert(TypedInsertQuery {
                                            full_table_name: insert.full_table_name,
                                            values: type_coerced,
                                        }))
                                        .execute(vec![])
                                        .map(|r| { let r: QueryEvent = r.into(); r })
                                        .map(|r| { let r: Vec<u8> = r.into(); r })
                                        .map_err(|e| { let e: QueryError = e.into(); e })
                                        .map_err(|e| { let e: Vec<u8> = e.into(); e }) {
                                        Ok(ok) => ok,
                                        Err(err) => err,
                                    };
                                    self.sender.lock().unwrap().send(&query_result).expect("To Send to client");
                                }
                                Ok(UntypedQuery::Select(select)) => {
                                    log::debug!("SELECT UNTYPED VALUES - {:?}", select.projection_items);
                                    let typed_values = select
                                        .projection_items
                                        .into_iter()
                                        .map(|value| self.type_inference.infer_dynamic(value, &[]))
                                        .collect::<Vec<DynamicTypedTree>>();
                                    log::debug!("SELECT TYPED VALUES - {:?}", typed_values);
                                    let type_checked_values = typed_values
                                        .into_iter()
                                        .map(|value| self.type_checker.check_dynamic(value))
                                        .collect::<Vec<DynamicTypedTree>>();
                                    log::debug!("SELECT TYPE CHECKED VALUES - {:?}", type_checked_values);
                                    let type_coerced_values = type_checked_values
                                        .into_iter()
                                        .map(|value| self.type_coercion.coerce_dynamic(value))
                                        .collect::<Vec<DynamicTypedTree>>();
                                    log::debug!("SELECT TYPE COERCED VALUES - {:?}", type_coerced_values);

                                    log::debug!("SELECT UNTYPED FILTER - {:?}", select.filter);
                                    let typed_filter = select
                                        .filter
                                        .map(|value| self.type_inference.infer_dynamic(value, &[]));
                                    log::debug!("SELECT TYPED FILTER - {:?}", typed_filter);
                                    let type_checked_filter = typed_filter
                                        .map(|value| self.type_checker.check_dynamic(value));
                                    log::debug!("SELECT TYPE CHECKED FILTER - {:?}", type_checked_filter);
                                    let type_coerced_filter = type_checked_filter
                                        .map(|value| self.type_coercion.coerce_dynamic(value));
                                    log::debug!("SELECT TYPE COERCED FILTER - {:?}", type_coerced_filter);

                                    let query_result = query_planner
                                        .plan(TypedQuery::Select(TypedSelectQuery {
                                            projection_items: type_coerced_values,
                                            full_table_name: select.full_table_name,
                                            filter: type_coerced_filter
                                        }))
                                        .execute(vec![])
                                        .map_err(|e| { let e: QueryError = e.into(); e })
                                        .map_err(|e| { let e: Vec<u8> = e.into(); e });
                                    match query_result {
                                        Ok(QueryExecutionResult::Selected((desc, data))) => {
                                            let x38: Vec<u8> = QueryEvent::RowDescription(desc).into();
                                            self.sender.lock().unwrap()
                                                .send(&x38)
                                                .expect("To Send to client");
                                            let len = data.len();
                                            for row in data {
                                                let x37: Vec<u8> = QueryEvent::DataRow(
                                                    row.into_iter().map(|scalar| scalar.as_text()).collect(),
                                                ).into();
                                                self.sender.lock().unwrap()
                                                    .send(&x37)
                                                    .expect("To Send to client");
                                            }
                                            let x36: Vec<u8> = QueryEvent::RecordsSelected(len).into();
                                            self.sender.lock().unwrap()
                                                .send(&x36)
                                                .expect("To Send to client");
                                        }
                                        Ok(_) => unreachable!(),
                                        Err(error) => {
                                            self.sender.lock().unwrap().send(&error).expect("To Send to client");
                                        }
                                    }
                                }
                                Err(error) => {
                                    let error: QueryError = error.into();
                                    let x35: Vec<u8> = error.into();
                                    self.sender.lock().unwrap().send(&x35).expect("To Send Error to Client");
                                }
                            },
                            Statement::Config(_) => {
                                // sending ok to the client to proceed with other requests
                                let x34: Vec<u8> = QueryEvent::VariableSet.into();
                                self.sender.lock().unwrap()
                                    .send(&x34)
                                    .expect("To Send Result to Client");
                            }
                        },
                        Err(parser_error) => {
                            let x33: Vec<u8> = QueryError::syntax_error(parser_error).into();
                            self.sender.lock().unwrap()
                                .send(&x33)
                                .expect("To Send ParseComplete Event");
                        }
                    }
                    let x32: Vec<u8> = QueryEvent::QueryComplete.into();
                    self.sender.lock().unwrap()
                        .send(&x32)
                        .expect("To Send Query Complete to Client");
                    Ok(())
                }
                Request::Parse {
                    statement_name,
                    sql,
                    param_types,
                } => {
                    match session.get_prepared_statement(&statement_name) {
                        Some(stmt) if stmt.raw_query() == sql => match query_parser.parse(&sql) {
                            Ok(mut statements) => match statements.pop() {
                                Some(Statement::Query(query)) => {
                                    stmt.parsed_with_params(
                                        query,
                                        param_types
                                            .iter()
                                            .filter(|o| **o != 0)
                                            .copied()
                                            .collect(),
                                    );
                                    let x31: Vec<u8> = QueryEvent::ParseComplete.into();
                                    self.sender.lock().unwrap().send(&x31).expect("To Send Result");
                                }
                                other => {
                                    let x30: Vec<u8> = QueryError::syntax_error(format!("{:?}", other)).into();
                                    self
                                        .sender.lock().unwrap()
                                        .send(&x30)
                                        .expect("To Send Result")
                                },
                            },
                            Err(parser_error) => {
                                let x29: Vec<u8> = QueryError::syntax_error(parser_error).into();
                                self.sender.lock().unwrap()
                                    .send(&x29)
                                    .expect("To Send Syntax Error Event");
                            }
                        },
                        _ => match query_parser.parse(&sql) {
                            Ok(mut statements) => match statements.pop() {
                                Some(Statement::Query(query)) => {
                                    if param_types.is_empty() || param_types.iter().all(|o| *o != 0) {
                                        let mut prep = PreparedStatement::parsed(sql.clone(), query.clone());
                                        prep.parsed_with_params(
                                            query,
                                            param_types.iter().copied().collect(),
                                        );
                                        session.set_prepared_statement(statement_name.clone(), prep);
                                    } else {
                                        session.set_prepared_statement(
                                            statement_name.clone(),
                                            PreparedStatement::parsed(sql.clone(), query),
                                        );
                                    }
                                    let x28: Vec<u8> = QueryEvent::ParseComplete.into();
                                    self.sender.lock().unwrap().send(&x28).expect("To Send Result");
                                }
                                other => {
                                    let x27: Vec<u8> = QueryError::syntax_error(format!("{:?}", other)).into();
                                    self
                                        .sender.lock().unwrap()
                                        .send(&x27)
                                        .expect("To Send Result")
                                },
                            },
                            Err(parser_error) => {
                                let x26: Vec<u8> = QueryError::syntax_error(parser_error).into();
                                self.sender.lock().unwrap()
                                    .send(&x26)
                                    .expect("To Send Syntax Error Event");
                            }
                        },
                    }
                    Ok(())
                }
                Request::DescribeStatement { name } => {
                    log::debug!("SESSION - {:?}", session);
                    match session.get_prepared_statement(&name) {
                        Some(statement) => match statement.param_types() {
                            None => match query_analyzer.analyze(statement.query().unwrap()) {
                                Ok(UntypedQuery::Insert(insert)) => {
                                    let table_definition = catalog
                                        .table_definition(insert.full_table_name.clone())
                                        .unwrap()
                                        .unwrap();
                                    let param_types = table_definition
                                        .columns()
                                        .iter()
                                        .map(ColumnDef::sql_type)
                                        .map(|sql_type| (&sql_type).into())
                                        .collect::<Vec<u32>>();
                                    let x25: Vec<u8> = QueryEvent::StatementParameters(param_types.to_vec()).into();
                                    self.sender.lock().unwrap()
                                        .send(&x25)
                                        .expect("To Send Statement Parameters to Client");
                                    let x24: Vec<u8> = QueryEvent::StatementDescription(vec![]).into();
                                    self.sender.lock().unwrap()
                                        .send(&x24)
                                        .expect("To Send Statement Description to Client");
                                    statement.described(UntypedQuery::Insert(insert), param_types);
                                }
                                Ok(UntypedQuery::Update(update)) => {
                                    let table_definition = catalog
                                        .table_definition(update.full_table_name.clone())
                                        .unwrap()
                                        .unwrap();
                                    let param_types = table_definition
                                        .columns()
                                        .iter()
                                        .map(ColumnDef::sql_type)
                                        .map(|sql_type| (&sql_type).into())
                                        .collect::<Vec<u32>>();
                                    let x23: Vec<u8> = QueryEvent::StatementParameters(param_types.to_vec()).into();
                                    self.sender.lock().unwrap()
                                        .send(&x23)
                                        .expect("To Send Statement Parameters to Client");
                                    let x22: Vec<u8> = QueryEvent::StatementDescription(vec![]).into();
                                    self.sender.lock().unwrap()
                                        .send(&x22)
                                        .expect("To Send Statement Description to Client");
                                    statement.described(UntypedQuery::Update(update), param_types);
                                }
                                Ok(UntypedQuery::Select(select)) => {
                                    let table_definition = catalog
                                        .table_definition(select.full_table_name.clone())
                                        .unwrap()
                                        .unwrap();
                                    let return_types = table_definition
                                        .columns()
                                        .iter()
                                        .map(|col_def| (col_def.name().to_owned(), col_def.sql_type()))
                                        .map(|(name, sql_type)| (name, (&sql_type).into()))
                                        .collect::<Vec<(String, u32)>>();
                                    let x21: Vec<u8> = QueryEvent::StatementParameters(vec![]).into();
                                    self.sender.lock().unwrap()
                                        .send(&x21)
                                        .expect("To Send Statement Parameters to Client");
                                    let x20: Vec<u8> = QueryEvent::StatementDescription(return_types).into();
                                    self.sender.lock().unwrap()
                                        .send(&x20)
                                        .expect("To Send Statement Description to Client");
                                    statement.described(UntypedQuery::Select(select), vec![]);
                                }
                                _ => {
                                    let x19: Vec<u8> = QueryError::prepared_statement_does_not_exist(name).into();
                                    self.sender.lock().unwrap()
                                        .send(&x19)
                                        .expect("To Send Error to Client");
                                }
                            },
                            Some(param_types) => match query_analyzer.analyze(statement.query().unwrap()) {
                                Ok(UntypedQuery::Insert(_insert)) => {
                                    let x18: Vec<u8> = QueryEvent::StatementParameters(param_types.to_vec()).into();
                                    self.sender.lock().unwrap()
                                        .send(&x18)
                                        .expect("To Send Statement Parameters to Client");
                                    let x17: Vec<u8> = QueryEvent::StatementDescription(vec![]).into();
                                    self.sender.lock().unwrap()
                                        .send(&x17)
                                        .expect("To Send Statement Description to Client");
                                }
                                Ok(UntypedQuery::Update(_update)) => {
                                    let x16: Vec<u8> = QueryEvent::StatementParameters(param_types.to_vec()).into();
                                    self.sender.lock().unwrap()
                                        .send(&x16)
                                        .expect("To Send Statement Parameters to Client");
                                    let x15: Vec<u8> = QueryEvent::StatementDescription(vec![]).into();
                                    self.sender.lock().unwrap()
                                        .send(&x15)
                                        .expect("To Send Statement Description to Client");
                                }
                                Ok(UntypedQuery::Select(select)) => {
                                    let table_definition =
                                        catalog.table_definition(select.full_table_name).unwrap().unwrap();
                                    let return_types = table_definition
                                        .columns()
                                        .iter()
                                        .map(|col_def| (col_def.name().to_owned(), col_def.sql_type()))
                                        .map(|(name, sql_type)| (name, (&sql_type).into()))
                                        .collect::<Vec<(String, u32)>>();
                                    let x14: Vec<u8> = QueryEvent::StatementParameters(param_types.to_vec()).into();
                                    self.sender.lock().unwrap()
                                        .send(&x14)
                                        .expect("To Send Statement Parameters to Client");
                                    let x13: Vec<u8> = QueryEvent::StatementDescription(return_types).into();
                                    self.sender.lock().unwrap()
                                        .send(&x13)
                                        .expect("To Send Statement Description to Client");
                                }
                                _ => {
                                    let bytes: Vec<u8> = QueryError::prepared_statement_does_not_exist(name).into();
                                    self.sender.lock().unwrap()
                                        .send(&bytes)
                                        .expect("To Send Error to Client");
                                }
                            },
                        },
                        other => {
                            log::debug!("STMT {:?} associated with {:?} key", other, name);
                            let x12: Vec<u8> = QueryError::prepared_statement_does_not_exist(name).into();
                            self.sender.lock().unwrap()
                                .send(&x12)
                                .expect("To Send Error to Client");
                        }
                    }
                    Ok(())
                }
                Request::Bind {
                    portal_name,
                    statement_name,
                    query_param_formats,
                    query_params,
                    result_value_formats,
                } => {
                    let portal = match session.get_prepared_statement(&statement_name) {
                        Some(statement) => {
                            if let Some(param_types) = statement.param_types() {
                                if param_types.len() != query_params.len() {
                                    let message = format!(
                                        "Bind message supplies {actual} parameters, but prepared statement \"{name}\" requires {expected}",
                                        name = statement_name,
                                        actual = query_params.len(),
                                        expected = param_types.len()
                                    );
                                    let x11: Vec<u8> = QueryError::protocol_violation(message).into();
                                    self.sender.lock().unwrap()
                                        .send(&x11)
                                        .expect("To Send Error to Client");
                                }

                                let mut param_values: Vec<ScalarValue> = vec![];
                                debug_assert!(
                                    query_params.len() == param_types.len() && query_params.len() == query_param_formats.len(),
                                    "encoded parameter values, their types and formats have to have same length"
                                );
                                for i in 0..query_params.len() {
                                    let raw_param = &query_params[i];
                                    let typ = param_types[i];
                                    let format = query_param_formats[i];
                                    match raw_param {
                                        None => param_values.push(ScalarValue::Null),
                                        Some(bytes) => {
                                            log::debug!("PG Type {:?}", typ);
                                            match decode(typ, format, &bytes) {
                                                Ok(param) => param_values.push(From::from(param)),
                                                Err(_error) => {
                                                    let x39: Vec<u8> = QueryError::invalid_parameter_value("").into();
                                                    self.sender.lock().unwrap()
                                                        .send(&x39)
                                                        .expect("To Send Error to Client");
                                                    return Err(ConflictableTransactionError::Abort);
                                                }
                                            }
                                        }
                                    }
                                }
                                Some(Portal::new(
                                    statement_name.clone(),
                                    query_analyzer.analyze(statement.query().unwrap()).unwrap(),
                                    result_value_formats.clone(),
                                    param_values,
                                    param_types.iter().map(From::from).collect::<Vec<SqlTypeFamily>>(),
                                ))
                            } else {
                                None
                            }
                        }
                        _ => None,
                    };
                    match portal {
                        Some(portal) => {
                            session.set_portal(portal_name.clone(), portal);
                            let x10: Vec<u8> = QueryEvent::BindComplete.into();
                            self.sender.lock().unwrap()
                                .send(&x10)
                                .expect("To Send Bind Complete Event");
                        }
                        None => {
                            let x9: Vec<u8> = QueryError::prepared_statement_does_not_exist(statement_name).into();
                            self.sender.lock().unwrap()
                                .send(&x9)
                                .expect("To Send Error to Client");
                        }
                    }
                    Ok(())
                }
                Request::DescribePortal { name } => {
                    match session.get_portal(&name) {
                        None => {
                            let x8: Vec<u8> = QueryError::portal_does_not_exist(name).into();
                            self.sender.lock().unwrap()
                                .send(&x8)
                                .expect("To Send Error to Client");
                        }
                        Some(_portal) => {
                            log::debug!("DESCRIBING PORTAL START");
                            let x7: Vec<u8> = QueryEvent::StatementDescription(vec![]).into();
                            self.sender.lock().unwrap()
                                .send(&x7)
                                .expect("To Send Statement Description to Client");
                            log::debug!("DESCRIBING PORTAL END");
                        }
                    }
                    Ok(())
                }
                Request::Execute {
                    portal_name,
                    max_rows: _max_rows,
                } => {
                    match session.get_portal(&portal_name) {
                        Some(portal) => match portal.stmt() {
                            UntypedQuery::Insert(insert) => {
                                log::debug!("INSERT UNTYPED VALUES {:?}", insert.values);
                                let typed_values = insert
                                    .values
                                    .into_iter()
                                    .map(|values| {
                                        values
                                            .into_iter()
                                            .map(|value| {
                                                value.map(|v| self.type_inference.infer_static(v, portal.param_types()))
                                            })
                                            .collect()
                                    })
                                    .collect::<Vec<Vec<Option<StaticTypedTree>>>>();
                                log::debug!("INSERT TYPED VALUES {:?}", typed_values);
                                let type_checked = typed_values
                                    .into_iter()
                                    .map(|values| {
                                        values
                                            .into_iter()
                                            .map(|value| value.map(|v| self.type_checker.check_static(v)))
                                            .collect()
                                    })
                                    .collect::<Vec<Vec<Option<StaticTypedTree>>>>();
                                log::debug!("INSERT TYPE CHECKED VALUES {:?}", type_checked);
                                let table_info = catalog
                                    .table_definition(insert.full_table_name.clone())
                                    .unwrap()
                                    .unwrap();
                                let table_columns = table_info.columns();
                                let mut type_coerced = vec![];
                                for checked in type_checked {
                                    let mut row = vec![];
                                    for (index, c) in checked.into_iter().enumerate() {
                                        row.push(
                                            c.map(|c| self.type_coercion.coerce_static(c, table_columns[index].sql_type())),
                                        );
                                    }
                                    type_coerced.push(row);
                                }
                                log::debug!("INSERT TYPE COERCED VALUES {:?}", type_coerced);
                                let query_result = match query_planner
                                    .plan(TypedQuery::Insert(TypedInsertQuery {
                                        full_table_name: insert.full_table_name,
                                        values: type_coerced,
                                    }))
                                    .execute(portal.param_values())
                                    .map(|r| { let r: QueryEvent = r.into(); r })
                                    .map(|r| { let r: Vec<u8> = r.into(); r })
                                    .map_err(|e| { let e: QueryError = e.into(); e })
                                    .map_err(|e| { let e: Vec<u8> = e.into(); e }) {
                                    Ok(ok) => ok,
                                    Err(err) => err,
                                };
                                self.sender.lock().unwrap().send(&query_result).expect("To Send to client");
                            }
                            UntypedQuery::Update(update) => {
                                let typed_values = update
                                    .assignments
                                    .into_iter()
                                    .map(|value| {
                                        value.map(|value| self.type_inference.infer_dynamic(value, portal.param_types()))
                                    })
                                    .collect::<Vec<Option<DynamicTypedTree>>>();
                                log::debug!("UPDATE TYPED VALUES - {:?}", typed_values);
                                let type_checked = typed_values
                                    .into_iter()
                                    .map(|value| value.map(|value| self.type_checker.check_dynamic(value)))
                                    .collect::<Vec<Option<DynamicTypedTree>>>();
                                log::debug!("UPDATE TYPE CHECKED VALUES - {:?}", type_checked);
                                let type_coerced = type_checked
                                    .into_iter()
                                    .map(|value| value.map(|value| self.type_coercion.coerce_dynamic(value)))
                                    .collect::<Vec<Option<DynamicTypedTree>>>();
                                log::debug!("UPDATE TYPE COERCED VALUES - {:?}", type_coerced);

                                log::debug!("UPDATE UNTYPED FILTER - {:?}", update.filter);
                                let typed_filter = update
                                    .filter
                                    .map(|value| self.type_inference.infer_dynamic(value, &[]));
                                log::debug!("UPDATE TYPED FILTER - {:?}", typed_filter);
                                let type_checked_filter = typed_filter
                                    .map(|value| self.type_checker.check_dynamic(value));
                                log::debug!("UPDATE TYPE CHECKED FILTER - {:?}", type_checked_filter);
                                let type_coerced_filter = type_checked_filter
                                    .map(|value| self.type_coercion.coerce_dynamic(value));
                                log::debug!("UPDATE TYPE COERCED FILTER - {:?}", type_coerced_filter);

                                let query_result = match query_planner
                                    .plan(TypedQuery::Update(TypedUpdateQuery {
                                        full_table_name: update.full_table_name,
                                        assignments: type_coerced,
                                        filter: type_coerced_filter
                                    }))
                                    .execute(portal.param_values())
                                    .map(|r| { let r: QueryEvent = r.into(); r })
                                    .map(|r| { let r: Vec<u8> = r.into(); r })
                                    .map_err(|e| { let e: QueryError = e.into(); e })
                                    .map_err(|e| { let e: Vec<u8> = e.into(); e }) {
                                    Ok(ok) => ok,
                                    Err(err) => err,
                                };
                                self.sender.lock().unwrap().send(&query_result).expect("To Send to client");
                            }
                            UntypedQuery::Select(select) => {
                                log::debug!("SELECT UNTYPED VALUES - {:?}", select.projection_items);
                                let typed_values = select
                                    .projection_items
                                    .into_iter()
                                    .map(|value| self.type_inference.infer_dynamic(value, &[]))
                                    .collect::<Vec<DynamicTypedTree>>();
                                log::debug!("SELECT TYPED VALUES - {:?}", typed_values);
                                let type_checked = typed_values
                                    .into_iter()
                                    .map(|value| self.type_checker.check_dynamic(value))
                                    .collect::<Vec<DynamicTypedTree>>();
                                log::debug!("SELECT TYPE CHECKED VALUES - {:?}", type_checked);
                                let type_coerced = type_checked
                                    .into_iter()
                                    .map(|value| self.type_coercion.coerce_dynamic(value))
                                    .collect::<Vec<DynamicTypedTree>>();
                                log::debug!("SELECT TYPE COERCED VALUES - {:?}", type_coerced);

                                log::debug!("SELECT UNTYPED FILTER - {:?}", select.filter);
                                let typed_filter = select
                                    .filter
                                    .map(|value| self.type_inference.infer_dynamic(value, &[]));
                                log::debug!("SELECT TYPED FILTER - {:?}", typed_filter);
                                let type_checked_filter = typed_filter
                                    .map(|value| self.type_checker.check_dynamic(value));
                                log::debug!("SELECT TYPE CHECKED FILTER - {:?}", type_checked_filter);
                                let type_coerced_filter = type_checked_filter
                                    .map(|value| self.type_coercion.coerce_dynamic(value));
                                log::debug!("SELECT TYPE COERCED FILTER - {:?}", type_coerced_filter);

                                let query_result = query_planner
                                    .plan(TypedQuery::Select(TypedSelectQuery {
                                        projection_items: type_coerced,
                                        full_table_name: select.full_table_name,
                                        filter: type_coerced_filter
                                    }))
                                    .execute(vec![])
                                    .map_err(|e| { let e: QueryError = e.into(); e })
                                    .map_err(|e| { let e: Vec<u8> = e.into(); e });
                                match query_result {
                                    Ok(QueryExecutionResult::Selected((desc, data))) => {
                                        let x5: Vec<u8> = QueryEvent::RowDescription(desc).into();
                                        self.sender.lock().unwrap()
                                            .send(&x5)
                                            .expect("To Send to client");
                                        let len = data.len();
                                        for row in data {
                                            let x6: Vec<u8> = QueryEvent::DataRow(
                                                row.into_iter().map(|scalar| scalar.as_text()).collect(),
                                            ).into();
                                            self.sender.lock().unwrap()
                                                .send(&x6)
                                                .expect("To Send to client");
                                        }
                                        let x4: Vec<u8> = QueryEvent::RecordsSelected(len).into();
                                        self.sender.lock().unwrap()
                                            .send(&x4)
                                            .expect("To Send to client");
                                    }
                                    Ok(_) => unreachable!(),
                                    Err(error) => {
                                        self.sender.lock().unwrap().send(&error).expect("To Send to client");
                                    }
                                }
                            }
                            UntypedQuery::Delete(delete) => {

                                log::debug!("DELETE UNTYPED FILTER - {:?}", delete.filter);
                                let typed_filter = delete
                                    .filter
                                    .map(|value| self.type_inference.infer_dynamic(value, &[]));
                                log::debug!("DELETE TYPED FILTER - {:?}", typed_filter);
                                let type_checked_filter = typed_filter
                                    .map(|value| self.type_checker.check_dynamic(value));
                                log::debug!("DELETE TYPE CHECKED FILTER - {:?}", type_checked_filter);
                                let type_coerced_filter = type_checked_filter
                                    .map(|value| self.type_coercion.coerce_dynamic(value));
                                log::debug!("DELETE TYPE COERCED FILTER - {:?}", type_coerced_filter);

                                let query_result = match query_planner
                                    .plan(TypedQuery::Delete(TypedDeleteQuery {
                                        full_table_name: delete.full_table_name,
                                        filter: type_coerced_filter
                                    }))
                                    .execute(vec![])
                                    .map(|r| { let r: QueryEvent = r.into(); r })
                                    .map(|r| { let r: Vec<u8> = r.into(); r })
                                    .map_err(|e| { let e: QueryError = e.into(); e })
                                    .map_err(|e| { let e: Vec<u8> = e.into(); e }) {
                                    Ok(ok) => ok,
                                    Err(err) => err,
                                };
                                self.sender.lock().unwrap().send(&query_result).expect("To Send to client");
                            }
                        },
                        None => {
                            let x3: Vec<u8> = QueryError::portal_does_not_exist(portal_name).into();
                            self.sender.lock().unwrap()
                                .send(&x3)
                                .expect("To Send Error to Client");
                        }
                    }
                    Ok(())
                }
                Request::CloseStatement { .. } => {
                    let x2: Vec<u8> = QueryEvent::QueryComplete.into();
                    self.sender.lock().unwrap()
                        .send(&x2)
                        .expect("To Send Query Complete to Client");
                    Ok(())
                }
                Request::ClosePortal { .. } => {
                    let x1: Vec<u8> = QueryEvent::QueryComplete.into();
                    self.sender.lock().unwrap()
                        .send(&x1)
                        .expect("To Send Query Complete to Client");
                    Ok(())
                }
                Request::Sync => {
                    let x: Vec<u8> = QueryEvent::QueryComplete.into();
                    self.sender.lock().unwrap()
                        .send(&x)
                        .expect("To Send Query Complete to Client");
                    Ok(())
                }
                Request::Flush => {
                    self.sender.lock().unwrap().flush().expect("Send All Buffered Messages to Client");
                    Ok(())
                }
                Request::Terminate => {
                    log::debug!("closing connection with client");
                    Err(ConflictableTransactionError::Abort)
                }
            };
            log::trace!("TRANSACTION END");
            result
        })
    }
}

#[cfg(test)]
mod tests;
