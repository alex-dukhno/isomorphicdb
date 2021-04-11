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
use catalog::CatalogHandler;
use data_definition::ExecutionOutcome;
use data_manipulation::{
    DynamicTypedTree, QueryPlanResult, StaticTypedTree, TypedDeleteQuery, TypedInsertQuery, TypedQuery,
    TypedSelectQuery, TypedUpdateQuery, UntypedQuery,
};
use definition_planner::DefinitionPlanner;
use entities::{ColumnDef, SqlType, SqlTypeFamily};
use postgres::{
    query_ast::{Extended, Statement},
    query_parser::QueryParser,
    query_response::{QueryError, QueryEvent},
    wire_protocol::{
        payload::{BackendMessage, ColumnMetadata, PgType},
        CommandMessage, Sender,
    },
};
use query_analyzer::QueryAnalyzer;
use query_planner::QueryPlanner;
use query_processing::{TypeChecker, TypeCoercion, TypeInference};
use scalar::ScalarValue;
use std::{
    rc::Rc,
    sync::{Arc, Mutex},
};
use storage::{ConflictableTransactionError, Database, TransactionResult};

pub(crate) struct QueryEngine {
    session: Arc<Mutex<Session>>,
    sender: Arc<dyn Sender>,
    type_inference: TypeInference,
    type_checker: TypeChecker,
    type_coercion: TypeCoercion,
    database: Database,
}

impl QueryEngine {
    pub(crate) fn new(sender: Arc<dyn Sender>, database: Database) -> QueryEngine {
        QueryEngine {
            session: Arc::default(),
            sender,
            type_inference: TypeInference::default(),
            type_checker: TypeChecker,
            type_coercion: TypeCoercion,
            database,
        }
    }

    pub(crate) fn execute(&mut self, command: CommandMessage) -> TransactionResult<()> {
        let inner = Rc::new(command);
        let mut session = self.session.lock().unwrap();
        self.database.transaction(|db| {
            log::trace!("TRANSACTION START");
            log::trace!("{:?}", db.table("DEFINITION_SCHEMA.TABLES"));
            let query_analyzer = QueryAnalyzer::from(db.clone());
            let definition_planner = DefinitionPlanner::from(db.clone());
            let query_planner = QueryPlanner::from(db.clone());
            let catalog = CatalogHandler::from(db.clone());
            let query_parser = QueryParser;
            let result = match &*inner {
                CommandMessage::Query { sql } => {
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
                                    self.sender
                                        .send(QueryEvent::StatementPrepared.into())
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
                                                    .map(|r| { let r: BackendMessage = r.into(); r })
                                                    .map_err(|e| { let e: QueryError = e.into(); e })
                                                    .map_err(|e| { let e: BackendMessage = e.into(); e }) {
                                                    Ok(ok) => ok,
                                                    Err(err) => err,
                                                };
                                                self.sender.send(query_result).expect("To Send to client");
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
                                                    .map(|r| { let r: BackendMessage = r.into(); r })
                                                    .map_err(|e| { let e: QueryError = e.into(); e })
                                                    .map_err(|e| { let e: BackendMessage = e.into(); e }) {
                                                    Ok(ok) => ok,
                                                    Err(err) => err,
                                                };
                                                self.sender.send(query_result).expect("To Send to client");
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
                                                    .map_err(|e| { let e: BackendMessage = e.into(); e });
                                                match query_result {
                                                    Ok(QueryPlanResult::Selected((desc, data))) => {
                                                        self.sender
                                                            .send(QueryEvent::RowDescription(
                                                                desc.into_iter()
                                                                    .map(|col_def| {
                                                                        let pg_type: PgType = (&col_def.sql_type()).into();
                                                                        ColumnMetadata::new(col_def.name(), pg_type)
                                                                    })
                                                                    .collect(),
                                                            ).into())
                                                            .expect("To Send to client");
                                                        let len = data.len();
                                                        for row in data {
                                                            self.sender
                                                                .send(QueryEvent::DataRow(
                                                                    row.into_iter()
                                                                        .map(|scalar| scalar.as_text())
                                                                        .collect(),
                                                                ).into())
                                                                .expect("To Send to client");
                                                        }
                                                        self.sender
                                                            .send(QueryEvent::RecordsSelected(len).into())
                                                            .expect("To Send to client");
                                                    }
                                                    Ok(_) => unreachable!(),
                                                    Err(error) => {
                                                        self.sender.send(error).expect("To Send to client");
                                                    }
                                                }
                                            }
                                            UntypedQuery::Delete(delete) => {
                                                let query_result = match query_planner
                                                    .plan(TypedQuery::Delete(TypedDeleteQuery {
                                                        full_table_name: delete.full_table_name,
                                                    }))
                                                    .execute(param_values)
                                                    .map(|r| { let r: QueryEvent = r.into(); r })
                                                    .map(|r| { let r: BackendMessage = r.into(); r })
                                                    .map_err(|e| { let e: QueryError = e.into(); e })
                                                    .map_err(|e| { let e: BackendMessage = e.into(); e }) {
                                                    Ok(ok) => ok,
                                                    Err(err) => err,
                                                };
                                                self.sender.send(query_result).expect("To Send to client");
                                            }
                                        },
                                        None => {
                                            self.sender
                                                .send(QueryError::prepared_statement_does_not_exist(name).into())
                                                .expect("To Send Error to Client");
                                        }
                                    }
                                }
                                Extended::Deallocate { name } => {
                                    session.remove_portal(&name);
                                    self.sender
                                        .send(QueryEvent::StatementDeallocated.into())
                                        .expect("To Send Statement Deallocated Event");
                                }
                            },
                            Statement::Definition(definition) => match definition_planner.plan(definition) {
                                Ok(schema_change) => {
                                    log::debug!("SCHEMA CHANGE - {:?}", schema_change);
                                    let query_result = match catalog.apply(schema_change) {
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
                                    self.sender.send(query_result).expect("To Send Result to Client");
                                }
                                Err(error) => {
                                    let error: QueryError = error.into();
                                    self.sender.send(error.into()).expect("To Send Result to Client")
                                },
                            },
                            Statement::Query(query) => match query_analyzer.analyze(query) {
                                Ok(UntypedQuery::Delete(delete)) => {
                                    let query_result = match query_planner
                                        .plan(TypedQuery::Delete(TypedDeleteQuery {
                                            full_table_name: delete.full_table_name,
                                        }))
                                        .execute(vec![])
                                        .map(|r| { let r: QueryEvent = r.into(); r })
                                        .map(|r| { let r: BackendMessage = r.into(); r })
                                        .map_err(|e| { let e: QueryError = e.into(); e })
                                        .map_err(|e| { let e: BackendMessage = e.into(); e }) {
                                        Ok(ok) => ok,
                                        Err(err) => err,
                                    };
                                    self.sender.send(query_result).expect("To Send to client");
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
                                        .map(|r| { let r: BackendMessage = r.into(); r })
                                        .map_err(|e| { let e: QueryError = e.into(); e })
                                        .map_err(|e| { let e: BackendMessage = e.into(); e }) {
                                        Ok(ok) => ok,
                                        Err(err) => err,
                                    };
                                    self.sender.send(query_result).expect("To Send to client");
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
                                        .map(|r| { let r: BackendMessage = r.into(); r })
                                        .map_err(|e| { let e: QueryError = e.into(); e })
                                        .map_err(|e| { let e: BackendMessage = e.into(); e }) {
                                        Ok(ok) => ok,
                                        Err(err) => err,
                                    };
                                    self.sender.send(query_result).expect("To Send to client");
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
                                        .map_err(|e| { let e: BackendMessage = e.into(); e });
                                    match query_result {
                                        Ok(QueryPlanResult::Selected((desc, data))) => {
                                            self.sender
                                                .send(QueryEvent::RowDescription(
                                                    desc.into_iter()
                                                        .map(|col_def| {
                                                            let pg_type: PgType = (&col_def.sql_type()).into();
                                                            ColumnMetadata::new(col_def.name(), pg_type)
                                                        })
                                                        .collect(),
                                                ).into())
                                                .expect("To Send to client");
                                            let len = data.len();
                                            for row in data {
                                                self.sender
                                                    .send(QueryEvent::DataRow(
                                                        row.into_iter().map(|scalar| scalar.as_text()).collect(),
                                                    ).into())
                                                    .expect("To Send to client");
                                            }
                                            self.sender
                                                .send(QueryEvent::RecordsSelected(len).into())
                                                .expect("To Send to client");
                                        }
                                        Ok(_) => unreachable!(),
                                        Err(error) => {
                                            self.sender.send(error).expect("To Send to client");
                                        }
                                    }
                                }
                                Err(error) => {
                                    let error: QueryError = error.into();
                                    self.sender.send(error.into()).expect("To Send Error to Client");
                                }
                            },
                            Statement::Config(_) => {
                                // sending ok to the client to proceed with other requests
                                self.sender
                                    .send(QueryEvent::VariableSet.into())
                                    .expect("To Send Result to Client");
                            }
                        },
                        Err(parser_error) => {
                            self.sender
                                .send(QueryError::syntax_error(parser_error).into())
                                .expect("To Send ParseComplete Event");
                        }
                    }
                    self.sender
                        .send(QueryEvent::QueryComplete.into())
                        .expect("To Send Query Complete to Client");
                    Ok(())
                }
                CommandMessage::Parse {
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
                                            .filter(|o| o.is_some())
                                            .map(|o| o.unwrap())
                                            .collect(),
                                    );
                                    self.sender.send(QueryEvent::ParseComplete.into()).expect("To Send Result");
                                }
                                other => self
                                    .sender
                                    .send(QueryError::syntax_error(format!("{:?}", other)).into())
                                    .expect("To Send Result"),
                            },
                            Err(parser_error) => {
                                self.sender
                                    .send(QueryError::syntax_error(parser_error).into())
                                    .expect("To Send Syntax Error Event");
                            }
                        },
                        _ => match query_parser.parse(&sql) {
                            Ok(mut statements) => match statements.pop() {
                                Some(Statement::Query(query)) => {
                                    if param_types.is_empty() || param_types.iter().all(Option::is_some) {
                                        let mut prep = PreparedStatement::parsed(sql.clone(), query.clone());
                                        prep.parsed_with_params(
                                            query,
                                            param_types.iter().map(|o| o.unwrap()).collect(),
                                        );
                                        session.set_prepared_statement(statement_name.clone(), prep);
                                    } else {
                                        session.set_prepared_statement(
                                            statement_name.clone(),
                                            PreparedStatement::parsed(sql.clone(), query),
                                        );
                                    }
                                    self.sender.send(QueryEvent::ParseComplete.into()).expect("To Send Result");
                                }
                                other => self
                                    .sender
                                    .send(QueryError::syntax_error(format!("{:?}", other)).into())
                                    .expect("To Send Result"),
                            },
                            Err(parser_error) => {
                                self.sender
                                    .send(QueryError::syntax_error(parser_error).into())
                                    .expect("To Send Syntax Error Event");
                            }
                        },
                    }
                    Ok(())
                }
                CommandMessage::DescribeStatement { name } => {
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
                                        .collect::<Vec<PgType>>();
                                    self.sender
                                        .send(QueryEvent::StatementParameters(param_types.to_vec()).into())
                                        .expect("To Send Statement Parameters to Client");
                                    self.sender
                                        .send(QueryEvent::StatementDescription(vec![]).into())
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
                                        .collect::<Vec<PgType>>();
                                    self.sender
                                        .send(QueryEvent::StatementParameters(param_types.to_vec()).into())
                                        .expect("To Send Statement Parameters to Client");
                                    self.sender
                                        .send(QueryEvent::StatementDescription(vec![]).into())
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
                                        .collect::<Vec<(String, PgType)>>();
                                    self.sender
                                        .send(QueryEvent::StatementParameters(vec![]).into())
                                        .expect("To Send Statement Parameters to Client");
                                    self.sender
                                        .send(QueryEvent::StatementDescription(return_types).into())
                                        .expect("To Send Statement Description to Client");
                                    statement.described(UntypedQuery::Select(select), vec![]);
                                }
                                _ => {
                                    self.sender
                                        .send(QueryError::prepared_statement_does_not_exist(name).into())
                                        .expect("To Send Error to Client");
                                }
                            },
                            Some(param_types) => match query_analyzer.analyze(statement.query().unwrap()) {
                                Ok(UntypedQuery::Insert(_insert)) => {
                                    self.sender
                                        .send(QueryEvent::StatementParameters(param_types.to_vec()).into())
                                        .expect("To Send Statement Parameters to Client");
                                    self.sender
                                        .send(QueryEvent::StatementDescription(vec![]).into())
                                        .expect("To Send Statement Description to Client");
                                }
                                Ok(UntypedQuery::Update(_update)) => {
                                    self.sender
                                        .send(QueryEvent::StatementParameters(param_types.to_vec()).into())
                                        .expect("To Send Statement Parameters to Client");
                                    self.sender
                                        .send(QueryEvent::StatementDescription(vec![]).into())
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
                                        .collect::<Vec<(String, PgType)>>();
                                    self.sender
                                        .send(QueryEvent::StatementParameters(param_types.to_vec()).into())
                                        .expect("To Send Statement Parameters to Client");
                                    self.sender
                                        .send(QueryEvent::StatementDescription(return_types).into())
                                        .expect("To Send Statement Description to Client");
                                }
                                _ => {
                                    self.sender
                                        .send(QueryError::prepared_statement_does_not_exist(name).into())
                                        .expect("To Send Error to Client");
                                }
                            },
                        },
                        other => {
                            log::debug!("STMT {:?} associated with {:?} key", other, name);
                            self.sender
                                .send(QueryError::prepared_statement_does_not_exist(name).into())
                                .expect("To Send Error to Client");
                        }
                    }
                    Ok(())
                }
                CommandMessage::Bind {
                    portal_name,
                    statement_name,
                    param_formats,
                    raw_params,
                    result_formats,
                } => {
                    let portal = match session.get_prepared_statement(&statement_name) {
                        Some(statement) => {
                            if let Some(param_types) = statement.param_types() {
                                if param_types.len() != raw_params.len() {
                                    let message = format!(
                                        "Bind message supplies {actual} parameters, but prepared statement \"{name}\" requires {expected}",
                                        name = statement_name,
                                        actual = raw_params.len(),
                                        expected = param_types.len()
                                    );
                                    self.sender
                                        .send(QueryError::protocol_violation(message).into())
                                        .expect("To Send Error to Client");
                                }

                                let mut param_values: Vec<ScalarValue> = vec![];
                                debug_assert!(
                                    raw_params.len() == param_types.len() && raw_params.len() == param_formats.len(),
                                    "encoded parameter values, their types and formats have to have same length"
                                );
                                for i in 0..raw_params.len() {
                                    let raw_param = &raw_params[i];
                                    let typ = param_types[i];
                                    let format = param_formats[i];
                                    match raw_param {
                                        None => param_values.push(ScalarValue::Null),
                                        Some(bytes) => {
                                            log::debug!("PG Type {:?}", typ);
                                            match typ.decode(&format, &bytes) {
                                                Ok(param) => param_values.push(From::from(param)),
                                                Err(error) => {
                                                    self.sender
                                                        .send(QueryError::invalid_parameter_value(error).into())
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
                                    result_formats.clone(),
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
                            self.sender
                                .send(QueryEvent::BindComplete.into())
                                .expect("To Send Bind Complete Event");
                        }
                        None => {
                            self.sender
                                .send(QueryError::prepared_statement_does_not_exist(statement_name).into())
                                .expect("To Send Error to Client");
                        }
                    }
                    Ok(())
                }
                CommandMessage::DescribePortal { name } => {
                    match session.get_portal(&name) {
                        None => {
                            self.sender
                                .send(QueryError::portal_does_not_exist(name).into())
                                .expect("To Send Error to Client");
                        }
                        Some(_portal) => {
                            log::debug!("DESCRIBING PORTAL START");
                            self.sender
                                .send(QueryEvent::StatementDescription(vec![]).into())
                                .expect("To Send Statement Description to Client");
                            log::debug!("DESCRIBING PORTAL END");
                        }
                    }
                    Ok(())
                }
                CommandMessage::Execute {
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
                                    .map(|r| { let r: BackendMessage = r.into(); r })
                                    .map_err(|e| { let e: QueryError = e.into(); e })
                                    .map_err(|e| { let e: BackendMessage = e.into(); e }) {
                                    Ok(ok) => ok,
                                    Err(err) => err,
                                };
                                self.sender.send(query_result).expect("To Send to client");
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
                                    .map(|r| { let r: BackendMessage = r.into(); r })
                                    .map_err(|e| { let e: QueryError = e.into(); e })
                                    .map_err(|e| { let e: BackendMessage = e.into(); e }) {
                                    Ok(ok) => ok,
                                    Err(err) => err,
                                };
                                self.sender.send(query_result).expect("To Send to client");
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
                                    .map_err(|e| { let e: BackendMessage = e.into(); e });
                                match query_result {
                                    Ok(QueryPlanResult::Selected((desc, data))) => {
                                        self.sender
                                            .send(QueryEvent::RowDescription(
                                                desc.into_iter()
                                                    .map(|col_def| {
                                                        let pg_type: PgType = (&col_def.sql_type()).into();
                                                        ColumnMetadata::new(col_def.name(), pg_type)
                                                    })
                                                    .collect(),
                                            ).into())
                                            .expect("To Send to client");
                                        let len = data.len();
                                        for row in data {
                                            self.sender
                                                .send(QueryEvent::DataRow(
                                                    row.into_iter().map(|scalar| scalar.as_text()).collect(),
                                                ).into())
                                                .expect("To Send to client");
                                        }
                                        self.sender
                                            .send(QueryEvent::RecordsSelected(len).into())
                                            .expect("To Send to client");
                                    }
                                    Ok(_) => unreachable!(),
                                    Err(error) => {
                                        self.sender.send(error).expect("To Send to client");
                                    }
                                }
                            }
                            UntypedQuery::Delete(delete) => {
                                let query_result = match query_planner
                                    .plan(TypedQuery::Delete(TypedDeleteQuery {
                                        full_table_name: delete.full_table_name,
                                    }))
                                    .execute(vec![])
                                    .map(|r| { let r: QueryEvent = r.into(); r })
                                    .map(|r| { let r: BackendMessage = r.into(); r })
                                    .map_err(|e| { let e: QueryError = e.into(); e })
                                    .map_err(|e| { let e: BackendMessage = e.into(); e }) {
                                    Ok(ok) => ok,
                                    Err(err) => err,
                                };
                                self.sender.send(query_result).expect("To Send to client");
                            }
                        },
                        None => {
                            self.sender
                                .send(QueryError::portal_does_not_exist(portal_name).into())
                                .expect("To Send Error to Client");
                        }
                    }
                    Ok(())
                }
                CommandMessage::CloseStatement { .. } => {
                    self.sender
                        .send(QueryEvent::QueryComplete.into())
                        .expect("To Send Query Complete to Client");
                    Ok(())
                }
                CommandMessage::ClosePortal { .. } => {
                    self.sender
                        .send(QueryEvent::QueryComplete.into())
                        .expect("To Send Query Complete to Client");
                    Ok(())
                }
                CommandMessage::Sync => {
                    self.sender
                        .send(QueryEvent::QueryComplete.into())
                        .expect("To Send Query Complete to Client");
                    Ok(())
                }
                CommandMessage::Flush => {
                    self.sender.flush().expect("Send All Buffered Messages to Client");
                    Ok(())
                }
                CommandMessage::Terminate => {
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
