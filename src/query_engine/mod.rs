// Copyright 2020 - present Alex Dukhno
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

use crate::{
    connection::Sender,
    pg_model::{session::Session, statement::PreparedStatement, Command},
};
use catalog::{CatalogDefinition, Database};
use data_definition::ExecutionOutcome;
use data_manipulation::{
    DynamicTypedTree, QueryPlanResult, StaticTypedTree, TypedDeleteQuery, TypedInsertQuery, TypedQuery,
    TypedSelectQuery, TypedUpdateQuery, UntypedQuery,
};
use definition_planner::DefinitionPlanner;
use itertools::izip;
use postgres::{
    query_ast::{Expr, Statement, Value},
    query_parser::QueryParser,
    query_response::{QueryError, QueryEvent},
    wire_protocol::{ColumnMetadata, PgFormat, PgType},
};
use query_analyzer::QueryAnalyzer;
use query_planner::QueryPlanner;
use query_processing::{TypeChecker, TypeCoercion, TypeInference};
use std::{iter, sync::Arc};

unsafe impl<D: Database + CatalogDefinition> Send for QueryEngine<D> {}

unsafe impl<D: Database + CatalogDefinition> Sync for QueryEngine<D> {}

pub(crate) struct QueryEngine<D: Database + CatalogDefinition> {
    session: Session<Statement>,
    sender: Arc<dyn Sender>,
    query_parser: QueryParser,
    definition_planner: DefinitionPlanner<D>,
    query_analyzer: QueryAnalyzer<D>,
    type_inference: TypeInference,
    type_checker: TypeChecker,
    type_coercion: TypeCoercion,
    query_planner: QueryPlanner<D>,
    database: Arc<D>,
}

impl<D: Database + CatalogDefinition> QueryEngine<D> {
    pub(crate) fn new(sender: Arc<dyn Sender>, database: Arc<D>) -> QueryEngine<D> {
        QueryEngine {
            session: Session::default(),
            sender: sender.clone(),
            query_parser: QueryParser,
            definition_planner: DefinitionPlanner::new(database.clone()),
            query_analyzer: QueryAnalyzer::new(database.clone()),
            type_inference: TypeInference::default(),
            type_checker: TypeChecker,
            type_coercion: TypeCoercion,
            query_planner: QueryPlanner::new(database.clone()),
            database,
        }
    }

    pub(crate) fn execute(&mut self, command: Command) -> Result<(), ()> {
        match command {
            Command::Bind {
                portal_name,
                statement_name,
                param_formats,
                raw_params,
                result_formats,
            } => {
                match self.session.get_prepared_statement(&statement_name) {
                    Some(prepared_statement) => {
                        let param_types = prepared_statement.param_types();
                        if param_types.len() != raw_params.len() {
                            let message = format!(
                                "Bind message supplies {actual} parameters, but prepared statement \"{name}\" requires {expected}",
                                name = statement_name,
                                actual = raw_params.len(),
                                expected = param_types.len()
                            );
                            self.sender
                                .send(Err(QueryError::protocol_violation(message)))
                                .expect("To Send Error to Client");
                        }
                        match self.bind_prepared_statement(
                            &prepared_statement,
                            param_formats.as_ref(),
                            raw_params.as_ref(),
                            result_formats.as_ref(),
                        ) {
                            Ok((new_stmt, result_formats)) => {
                                self.session.set_portal(
                                    portal_name,
                                    statement_name.to_owned(),
                                    new_stmt,
                                    result_formats,
                                );
                                self.sender
                                    .send(Ok(QueryEvent::BindComplete))
                                    .expect("To Send Bind Complete Event");
                            }
                            Err(error) => log::error!("{:?}", error),
                        }
                    }
                    None => {
                        self.sender
                            .send(Err(QueryError::prepared_statement_does_not_exist(statement_name)))
                            .expect("To Send Error to Client");
                    }
                }
                Ok(())
            }
            Command::Continue => {
                self.sender
                    .send(Ok(QueryEvent::QueryComplete))
                    .expect("To Send Query Complete to Client");
                Ok(())
            }
            Command::DescribeStatement { name } => {
                match self.session.get_prepared_statement(&name) {
                    Some(stmt) => {
                        self.sender
                            .send(Ok(QueryEvent::StatementParameters(stmt.param_types().to_vec())))
                            .expect("To Send Statement Parameters to Client");
                        self.sender
                            .send(Ok(QueryEvent::StatementDescription(stmt.description().to_vec())))
                            .expect("To Send Statement Description to Client");
                    }
                    None => {
                        self.sender
                            .send(Err(QueryError::prepared_statement_does_not_exist(name)))
                            .expect("To Send Error to Client");
                    }
                }
                Ok(())
            }
            Command::DescribePortal { name } => {
                match self.session.get_portal(&name) {
                    None => {
                        self.sender
                            .send(Err(QueryError::portal_does_not_exist(name)))
                            .expect("To Send Error to Client");
                    }
                    Some(_portal) => {
                        log::debug!("DESCRIBING PORTAL START");
                        self.sender
                            .send(Ok(QueryEvent::StatementDescription(vec![])))
                            .expect("To Send Statement Description to Client");
                        log::debug!("DESCRIBING PORTAL END");
                    }
                }
                Ok(())
            }
            // TODO: Parameter `max_rows` should be handled.
            Command::Execute {
                portal_name,
                max_rows: _max_rows,
            } => {
                match self.session.get_portal(&portal_name) {
                    Some(portal) => {
                        self.sender
                            .send(Err(QueryError::syntax_error(portal.stmt())))
                            .expect("To Send Error to Client");
                    }
                    None => {
                        self.sender
                            .send(Err(QueryError::portal_does_not_exist(portal_name)))
                            .expect("To Send Error to Client");
                    }
                }
                Ok(())
            }
            Command::Flush => {
                self.sender.flush().expect("Send All Buffered Messages to Client");
                Ok(())
            }
            Command::Parse {
                statement_name,
                sql,
                param_types,
            } => {
                match self.query_parser.parse(&sql) {
                    Ok(mut statements) => {
                        let statement = statements.pop().expect("single statement");
                        match self.create_prepared_statement(statement_name, statement, param_types) {
                            Ok(()) => {
                                self.sender.send(Ok(QueryEvent::ParseComplete)).expect("To Send Result");
                            }
                            Err(error) => self.sender.send(Err(error)).expect("To Send Result"),
                        }
                    }
                    Err(parser_error) => {
                        self.sender
                            .send(Err(QueryError::syntax_error(parser_error)))
                            .expect("To Send Syntax Error Event");
                    }
                }
                Ok(())
            }
            Command::Query { sql } => {
                match self.query_parser.parse(&sql) {
                    Ok(mut statements) => match statements.pop().expect("single query") {
                        // Statement::Prepare {
                        //     name,
                        //     data_types,
                        //     statement,
                        // } => {
                        //     let Ident { value: name, .. } = name;
                        //     let mut pg_types = vec![];
                        //     for data_type in data_types {
                        //         match SqlType::try_from(&data_type) {
                        //             Ok(sql_type) => pg_types.push(Some((&sql_type).into())),
                        //             Err(_) => {
                        //                 self.sender
                        //                     .send(Err(QueryError::type_does_not_exist(data_type)))
                        //                     .expect("To Send Error to Client");
                        //                 self.sender
                        //                     .send(Ok(QueryEvent::QueryComplete))
                        //                     .expect("To Send Error to Client");
                        //                 return Ok(());
                        //             }
                        //         }
                        //     }
                        //     match self.create_prepared_statement(name, *statement, pg_types) {
                        //         Ok(()) => {
                        //             self.sender
                        //                 .send(Ok(QueryEvent::StatementPrepared))
                        //                 .expect("To Send Result");
                        //         }
                        //         Err(error) => {
                        //             self.sender.send(Err(error)).expect("To Send Result");
                        //             self.sender
                        //                 .send(Ok(QueryEvent::QueryComplete))
                        //                 .expect("To Send Error to Client");
                        //             return Ok(());
                        //         }
                        //     }
                        // }
                        // Statement::Execute { name, parameters } => {
                        //     let Ident { value: name, .. } = name;
                        //     match self.session.get_prepared_statement(&name) {
                        //         Some(prepared_statement) => {
                        //             let param_types = prepared_statement.param_types();
                        //             if param_types.len() != parameters.len() {
                        //                 let message = format!(
                        //                     "Bind message supplies {actual} parameters, but prepared statement \"{name}\" requires {expected}",
                        //                     name = name,
                        //                     actual = parameters.len(),
                        //                     expected = param_types.len()
                        //                 );
                        //                 self.sender
                        //                     .send(Err(QueryError::protocol_violation(message)))
                        //                     .expect("To Send Error to Client");
                        //             }
                        //             self.sender
                        //                 .send(Err(QueryError::syntax_error(prepared_statement.stmt())))
                        //                 .expect("To Send Error to Client");
                        //         }
                        //         None => {
                        //             self.sender
                        //                 .send(Err(QueryError::prepared_statement_does_not_exist(name)))
                        //                 .expect("To Send Error to Client");
                        //         }
                        //     }
                        // }
                        // Statement::Deallocate { name, .. } => {
                        //     let Ident { value: name, .. } = name;
                        //     self.session.remove_prepared_statement(&name);
                        //     self.sender
                        //         .send(Ok(QueryEvent::StatementDeallocated))
                        //         .expect("To Send Statement Deallocated Event");
                        // }
                        Statement::DDL(definition) => match self.definition_planner.plan(definition) {
                            Ok(schema_change) => {
                                log::debug!("SCHEMA CHANGE - {:?}", schema_change);
                                let query_result = match self.database.execute(schema_change) {
                                    Ok(ExecutionOutcome::SchemaCreated) => Ok(QueryEvent::SchemaCreated),
                                    Ok(ExecutionOutcome::SchemaDropped) => Ok(QueryEvent::SchemaDropped),
                                    Ok(ExecutionOutcome::TableCreated) => Ok(QueryEvent::TableCreated),
                                    Ok(ExecutionOutcome::TableDropped) => Ok(QueryEvent::TableDropped),
                                    Ok(ExecutionOutcome::IndexCreated) => Ok(QueryEvent::IndexCreated),
                                    Err(error) => Err(error.into()),
                                };
                                self.sender.send(query_result).expect("To Send Result to Client");
                            }
                            Err(error) => self.sender.send(Err(error.into())).expect("To Send Result to Client"),
                        },
                        Statement::DML(manipulation) => match self.query_analyzer.analyze(manipulation) {
                            Ok(UntypedQuery::Delete(delete)) => {
                                let query_result = self
                                    .query_planner
                                    .plan(TypedQuery::Delete(TypedDeleteQuery {
                                        full_table_name: delete.full_table_name,
                                    }))
                                    .execute()
                                    .map(Into::into)
                                    .map_err(Into::into);
                                self.sender.send(query_result).expect("To Send to client");
                            }
                            Ok(UntypedQuery::Update(update)) => {
                                let typed_values = update
                                    .assignments
                                    .into_iter()
                                    .map(|value| value.map(|value| self.type_inference.infer_dynamic(value)))
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
                                let query_result = self
                                    .query_planner
                                    .plan(TypedQuery::Update(TypedUpdateQuery {
                                        full_table_name: update.full_table_name,
                                        assignments: type_coerced,
                                    }))
                                    .execute()
                                    .map(Into::into)
                                    .map_err(Into::into);
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
                                            .map(|value| value.map(|v| self.type_inference.infer_static(v)))
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
                                let table_info = self
                                    .database
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
                                let query_result = self
                                    .query_planner
                                    .plan(TypedQuery::Insert(TypedInsertQuery {
                                        full_table_name: insert.full_table_name,
                                        values: type_coerced,
                                    }))
                                    .execute()
                                    .map(Into::into)
                                    .map_err(Into::into);
                                self.sender.send(query_result).expect("To Send to client");
                            }
                            Ok(UntypedQuery::Select(select)) => {
                                log::debug!("SELECT UNTYPED VALUES - {:?}", select.projection_items);
                                let typed_values = select
                                    .projection_items
                                    .into_iter()
                                    .map(|value| self.type_inference.infer_dynamic(value))
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
                                let query_result = self
                                    .query_planner
                                    .plan(TypedQuery::Select(TypedSelectQuery {
                                        projection_items: type_coerced,
                                        full_table_name: select.full_table_name,
                                    }))
                                    .execute()
                                    .map_err(Into::into);
                                match query_result {
                                    Ok(QueryPlanResult::Selected((desc, data))) => {
                                        self.sender
                                            .send(Ok(QueryEvent::RowDescription(
                                                desc.into_iter()
                                                    .map(|col_def| {
                                                        let pg_type: PgType = (&col_def.sql_type()).into();
                                                        ColumnMetadata::new(col_def.name(), pg_type)
                                                    })
                                                    .collect(),
                                            )))
                                            .expect("To Send to client");
                                        let len = data.len();
                                        for row in data {
                                            self.sender
                                                .send(Ok(QueryEvent::DataRow(
                                                    row.into_iter().map(|scalar| scalar.as_text()).collect(),
                                                )))
                                                .expect("To Send to client");
                                        }
                                        self.sender
                                            .send(Ok(QueryEvent::RecordsSelected(len)))
                                            .expect("To Send to client");
                                    }
                                    Ok(_) => unreachable!(),
                                    Err(error) => {
                                        self.sender.send(Err(error)).expect("To Send to client");
                                    }
                                }
                            }
                            Err(error) => {
                                self.sender.send(Err(error.into())).expect("To Send Error to Client");
                            }
                        },
                        Statement::Config(_) => {
                            // sending ok to the client to proceed with other requests
                            self.sender
                                .send(Ok(QueryEvent::VariableSet))
                                .expect("To Send Result to Client");
                        }
                    },
                    Err(parser_error) => {
                        self.sender
                            .send(Err(QueryError::syntax_error(parser_error)))
                            .expect("To Send ParseComplete Event");
                    }
                }
                self.sender
                    .send(Ok(QueryEvent::QueryComplete))
                    .expect("To Send Query Complete to Client");
                Ok(())
            }
            Command::Terminate => {
                log::debug!("closing connection with client");
                Err(())
            }
        }
    }

    fn bind_prepared_statement(
        &self,
        prepared_statement: &PreparedStatement<Statement>,
        param_formats: &[PgFormat],
        raw_params: &[Option<Vec<u8>>],
        _result_formats: &[PgFormat],
    ) -> Result<(Statement, Vec<PgFormat>), ()> {
        log::debug!("prepared statement -  {:#?}", prepared_statement);
        let param_formats = match pad_formats(param_formats, raw_params.len()) {
            Ok(param_formats) => param_formats,
            Err(msg) => {
                self.sender
                    .send(Err(QueryError::protocol_violation(msg)))
                    .expect("To Send Error to Client");
                return Err(());
            }
        };

        let mut params: Vec<Expr> = vec![];
        for (raw_param, typ, format) in izip!(raw_params, prepared_statement.param_types(), param_formats) {
            match raw_param {
                None => params.push(Expr::Value(Value::Null)),
                Some(bytes) => {
                    log::debug!("PG Type {:?}", typ);
                    match typ.decode(&format, &bytes) {
                        Ok(param) => params.push(value_to_expr(param)),
                        Err(msg) => {
                            self.sender
                                .send(Err(QueryError::invalid_parameter_value(msg)))
                                .expect("To Send Error to Client");
                            return Err(());
                        }
                    }
                }
            }
        }

        self.sender
            .send(Err(QueryError::syntax_error(prepared_statement.stmt())))
            .expect("To Send Error to Client");

        Err(())
    }

    fn create_prepared_statement(
        &mut self,
        _statement_name: String,
        statement: Statement,
        _param_types: Vec<Option<PgType>>,
    ) -> Result<(), QueryError> {
        Err(QueryError::syntax_error(statement))
    }
}

fn pad_formats(formats: &[PgFormat], param_len: usize) -> Result<Vec<PgFormat>, String> {
    match (formats.len(), param_len) {
        (0, n) => Ok(vec![PgFormat::Text; n]),
        (1, n) => Ok(iter::repeat(formats[0]).take(n).collect()),
        (m, n) if m == n => Ok(formats.to_vec()),
        (m, n) => Err(format!("expected {} field format specifiers, but got {}", m, n)),
    }
}

fn value_to_expr(value: postgres::wire_protocol::Value) -> Expr {
    match value {
        postgres::wire_protocol::Value::Null => Expr::Value(Value::Null),
        postgres::wire_protocol::Value::True => Expr::Value(Value::Boolean(true)),
        postgres::wire_protocol::Value::False => Expr::Value(Value::Boolean(false)),
        postgres::wire_protocol::Value::Int16(i) => Expr::Value(Value::Int(i as i32)),
        postgres::wire_protocol::Value::Int32(i) => Expr::Value(Value::Int(i)),
        postgres::wire_protocol::Value::Int64(i) => Expr::Value(Value::Int(i as i32)),
        postgres::wire_protocol::Value::String(s) => Expr::Value(Value::String(s)),
    }
}

#[cfg(test)]
mod tests;
