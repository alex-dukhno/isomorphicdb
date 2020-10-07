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

use binder::ParamBinder;
use data_manager::DataManager;
use itertools::izip;
use metadata::{DataDefinition, MetadataView};
use parser::QueryParser;
use plan::{Plan, SelectInput};
use protocol::{
    pgsql_types::{PostgreSqlFormat, PostgreSqlValue},
    results::{Description, QueryError, QueryEvent},
    session::Session,
    statement::PreparedStatement,
    Command, Sender,
};
use query_executor::QueryExecutor;
use query_planner::QueryPlanner;
use sqlparser::ast::Statement;
use std::{iter, ops::Deref, sync::Arc};

pub(crate) struct QueryEngine {
    session: Session<Statement>,
    sender: Arc<dyn Sender>,
    data_manager: Arc<DataManager>,
    query_parser: QueryParser,
    param_binder: ParamBinder,
    query_planner: QueryPlanner,
    query_executor: QueryExecutor,
}

impl QueryEngine {
    pub(crate) fn new(
        sender: Arc<dyn Sender>,
        metadata: Arc<DataDefinition>,
        data_manager: Arc<DataManager>,
    ) -> QueryEngine {
        QueryEngine {
            session: Session::default(),
            sender: sender.clone(),
            data_manager: data_manager.clone(),
            query_parser: QueryParser::default(),
            param_binder: ParamBinder,
            query_planner: QueryPlanner::new(metadata, sender.clone()),
            query_executor: QueryExecutor::new(data_manager, sender),
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
                        match self.bind_prepared_statement_to_portal(
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
                                    .expect("To Send BindComplete Event");
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
            Command::Continue => Ok(()),
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
            // TODO: Parameter `max_rows` should be handled.
            Command::Execute {
                portal_name,
                max_rows: _max_rows,
            } => {
                match self.session.get_portal(&portal_name) {
                    Some(portal) => {
                        if let Ok(plan) = self.query_planner.plan(portal.stmt()) {
                            self.query_executor.execute(plan);
                        }
                    }
                    None => {
                        self.sender
                            .send(Err(QueryError::portal_does_not_exist(portal_name)))
                            .expect("To Send Error to Client");
                    }
                }
                self.sender
                    .send(Ok(QueryEvent::QueryComplete))
                    .expect("To Send Error to Client");
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
                        match self.query_planner.plan(&statement) {
                            Ok(plan) => match plan {
                                Plan::Select(select_input) => match self.describe(select_input) {
                                    Ok(description) => {
                                        let statement =
                                            PreparedStatement::new(statement, param_types.to_vec(), description);
                                        self.sender
                                            .send(Ok(QueryEvent::ParseComplete))
                                            .expect("To Send ParseComplete Event");
                                        self.session.set_prepared_statement(statement_name, statement);
                                    }
                                    Err(()) => {}
                                },
                                Plan::Insert(_insert_table) => {
                                    let statement = PreparedStatement::new(statement, param_types.to_vec(), vec![]);
                                    self.sender
                                        .send(Ok(QueryEvent::ParseComplete))
                                        .expect("To Send ParseComplete Event");
                                    self.session.set_prepared_statement(statement_name, statement);
                                }
                                Plan::Update(_table_updates) => {
                                    let statement = PreparedStatement::new(statement, param_types.to_vec(), vec![]);
                                    self.sender
                                        .send(Ok(QueryEvent::ParseComplete))
                                        .expect("To Send ParseComplete Event");
                                    self.session.set_prepared_statement(statement_name, statement);
                                }
                                Plan::NotProcessed(statement) => match statement.deref() {
                                    stmt @ Statement::SetVariable { .. } => {
                                        let statement =
                                            PreparedStatement::new(stmt.clone(), param_types.to_vec(), vec![]);
                                        self.sender
                                            .send(Ok(QueryEvent::ParseComplete))
                                            .expect("To Send ParseComplete Event");
                                        self.session.set_prepared_statement(statement_name, statement)
                                    }
                                    stmt => log::error!(
                                        "Error while describing not supported extended query for {:?}",
                                        stmt
                                    ),
                                },
                                plan => log::error!("Error while planning not supported extended query for {:?}", plan),
                            },
                            Err(()) => {}
                        }
                    }
                    Err(syntax_error) => {
                        self.sender
                            .send(Err(syntax_error))
                            .expect("To Send ParseComplete Event");
                    }
                }
                Ok(())
            }
            Command::Query { sql } => {
                match self.query_parser.parse(sql.as_str()) {
                    Ok(mut statements) => {
                        let statement = statements.pop().expect("single query");
                        if let Ok(plan) = self.query_planner.plan(&statement) {
                            self.query_executor.execute(plan);
                        }
                    }
                    Err(syntax_error) => {
                        self.sender
                            .send(Err(syntax_error))
                            .expect("To Send ParseComplete Event");
                    }
                }
                self.sender
                    .send(Ok(QueryEvent::QueryComplete))
                    .expect("To Send Error to Client");
                Ok(())
            }
            Command::Terminate => {
                log::debug!("closing connection with client");
                Err(())
            }
        }
    }

    fn bind_prepared_statement_to_portal(
        &self,
        prepared_statement: &PreparedStatement<Statement>,
        param_formats: &[PostgreSqlFormat],
        raw_params: &[Option<Vec<u8>>],
        result_formats: &[PostgreSqlFormat],
    ) -> Result<(Statement, Vec<PostgreSqlFormat>), ()> {
        let param_formats = match pad_formats(param_formats, raw_params.len()) {
            Ok(param_formats) => param_formats,
            Err(msg) => {
                self.sender
                    .send(Err(QueryError::protocol_violation(msg)))
                    .expect("To Send Error to Client");
                return Err(());
            }
        };

        let mut params: Vec<PostgreSqlValue> = vec![];
        for (raw_param, typ, format) in izip!(raw_params, prepared_statement.param_types(), param_formats) {
            match raw_param {
                None => params.push(PostgreSqlValue::Null),
                Some(bytes) => match typ.decode(&format, &bytes) {
                    Ok(param) => params.push(param),
                    Err(msg) => {
                        self.sender
                            .send(Err(QueryError::invalid_parameter_value(msg)))
                            .expect("To Send Error to Client");
                        return Err(());
                    }
                },
            }
        }

        let mut new_stmt = prepared_statement.stmt().clone();
        if self.param_binder.bind(&mut new_stmt, &params).is_err() {
            return Err(());
        }

        let result_formats = match pad_formats(result_formats, prepared_statement.description().len()) {
            Ok(result_formats) => result_formats,
            Err(msg) => {
                self.sender
                    .send(Err(QueryError::protocol_violation(msg)))
                    .expect("To Send Error to Client");
                return Err(());
            }
        };

        Ok((new_stmt, result_formats))
    }

    pub(crate) fn describe(&self, select_input: SelectInput) -> Result<Description, ()> {
        Ok(self
            .data_manager
            .column_defs(&select_input.table_id, &select_input.selected_columns)
            .into_iter()
            .map(|column_definition| (column_definition.name(), (&column_definition.sql_type()).into()))
            .collect())
    }
}

fn pad_formats(formats: &[PostgreSqlFormat], param_len: usize) -> Result<Vec<PostgreSqlFormat>, String> {
    match (formats.len(), param_len) {
        (0, n) => Ok(vec![PostgreSqlFormat::Text; n]),
        (1, n) => Ok(iter::repeat(formats[0]).take(n).collect()),
        (m, n) if m == n => Ok(formats.to_vec()),
        (m, n) => Err(format!("expected {} field format specifiers, but got {}", m, n)),
    }
}

#[cfg(test)]
mod tests;
