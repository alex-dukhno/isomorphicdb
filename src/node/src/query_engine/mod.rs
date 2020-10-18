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
use description::{Description, DescriptionError};
use itertools::izip;
use metadata::{DataDefinition, MetadataView};
use parser::QueryParser;
use pg_model::pg_types::{PostgreSqlFormat, PostgreSqlType};
use plan::{Plan, SelectInput};
use protocol::{
    results::{QueryError, QueryEvent},
    session::Session,
    statement::PreparedStatement,
    Command, Sender,
};
use query_analyzer::Analyzer;
use query_executor::QueryExecutor;
use query_planner::{PlanError, QueryPlanner};
use sqlparser::ast::{Expr, Ident, Statement, Value};
use std::{
    convert::{TryFrom, TryInto},
    iter,
    ops::Deref,
    sync::Arc,
};

pub(crate) struct QueryEngine {
    session: Session<Statement>,
    sender: Arc<dyn Sender>,
    data_manager: Arc<DataManager>,
    param_binder: ParamBinder,
    query_analyzer: Analyzer,
    query_parser: QueryParser,
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
            param_binder: ParamBinder,
            query_analyzer: Analyzer::new(metadata.clone()),
            query_parser: QueryParser::default(),
            query_planner: QueryPlanner::new(metadata),
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
                    .expect("To Send Query Complete to Client");
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
                        self.create_prepared_statement(statement_name, statement, param_types)
                            .map_or_else(
                                |query_errors| query_errors.into_iter().map(Err).collect(),
                                |_| vec![Ok(QueryEvent::ParseComplete)],
                            )
                            .into_iter()
                            .for_each(|query_result| self.sender.send(query_result).expect("To Send Result"));
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
                    Ok(mut statements) => {
                        let statement = statements.pop().expect("single query");
                        if !self.handle_prepared_statement_commands(&statement) {
                            match self.query_planner.plan(&statement) {
                                Ok(plan) => self.query_executor.execute(plan),
                                Err(errors) => {
                                    for error in errors {
                                        let query_error = match error {
                                            PlanError::SchemaAlreadyExists(schema) => {
                                                QueryError::schema_already_exists(schema)
                                            }
                                            PlanError::SchemaDoesNotExist(schema) => {
                                                QueryError::schema_does_not_exist(schema)
                                            }
                                            PlanError::TableAlreadyExists(table) => {
                                                QueryError::table_already_exists(table)
                                            }
                                            PlanError::TableDoesNotExist(table) => {
                                                QueryError::table_does_not_exist(table)
                                            }
                                            PlanError::DuplicateColumn(column) => QueryError::duplicate_column(column),
                                            PlanError::ColumnDoesNotExist(column) => {
                                                QueryError::column_does_not_exist(column)
                                            }
                                            PlanError::SyntaxError(syntax_error) => {
                                                QueryError::syntax_error(syntax_error)
                                            }
                                            PlanError::FeatureNotSupported(feature_desc) => {
                                                QueryError::feature_not_supported(feature_desc)
                                            }
                                        };
                                        self.sender.send(Err(query_error)).expect("To Send Error to Client");
                                    }
                                }
                            }
                        }
                    }
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
        param_formats: &[PostgreSqlFormat],
        raw_params: &[Option<Vec<u8>>],
        result_formats: &[PostgreSqlFormat],
    ) -> Result<(Statement, Vec<PostgreSqlFormat>), ()> {
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
                        Ok(param) => params.push(param.try_into().unwrap()),
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

        log::debug!("statement - {:?}, formats - {:?}", new_stmt, result_formats);
        Ok((new_stmt, result_formats))
    }

    fn create_prepared_statement(
        &mut self,
        statement_name: String,
        statement: Statement,
        param_types: Vec<PostgreSqlType>,
    ) -> Result<(), Vec<QueryError>> {
        match self.query_planner.plan(&statement) {
            Ok(plan) => match plan {
                Plan::Select(select_input) => {
                    let description = self.describe(select_input);
                    let statement = PreparedStatement::new(statement, param_types.to_vec(), description);
                    self.session.set_prepared_statement(statement_name, statement);
                    Ok(())
                }
                Plan::Insert(_insert_table) => match self.query_analyzer.describe(&statement) {
                    Ok(Description::Insert(_insert_statement)) => {
                        let statement = PreparedStatement::new(statement, param_types.to_vec(), vec![]);
                        self.session.set_prepared_statement(statement_name, statement);
                        Ok(())
                    }
                    Err(DescriptionError::TableDoesNotExist(table_name)) => {
                        Err(vec![QueryError::table_does_not_exist(table_name)])
                    }
                    Err(DescriptionError::SchemaDoesNotExist(schema_name)) => {
                        Err(vec![QueryError::table_does_not_exist(schema_name)])
                    }
                },
                Plan::Update(_table_updates) => {
                    let statement = PreparedStatement::new(statement, param_types.to_vec(), vec![]);
                    self.session.set_prepared_statement(statement_name, statement);
                    Ok(())
                }
                Plan::NotProcessed(statement) => match statement.deref() {
                    stmt @ Statement::SetVariable { .. } => {
                        let statement = PreparedStatement::new(stmt.clone(), param_types.to_vec(), vec![]);
                        self.session.set_prepared_statement(statement_name, statement);
                        Ok(())
                    }
                    stmt => {
                        log::error!("Error while describing not supported extended query for {:?}", stmt);
                        Ok(())
                    }
                },
                plan => {
                    log::error!("Error while planning not supported extended query for {:?}", plan);
                    Ok(())
                }
            },
            Err(errors) => Err(errors
                .iter()
                .map(|e| match e {
                    PlanError::SchemaAlreadyExists(schema) => QueryError::schema_already_exists(schema),
                    PlanError::SchemaDoesNotExist(schema) => QueryError::schema_does_not_exist(schema),
                    PlanError::TableAlreadyExists(table) => QueryError::table_already_exists(table),
                    PlanError::TableDoesNotExist(table) => QueryError::table_does_not_exist(table),
                    PlanError::DuplicateColumn(column) => QueryError::duplicate_column(column),
                    PlanError::ColumnDoesNotExist(column) => QueryError::column_does_not_exist(column),
                    PlanError::SyntaxError(syntax_error) => QueryError::syntax_error(syntax_error),
                    PlanError::FeatureNotSupported(feature_desc) => QueryError::feature_not_supported(feature_desc),
                })
                .collect()),
        }
    }

    fn describe(&self, select_input: SelectInput) -> protocol::results::Description {
        self.data_manager
            .column_defs(&select_input.table_id, &select_input.selected_columns)
            .into_iter()
            .map(|column_definition| (column_definition.name(), (&column_definition.sql_type()).into()))
            .collect()
    }

    // Returns true if the statement is handled as a prepared statement command,
    // otherwise false.
    fn handle_prepared_statement_commands(&mut self, statement: &Statement) -> bool {
        match statement {
            Statement::Prepare {
                name,
                data_types,
                statement,
            } => {
                let Ident { value: name, .. } = name;
                let mut pg_types = vec![];
                for t in data_types {
                    match PostgreSqlType::try_from(t) {
                        Ok(pg_type) => pg_types.push(pg_type),
                        Err(_) => {
                            self.sender
                                .send(Err(QueryError::type_does_not_exist(t)))
                                .expect("To Send Error to Client");
                            return true;
                        }
                    }
                }
                self.create_prepared_statement(name.to_owned(), *statement.clone(), pg_types)
                    .map_or_else(
                        |query_errors| query_errors.into_iter().map(Err).collect(),
                        |_| vec![Ok(QueryEvent::StatementPrepared)],
                    )
                    .into_iter()
                    .for_each(|query_result| self.sender.send(query_result).expect("To Send Result"));
                true
            }
            Statement::Execute { name, parameters } => {
                let Ident { value: name, .. } = name;
                match self.session.get_prepared_statement(&name) {
                    Some(prepared_statement) => {
                        let param_types = prepared_statement.param_types();
                        if param_types.len() != parameters.len() {
                            let message = format!(
                                "Bind message supplies {actual} parameters, but prepared statement \"{name}\" requires {expected}",
                                name = name,
                                actual = parameters.len(),
                                expected = param_types.len()
                            );
                            self.sender
                                .send(Err(QueryError::protocol_violation(message)))
                                .expect("To Send Error to Client");
                        }
                        let mut new_stmt = prepared_statement.stmt().clone();
                        if let Err(error) = self.param_binder.bind(&mut new_stmt, parameters) {
                            log::error!("{:?}", error);
                            return true;
                        }
                        match self.query_planner.plan(&new_stmt) {
                            Ok(plan) => self.query_executor.execute(plan),
                            Err(error) => log::error!("{:?}", error),
                        }
                    }
                    None => {
                        self.sender
                            .send(Err(QueryError::prepared_statement_does_not_exist(name)))
                            .expect("To Send Error to Client");
                    }
                }
                true
            }
            Statement::Deallocate { name, .. } => {
                let Ident { value: name, .. } = name;
                self.session.remove_prepared_statement(name);
                self.sender
                    .send(Ok(QueryEvent::StatementDeallocated))
                    .expect("To Send Statement Deallocated Event");
                true
            }
            _ => false,
        }
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
