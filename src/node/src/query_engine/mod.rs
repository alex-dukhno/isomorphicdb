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
use parser::QueryParser;
use protocol::{
    pgsql_types::{PostgreSqlFormat, PostgreSqlValue},
    results::{QueryError, QueryEvent},
    session::Session,
    statement::PreparedStatement,
    Command, Sender,
};
use query_executor::QueryExecutor;
use sqlparser::ast::Statement;
use std::{iter, sync::Arc};

pub(crate) struct QueryEngine {
    session: Session<Statement>,
    sender: Arc<dyn Sender>,
    query_executor: QueryExecutor,
    param_binder: ParamBinder,
    query_parser: QueryParser,
}

impl QueryEngine {
    pub(crate) fn new(sender: Arc<dyn Sender>, data_manager: Arc<DataManager>) -> QueryEngine {
        QueryEngine {
            session: Session::default(),
            sender: sender.clone(),
            query_executor: QueryExecutor::new(data_manager.clone(), sender.clone()),
            param_binder: ParamBinder,
            query_parser: QueryParser::new(sender.clone(), data_manager),
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
                self.describe_prepared_statement(name.as_str());
                Ok(())
            }
            // TODO: Parameter `max_rows` should be handled.
            Command::Execute {
                portal_name,
                max_rows: _max_rows,
            } => {
                match self.session.get_portal(&portal_name) {
                    Some(portal) => self.query_executor.execute(portal.stmt()),
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
                match self.query_parser.parse_prepared_statement(&sql, &param_types) {
                    Ok(statement) => {
                        self.session.set_prepared_statement(statement_name, statement);
                    }
                    Err(error) => log::error!("{:?}", error),
                }
                Ok(())
            }
            Command::Query { sql } => {
                if let Ok(statement) = self.query_parser.parse(sql.as_str()) {
                    self.query_executor.execute(&statement);
                    self.sender.flush().expect("Send All Buffered Messages to Client");
                }
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

    fn describe_prepared_statement(&self, name: &str) {
        match self.session.get_prepared_statement(name) {
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
