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

use std::{iter, sync::Arc};

use itertools::izip;
use sqlparser::ast::Statement;

use data_manager::DataManager;
use kernel::SystemResult;
use protocol::{
    pgsql_types::{PostgreSqlFormat, PostgreSqlType, PostgreSqlValue},
    results::{QueryError, QueryEvent},
    session::Session,
    statement::PreparedStatement,
    Sender,
};

use crate::{
    ddl::{
        create_schema::CreateSchemaCommand, create_table::CreateTableCommand, drop_schema::DropSchemaCommand,
        drop_table::DropTableCommand,
    },
    dml::{delete::DeleteCommand, insert::InsertCommand, select::SelectCommand, update::UpdateCommand},
    query::bind::ParamBinder,
};
use parser::QueryParser;
use query_planner::{plan::Plan, planner::QueryPlanner};

mod ddl;
mod dml;
pub mod query;

pub struct QueryExecutor {
    data_manager: Arc<DataManager>,
    sender: Arc<dyn Sender>,
    session: Session<Statement>,
    query_planner: QueryPlanner,
    param_binder: ParamBinder,
    query_parser: QueryParser,
}

impl QueryExecutor {
    pub fn new(data_manager: Arc<DataManager>, sender: Arc<dyn Sender>) -> Self {
        Self {
            data_manager: data_manager.clone(),
            sender: sender.clone(),
            session: Session::default(),
            query_planner: QueryPlanner::new(data_manager.clone(), sender.clone()),
            param_binder: ParamBinder::new(sender.clone()),
            query_parser: QueryParser::new(sender, data_manager),
        }
    }

    pub fn execute(&self, statement: &Statement) {
        self.process_statement(&statement);
        self.sender
            .send(Ok(QueryEvent::QueryComplete))
            .expect("To Send Query Complete Event to Client");
    }

    pub fn parse_prepared_statement(
        &mut self,
        statement_name: &str,
        raw_sql_query: &str,
        param_types: &[PostgreSqlType],
    ) -> Result<(), ()> {
        let statement = self.query_parser.parse(raw_sql_query)?;

        let description = match self.query_planner.plan(&statement) {
            Ok(Plan::Select(select_input)) => {
                SelectCommand::new(select_input, self.data_manager.clone(), self.sender.clone()).describe()?
            }
            _ => vec![],
        };

        let prepared_statement = PreparedStatement::new(statement, param_types.to_vec(), description);
        self.session
            .set_prepared_statement(statement_name.to_owned(), prepared_statement);

        self.sender
            .send(Ok(QueryEvent::ParseComplete))
            .expect("To Send ParseComplete Event");

        Ok(())
    }

    pub fn describe_prepared_statement(&self, name: &str) -> SystemResult<()> {
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
        };

        Ok(())
    }

    pub fn bind_prepared_statement_to_portal(
        &mut self,
        prepared_statement: &PreparedStatement<Statement>,
        param_formats: &[PostgreSqlFormat],
        raw_params: &[Option<Vec<u8>>],
        result_formats: &[PostgreSqlFormat],
    ) -> Result<(Statement, Vec<PostgreSqlFormat>), ()> {
        // let prepared_statement = match self.session.get_prepared_statement(statement_name) {
        //     Some(prepared_statement) => prepared_statement,
        //     None => {
        //         self.sender
        //             .send(Err(QueryError::prepared_statement_does_not_exist(statement_name)))
        //             .expect("To Send Error to Client");
        //         return Ok(());
        //     }
        // };

        // let param_types = prepared_statement.param_types();
        // if param_types.len() != raw_params.len() {
        //     let message = format!(
        //         "Bind message supplies {actual} parameters, \
        //          but prepared statement \"{name}\" requires {expected}",
        //         name = statement_name,
        //         actual = raw_params.len(),
        //         expected = param_types.len()
        //     );
        //     self.sender
        //         .send(Err(QueryError::protocol_violation(message)))
        //         .expect("To Send Error to Client");
        //     return Err(());
        // }

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

        // self.session.set_portal(
        //     portal_name.to_owned(),
        //     statement_name.to_owned(),
        //     new_stmt,
        //     result_formats,
        // );

        self.sender
            .send(Ok(QueryEvent::BindComplete))
            .expect("To Send BindComplete Event");

        Ok((new_stmt, result_formats))
    }

    // TODO: Parameter `max_rows` should be handled.
    pub fn execute_portal(&self, portal_name: &str, _max_rows: i32) -> Result<(), ()> {
        let portal = match self.session.get_portal(portal_name) {
            Some(portal) => portal,
            None => {
                self.sender
                    .send(Err(QueryError::portal_does_not_exist(portal_name)))
                    .expect("To Send Error to Client");
                return Ok(());
            }
        };

        self.process_statement(portal.stmt());

        self.sender
            .send(Ok(QueryEvent::QueryComplete))
            .expect("To Send Query Complete Event to Client");

        Ok(())
    }

    pub fn flush(&self) {
        match self.sender.flush() {
            Ok(_) => {}
            Err(e) => {
                log::error!("Flush error: {:?}", e);
            }
        };
    }

    fn process_statement(&self, statement: &Statement) {
        log::trace!("query statement = {}", statement);
        match self.query_planner.plan(statement) {
            Ok(Plan::CreateSchema(creation_info)) => {
                CreateSchemaCommand::new(creation_info, self.data_manager.clone(), self.sender.clone()).execute()
            }
            Ok(Plan::CreateTable(creation_info)) => {
                CreateTableCommand::new(creation_info, self.data_manager.clone(), self.sender.clone()).execute()
            }
            Ok(Plan::DropSchemas(schemas)) => {
                for (schema, cascade) in schemas {
                    DropSchemaCommand::new(schema, cascade, self.data_manager.clone(), self.sender.clone()).execute();
                }
            }
            Ok(Plan::DropTables(tables)) => {
                for table in tables {
                    DropTableCommand::new(table, self.data_manager.clone(), self.sender.clone()).execute();
                }
            }
            Ok(Plan::Insert(table_insert)) => {
                InsertCommand::new(table_insert, self.data_manager.clone(), self.sender.clone()).execute()
            }
            Ok(Plan::Update(table_update)) => {
                UpdateCommand::new(table_update, self.data_manager.clone(), self.sender.clone()).execute()
            }
            Ok(Plan::Delete(table_delete)) => {
                DeleteCommand::new(table_delete, self.data_manager.clone(), self.sender.clone()).execute()
            }
            Ok(Plan::Select(select_input)) => {
                SelectCommand::new(select_input, self.data_manager.clone(), self.sender.clone()).execute()
            }
            Ok(Plan::NotProcessed(statement)) => match *statement {
                Statement::StartTransaction { .. } => {
                    self.sender
                        .send(Ok(QueryEvent::TransactionStarted))
                        .expect("To Send Query Result to Client");
                }
                Statement::SetVariable { .. } => {
                    self.sender
                        .send(Ok(QueryEvent::VariableSet))
                        .expect("To Send Query Result to Client");
                }
                Statement::Drop { .. } => {
                    self.sender
                        .send(Err(QueryError::feature_not_supported(statement)))
                        .expect("To Send Query Result to Client");
                }
                _ => {
                    self.sender
                        .send(Err(QueryError::feature_not_supported(statement)))
                        .expect("To Send Query Result to Client");
                }
            },
            Err(()) => {}
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
