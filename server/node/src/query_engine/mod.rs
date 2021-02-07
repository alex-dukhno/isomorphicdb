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

use bigdecimal::BigDecimal;
use catalog::{CatalogDefinition, Database};
use connection::Sender;
use data_definition_execution_plan::{ExecutionError, ExecutionOutcome};
use data_manipulation_query_result::{QueryExecution, QueryExecutionError};
use data_manipulation_typed_queries::{DeleteQuery, InsertQuery, TypedSelectQuery, TypedWrite, UpdateQuery};
use data_manipulation_typed_tree::{DynamicTypedTree, StaticTypedTree};
use data_manipulation_untyped_queries::UntypedWrite;
use itertools::izip;
use pg_model::{session::Session, statement::PreparedStatement, Command};
use pg_result::{QueryError, QueryEvent};
use pg_wire::{ColumnMetadata, PgFormat, PgType};
use query_analyzer::{AnalysisError, Analyzer, QueryAnalysis};
use query_processing_type_check::TypeChecker;
use query_processing_type_coercion::TypeCoercion;
use query_processing_type_inference::TypeInference;
use read_query_executor::ReadQueryExecutor;
use read_query_planner::ReadQueryPlanner;
use sql_ast::{Expr, Ident, Statement, Value};
use std::{convert::TryFrom, iter, sync::Arc};
use types::SqlType;
use write_query_executor::WriteQueryExecutor;

unsafe impl<D: Database + CatalogDefinition> Send for QueryEngine<D> {}

unsafe impl<D: Database + CatalogDefinition> Sync for QueryEngine<D> {}

pub(crate) struct QueryEngine<D: Database + CatalogDefinition> {
    session: Session<Statement>,
    sender: Arc<dyn Sender>,
    query_analyzer: Analyzer<D>,
    type_inference: TypeInference,
    type_checker: TypeChecker,
    type_coercion: TypeCoercion,
    write_query_executor: WriteQueryExecutor<D>,
    read_query_planner: ReadQueryPlanner<D>,
    read_query_executor: ReadQueryExecutor<D>,
    database: Arc<D>,
}

impl<D: Database + CatalogDefinition> QueryEngine<D> {
    pub(crate) fn new(sender: Arc<dyn Sender>, database: Arc<D>) -> QueryEngine<D> {
        QueryEngine {
            session: Session::default(),
            sender: sender.clone(),
            query_analyzer: Analyzer::new(database.clone()),
            type_inference: TypeInference::default(),
            type_checker: TypeChecker,
            type_coercion: TypeCoercion,
            write_query_executor: WriteQueryExecutor::new(database.clone()),
            read_query_planner: ReadQueryPlanner::new(database.clone()),
            read_query_executor: ReadQueryExecutor::new(database.clone()),
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
                match parser::Parser::parse_sql(&parser::PreparedStatementDialect, &sql) {
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
                match parser::Parser::parse_sql(&parser::PreparedStatementDialect, &sql) {
                    Ok(mut statements) => match statements.pop().expect("single query") {
                        Statement::Prepare {
                            name,
                            data_types,
                            statement,
                        } => {
                            let Ident { value: name, .. } = name;
                            let mut pg_types = vec![];
                            for data_type in data_types {
                                match SqlType::try_from(&data_type) {
                                    Ok(sql_type) => pg_types.push(Some((&sql_type).into())),
                                    Err(_) => {
                                        self.sender
                                            .send(Err(QueryError::type_does_not_exist(data_type)))
                                            .expect("To Send Error to Client");
                                        self.sender
                                            .send(Ok(QueryEvent::QueryComplete))
                                            .expect("To Send Error to Client");
                                        return Ok(());
                                    }
                                }
                            }
                            match self.create_prepared_statement(name, *statement, pg_types) {
                                Ok(()) => {
                                    self.sender
                                        .send(Ok(QueryEvent::StatementPrepared))
                                        .expect("To Send Result");
                                }
                                Err(error) => {
                                    self.sender.send(Err(error)).expect("To Send Result");
                                    self.sender
                                        .send(Ok(QueryEvent::QueryComplete))
                                        .expect("To Send Error to Client");
                                    return Ok(());
                                }
                            }
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
                                    self.sender
                                        .send(Err(QueryError::syntax_error(prepared_statement.stmt())))
                                        .expect("To Send Error to Client");
                                }
                                None => {
                                    self.sender
                                        .send(Err(QueryError::prepared_statement_does_not_exist(name)))
                                        .expect("To Send Error to Client");
                                }
                            }
                        }
                        Statement::Deallocate { name, .. } => {
                            let Ident { value: name, .. } = name;
                            self.session.remove_prepared_statement(&name);
                            self.sender
                                .send(Ok(QueryEvent::StatementDeallocated))
                                .expect("To Send Statement Deallocated Event");
                        }
                        statement @ Statement::CreateSchema { .. }
                        | statement @ Statement::CreateTable { .. }
                        | statement @ Statement::CreateIndex { .. }
                        | statement @ Statement::Drop { .. } => match self.query_analyzer.analyze(statement) {
                            Ok(QueryAnalysis::DataDefinition(schema_change)) => {
                                log::debug!("SCHEMA CHANGE - {:?}", schema_change);
                                let query_result = match self.database.execute(schema_change) {
                                    Ok(ExecutionOutcome::SchemaCreated) => Ok(QueryEvent::SchemaCreated),
                                    Ok(ExecutionOutcome::SchemaDropped) => Ok(QueryEvent::SchemaDropped),
                                    Ok(ExecutionOutcome::TableCreated) => Ok(QueryEvent::TableCreated),
                                    Ok(ExecutionOutcome::TableDropped) => Ok(QueryEvent::TableDropped),
                                    Ok(ExecutionOutcome::IndexCreated) => Ok(QueryEvent::IndexCreated),
                                    Err(ExecutionError::SchemaAlreadyExists(schema_name)) => {
                                        Err(QueryError::schema_already_exists(schema_name))
                                    }
                                    Err(ExecutionError::SchemaDoesNotExist(schema_name)) => {
                                        Err(QueryError::schema_does_not_exist(schema_name))
                                    }
                                    Err(ExecutionError::TableAlreadyExists(schema_name, table_name)) => Err(
                                        QueryError::table_already_exists(format!("{}.{}", schema_name, table_name)),
                                    ),
                                    Err(ExecutionError::TableDoesNotExist(schema_name, table_name)) => Err(
                                        QueryError::table_does_not_exist(format!("{}.{}", schema_name, table_name)),
                                    ),
                                    Err(ExecutionError::SchemaHasDependentObjects(schema_name)) => {
                                        Err(QueryError::schema_has_dependent_objects(schema_name))
                                    }
                                    Err(ExecutionError::ColumnNotFound(column_name)) => {
                                        Err(QueryError::column_does_not_exist(column_name))
                                    }
                                };
                                self.sender.send(query_result).expect("To Send Result to Client");
                            }
                            Err(AnalysisError::SchemaDoesNotExist(schema_name)) => self
                                .sender
                                .send(Err(QueryError::schema_does_not_exist(schema_name)))
                                .expect("To Send Result to Client"),
                            analysis => unreachable!("that couldn't happen {:?}", analysis),
                        },
                        statement @ Statement::Insert { .. }
                        | statement @ Statement::Update { .. }
                        | statement @ Statement::Delete { .. }
                        | statement @ Statement::Query(_) => match self.query_analyzer.analyze(statement) {
                            Ok(QueryAnalysis::Write(UntypedWrite::Delete(delete))) => {
                                match self.write_query_executor.execute(TypedWrite::Delete(DeleteQuery {
                                    full_table_name: delete.full_table_name,
                                })) {
                                    Ok(QueryExecution::Deleted(deleted)) => {
                                        self.sender
                                            .send(Ok(QueryEvent::RecordsDeleted(deleted)))
                                            .expect("To Send to client");
                                    }
                                    Ok(_) => unimplemented!(),
                                    Err(QueryExecutionError::SchemaDoesNotExist(schema_name)) => {
                                        self.sender
                                            .send(Err(QueryError::schema_does_not_exist(schema_name)))
                                            .expect("To Send to client");
                                    }
                                    Err(_) => unimplemented!(),
                                }
                            }
                            Ok(QueryAnalysis::Write(UntypedWrite::Update(update))) => {
                                let typed_values = update
                                    .assignments
                                    .into_iter()
                                    .map(|value| self.type_inference.infer_dynamic(value))
                                    .collect::<Vec<DynamicTypedTree>>();
                                log::debug!("UPDATE TYPED VALUES - {:?}", typed_values);
                                let type_checked = typed_values
                                    .into_iter()
                                    .map(|value| self.type_checker.check_dynamic(value))
                                    .collect::<Vec<DynamicTypedTree>>();
                                log::debug!("UPDATE TYPE CHECKED VALUES - {:?}", type_checked);
                                let type_coerced = type_checked
                                    .into_iter()
                                    .map(|value| self.type_coercion.coerce_dynamic(value))
                                    .collect::<Vec<DynamicTypedTree>>();
                                log::debug!("UPDATE TYPE COERCED VALUES - {:?}", type_coerced);
                                match self.write_query_executor.execute(TypedWrite::Update(UpdateQuery {
                                    full_table_name: update.full_table_name,
                                    column_names: update.column_names,
                                    assignments: type_coerced,
                                })) {
                                    Ok(QueryExecution::Updated(updated)) => {
                                        self.sender
                                            .send(Ok(QueryEvent::RecordsUpdated(updated)))
                                            .expect("To Send to client");
                                    }
                                    Ok(_) => unimplemented!(),
                                    Err(QueryExecutionError::SchemaDoesNotExist(schema_name)) => {
                                        self.sender
                                            .send(Err(QueryError::schema_does_not_exist(schema_name)))
                                            .expect("To Send to client");
                                    }
                                    Err(_) => unimplemented!(),
                                }
                            }
                            Ok(QueryAnalysis::Write(UntypedWrite::Insert(insert))) => {
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
                                match self.write_query_executor.execute(TypedWrite::Insert(InsertQuery {
                                    full_table_name: insert.full_table_name,
                                    values: type_coerced,
                                })) {
                                    Ok(QueryExecution::Inserted(inserted)) => {
                                        self.sender
                                            .send(Ok(QueryEvent::RecordsInserted(inserted)))
                                            .expect("To Send to client");
                                    }
                                    Ok(_) => unimplemented!(),
                                    Err(error) => {
                                        self.sender.send(Err(error.into())).expect("To Send to client");
                                    }
                                }
                            }
                            Ok(QueryAnalysis::Read(select)) => {
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
                                let plan = self.read_query_planner.plan(TypedSelectQuery {
                                    projection_items: type_coerced,
                                    full_table_name: select.full_table_name,
                                });
                                match self.read_query_executor.execute(plan) {
                                    Ok(QueryExecution::Selected((desc, data))) => {
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
                                    Ok(_) => unimplemented!(),
                                    Err(_) => unimplemented!(),
                                }
                            }
                            Err(AnalysisError::TableDoesNotExist(full_table_name)) => {
                                self.sender
                                    .send(Err(QueryError::table_does_not_exist(full_table_name)))
                                    .expect("To Send Error to Client");
                            }
                            Err(AnalysisError::ColumnNotFound(column_name)) => {
                                self.sender
                                    .send(Err(QueryError::column_does_not_exist(column_name)))
                                    .expect("To Send Error to Client");
                            }
                            Err(AnalysisError::SyntaxError(message)) => {
                                self.sender
                                    .send(Err(QueryError::syntax_error(message)))
                                    .expect("To Send Error to Client");
                            }
                            Err(AnalysisError::SchemaDoesNotExist(schema_name)) => {
                                self.sender
                                    .send(Err(QueryError::schema_does_not_exist(schema_name)))
                                    .expect("To Send Error to Client");
                            }
                            branch => unimplemented!("handling {:?} is not implemented", branch),
                        },
                        sql_ast::Statement::SetVariable { .. } => {
                            // sending ok to the client to proceed with other requests
                            self.sender
                                .send(Ok(QueryEvent::VariableSet))
                                .expect("To Send Result to Client");
                        }
                        sql_ast::Statement::Copy { .. } => unimplemented!(),
                        sql_ast::Statement::CreateView { .. } => unimplemented!(),
                        sql_ast::Statement::CreateVirtualTable { .. } => unimplemented!(),
                        sql_ast::Statement::AlterTable { .. } => unimplemented!(),
                        sql_ast::Statement::ShowVariable { .. } => unimplemented!(),
                        sql_ast::Statement::ShowColumns { .. } => unimplemented!(),
                        sql_ast::Statement::StartTransaction { .. } => unimplemented!(),
                        sql_ast::Statement::SetTransaction { .. } => unimplemented!(),
                        sql_ast::Statement::Commit { .. } => unimplemented!(),
                        sql_ast::Statement::Rollback { .. } => unimplemented!(),
                        sql_ast::Statement::Assert { .. } => unimplemented!(),
                        sql_ast::Statement::Analyze { .. } => unimplemented!(),
                        sql_ast::Statement::Explain { .. } => unimplemented!(),
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

fn value_to_expr(value: pg_wire::Value) -> Expr {
    match value {
        pg_wire::Value::Null => Expr::Value(Value::Null),
        pg_wire::Value::True => Expr::Value(Value::Boolean(true)),
        pg_wire::Value::False => Expr::Value(Value::Boolean(false)),
        pg_wire::Value::Int16(i) => Expr::Value(Value::Number(BigDecimal::from(i))),
        pg_wire::Value::Int32(i) => Expr::Value(Value::Number(BigDecimal::from(i))),
        pg_wire::Value::Int64(i) => Expr::Value(Value::Number(BigDecimal::from(i))),
        pg_wire::Value::String(s) => Expr::Value(Value::SingleQuotedString(s)),
    }
}

#[cfg(test)]
mod tests;
