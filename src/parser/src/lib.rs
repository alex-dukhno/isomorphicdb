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

use data_manager::DataManager;
use protocol::{
    pgsql_types::PostgreSqlType,
    results::{Description, QueryError, QueryEvent},
    statement::PreparedStatement,
    Sender,
};
use query_planner::FullTableName;
use sql_model::Id;
use sqlparser::{
    ast::{Expr, Ident, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins},
    dialect::Dialect,
    parser::Parser,
};
use std::{convert::TryFrom, ops::Deref, sync::Arc};

pub struct QueryParser {
    sender: Arc<dyn Sender>,
    data_manager: Arc<DataManager>,
}

impl QueryParser {
    pub fn new(sender: Arc<dyn Sender>, data_manager: Arc<DataManager>) -> QueryParser {
        QueryParser { sender, data_manager }
    }

    pub fn parse(&self, raw_sql_query: &str) -> Result<Statement, ()> {
        match Parser::parse_sql(&PreparedStatementDialect {}, raw_sql_query) {
            Ok(mut statements) => {
                log::info!("stmts: {:#?}", statements);
                Ok(statements.pop().unwrap())
            }
            Err(e) => {
                log::error!("{:?} can't be parsed. Error: {:?}", raw_sql_query, e);
                self.sender
                    .send(Err(QueryError::syntax_error(format!(
                        "{:?} can't be parsed",
                        raw_sql_query
                    ))))
                    .expect("To Send Query Result to Client");
                Err(())
            }
        }
    }

    pub fn parse_prepared_statement(
        &mut self,
        raw_sql_query: &str,
        param_types: &[PostgreSqlType],
    ) -> Result<PreparedStatement<Statement>, ()> {
        let statement = self.parse(raw_sql_query)?;

        let description = match self.plan(&statement) {
            Ok(select_input) => self.describe(select_input)?,
            _ => vec![],
        };

        let prepared_statement = PreparedStatement::new(statement, param_types.to_vec(), description);

        self.sender
            .send(Ok(QueryEvent::ParseComplete))
            .expect("To Send ParseComplete Event");

        Ok(prepared_statement)
    }

    pub(crate) fn plan(&self, statement: &Statement) -> Result<SelectInput, ()> {
        if let Statement::Query(query) = statement {
            let Query { body, .. } = query.deref();
            if let SetExpr::Select(select) = body {
                let Select { projection, from, .. } = select.deref();
                let TableWithJoins { relation, .. } = &from[0];
                let name = match relation {
                    TableFactor::Table { name, .. } => name,
                    _ => {
                        self.sender
                            .send(Err(QueryError::feature_not_supported(query.deref())))
                            .expect("To Send Query Result to Client");
                        return Err(());
                    }
                };

                match FullTableName::try_from(name) {
                    Ok(full_table_name) => {
                        let (schema_name, table_name) = full_table_name.as_tuple();
                        match self.data_manager.table_exists(&schema_name, &table_name) {
                            None => {
                                self.sender
                                    .send(Err(QueryError::schema_does_not_exist(schema_name)))
                                    .expect("To Send Result to Client");
                                Err(())
                            }
                            Some((_, None)) => {
                                self.sender
                                    .send(Err(QueryError::table_does_not_exist(
                                        schema_name.to_owned() + "." + table_name,
                                    )))
                                    .expect("To Send Result to Client");
                                Err(())
                            }
                            Some((schema_id, Some(table_id))) => {
                                let selected_columns = {
                                    let projection = projection.clone();
                                    let mut columns: Vec<String> = vec![];
                                    for item in projection {
                                        match item {
                                            SelectItem::Wildcard => {
                                                let all_columns = self
                                                    .data_manager
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
                                                columns.push(value.clone())
                                            }
                                            _ => {
                                                self.sender
                                                    .send(Err(QueryError::feature_not_supported(query.deref())))
                                                    .expect("To Send Query Result to Client");
                                                return Err(());
                                            }
                                        }
                                    }
                                    columns
                                };

                                Ok(SelectInput {
                                    table_id: TableId((schema_id, table_id)),
                                    selected_columns,
                                })
                            }
                        }
                    }
                    Err(error) => {
                        self.sender
                            .send(Err(QueryError::syntax_error(error)))
                            .expect("To Send Query Result to Client");
                        Err(())
                    }
                }
            } else {
                self.sender
                    .send(Err(QueryError::feature_not_supported(&*statement)))
                    .expect("To Send Query Result to Client");
                Err(())
            }
        } else {
            Err(())
        }
    }

    pub(crate) fn describe(&self, select_input: SelectInput) -> Result<Description, ()> {
        let all_columns = self.data_manager.table_columns(&select_input.table_id)?;
        let mut column_definitions = vec![];
        let mut has_error = false;
        for column_name in &select_input.selected_columns {
            let mut found = None;
            for column_definition in &all_columns {
                if column_definition.has_name(&column_name) {
                    found = Some(column_definition);
                    break;
                }
            }

            if let Some(column_definition) = found {
                column_definitions.push(column_definition);
            } else {
                self.sender
                    .send(Err(QueryError::column_does_not_exist(column_name)))
                    .expect("To Send Result to Client");
                has_error = true;
            }
        }

        if has_error {
            return Err(());
        }

        let description = column_definitions
            .into_iter()
            .map(|column_definition| (column_definition.name(), (&column_definition.sql_type()).into()))
            .collect();

        Ok(description)
    }
}

/// represents a table uniquely
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct TableId((Id, Id));

impl AsRef<(Id, Id)> for TableId {
    fn as_ref(&self) -> &(Id, Id) {
        &self.0
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct SelectInput {
    pub table_id: TableId,
    pub selected_columns: Vec<String>,
}

#[derive(Debug)]
struct PreparedStatementDialect {}

impl Dialect for PreparedStatementDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '$' || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '$' || ch == '_'
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::results::QueryResult;
    use std::{io, sync::Mutex};

    struct Collector(Mutex<Vec<QueryResult>>);

    impl Sender for Collector {
        fn flush(&self) -> io::Result<()> {
            Ok(())
        }

        fn send(&self, query_result: QueryResult) -> io::Result<()> {
            self.0.lock().expect("locked").push(query_result);
            Ok(())
        }
    }

    impl Collector {
        fn assert_content(&self, expected: Vec<QueryResult>) {
            let result = self.0.lock().expect("locked");
            assert_eq!(&*result, &expected)
        }
    }

    type ResultCollector = Arc<Collector>;

    fn sender() -> ResultCollector {
        Arc::new(Collector(Mutex::new(vec![])))
    }

    #[cfg(test)]
    mod parsing_errors {
        use super::*;

        #[test]
        fn parse_wrong_select_syntax() {
            let collector = sender();
            let data_manager = DataManager::in_memory().expect("to create data manager");
            let parser = QueryParser::new(collector.clone(), Arc::new(data_manager));

            assert_eq!(parser.parse("selec col from schema_name.table_name"), Err(()));

            collector.assert_content(vec![Err(QueryError::syntax_error(
                "\"selec col from schema_name.table_name\" can\'t be parsed",
            ))]);
        }
    }
}
