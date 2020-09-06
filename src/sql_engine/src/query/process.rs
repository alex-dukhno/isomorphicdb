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

///! Module for transforming the input Query AST into representation the engine can process.
use std::{convert::TryFrom, sync::Arc};

use sqlparser::ast::{
    ColumnDef, DataType, Expr, Ident, ObjectName, ObjectType, Query, Select, SelectItem, SetExpr, Statement,
    TableFactor, TableWithJoins,
};

use data_manager::{ColumnDefinition, DataManager};
use protocol::{results::QueryError, Sender};
use sql_types::SqlType;

use crate::query::{
    plan::{Plan, SchemaCreationInfo, SelectInput, TableCreationInfo, TableDeletes, TableInserts, TableUpdates},
    SchemaId, SchemaNamingError, TableId, TableNamingError,
};
use kernel::{SystemError, SystemResult};
use std::ops::Deref;

type Result<T> = std::result::Result<T, ()>;

pub(crate) struct QueryProcessor {
    storage: Arc<DataManager>,
    sender: Arc<dyn Sender>,
}

impl QueryProcessor {
    pub fn new(storage: Arc<DataManager>, sender: Arc<dyn Sender>) -> Self {
        Self { storage, sender }
    }

    pub fn process(&self, stmt: Statement) -> Result<Plan> {
        match stmt {
            Statement::CreateTable { name, columns, .. } => self.handle_create_table(name, &columns),
            Statement::CreateSchema { schema_name, .. } => {
                let schema_id = match SchemaId::try_from(schema_name) {
                    Ok(schema_id) => schema_id,
                    Err(SchemaNamingError(message)) => {
                        self.sender
                            .send(Err(QueryError::syntax_error(message)))
                            .expect("To Send Query Result to Client");
                        return Err(());
                    }
                };
                match self.storage.schema_exists(schema_id.name()) {
                    Some(_) => {
                        self.sender
                            .send(Err(QueryError::schema_already_exists(schema_id.name().to_string())))
                            .expect("To Send Query Result to Client");
                        Err(())
                    }
                    None => Ok(Plan::CreateSchema(SchemaCreationInfo {
                        schema_name: schema_id.name().to_string(),
                    })),
                }
            }
            Statement::Drop {
                object_type,
                names,
                cascade,
                ..
            } => self.handle_drop(&object_type, &names, cascade),
            Statement::Insert {
                table_name,
                columns,
                source,
            } => match TableId::try_from(table_name) {
                Ok(table_id) => Ok(Plan::Insert(TableInserts {
                    table_id,
                    column_indices: columns,
                    input: source,
                })),
                Err(TableNamingError(message)) => {
                    self.sender
                        .send(Err(QueryError::syntax_error(message)))
                        .expect("To Send Query Result to Client");
                    Err(())
                }
            },
            Statement::Update {
                table_name,
                assignments,
                ..
            } => match TableId::try_from(table_name) {
                Ok(table_id) => Ok(Plan::Update(TableUpdates { table_id, assignments })),
                Err(TableNamingError(message)) => {
                    self.sender
                        .send(Err(QueryError::syntax_error(message)))
                        .expect("To Send Query Result to Client");
                    Err(())
                }
            },
            Statement::Delete { table_name, .. } => match TableId::try_from(table_name) {
                Ok(table_id) => Ok(Plan::Delete(TableDeletes { table_id })),
                Err(TableNamingError(message)) => {
                    self.sender
                        .send(Err(QueryError::syntax_error(message)))
                        .expect("To Send Query Result to Client");
                    Err(())
                }
            },
            // TODO: ad-hock solution, duh
            Statement::Query(query) => {
                let result = self.parse_select_input(query);
                eprintln!("{:?}", result);
                Ok(Plan::Select(result.map_err(|_| ())?))
            }
            _ => Ok(Plan::NotProcessed(Box::new(stmt.clone()))),
        }
    }

    fn parse_select_input(&self, query: Box<Query>) -> SystemResult<SelectInput> {
        let Query { body, .. } = &*query;
        if let SetExpr::Select(select) = body {
            let Select { projection, from, .. } = select.deref();
            let TableWithJoins { relation, .. } = &from[0];
            let (schema_name, table_name) = match relation {
                TableFactor::Table { name, .. } => {
                    let table_name = name.0[1].to_string();
                    let schema_name = name.0[0].to_string();
                    (schema_name, table_name)
                }
                _ => {
                    self.sender
                        .send(Err(QueryError::feature_not_supported(query.to_string())))
                        .expect("To Send Query Result to Client");
                    return Err(SystemError::runtime_check_failure("Feature Not Supported".to_owned()));
                }
            };

            match self.storage.table_exists(&schema_name, &table_name) {
                None => {
                    self.sender
                        .send(Err(QueryError::schema_does_not_exist(schema_name)))
                        .expect("To Send Result to Client");
                    Err(SystemError::runtime_check_failure("Schema Does Not Exist".to_owned()))
                }
                Some((_, None)) => {
                    self.sender
                        .send(Err(QueryError::table_does_not_exist(
                            schema_name + "." + table_name.as_str(),
                        )))
                        .expect("To Send Result to Client");
                    Err(SystemError::runtime_check_failure("Table Does Not Exist".to_owned()))
                }
                Some((schema_id, Some(table_id))) => {
                    let selected_columns = {
                        let projection = projection.clone();
                        let mut columns: Vec<String> = vec![];
                        for item in projection {
                            match item {
                                SelectItem::Wildcard => {
                                    let all_columns = self.storage.table_columns(schema_id, table_id)?;
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
                                        .send(Err(QueryError::feature_not_supported(query.to_string())))
                                        .expect("To Send Query Result to Client");
                                    return Err(SystemError::runtime_check_failure("Feature Not Supported".to_owned()));
                                }
                            }
                        }
                        columns
                    };

                    Ok(SelectInput {
                        schema_name,
                        table_name,
                        selected_columns,
                    })
                }
            }
        } else {
            self.sender
                .send(Err(QueryError::feature_not_supported(query.to_string())))
                .expect("To Send Query Result to Client");
            Err(SystemError::runtime_check_failure("Feature Not Supported".to_owned()))
        }
    }

    fn sql_type_from_datatype(&self, datatype: &DataType) -> Result<SqlType> {
        match datatype {
            DataType::SmallInt => Ok(SqlType::SmallInt(i16::min_value())),
            DataType::Int => Ok(SqlType::Integer(i32::min_value())),
            DataType::BigInt => Ok(SqlType::BigInt(i64::min_value())),
            DataType::Char(len) => Ok(SqlType::Char(len.unwrap_or(255))),
            DataType::Varchar(len) => Ok(SqlType::VarChar(len.unwrap_or(255))),
            DataType::Boolean => Ok(SqlType::Bool),
            DataType::Custom(name) => {
                let name = name.to_string();
                match name.as_str() {
                    "serial" => Ok(SqlType::Integer(1)),
                    "smallserial" => Ok(SqlType::SmallInt(1)),
                    "bigserial" => Ok(SqlType::BigInt(1)),
                    other_type => {
                        self.sender
                            .send(Err(QueryError::feature_not_supported(format!(
                                "{} type is not supported",
                                other_type
                            ))))
                            .expect("To Send Query Result to Client");
                        Err(())
                    }
                }
            }
            other_type => {
                self.sender
                    .send(Err(QueryError::feature_not_supported(format!(
                        "{} type is not supported",
                        other_type
                    ))))
                    .expect("To Send Query Result to Client");
                Err(())
            }
        }
    }

    fn resolve_column_definitions(&self, columns: &[ColumnDef]) -> Result<Vec<ColumnDefinition>> {
        let mut column_defs = Vec::new();
        for column in columns {
            let sql_type = self.sql_type_from_datatype(&column.data_type)?;
            // maybe a different type should be used to represent this instead of the storage's representation.
            let column_definition = ColumnDefinition::new(column.name.value.as_str(), sql_type);
            column_defs.push(column_definition);
        }
        Ok(column_defs)
    }

    fn handle_create_table(&self, name: ObjectName, columns: &[ColumnDef]) -> Result<Plan> {
        let table_id = match TableId::try_from(name) {
            Ok(table_id) => table_id,
            Err(TableNamingError(message)) => {
                self.sender
                    .send(Err(QueryError::syntax_error(message)))
                    .expect("To Send Query Result to Client");
                return Err(());
            }
        };
        let schema_name = table_id.schema_name();
        let table_name = table_id.name();
        match self.storage.table_exists(&schema_name, &table_name) {
            None => {
                self.sender
                    .send(Err(QueryError::schema_does_not_exist(schema_name.to_owned())))
                    .expect("To Send Query Result to Client");
                Err(())
            }
            Some((_, Some(_))) => {
                self.sender
                    .send(Err(QueryError::table_already_exists(format!(
                        "{}.{}",
                        schema_name, table_name
                    ))))
                    .expect("To Send Query Result to Client");
                Err(())
            }
            Some((_, None)) => {
                let columns = self.resolve_column_definitions(columns)?;
                let table_info = TableCreationInfo {
                    schema_name: schema_name.to_owned(),
                    table_name: table_name.to_owned(),
                    columns,
                };
                Ok(Plan::CreateTable(table_info))
            }
        }
    }

    fn handle_drop(&self, object_type: &ObjectType, names: &[ObjectName], cascade: bool) -> Result<Plan> {
        match object_type {
            ObjectType::Table => {
                let mut table_names = Vec::with_capacity(names.len());
                for name in names {
                    // I like the idea of abstracting this to a resolve_table_name(name) which would do
                    // this check for us and can be reused else where. ideally this function could handle aliasing as well.
                    let table_id = match TableId::try_from(name.clone()) {
                        Ok(table_id) => table_id,
                        Err(TableNamingError(message)) => {
                            self.sender
                                .send(Err(QueryError::syntax_error(message)))
                                .expect("To Send Query Result to Client");
                            return Err(());
                        }
                    };
                    let schema_name = table_id.schema_name();
                    let table_name = table_id.name();
                    match self.storage.table_exists(&schema_name, &table_name) {
                        None => {
                            self.sender
                                .send(Err(QueryError::schema_does_not_exist(schema_name.to_owned())))
                                .expect("To Send Query Result to Client");
                            return Err(());
                        }
                        Some((_, None)) => {
                            self.sender
                                .send(Err(QueryError::table_does_not_exist(format!(
                                    "{}.{}",
                                    schema_name, table_name
                                ))))
                                .expect("To Send Query Result to Client");
                            return Err(());
                        }
                        Some((_, Some(_))) => table_names.push(table_id),
                    }
                }
                Ok(Plan::DropTables(table_names))
            }
            ObjectType::Schema => {
                let mut schema_names = Vec::with_capacity(names.len());
                for name in names {
                    let schema_id = match SchemaId::try_from(name.clone()) {
                        Ok(schema_id) => schema_id,
                        Err(SchemaNamingError(message)) => {
                            self.sender
                                .send(Err(QueryError::syntax_error(message)))
                                .expect("To Send Query Result to Client");
                            return Err(());
                        }
                    };
                    match self.storage.schema_exists(schema_id.name()) {
                        None => {
                            self.sender
                                .send(Err(QueryError::schema_does_not_exist(schema_id.name().to_owned())))
                                .expect("To Send Query Result to Client");
                            return Err(());
                        }
                        Some(_) => schema_names.push((schema_id, cascade)),
                    }
                }
                Ok(Plan::DropSchemas(schema_names))
            }
            _ => unimplemented!(),
        }
    }
}
