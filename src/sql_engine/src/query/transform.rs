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

use super::{
    plan::{Plan, PlanError, SchemaCreationInfo, TableCreationInfo},
    SchemaId, TableId, TransformError,
};
///! Module for transforming the input Query AST into representation the engine can proecess.
use sql_types::SqlType;
use sqlparser::ast::*;
use std::sync::{Arc, Mutex, MutexGuard};
use storage::{backend::BackendStorage, frontend::FrontendStorage, ColumnDefinition};

// I do not know what the error type is yet.
type Result<T> = ::std::result::Result<T, TransformError>;

// this could probably just be a function.
/// structure for maintaining state while transforming the input statement.
pub struct QueryProcessor<B: BackendStorage> {
    /// access to table and schema information.
    storage: Arc<Mutex<FrontendStorage<B>>>,
}

// this was moved out to clean up the code. This is a good place
// to start but should not be the final code.
fn table_from_object(object: &ObjectName) -> Result<TableId> {
    if object.0.len() == 1 {
        Err(TransformError::SyntaxError(format!(
            "unsupported table name '{}'. All table names must be qualified",
            object.to_string()
        )))
    } else if object.0.len() != 2 {
        Err(TransformError::SyntaxError(format!(
            "unable to process table name '{}'",
            object.to_string()
        )))
    } else {
        let table_name = object.0.last().unwrap().value.clone();
        let schema_name = object.0.first().unwrap().value.clone();
        Ok(TableId(SchemaId(schema_name), table_name))
    }
}

fn schema_from_object(object: &ObjectName) -> Result<SchemaId> {
    if object.0.len() != 1 {
        Err(TransformError::SyntaxError(format!(
            "only unqualified schema names are supported, '{}'",
            object.to_string()
        )))
    } else {
        let schema_name = object.to_string();
        Ok(SchemaId(schema_name))
    }
}

fn sql_type_from_datatype(datatype: &DataType) -> Result<SqlType> {
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
                other_type => Err(TransformError::UnimplementedFeature(format!(
                    "{} type is not supported",
                    other_type
                ))),
            }
        }
        other_type => Err(TransformError::UnimplementedFeature(format!(
            "{} type is not supported",
            other_type
        ))),
    }
}

impl<B: BackendStorage> QueryProcessor<B> {
    pub fn new(storage: Arc<Mutex<FrontendStorage<B>>>) -> Self {
        Self { storage }
    }

    pub fn storage(&self) -> MutexGuard<FrontendStorage<B>> {
        self.storage.lock().unwrap()
    }

    pub fn process(&mut self, stmt: Statement) -> Result<Plan> {
        self.handle_statement(&stmt)
    }

    fn handle_statement(&mut self, stmt: &Statement) -> Result<Plan> {
        match stmt {
            Statement::CreateTable { name, columns, .. } => self.handle_create_table(name, columns),
            Statement::CreateSchema { schema_name, .. } => {
                let schema_id = schema_from_object(schema_name)?;
                if self.storage().schema_exists(schema_id.name()) {
                    Err(TransformError::from(PlanError::SchemaAlreadyExists(
                        schema_id.name().to_string(),
                    )))
                } else {
                    Ok(Plan::CreateSchema(SchemaCreationInfo {
                        schema_name: schema_id.name().to_string(),
                    }))
                }
            }
            Statement::Drop { object_type, names, .. } => self.handle_drop(object_type, names),
            _ => Err(TransformError::NotProcessed(stmt.clone())),
        }
    }

    fn resolve_column_definitions(&self, columns: &[ColumnDef]) -> Result<Vec<ColumnDefinition>> {
        let mut column_defs = Vec::new();
        for column in columns {
            let sql_type = sql_type_from_datatype(&column.data_type)?;
            // maybe a different type should be used to represent this instead of the storage's representation.
            let column_definition = ColumnDefinition::new(column.name.value.as_str(), sql_type);
            column_defs.push(column_definition);
        }
        Ok(column_defs)
    }

    fn handle_create_table(&mut self, name: &ObjectName, columns: &[ColumnDef]) -> Result<Plan> {
        let table_id = table_from_object(name)?;
        let schema_name = table_id.schema_name();
        let table_name = table_id.name();
        if !self.storage().schema_exists(schema_name) {
            Err(TransformError::from(PlanError::InvalidSchema(schema_name.to_string())))
        } else if self.storage().table_exists(schema_name, table_name) {
            Err(TransformError::from(PlanError::TableAlreadyExists(format!(
                "{}.{}",
                schema_name, table_name
            ))))
        } else {
            let columns = self.resolve_column_definitions(columns)?;
            let table_info = TableCreationInfo {
                schema_name: schema_name.to_owned(),
                table_name: table_name.to_owned(),
                columns,
            };
            Ok(Plan::CreateTable(table_info))
        }
    }

    fn handle_drop(&mut self, object_type: &ObjectType, names: &[ObjectName]) -> Result<Plan> {
        match object_type {
            ObjectType::Table => {
                let mut table_names = Vec::with_capacity(names.len());
                for name in names {
                    // I like the idea of abstracting this to a resolve_table_name(name) which would do
                    // this check for us and can be reused else where. ideally this function could handle aliassing as well.
                    let table_id = table_from_object(name)?;
                    let schema_name = table_id.schema_name();
                    let table_name = table_id.name();
                    if !self.storage().schema_exists(schema_name) {
                        return Err(TransformError::from(PlanError::InvalidSchema(schema_name.to_string())));
                    } else if !self.storage().table_exists(schema_name, table_name) {
                        return Err(TransformError::from(PlanError::InvalidTable(format!(
                            "{}.{}",
                            schema_name, table_name
                        ))));
                    } else {
                        table_names.push(table_id);
                    }
                }
                Ok(Plan::DropTables(table_names))
            }
            ObjectType::Schema => {
                let mut schema_names = Vec::with_capacity(names.len());
                for name in names {
                    let schema_id = schema_from_object(name)?;
                    if !self.storage().schema_exists(schema_id.name()) {
                        return Err(TransformError::from(PlanError::InvalidSchema(
                            schema_id.name().to_string(),
                        )));
                    }

                    schema_names.push(schema_id);
                }
                Ok(Plan::DropSchemas(schema_names))
            }
            _ => unimplemented!(),
        }
    }
}
