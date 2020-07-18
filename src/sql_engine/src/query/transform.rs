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

///! Module for transforming the input Query AST into representation the engine can proecess.
use crate::query::plan::SchemaCreationInfo;
use crate::query::plan::{Plan, PlanError};
use crate::query::{SchemaId, TransformError};
use sqlparser::ast::{ObjectName, ObjectType, Statement};
use std::sync::{Arc, Mutex};
use storage::backend::BackendStorage;
use storage::frontend::FrontendStorage;

type Result<T> = ::std::result::Result<T, TransformError>;

/// structure for maintaining state while transforming the input statement.
pub struct QueryProcessor<B: BackendStorage> {
    /// access to table and schema information.
    storage: Arc<Mutex<FrontendStorage<B>>>,
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

impl<B: BackendStorage> QueryProcessor<B> {
    pub fn new(storage: Arc<Mutex<FrontendStorage<B>>>) -> Self {
        Self { storage }
    }

    pub fn process(&mut self, stmt: Statement) -> Result<Plan> {
        match stmt {
            Statement::CreateSchema { schema_name, .. } => {
                let schema_id = schema_from_object(&schema_name)?;
                if (self.storage.lock().unwrap()).schema_exists(schema_id.name()) {
                    Err(TransformError::from(PlanError::SchemaAlreadyExists(
                        schema_id.name().to_string(),
                    )))
                } else {
                    Ok(Plan::CreateSchema(SchemaCreationInfo {
                        schema_name: schema_id.name().to_string(),
                    }))
                }
            }
            Statement::Drop {
                object_type: ObjectType::Schema,
                names,
                ..
            } => {
                let mut schema_names = Vec::with_capacity(names.len());
                for name in names {
                    let schema_id = schema_from_object(&name)?;
                    if !(self.storage.lock().unwrap()).schema_exists(schema_id.name()) {
                        return Err(TransformError::from(PlanError::InvalidSchema(
                            schema_id.name().to_string(),
                        )));
                    }
                    schema_names.push(schema_id);
                }
                Ok(Plan::DropSchemas(schema_names))
            }
            other => Err(TransformError::NotProcessed(other)),
        }
    }
}
