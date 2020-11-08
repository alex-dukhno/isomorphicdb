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

use crate::{PlanError, Planner, Result};
use metadata::{DataDefinition, MetadataView};
use plan::{Plan, SchemaCreationInfo, SchemaName};
use sqlparser::ast::ObjectName;
use std::{convert::TryFrom, sync::Arc};

pub(crate) struct CreateSchemaPlanner<'csp> {
    schema_name: &'csp ObjectName,
}

impl CreateSchemaPlanner<'_> {
    pub(crate) fn new(schema_name: &ObjectName) -> CreateSchemaPlanner<'_> {
        CreateSchemaPlanner { schema_name }
    }
}

impl Planner for CreateSchemaPlanner<'_> {
    fn plan(self, metadata: Arc<DataDefinition>) -> Result<Plan> {
        match SchemaName::try_from(self.schema_name) {
            Ok(schema_name) => match metadata.schema_exists(&schema_name) {
                Some(_) => Err(PlanError::schema_already_exists(&schema_name)),
                None => Ok(Plan::CreateSchema(SchemaCreationInfo::new(schema_name))),
            },
            Err(error) => Err(PlanError::syntax_error(&error)),
        }
    }
}
