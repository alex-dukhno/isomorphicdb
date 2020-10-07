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

use crate::{Planner, Result};
use metadata::DataDefinition;
use plan::{Plan, SchemaId, SchemaName};
use protocol::{
    results::{QueryError, QueryEvent},
    Sender,
};
use sql_model::DEFAULT_CATALOG;
use sqlparser::ast::ObjectName;
use std::{convert::TryFrom, sync::Arc};

pub(crate) struct DropSchemaPlanner<'dsp> {
    names: &'dsp [ObjectName],
    cascade: bool,
    if_exists: bool,
}

impl DropSchemaPlanner<'_> {
    pub(crate) fn new(names: &[ObjectName], cascade: bool, if_exists: bool) -> DropSchemaPlanner<'_> {
        DropSchemaPlanner {
            names,
            cascade,
            if_exists,
        }
    }
}

impl Planner for DropSchemaPlanner<'_> {
    fn plan(self, metadata: Arc<DataDefinition>, sender: Arc<dyn Sender>) -> Result<Plan> {
        let mut schemas = Vec::with_capacity(self.names.len());
        for name in self.names {
            match SchemaName::try_from(name) {
                Ok(schema_name) => match metadata.schema_exists(DEFAULT_CATALOG, schema_name.as_ref()) {
                    None => return Err(()), // TODO catalog does not exists
                    Some((_, None)) => {
                        if self.if_exists {
                            sender
                                .send(Ok(QueryEvent::QueryComplete))
                                .expect("To Send Query Result to Client");
                        } else {
                            sender
                                .send(Err(QueryError::schema_does_not_exist(schema_name)))
                                .expect("To Send Query Result to Client");
                        }
                        return Err(());
                    }
                    Some((_, Some(schema_id))) => schemas.push((SchemaId::from(schema_id), self.cascade)),
                },
                Err(error) => {
                    sender
                        .send(Err(QueryError::syntax_error(error)))
                        .expect("To Send Query Result to Client");
                    return Err(());
                }
            }
        }
        Ok(Plan::DropSchemas(schemas))
    }
}
