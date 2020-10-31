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

use description::{Description, DescriptionError, FullTableId, FullTableName, InsertStatement, TableNamingError};
use metadata::{DataDefinition, MetadataView};
use sql_model::sql_errors::NotFoundError;
use sqlparser::ast::Statement;
use std::{convert::TryFrom, sync::Arc};

pub struct Analyzer {
    metadata: Arc<DataDefinition>,
}

impl Analyzer {
    pub fn new(metadata: Arc<DataDefinition>) -> Analyzer {
        Analyzer { metadata }
    }

    pub fn describe(&self, statement: &Statement) -> Result<Description, DescriptionError> {
        match statement {
            Statement::Insert { table_name, .. } => match FullTableName::try_from(table_name) {
                Ok(full_table_name) => match self.metadata.table_desc((&full_table_name).into()) {
                    Ok(table_def) => Ok(Description::Insert(InsertStatement {
                        table_id: FullTableId::from(table_def.full_table_id()),
                        sql_types: table_def.column_types(),
                    })),
                    Err(NotFoundError::Object) => Err(DescriptionError::table_does_not_exist(&full_table_name)),
                    Err(NotFoundError::Schema) => {
                        Err(DescriptionError::schema_does_not_exist(full_table_name.schema()))
                    }
                },
                Err(error) => Err(DescriptionError::syntax_error(&error)),
            },
            Statement::CreateTable { name, .. } => Err(DescriptionError::schema_does_not_exist(&name.0[0])),
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests;
