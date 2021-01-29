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

use ast::{
    operations::ScalarOp,
    predicates::{PredicateOp, PredicateValue},
};
use constraints::TypeConstraint;
use meta_def::{DeprecatedColumnDefinition, Id};
use sql_ast::{ObjectName, Statement};
use std::{
    convert::TryFrom,
    fmt::{self, Display, Formatter},
    ops::Deref,
};
use types::SqlType;

#[derive(Debug, PartialEq)]
pub enum QueryPlan {
    CreateSchema,
}

/// represents a schema uniquely by its id
#[derive(PartialEq, Debug, Clone)]
pub struct DeprecatedSchemaId(Id);

impl From<Id> for DeprecatedSchemaId {
    fn from(id: Id) -> Self {
        DeprecatedSchemaId(id)
    }
}

impl Deref for DeprecatedSchemaId {
    type Target = Id;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<Id> for DeprecatedSchemaId {
    fn as_ref(&self) -> &Id {
        &self.0
    }
}

/// represents a schema uniquely by its name
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct DeprecatedSchemaName(String);

impl AsRef<str> for DeprecatedSchemaName {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl Deref for DeprecatedSchemaName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<&ObjectName> for DeprecatedSchemaName {
    type Error = DeprecatedSchemaNamingError;

    fn try_from(object: &ObjectName) -> Result<Self, Self::Error> {
        if object.0.len() != 1 {
            Err(DeprecatedSchemaNamingError(object.to_string()))
        } else {
            Ok(DeprecatedSchemaName(object.to_string().to_lowercase()))
        }
    }
}

impl Display for DeprecatedSchemaName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct DeprecatedSchemaNamingError(String);

impl Display for DeprecatedSchemaNamingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "only unqualified schema names are supported, '{}'", self.0)
    }
}

/// represents a table uniquely
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct DeprecatedPlanFullTableName(DeprecatedSchemaName, String);

impl DeprecatedPlanFullTableName {
    pub fn as_tuple(&self) -> (&str, &str) {
        (&self.0.as_ref(), &self.1)
    }
}

impl Display for DeprecatedPlanFullTableName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.0.as_ref(), self.1)
    }
}

impl TryFrom<&ObjectName> for DeprecatedPlanFullTableName {
    type Error = DeprecatedTableNamingError;

    fn try_from(object: &ObjectName) -> Result<Self, Self::Error> {
        if object.0.len() == 1 {
            Err(DeprecatedTableNamingError::Unqualified(object.to_string()))
        } else if object.0.len() != 2 {
            Err(DeprecatedTableNamingError::NotProcessed(object.to_string()))
        } else {
            let table_name = object.0.last().unwrap().value.clone();
            let schema_name = object.0.first().unwrap().value.clone();
            Ok(DeprecatedPlanFullTableName(
                DeprecatedSchemaName(schema_name.to_lowercase()),
                table_name.to_lowercase(),
            ))
        }
    }
}

pub enum DeprecatedTableNamingError {
    Unqualified(String),
    NotProcessed(String),
}

impl Display for DeprecatedTableNamingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DeprecatedTableNamingError::Unqualified(table_name) => write!(
                f,
                "unsupported table name '{}'. All table names must be qualified",
                table_name
            ),
            DeprecatedTableNamingError::NotProcessed(table_name) => {
                write!(f, "unable to process table name '{}'", table_name)
            }
        }
    }
}

/// A type of a column
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeprecatedColumnType {
    #[allow(dead_code)]
    nullable: bool,
    sql_type: SqlType,
}

/// represents a table uniquely
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct DeprecatedFullTableId((Id, Id));

impl From<(Id, Id)> for DeprecatedFullTableId {
    fn from(tuple: (Id, Id)) -> DeprecatedFullTableId {
        DeprecatedFullTableId(tuple)
    }
}

impl Deref for DeprecatedFullTableId {
    type Target = (Id, Id);

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct DeprecatedTableCreationInfo {
    pub schema_id: Id,
    pub table_name: String,
    pub columns: Vec<DeprecatedColumnDefinition>,
}

impl DeprecatedTableCreationInfo {
    pub fn new<S: ToString>(
        schema_id: Id,
        table_name: S,
        columns: Vec<DeprecatedColumnDefinition>,
    ) -> DeprecatedTableCreationInfo {
        DeprecatedTableCreationInfo {
            schema_id,
            table_name: table_name.to_string(),
            columns,
        }
    }

    pub fn as_tuple(&self) -> (Id, &str, &[DeprecatedColumnDefinition]) {
        (self.schema_id, self.table_name.as_str(), self.columns.as_slice())
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct DeprecatedSchemaCreationInfo {
    pub schema_name: String,
}

impl DeprecatedSchemaCreationInfo {
    pub fn new<S: ToString>(schema_name: S) -> DeprecatedSchemaCreationInfo {
        DeprecatedSchemaCreationInfo {
            schema_name: schema_name.to_string(),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct DeprecatedTableInserts {
    pub table_id: DeprecatedFullTableId,
    pub column_indices: Vec<(usize, String, SqlType, TypeConstraint)>,
    pub input: Vec<Vec<ScalarOp>>,
}

#[derive(PartialEq, Debug, Clone)]
pub struct DeprecatedTableUpdates {
    pub table_id: DeprecatedFullTableId,
    pub column_indices: Vec<(usize, String, SqlType, TypeConstraint)>,
    pub input: Vec<ScalarOp>,
}

#[derive(PartialEq, Debug, Clone)]
pub struct DeprecatedTableDeletes {
    pub table_id: DeprecatedFullTableId,
}

#[derive(PartialEq, Debug, Clone)]
pub struct DeprecatedSelectInput {
    pub table_id: DeprecatedFullTableId,
    pub selected_columns: Vec<Id>,
    pub predicate: Option<(PredicateValue, PredicateOp, PredicateValue)>,
}

#[derive(PartialEq, Debug, Clone)]
pub enum DeprecatedPlan {
    Select(DeprecatedSelectInput),
    Update(DeprecatedTableUpdates),
    Delete(DeprecatedTableDeletes),
    Insert(DeprecatedTableInserts),
    NotProcessed(Box<Statement>),
}
