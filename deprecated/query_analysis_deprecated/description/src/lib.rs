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

use meta_def::Id;
use pg_wire::PgType;
use sql_ast::ObjectName;
use std::{
    collections::HashMap,
    convert::TryFrom,
    fmt::{self, Display, Formatter},
    ops::Deref,
};
use types::SqlType;

pub type ParamIndex = usize;
pub type ParamTypes = HashMap<ParamIndex, SqlType>;

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

impl AsRef<(Id, Id)> for DeprecatedFullTableId {
    fn as_ref(&self) -> &(Id, Id) {
        &self.0
    }
}

/// represents a table uniquely
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct DeprecatedFullTableName((String, String));

impl DeprecatedFullTableName {
    pub fn schema(&self) -> &str {
        &(self.0).0
    }
}

impl<'f> Into<(&'f str, &'f str)> for &'f DeprecatedFullTableName {
    fn into(self) -> (&'f str, &'f str) {
        (&(self.0).0, &(self.0).1)
    }
}

impl Display for DeprecatedFullTableName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.0 .0, self.0 .1)
    }
}

impl TryFrom<&ObjectName> for DeprecatedFullTableName {
    type Error = TableNamingError;

    fn try_from(object: &ObjectName) -> Result<Self, Self::Error> {
        if object.0.len() == 1 {
            Err(TableNamingError::Unqualified(object.to_string()))
        } else if object.0.len() != 2 {
            Err(TableNamingError::NotProcessed(object.to_string()))
        } else {
            let table_name = object.0.last().unwrap().value.clone();
            let schema_name = object.0.first().unwrap().value.clone();
            Ok(DeprecatedFullTableName((
                schema_name.to_lowercase(),
                table_name.to_lowercase(),
            )))
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum TableNamingError {
    Unqualified(String),
    NotProcessed(String),
}

impl Display for TableNamingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TableNamingError::Unqualified(table_name) => write!(
                f,
                "Unsupported table name '{}'. All table names must be qualified",
                table_name
            ),
            TableNamingError::NotProcessed(table_name) => write!(f, "Unable to process table name '{}'", table_name),
        }
    }
}

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
        write!(f, "Only unqualified schema names are supported, '{}'", self.0)
    }
}

#[derive(PartialEq, Debug)]
pub struct DeprecatedSchemaId(Id);

impl From<Id> for DeprecatedSchemaId {
    fn from(id: Id) -> Self {
        DeprecatedSchemaId(id)
    }
}

impl AsRef<Id> for DeprecatedSchemaId {
    fn as_ref(&self) -> &Id {
        &self.0
    }
}

#[derive(PartialEq, Debug)]
pub struct DeprecatedInsertStatement {
    pub table_id: DeprecatedFullTableId,
    pub param_count: usize,
    pub param_types: ParamTypes,
}

#[derive(PartialEq, Debug)]
pub struct DeprecatedUpdateStatement {
    pub table_id: DeprecatedFullTableId,
    pub param_count: usize,
    pub param_types: ParamTypes,
}

#[derive(PartialEq, Debug)]
pub struct DeprecatedColumnDesc {
    pub name: String,
    pub pg_type: PgType,
}

#[derive(PartialEq, Debug)]
pub struct DeprecatedTableCreationInfo {
    pub schema_id: Id,
    pub table_name: String,
    pub columns: Vec<DeprecatedColumnDesc>,
}

#[derive(PartialEq, Debug)]
pub struct DeprecatedSchemaCreationInfo {
    pub schema_name: String,
}

#[derive(PartialEq, Debug)]
pub struct DeprecatedDropSchemasInfo {
    pub schema_ids: Vec<DeprecatedSchemaId>,
    pub cascade: bool,
    pub if_exists: bool,
}

#[derive(PartialEq, Debug)]
pub struct DeprecatedDropTablesInfo {
    pub full_table_ids: Vec<DeprecatedFullTableId>,
    pub cascade: bool,
    pub if_exists: bool,
}

#[derive(PartialEq, Debug)]
pub struct DeprecatedSelectStatement {
    pub full_table_id: DeprecatedFullTableId,
    pub projection_items: Vec<DeprecatedProjectionItem>,
}

#[derive(PartialEq, Debug)]
pub enum DeprecatedProjectionItem {
    Column(Id, SqlType),
    Const(u64),
}

#[derive(PartialEq, Debug)]
pub enum DeprecatedDescription {
    CreateSchema(DeprecatedSchemaCreationInfo),
    CreateTable(DeprecatedTableCreationInfo),
    DropSchemas(DeprecatedDropSchemasInfo),
    DropTables(DeprecatedDropTablesInfo),
    Insert(DeprecatedInsertStatement),
    Select(DeprecatedSelectStatement),
    Update(DeprecatedUpdateStatement),
}

#[derive(PartialEq, Debug)]
pub enum DeprecatedDescriptionError {
    SyntaxError(String),
    ColumnDoesNotExist(String),
    TableDoesNotExist(String),
    TableAlreadyExists(String),
    SchemaDoesNotExist(String),
    SchemaAlreadyExists(String),
    FeatureNotSupported(String),
}

impl DeprecatedDescriptionError {
    pub fn syntax_error<M: ToString>(message: &M) -> DeprecatedDescriptionError {
        DeprecatedDescriptionError::SyntaxError(message.to_string())
    }

    pub fn column_does_not_exist<T: ToString>(column: &T) -> DeprecatedDescriptionError {
        DeprecatedDescriptionError::ColumnDoesNotExist(column.to_string())
    }

    pub fn table_does_not_exist<T: ToString>(table: &T) -> DeprecatedDescriptionError {
        DeprecatedDescriptionError::TableDoesNotExist(table.to_string())
    }

    pub fn table_already_exists<T: ToString>(table: &T) -> DeprecatedDescriptionError {
        DeprecatedDescriptionError::TableAlreadyExists(table.to_string())
    }

    pub fn schema_does_not_exist<S: ToString>(schema: &S) -> DeprecatedDescriptionError {
        DeprecatedDescriptionError::SchemaDoesNotExist(schema.to_string())
    }

    pub fn schema_already_exists<S: ToString>(schema: &S) -> DeprecatedDescriptionError {
        DeprecatedDescriptionError::SchemaAlreadyExists(schema.to_string())
    }

    pub fn feature_not_supported<M: ToString>(message: &M) -> DeprecatedDescriptionError {
        DeprecatedDescriptionError::FeatureNotSupported(message.to_string())
    }
}
