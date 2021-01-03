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

use meta_def::Id;
use pg_wire::PgType;
use sqlparser::ast::ObjectName;
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
pub struct FullTableId((Id, Id));

impl From<(Id, Id)> for FullTableId {
    fn from(tuple: (Id, Id)) -> FullTableId {
        FullTableId(tuple)
    }
}

impl Deref for FullTableId {
    type Target = (Id, Id);

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<(Id, Id)> for FullTableId {
    fn as_ref(&self) -> &(Id, Id) {
        &self.0
    }
}

/// represents a table uniquely
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct FullTableName((String, String));

impl FullTableName {
    pub fn schema(&self) -> &str {
        &(self.0).0
    }
}

impl<'f> Into<(&'f str, &'f str)> for &'f FullTableName {
    fn into(self) -> (&'f str, &'f str) {
        (&(self.0).0, &(self.0).1)
    }
}

impl Display for FullTableName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.0 .0, self.0 .1)
    }
}

impl TryFrom<&ObjectName> for FullTableName {
    type Error = TableNamingError;

    fn try_from(object: &ObjectName) -> Result<Self, Self::Error> {
        if object.0.len() == 1 {
            Err(TableNamingError::Unqualified(object.to_string()))
        } else if object.0.len() != 2 {
            Err(TableNamingError::NotProcessed(object.to_string()))
        } else {
            let table_name = object.0.last().unwrap().value.clone();
            let schema_name = object.0.first().unwrap().value.clone();
            Ok(FullTableName((schema_name.to_lowercase(), table_name.to_lowercase())))
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
pub struct SchemaName(String);

impl AsRef<str> for SchemaName {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl Deref for SchemaName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<&ObjectName> for SchemaName {
    type Error = SchemaNamingError;

    fn try_from(object: &ObjectName) -> Result<Self, Self::Error> {
        if object.0.len() != 1 {
            Err(SchemaNamingError(object.to_string()))
        } else {
            Ok(SchemaName(object.to_string().to_lowercase()))
        }
    }
}

impl Display for SchemaName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct SchemaNamingError(String);

impl Display for SchemaNamingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Only unqualified schema names are supported, '{}'", self.0)
    }
}

#[derive(PartialEq, Debug)]
pub struct SchemaId(Id);

impl From<Id> for SchemaId {
    fn from(id: Id) -> Self {
        SchemaId(id)
    }
}

impl AsRef<Id> for SchemaId {
    fn as_ref(&self) -> &Id {
        &self.0
    }
}

#[derive(PartialEq, Debug)]
pub struct InsertStatement {
    pub table_id: FullTableId,
    pub param_count: usize,
    pub param_types: ParamTypes,
}

#[derive(PartialEq, Debug)]
pub struct UpdateStatement {
    pub table_id: FullTableId,
    pub param_count: usize,
    pub param_types: ParamTypes,
}

#[derive(PartialEq, Debug)]
pub struct ColumnDesc {
    pub name: String,
    pub pg_type: PgType,
}

#[derive(PartialEq, Debug)]
pub struct TableCreationInfo {
    pub schema_id: Id,
    pub table_name: String,
    pub columns: Vec<ColumnDesc>,
}

#[derive(PartialEq, Debug)]
pub struct SchemaCreationInfo {
    pub schema_name: String,
}

#[derive(PartialEq, Debug)]
pub struct DropSchemasInfo {
    pub schema_ids: Vec<SchemaId>,
    pub cascade: bool,
    pub if_exists: bool,
}

#[derive(PartialEq, Debug)]
pub struct DropTablesInfo {
    pub full_table_ids: Vec<FullTableId>,
    pub cascade: bool,
    pub if_exists: bool,
}

#[derive(PartialEq, Debug)]
pub struct SelectStatement {
    pub full_table_id: FullTableId,
    pub projection_items: Vec<ProjectionItem>,
}

#[derive(PartialEq, Debug)]
pub enum ProjectionItem {
    Column(Id, SqlType),
    Const(u64),
}

#[derive(PartialEq, Debug)]
pub enum Description {
    CreateSchema(SchemaCreationInfo),
    CreateTable(TableCreationInfo),
    DropSchemas(DropSchemasInfo),
    DropTables(DropTablesInfo),
    Insert(InsertStatement),
    Select(SelectStatement),
    Update(UpdateStatement),
}

#[derive(PartialEq, Debug)]
pub enum DescriptionError {
    SyntaxError(String),
    ColumnDoesNotExist(String),
    TableDoesNotExist(String),
    TableAlreadyExists(String),
    SchemaDoesNotExist(String),
    SchemaAlreadyExists(String),
    FeatureNotSupported(String),
}

impl DescriptionError {
    pub fn syntax_error<M: ToString>(message: &M) -> DescriptionError {
        DescriptionError::SyntaxError(message.to_string())
    }

    pub fn column_does_not_exist<T: ToString>(column: &T) -> DescriptionError {
        DescriptionError::ColumnDoesNotExist(column.to_string())
    }

    pub fn table_does_not_exist<T: ToString>(table: &T) -> DescriptionError {
        DescriptionError::TableDoesNotExist(table.to_string())
    }

    pub fn table_already_exists<T: ToString>(table: &T) -> DescriptionError {
        DescriptionError::TableAlreadyExists(table.to_string())
    }

    pub fn schema_does_not_exist<S: ToString>(schema: &S) -> DescriptionError {
        DescriptionError::SchemaDoesNotExist(schema.to_string())
    }

    pub fn schema_already_exists<S: ToString>(schema: &S) -> DescriptionError {
        DescriptionError::SchemaAlreadyExists(schema.to_string())
    }

    pub fn feature_not_supported<M: ToString>(message: &M) -> DescriptionError {
        DescriptionError::FeatureNotSupported(message.to_string())
    }
}
