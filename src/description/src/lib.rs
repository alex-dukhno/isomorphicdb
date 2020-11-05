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

use pg_wire::PgType;
use sql_model::{sql_types::SqlType, Id};
use sqlparser::ast::ObjectName;
use std::{
    convert::TryFrom,
    fmt::{self, Display, Formatter},
};

#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct FullTableId((Id, Id));

impl From<(Id, Id)> for FullTableId {
    fn from(tuple: (Id, Id)) -> FullTableId {
        FullTableId(tuple)
    }
}

/// represents a table uniquely
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct FullTableName<S: AsRef<str>>((S, S));

impl<S: AsRef<str>> FullTableName<S> {
    pub fn schema(&self) -> &S {
        &(self.0).0
    }
}

impl<'f, S: AsRef<str>> Into<(&'f S, &'f S)> for &'f FullTableName<S> {
    fn into(self) -> (&'f S, &'f S) {
        (&(self.0).0, &(self.0).1)
    }
}

impl<S: AsRef<str> + Display> Display for FullTableName<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.0.0, self.0.1)
    }
}

impl TryFrom<&ObjectName> for FullTableName<String> {
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
            TableNamingError::NotProcessed(table_name) => write!(f, "unable to process table name '{}'", table_name),
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct InsertStatement {
    pub table_id: FullTableId,
    pub sql_types: Vec<SqlType>,
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
pub enum Description {
    CreateTable(TableCreationInfo),
    Insert(InsertStatement),
}

#[derive(PartialEq, Debug)]
pub enum DescriptionError {
    SyntaxError(String),
    TableDoesNotExist(String),
    TableAlreadyExists(String),
    SchemaDoesNotExist(String),
}

impl DescriptionError {
    pub fn syntax_error<M: ToString>(message: &M) -> DescriptionError {
        DescriptionError::SyntaxError(message.to_string())
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
}
