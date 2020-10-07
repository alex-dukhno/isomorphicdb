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

use sql_model::Id;
use sqlparser::ast::ObjectName;
use std::{
    convert::TryFrom,
    fmt::{self, Display, Formatter},
};

#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct TableId((Id, Id));

impl From<(Id, Id)> for TableId {
    fn from(tuple: (Id, Id)) -> TableId {
        TableId(tuple)
    }
}

/// represents a table uniquely
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct FullTableName<S: AsRef<str>>((S, S));

impl<S: AsRef<str>> FullTableName<S> {
    pub fn as_tuple(&self) -> (&S, &S) {
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

#[derive(PartialEq, Debug)]
pub struct InsertStatement {
    pub table_id: TableId,
}

#[derive(PartialEq, Debug)]
pub enum Description {
    Insert(InsertStatement),
}
