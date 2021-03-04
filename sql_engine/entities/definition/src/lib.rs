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

use std::fmt::{self, Display, Formatter};
use types::SqlType;

#[derive(Debug, PartialEq)]
pub struct FullIndexName {
    full_table_name: FullTableName,
    index: String,
}

impl FullIndexName {
    pub fn table(&self) -> &FullTableName {
        &self.full_table_name
    }

    pub fn index(&self) -> &str {
        &self.index
    }
}

impl<S: ToString, T: ToString, I: ToString> From<(&S, &T, &I)> for FullIndexName {
    fn from(tuple: (&S, &T, &I)) -> FullIndexName {
        let (schema, table, index) = tuple;
        FullIndexName {
            full_table_name: FullTableName::from((schema, table)),
            index: index.to_string(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct FullTableName {
    schema: Option<String>,
    table: String,
}

impl FullTableName {
    pub fn schema(&self) -> &str {
        self.schema.as_deref().unwrap_or("public")
    }

    pub fn table(&self) -> &str {
        &self.table
    }
}

impl<S: ToString, T: ToString> From<(&S, &T)> for FullTableName {
    fn from(tuple: (&S, &T)) -> Self {
        let (schema, table) = tuple;
        FullTableName {
            schema: Some(schema.to_string()),
            table: table.to_string(),
        }
    }
}

impl From<&str> for FullTableName {
    fn from(table: &str) -> Self {
        FullTableName {
            schema: None,
            table: table.to_owned(),
        }
    }
}

impl Display for FullTableName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.schema.as_ref() {
            None => write!(f, "{}", self.table),
            Some(schema_name) => write!(f, "{}.{}", schema_name, self.table),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct SchemaName(String);

impl AsRef<str> for SchemaName {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl SchemaName {
    pub fn from<S: ToString>(schema_name: &S) -> SchemaName {
        SchemaName(schema_name.to_string())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    name: String,
    sql_type: SqlType,
    ord_num: usize,
}

impl ColumnDef {
    pub fn new(name: String, sql_type: SqlType, ord_num: usize) -> ColumnDef {
        ColumnDef {
            name,
            sql_type,
            ord_num,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn sql_type(&self) -> SqlType {
        self.sql_type
    }

    pub fn has_name(&self, name: &str) -> bool {
        self.name == name
    }

    pub fn index(&self) -> usize {
        self.ord_num as usize
    }
}

#[derive(Debug)]
pub struct TableDef {
    full_table_name: FullTableName,
    columns: Vec<ColumnDef>,
}

impl TableDef {
    pub fn new(full_table_name: FullTableName, columns: Vec<ColumnDef>) -> TableDef {
        TableDef {
            full_table_name,
            columns,
        }
    }

    pub fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }

    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|col| col.name.clone()).collect()
    }

    pub fn has_column(&self, column_name: &str) -> bool {
        self.columns.iter().any(|col| col.name == column_name)
    }
}
