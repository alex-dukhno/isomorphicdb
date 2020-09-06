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

///! Module for representing how a query will be parameters bound, executed and
///! values represented during runtime.
pub mod bind;
pub mod plan;
pub mod process;

use sql_types::SqlType;
use sqlparser::ast::ObjectName;
use std::convert::TryFrom;
use data_manager::InnerId;

/// A type of a column
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnType {
    #[allow(dead_code)]
    nullable: bool,
    sql_type: SqlType,
}

/// represents a schema uniquely
///
/// this would be a u32
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct SchemaId(InnerId);

impl SchemaId {
    pub fn name(&self) -> InnerId {
        self.0
    }
}

/// represents a table uniquely
///
/// I would like this to be a single 64 bit number where the top bits are the
/// schema id and lower bits are the table id.
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct TableId(InnerId, InnerId);

impl TableId {
    pub fn schema(&self) ->  SchemaId {
        SchemaId(self.0)
    }

    pub fn name(&self) -> InnerId {
        self.1
    }
}

pub struct CatalogNameingError(String);
pub struct TableNamingError(String);
pub struct SchemaNamingError(String);
