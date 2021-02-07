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

#[cfg(test)]
mod index;
#[cfg(test)]
mod insert;
#[cfg(test)]
mod schema;
#[cfg(test)]
mod table;

use super::*;
use bigdecimal::BigDecimal;
use data_definition_execution_plan::ColumnInfo;
use data_manipulation_typed_tree::StaticTypedItem;
use types::SqlType;

const SCHEMA: &str = "schema_name";
const OTHER_SCHEMA: &str = "other_schema_name";
const TABLE: &str = "table_name";
const OTHER_TABLE: &str = "other_table_name";

fn database() -> Arc<InMemoryDatabase> {
    InMemoryDatabase::new()
}
