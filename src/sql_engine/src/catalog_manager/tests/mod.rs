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

use super::*;
use representation::Binary;

#[cfg(test)]
mod persistence;
#[cfg(test)]
mod queries;
#[cfg(test)]
mod schema;
#[cfg(test)]
mod table;

const SCHEMA: &str = "schema_name";
const SCHEMA_1: &str = "schema_name_1";
const SCHEMA_2: &str = "schema_name_2";

#[rstest::fixture]
fn catalog_manager() -> CatalogManager {
    CatalogManager::default()
}

#[rstest::fixture]
fn catalog_manager_with_schema(catalog_manager: CatalogManager) -> CatalogManager {
    catalog_manager.create_schema(SCHEMA).expect("schema is created");
    catalog_manager
}
