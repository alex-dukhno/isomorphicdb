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

use crate::{CatalogDefinition, Database, SqlSchema, SqlTable, TableInfo};
use definition_operations::{ExecutionError, ExecutionOutcome, SystemOperation};
use std::sync::Arc;
use types::SqlType;

pub struct OnDiskDatabase;

impl OnDiskDatabase {
    pub fn new(_name: &str) -> Arc<OnDiskDatabase> {
        Arc::new(OnDiskDatabase)
    }
}

impl CatalogDefinition for OnDiskDatabase {
    fn table_info(&self, table_full_name: (&str, &str)) -> Option<Option<TableInfo>> {
        unimplemented!()
    }
}

impl Database for OnDiskDatabase {
    type Schema = OnDiskSchema;
    type Table = OnDiskTable;

    fn execute(&self, _operation: SystemOperation) -> Result<ExecutionOutcome, ExecutionError> {
        unimplemented!()
    }
}

pub struct OnDiskSchema;

impl SqlSchema for OnDiskSchema {}

pub struct OnDiskTable;

impl SqlTable for OnDiskTable {}
