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

use data_manager::DataManager;
use plan::SchemaId;
use sql_model::DropStrategy;
use std::sync::Arc;

pub(crate) struct DropSchemaCommand {
    schema_id: SchemaId,
    cascade: bool,
    data_manager: Arc<DataManager>,
}

impl DropSchemaCommand {
    pub(crate) fn new(schema_id: SchemaId, cascade: bool, data_manager: Arc<DataManager>) -> DropSchemaCommand {
        DropSchemaCommand {
            schema_id,
            cascade,
            data_manager,
        }
    }

    pub(crate) fn execute(&mut self) {
        let strategy = if self.cascade {
            DropStrategy::Cascade
        } else {
            DropStrategy::Restrict
        };
        self.data_manager
            .drop_schema(&self.schema_id, strategy)
            .expect("schema dropped")
            .expect("no storage error")
    }
}
