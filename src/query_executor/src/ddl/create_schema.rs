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
use plan::SchemaCreationInfo;
use std::sync::Arc;

pub(crate) struct CreateSchemaCommand {
    schema_info: SchemaCreationInfo,
    data_manager: Arc<DataManager>,
}

impl CreateSchemaCommand {
    pub(crate) fn new(schema_info: SchemaCreationInfo, data_manager: Arc<DataManager>) -> CreateSchemaCommand {
        CreateSchemaCommand {
            schema_info,
            data_manager,
        }
    }

    pub(crate) fn execute(&mut self) {
        let schema_name = &self.schema_info.schema_name;
        if let Err(()) = self.data_manager.create_schema(schema_name) {
            log::error!("Error while creating schema {}", schema_name);
        }
    }
}
