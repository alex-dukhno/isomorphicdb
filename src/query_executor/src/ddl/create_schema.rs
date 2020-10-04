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

use std::sync::Arc;

use data_manager::DataManager;
use plan::SchemaCreationInfo;
use protocol::{results::QueryEvent, Sender};
use storage::Database;

pub(crate) struct CreateSchemaCommand<D: Database> {
    schema_info: SchemaCreationInfo,
    data_manager: Arc<DataManager<D>>,
    sender: Arc<dyn Sender>,
}

impl<D: Database> CreateSchemaCommand<D> {
    pub(crate) fn new(
        schema_info: SchemaCreationInfo,
        data_manager: Arc<DataManager<D>>,
        sender: Arc<dyn Sender>,
    ) -> CreateSchemaCommand<D> {
        CreateSchemaCommand {
            schema_info,
            data_manager,
            sender,
        }
    }

    pub(crate) fn execute(&mut self) {
        let schema_name = &self.schema_info.schema_name;
        if let Err(()) = self.data_manager.create_schema(schema_name) {
            log::error!("Error while creating schema {}", schema_name);
        }
        self.sender
            .send(Ok(QueryEvent::SchemaCreated))
            .expect("To Send Query Result to Client");
    }
}
