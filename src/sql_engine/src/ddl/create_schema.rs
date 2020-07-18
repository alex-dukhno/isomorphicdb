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

use crate::query::plan::SchemaCreationInfo;
use kernel::SystemResult;
use protocol::results::{QueryErrorBuilder, QueryEvent, QueryResult};
use std::sync::{Arc, Mutex};
use storage::{backend::BackendStorage, frontend::FrontendStorage, SchemaAlreadyExists};

pub(crate) struct CreateSchemaCommand<P: BackendStorage> {
    schema_info: SchemaCreationInfo,
    storage: Arc<Mutex<FrontendStorage<P>>>,
}

impl<P: BackendStorage> CreateSchemaCommand<P> {
    pub(crate) fn new(
        schema_info: SchemaCreationInfo,
        storage: Arc<Mutex<FrontendStorage<P>>>,
    ) -> CreateSchemaCommand<P> {
        CreateSchemaCommand { schema_info, storage }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<QueryResult> {
        let schema_name = &self.schema_info.schema_name;
        match (self.storage.lock().unwrap()).create_schema(schema_name)? {
            Ok(()) => Ok(Ok(QueryEvent::SchemaCreated)),
            Err(SchemaAlreadyExists) => Ok(Err(QueryErrorBuilder::new()
                .schema_already_exists(schema_name.clone())
                .build())),
        }
    }
}
