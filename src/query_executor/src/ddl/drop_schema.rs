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

use data_manager::{DataManager, Database, DropSchemaError, DropStrategy};
use plan::SchemaId;
use protocol::{results::QueryEvent, Sender};

pub(crate) struct DropSchemaCommand<D: Database> {
    schema_id: SchemaId,
    cascade: bool,
    data_manager: Arc<DataManager<D>>,
    sender: Arc<dyn Sender>,
}

impl<D: Database> DropSchemaCommand<D> {
    pub(crate) fn new(
        schema_id: SchemaId,
        cascade: bool,
        data_manager: Arc<DataManager<D>>,
        sender: Arc<dyn Sender>,
    ) -> DropSchemaCommand<D> {
        DropSchemaCommand {
            schema_id,
            cascade,
            data_manager,
            sender,
        }
    }

    pub(crate) fn execute(&mut self) {
        let strategy = if self.cascade {
            DropStrategy::Cascade
        } else {
            DropStrategy::Restrict
        };
        match self.data_manager.drop_schema(&self.schema_id, strategy) {
            Err(()) => log::error!("Error while dropping schema {:?}", self.schema_id),
            Ok(Err(DropSchemaError::CatalogDoesNotExist)) => {
                //ignore. Catalogs are not implemented
            }
            Ok(Err(DropSchemaError::HasDependentObjects)) => {
                // self.sender
                //     .send(Err(QueryError::schema_has_dependent_objects(self.name.name().to_string())))
                //     .expect("To Send Query Result to Client");
                // ignore. need to be able to lookup the object name from the id.
            }
            Ok(Err(DropSchemaError::DoesNotExist)) => {
                // self.sender
                //     .send(Err(QueryError::schema_does_not_exist(schema_name)))
                //     .expect("To Send Query Result to Client");
                // ignore. parallel query execution is not implemented
            }
            Ok(Ok(())) => {
                self.sender
                    .send(Ok(QueryEvent::SchemaDropped))
                    .expect("To Send Query Result to Client");
            }
        }
    }
}
