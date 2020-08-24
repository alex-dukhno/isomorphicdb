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

use crate::catalog_manager::DropStrategy;
use crate::{catalog_manager::CatalogManager, query::SchemaId};
use kernel::SystemResult;
use protocol::{results::QueryEvent, Sender};
use std::sync::Arc;

pub(crate) struct DropSchemaCommand {
    name: SchemaId,
    cascade: bool,
    storage: Arc<CatalogManager>,
    session: Arc<dyn Sender>,
}

impl DropSchemaCommand {
    pub(crate) fn new(
        name: SchemaId,
        cascade: bool,
        storage: Arc<CatalogManager>,
        session: Arc<dyn Sender>,
    ) -> DropSchemaCommand {
        DropSchemaCommand {
            name,
            cascade,
            storage,
            session,
        }
    }

    pub(crate) fn execute(&mut self) -> SystemResult<()> {
        let schema_name = self.name.name().to_string();
        let strategy = if self.cascade {
            DropStrategy::Cascade
        } else {
            DropStrategy::Restrict
        };
        match self.storage.drop_schema(&schema_name, strategy) {
            Err(error) => Err(error),
            Ok(()) => {
                self.session
                    .send(Ok(QueryEvent::SchemaDropped))
                    .expect("To Send Query Result to Client");
                Ok(())
            }
        }
    }
}
