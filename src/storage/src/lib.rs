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

use binary::{Key, ReadCursor, StorageError, Values};
use sql_model::sql_errors::DefinitionError;
use std::io;

pub use in_memory::{InMemoryDatabase, InMemorySequence};
pub use persistent::{PersistentDatabase, PersistentSequence};
use std::sync::Arc;

mod in_memory;
mod persistent;

pub type SchemaName<'s> = &'s str;
pub type ObjectName<'o> = &'o str;
pub type Identifier = u64;
type Name = String;

pub enum InitStatus {
    Created,
    Loaded,
}

impl From<bool> for InitStatus {
    fn from(recovered: bool) -> Self {
        match recovered {
            true => InitStatus::Loaded,
            false => InitStatus::Created,
        }
    }
}

pub trait Sequence {
    fn next(&self) -> Identifier;
}

pub trait Database {
    fn create_sequence(&self, schema_name: &str, sequence_name: &str) -> Result<Arc<dyn Sequence>, DefinitionError> {
        self.create_sequence_with_step(schema_name, sequence_name, 1)
    }

    fn create_sequence_with_step(
        &self,
        schema_name: &str,
        sequence_name: &str,
        step: u64,
    ) -> Result<Arc<dyn Sequence>, DefinitionError>;

    fn drop_sequence(&self, schema_name: &str, sequence_name: &str) -> Result<(), DefinitionError>;

    fn get_sequence(&self, schema_name: &str, sequence_name: &str) -> Result<Arc<dyn Sequence>, DefinitionError>;

    fn create_schema(&self, schema_name: SchemaName) -> io::Result<Result<Result<(), DefinitionError>, StorageError>>;

    fn drop_schema(&self, schema_name: SchemaName) -> io::Result<Result<Result<(), DefinitionError>, StorageError>>;

    fn create_object(
        &self,
        schema_name: SchemaName,
        object_name: ObjectName,
    ) -> io::Result<Result<Result<(), DefinitionError>, StorageError>>;

    fn drop_object(
        &self,
        schema_name: SchemaName,
        object_name: ObjectName,
    ) -> io::Result<Result<Result<(), DefinitionError>, StorageError>>;

    fn write(
        &self,
        schema_name: SchemaName,
        object_name: ObjectName,
        values: Vec<(Key, Values)>,
    ) -> io::Result<Result<Result<usize, DefinitionError>, StorageError>>;

    fn read(
        &self,
        schema_name: SchemaName,
        object_name: ObjectName,
    ) -> io::Result<Result<Result<ReadCursor, DefinitionError>, StorageError>>;

    fn delete(
        &self,
        schema_name: SchemaName,
        object_name: ObjectName,
        keys: Vec<Key>,
    ) -> io::Result<Result<Result<usize, DefinitionError>, StorageError>>;
}

#[cfg(test)]
mod tests;
