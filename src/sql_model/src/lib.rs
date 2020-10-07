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

pub mod sql_errors;
pub mod sql_types;

pub type Id = u64;
pub type RecordId = Id;
pub type CatalogId = Id;
pub type SchemaId = Id;
pub type TableId = (Id, Id);

pub const DEFAULT_CATALOG: &str = "public";
pub const SYSTEM_CATALOG: &str = "system";

pub enum DropStrategy {
    Restrict,
    Cascade,
}

#[derive(Debug, PartialEq)]
pub enum DropSchemaError {
    CatalogDoesNotExist,
    DoesNotExist,
    HasDependentObjects,
}
