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

use definition_operations::SystemOperation;
use meta_def::{ColumnDefinition, Id};

pub type OptionalSchemaId = Option<Id>;
pub type OptionalTableId = Option<(Id, Option<Id>)>;
pub type OptionalTableDesc = Option<(Id, Option<(Id, Vec<ColumnDefinition>)>)>;

pub trait DataDefReader {
    fn schema_exists(&self, schema_name: &str) -> OptionalSchemaId;

    fn table_exists_tuple(&self, full_table_name: (&str, &str)) -> OptionalTableId {
        let (schema_name, table_name) = full_table_name;
        self.table_exists(schema_name, table_name)
    }

    fn table_desc(&self, full_table_name: (&str, &str)) -> OptionalTableDesc {
        match self.table_exists_tuple(full_table_name) {
            Some((schema_id, Some(table_id))) => {
                let columns = self
                    .table_columns(&(schema_id, table_id))
                    .expect("table exists")
                    .into_iter()
                    .map(|(_column_id, column)| column)
                    .collect::<Vec<ColumnDefinition>>();
                Some((schema_id, Some((table_id, columns))))
            }
            None => None,
            Some((schema_id, None)) => Some((schema_id, None)),
        }
    }

    fn table_exists(&self, schema_name: &str, table_name: &str) -> OptionalTableId;

    #[allow(clippy::result_unit_err)]
    fn table_columns(&self, table_id: &(Id, Id)) -> Result<Vec<(Id, ColumnDefinition)>, ()>;

    #[allow(clippy::result_unit_err)]
    fn column_ids(&self, table_id: &(Id, Id), names: &[String]) -> Result<(Vec<Id>, Vec<String>), ()>;

    fn column_defs(&self, table_id: &(Id, Id), ids: &[Id]) -> Vec<ColumnDefinition>;
}

pub trait DataDefOperationExecutor {
    #[allow(clippy::result_unit_err)]
    fn execute(&self, operation: &SystemOperation) -> Result<(), ()>;
}
