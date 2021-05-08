// Copyright 2020 - 2021 Alex Dukhno
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

use binary::BinaryValue;
use data_definition_execution_plan::{
    CreateIndexQuery, CreateSchemaQuery, CreateTableQuery, DropSchemasQuery, DropTablesQuery, ExecutionError,
    ExecutionOutcome, SchemaChange,
};
use definition::{ColumnDef, FullTableName, SchemaName, TableDef};
use storage::Transaction;
use types::{SqlType, SqlTypeFamily};

const DEFINITION_SCHEMA: &str = "DEFINITION_SCHEMA";
const SCHEMATA_TABLE: &str = "SCHEMATA";
const TABLES_TABLE: &str = "TABLES";
const INDEXES_TABLE: &str = "TABLES";
const COLUMNS_TABLE: &str = "COLUMNS";

pub struct CatalogHandler<'c> {
    transaction: Transaction<'c>,
}

impl<'c> From<Transaction<'c>> for CatalogHandler<'c> {
    fn from(transaction: Transaction<'c>) -> CatalogHandler {
        CatalogHandler { transaction }
    }
}

impl<'c> CatalogHandler<'c> {
    pub fn schema_exists(&self, schema_name: &SchemaName) -> bool {
        self.transaction
            .lookup_table_ref(format!("{}.{}", DEFINITION_SCHEMA, SCHEMATA_TABLE))
            .scan()
            .any(|(_key, value)| {
                let value = value;
                value[1] == schema_name.as_ref()
            })
    }

    pub fn table_definition(&self, full_table_name: FullTableName) -> Option<Option<TableDef>> {
        if !self.schema_exists(&SchemaName::from(&full_table_name.schema())) {
            None
        } else {
            match self
                .transaction
                .lookup_table_ref(format!("{}.{}", DEFINITION_SCHEMA, TABLES_TABLE))
                .scan()
                .find(|(_key, value)| {
                    let value = value;
                    value[1] == full_table_name.schema() && value[2] == full_table_name.table()
                })
                .map(|(key, _value)| key)
            {
                Some(full_table_id) => {
                    let columns = self
                        .transaction
                        .lookup_table_ref(format!("{}.{}", DEFINITION_SCHEMA, COLUMNS_TABLE))
                        .scan()
                        .filter(|(key, _value)| key.starts_with(&full_table_id))
                        .map(|(_key, value)| {
                            let row = value;
                            let name = row[3].as_string();
                            let sql_type = SqlType::from_type_id(row[4].as_u64(), row[5].as_u64());
                            let ord_num = row[6].as_u64() as usize;
                            ColumnDef::new(name, sql_type, ord_num)
                        })
                        .collect();

                    Some(Some(TableDef::new(full_table_name, columns)))
                }
                None => Some(None),
            }
        }
    }

    pub fn columns(&self, full_table_name: &FullTableName) -> Vec<(String, SqlTypeFamily)> {
        self.columns_short(full_table_name)
            .into_iter()
            .map(|(name, sql_type)| (name, sql_type.family()))
            .collect()
    }

    pub fn columns_short(&self, full_table_name: &FullTableName) -> Vec<(String, SqlType)> {
        let full_table_id = self
            .transaction
            .lookup_table_ref(format!("{}.{}", DEFINITION_SCHEMA, TABLES_TABLE))
            .scan()
            .find(|(_key, value)| {
                let value = value;
                value[1] == full_table_name.schema() && value[2] == full_table_name.table()
            })
            .map(|(key, _value)| key)
            .unwrap();

        self.transaction
            .lookup_table_ref(format!("{}.{}", DEFINITION_SCHEMA, COLUMNS_TABLE))
            .scan()
            .filter(|(key, _value)| key.starts_with(&full_table_id))
            .map(|(_key, value)| {
                let row = value;
                let name = row[3].as_string();
                let sql_type = SqlType::from_type_id(row[4].as_u64(), row[5].as_u64());
                (name, sql_type)
            })
            .collect()
    }

    pub fn apply(&self, schema_change: SchemaChange) -> Result<ExecutionOutcome, ExecutionError> {
        match schema_change {
            SchemaChange::CreateSchema(CreateSchemaQuery {
                schema_name,
                if_not_exists,
            }) => {
                if self.schema_exists(&SchemaName::from(&schema_name.as_ref())) {
                    if if_not_exists {
                        Ok(ExecutionOutcome::SchemaCreated)
                    } else {
                        Err(ExecutionError::SchemaAlreadyExists(schema_name.as_ref().to_owned()))
                    }
                } else {
                    self.transaction
                        .lookup_table_ref(format!("{}.{}", DEFINITION_SCHEMA, SCHEMATA_TABLE))
                        .write(vec![
                            BinaryValue::from("IN_MEMORY"),
                            BinaryValue::from(schema_name.as_ref()),
                        ]);
                    Ok(ExecutionOutcome::SchemaCreated)
                }
            }
            SchemaChange::DropSchemas(DropSchemasQuery {
                schema_names,
                cascade,
                if_exists,
            }) => {
                let schemas_table = self
                    .transaction
                    .lookup_table_ref(format!("{}.{}", DEFINITION_SCHEMA, SCHEMATA_TABLE));
                let tables_table = self
                    .transaction
                    .lookup_table_ref(format!("{}.{}", DEFINITION_SCHEMA, TABLES_TABLE));
                for schema_name in schema_names {
                    let full_schema_name =
                        vec![BinaryValue::from("IN_MEMORY"), BinaryValue::from(schema_name.as_ref())];

                    let schema_id = schemas_table
                        .scan()
                        .find(|(_key, value)| value.starts_with(&full_schema_name))
                        .map(|(key, _value)| key);

                    match schema_id {
                        None => {
                            if !if_exists {
                                return Err(ExecutionError::SchemaDoesNotExist(schema_name.as_ref().to_owned()));
                            }
                        }
                        Some(schema_id) => {
                            let is_empty = tables_table
                                .scan()
                                .find(|(_key, value)| {
                                    let value = value;
                                    value[0] == "IN_MEMORY" && value[1] == schema_name.as_ref()
                                })
                                .is_none();
                            if !is_empty && !cascade {
                                return Err(ExecutionError::SchemaHasDependentObjects(
                                    schema_name.as_ref().to_owned(),
                                ));
                            } else {
                                let columns_table = self
                                    .transaction
                                    .lookup_table_ref(format!("{}.{}", DEFINITION_SCHEMA, COLUMNS_TABLE));
                                for column_key in columns_table
                                    .scan()
                                    .filter(|(_key, value)| {
                                        let value = value;
                                        value[1] == schema_name.as_ref()
                                    })
                                    .map(|(key, _value)| key)
                                {
                                    columns_table.write_key(column_key, None);
                                }

                                for (table_key, table_name) in tables_table
                                    .scan()
                                    .filter(|(_key, value)| {
                                        let value = value;
                                        value[1] == schema_name.as_ref()
                                    })
                                    .map(|(key, value)| {
                                        let value = value;
                                        (key, format!("{}.{}", value[1], value[2]))
                                    })
                                {
                                    tables_table.write_key(table_key, None);
                                    self.transaction.drop_tree(table_name);
                                }

                                schemas_table.write_key(schema_id, None);
                            }
                        }
                    }
                }
                Ok(ExecutionOutcome::SchemaDropped)
            }
            SchemaChange::CreateTable(CreateTableQuery {
                full_table_name,
                column_defs,
                if_not_exists,
            }) => {
                let schemas_table = self
                    .transaction
                    .lookup_table_ref(format!("{}.{}", DEFINITION_SCHEMA, SCHEMATA_TABLE));
                let full_schema_name = vec![
                    BinaryValue::from("IN_MEMORY"),
                    BinaryValue::from(full_table_name.schema()),
                ];

                let schema_id = schemas_table
                    .scan()
                    .find(|(_key, value)| value.starts_with(&full_schema_name))
                    .map(|(key, _value)| key);

                match schema_id {
                    None => Err(ExecutionError::SchemaDoesNotExist(full_table_name.schema().to_owned())),
                    Some(_full_schema_id) => {
                        let tables_table = self
                            .transaction
                            .lookup_table_ref(format!("{}.{}", DEFINITION_SCHEMA, TABLES_TABLE));
                        let table_id = tables_table.scan().find(|(_key, value)| {
                            let value = value;
                            value[0] == "IN_MEMORY"
                                && value[1] == full_table_name.schema()
                                && value[2] == full_table_name.table()
                        });
                        log::trace!("DEBUG {:?}", table_id);
                        match table_id {
                            Some(_table_id) => {
                                if if_not_exists {
                                    Ok(ExecutionOutcome::TableCreated)
                                } else {
                                    Err(ExecutionError::TableAlreadyExists(
                                        full_table_name.schema().to_owned(),
                                        full_table_name.table().to_owned(),
                                    ))
                                }
                            }
                            None => {
                                let full_table_name_record = vec![
                                    BinaryValue::from("IN_MEMORY"),
                                    BinaryValue::from(full_table_name.schema()),
                                    BinaryValue::from(full_table_name.table()),
                                ];
                                let full_table_id = tables_table.write(full_table_name_record);

                                let columns_table = self
                                    .transaction
                                    .lookup_table_ref(format!("{}.{}", DEFINITION_SCHEMA, COLUMNS_TABLE));

                                for (index, def) in column_defs.iter().enumerate() {
                                    let record = vec![
                                        BinaryValue::from("IN_MEMORY"),
                                        BinaryValue::from(full_table_name.schema()),
                                        BinaryValue::from(full_table_name.table()),
                                        BinaryValue::from(def.name.clone()),
                                        BinaryValue::from_u64(def.sql_type.type_id()),
                                        BinaryValue::from_u64(def.sql_type.chars_len().unwrap_or_default()),
                                        BinaryValue::from_u64(index as u64),
                                    ];
                                    let mut key = full_table_id.clone();
                                    key.push(BinaryValue::from_u64(index as u64));
                                    columns_table.write_key(key, Some(record));
                                }

                                self.transaction.create_tree(&full_table_name);

                                Ok(ExecutionOutcome::TableCreated)
                            }
                        }
                    }
                }
            }
            // cascade does not make sense for now, but when `FOREIGN KEY`s will be introduce it will become relevant
            SchemaChange::DropTables(DropTablesQuery {
                full_table_names,
                cascade: _cascade,
                if_exists,
            }) => {
                let schemas_table = self
                    .transaction
                    .lookup_table_ref(format!("{}.{}", DEFINITION_SCHEMA, SCHEMATA_TABLE));
                let tables_table = self
                    .transaction
                    .lookup_table_ref(format!("{}.{}", DEFINITION_SCHEMA, TABLES_TABLE));
                let columns_table = self
                    .transaction
                    .lookup_table_ref(format!("{}.{}", DEFINITION_SCHEMA, COLUMNS_TABLE));

                for full_table_name in full_table_names {
                    let full_schema_name = vec![
                        BinaryValue::from("IN_MEMORY"),
                        BinaryValue::from(full_table_name.schema()),
                    ];

                    let schema_id = schemas_table
                        .scan()
                        .find(|(_key, value)| value.starts_with(&full_schema_name))
                        .map(|(key, _value)| key);

                    match schema_id {
                        None => return Err(ExecutionError::SchemaDoesNotExist(full_table_name.schema().to_owned())),
                        Some(_full_schema_id) => {
                            let table_id = tables_table
                                .scan()
                                .find(|(_key, value)| {
                                    let value = value;
                                    value[1] == full_table_name.schema() && value[2] == full_table_name.table()
                                })
                                .map(|(key, _value)| key);
                            match table_id {
                                None => {
                                    if !if_exists {
                                        return Err(ExecutionError::TableDoesNotExist(
                                            full_table_name.schema().to_owned(),
                                            full_table_name.table().to_owned(),
                                        ));
                                    }
                                }
                                Some(full_table_id) => {
                                    for column_key in columns_table
                                        .scan()
                                        .filter(|(key, _value)| key.starts_with(&full_table_id))
                                        .map(|(key, _value)| key)
                                    {
                                        columns_table.write_key(column_key, None);
                                    }
                                    tables_table.write_key(full_table_id, None);
                                    self.transaction.drop_tree(&full_table_name);
                                }
                            }
                        }
                    }
                }
                Ok(ExecutionOutcome::TableDropped)
            }
            SchemaChange::CreateIndex(CreateIndexQuery {
                name,
                full_table_name,
                column_names,
            }) => {
                let schemas_table = self
                    .transaction
                    .lookup_table_ref(format!("{}.{}", DEFINITION_SCHEMA, SCHEMATA_TABLE));
                let tables_table = self
                    .transaction
                    .lookup_table_ref(format!("{}.{}", DEFINITION_SCHEMA, TABLES_TABLE));
                let columns_table = self
                    .transaction
                    .lookup_table_ref(format!("{}.{}", DEFINITION_SCHEMA, COLUMNS_TABLE));
                let indexes_table = self
                    .transaction
                    .lookup_table_ref(format!("{}.{}", DEFINITION_SCHEMA, INDEXES_TABLE));

                let full_schema_name = vec![
                    BinaryValue::from("IN_MEMORY"),
                    BinaryValue::from(full_table_name.schema()),
                ];

                let schema_id = schemas_table
                    .scan()
                    .find(|(_key, value)| value.starts_with(&full_schema_name))
                    .map(|(key, _value)| key);

                match schema_id {
                    None => Err(ExecutionError::SchemaDoesNotExist(full_table_name.schema().to_owned())),
                    Some(full_schema_id) => {
                        let table_id = tables_table
                            .scan()
                            .find(|(key, value)| {
                                let value = value;
                                key.starts_with(&full_schema_id) && value[2] == full_table_name.table()
                            })
                            .map(|(key, _value)| key);
                        match table_id {
                            None => Err(ExecutionError::TableDoesNotExist(
                                full_table_name.schema().to_owned(),
                                full_table_name.table().to_owned(),
                            )),
                            Some(full_table_id) => {
                                let table_columns = columns_table
                                    .scan()
                                    .filter(|(key, _value)| key.starts_with(&full_table_id))
                                    .map(|(_key, value)| {
                                        let row = value;
                                        let name = row[3].as_string();
                                        let sql_type = SqlType::from_type_id(row[4].as_u64(), row[5].as_u64());
                                        let ord_num = row[6].as_u64() as usize;
                                        ColumnDef::new(name, sql_type, ord_num)
                                    })
                                    .collect::<Vec<_>>();
                                let mut column_indexes = vec![];
                                for column_name in column_names.iter() {
                                    if let Some(col_def) = table_columns.iter().find(|col| col.has_name(column_name)) {
                                        column_indexes.push(col_def.index());
                                    } else {
                                        return Err(ExecutionError::ColumnNotFound(column_names[0].to_owned()));
                                    }
                                }
                                indexes_table.write(vec![
                                    BinaryValue::from("IN_MEMORY"),
                                    BinaryValue::from(full_table_name.schema()),
                                    BinaryValue::from(full_table_name.table()),
                                    BinaryValue::from(name.clone()),
                                    BinaryValue::from(column_names.join(", ")),
                                ]);

                                self.transaction.create_tree(format!(
                                    "{}.{}.{}",
                                    full_table_name.schema(),
                                    full_table_name.table(),
                                    name
                                ));
                                Ok(ExecutionOutcome::IndexCreated)
                            }
                        }
                    }
                }
            }
        }
    }
}
