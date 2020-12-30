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

use analysis::{CreateTableQuery, DropSchemasQuery, DropTablesQuery, SchemaChange, TableInfo};
use catalog::DatabaseHandleNew;
use data_definition::DataDefOperationExecutor;
use data_manager::DatabaseHandle;
use definition_operations::{SystemObject, SystemOperation};
use std::sync::Arc;

pub struct SystemSchemaExecutor {
    data_manager: Arc<DatabaseHandle>,
    database: DatabaseHandleNew,
}

impl SystemSchemaExecutor {
    pub fn new(data_manager: Arc<DatabaseHandle>, database: DatabaseHandleNew) -> SystemSchemaExecutor {
        SystemSchemaExecutor { data_manager, database }
    }

    pub fn execute(
        &self,
        change: &SchemaChange,
        operations: Vec<SystemOperation>,
    ) -> Result<ExecutionOutcome, ExecutionError> {
        for operation in operations {
            match self.execute_on_data_manager(change, &operation) {
                Ok(false) => {}
                Ok(true) => break,
                Err(e) => return Err(e),
            }
        }
        match change {
            SchemaChange::CreateSchema(_) => Ok(ExecutionOutcome::SchemaCreated),
            SchemaChange::DropSchemas(_) => Ok(ExecutionOutcome::SchemaDropped),
            SchemaChange::CreateTable(_) => Ok(ExecutionOutcome::TableCreated),
            SchemaChange::DropTables(_) => Ok(ExecutionOutcome::TableDropped),
        }
    }

    fn execute_on_data_manager(
        &self,
        change: &SchemaChange,
        operation: &SystemOperation,
    ) -> Result<bool, ExecutionError> {
        let result = self.data_manager.execute(&operation);
        match (change, operation, result) {
            (SchemaChange::CreateSchema(_), SystemOperation::CheckExistence { object_name, .. }, Ok(())) => {
                return Err(ExecutionError::SchemaAlreadyExists(object_name.clone()))
            }
            (SchemaChange::CreateSchema(_), _, _) => {}
            (
                SchemaChange::DropSchemas(DropSchemasQuery { if_exists: true, .. }),
                SystemOperation::CheckExistence { .. },
                Err(()),
            ) => return Ok(true),
            (
                SchemaChange::DropSchemas(DropSchemasQuery { if_exists: false, .. }),
                SystemOperation::CheckExistence { object_name, .. },
                Err(()),
            ) => return Err(ExecutionError::SchemaDoesNotExist(object_name.clone())),
            (SchemaChange::DropSchemas(_), _, _) => {}
            (
                SchemaChange::CreateTable(CreateTableQuery {
                    table_info:
                        TableInfo {
                            schema_name,
                            table_name,
                            ..
                        },
                    ..
                }),
                SystemOperation::CheckExistence {
                    system_object: SystemObject::Table,
                    ..
                },
                Ok(()),
            ) => {
                return Err(ExecutionError::TableAlreadyExists(
                    schema_name.to_owned(),
                    table_name.to_owned(),
                ))
            }
            (
                SchemaChange::CreateTable(CreateTableQuery {
                    table_info: TableInfo { schema_name, .. },
                    ..
                }),
                SystemOperation::CheckExistence {
                    system_object: SystemObject::Schema,
                    ..
                },
                Err(()),
            ) => return Err(ExecutionError::SchemaDoesNotExist(schema_name.to_owned())),
            (SchemaChange::CreateTable(_), _, _) => {}
            (
                SchemaChange::DropTables(DropTablesQuery { if_exists: false, .. }),
                SystemOperation::CheckExistence {
                    system_object: SystemObject::Schema,
                    object_name,
                },
                Err(()),
            ) => return Err(ExecutionError::SchemaDoesNotExist(object_name.clone())),
            (
                SchemaChange::DropTables(DropTablesQuery {
                    table_infos,
                    if_exists: false,
                    ..
                }),
                SystemOperation::CheckExistence {
                    system_object: SystemObject::Table,
                    ..
                },
                Err(()),
            ) => {
                return Err(ExecutionError::TableDoesNotExists(
                    table_infos[0].schema_name.to_string(),
                    table_infos[0].table_name.to_string(),
                ))
            }
            (
                SchemaChange::DropTables(DropTablesQuery { if_exists: true, .. }),
                SystemOperation::CheckExistence {
                    system_object: SystemObject::Table,
                    ..
                },
                Err(()),
            ) => {
                return Ok(true);
            }
            (SchemaChange::DropTables(_), _, _) => {}
        }
        Ok(false)
    }
}

#[derive(Debug, PartialEq)]
pub enum ExecutionOutcome {
    SchemaCreated,
    SchemaDropped,
    TableCreated,
    TableDropped,
}

#[derive(Debug, PartialEq)]
pub enum ExecutionError {
    SchemaAlreadyExists(String),
    SchemaDoesNotExist(String),
    TableAlreadyExists(String, String),
    TableDoesNotExists(String, String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use analysis::{CreateSchemaQuery, CreateTableQuery, DropSchemasQuery, DropTablesQuery, SchemaName, TableInfo};
    use data_manager::{DEFAULT_CATALOG, DEFINITION_SCHEMA, SCHEMATA_TABLE, TABLES_TABLE};
    use definition_operations::{Record, SystemObject};

    const SCHEMA: &str = "schema_name";
    const TABLE: &str = "table_name";

    fn executor() -> SystemSchemaExecutor {
        SystemSchemaExecutor::new(Arc::new(DatabaseHandle::in_memory()), DatabaseHandleNew::in_memory())
    }

    #[test]
    fn create_same_schema() {
        let executor = executor();
        assert_eq!(
            executor.execute(
                &SchemaChange::CreateSchema(CreateSchemaQuery {
                    schema_name: SchemaName::from(&SCHEMA),
                    if_not_exists: false,
                }),
                vec![
                    SystemOperation::CheckExistence {
                        system_object: SystemObject::Schema,
                        object_name: SCHEMA.to_owned(),
                    },
                    SystemOperation::CreateFolder {
                        name: SCHEMA.to_owned(),
                    },
                    SystemOperation::CreateRecord {
                        system_schema: DEFINITION_SCHEMA.to_owned(),
                        system_table: SCHEMATA_TABLE.to_owned(),
                        record: Record::Schema {
                            catalog_name: DEFAULT_CATALOG.to_owned(),
                            schema_name: SCHEMA.to_owned(),
                        },
                    },
                ]
            ),
            Ok(ExecutionOutcome::SchemaCreated)
        );

        assert_eq!(
            executor.execute(
                &SchemaChange::CreateSchema(CreateSchemaQuery {
                    schema_name: SchemaName::from(&SCHEMA),
                    if_not_exists: false,
                }),
                vec![
                    SystemOperation::CheckExistence {
                        system_object: SystemObject::Schema,
                        object_name: SCHEMA.to_owned(),
                    },
                    SystemOperation::CreateFolder {
                        name: SCHEMA.to_owned(),
                    },
                    SystemOperation::CreateRecord {
                        system_schema: DEFINITION_SCHEMA.to_owned(),
                        system_table: SCHEMATA_TABLE.to_owned(),
                        record: Record::Schema {
                            catalog_name: DEFAULT_CATALOG.to_owned(),
                            schema_name: SCHEMA.to_owned(),
                        },
                    },
                ]
            ),
            Err(ExecutionError::SchemaAlreadyExists(SCHEMA.to_owned()))
        );
    }

    #[test]
    fn drop_nonexistent_schema() {
        let executor = executor();

        assert_eq!(
            executor.execute(
                &SchemaChange::DropSchemas(DropSchemasQuery {
                    schema_names: vec![SchemaName::from(&SCHEMA)],
                    cascade: false,
                    if_exists: false
                }),
                vec![
                    SystemOperation::CheckExistence {
                        system_object: SystemObject::Schema,
                        object_name: SCHEMA.to_owned()
                    },
                    SystemOperation::CheckDependants {
                        system_object: SystemObject::Schema,
                        object_name: SCHEMA.to_owned()
                    },
                    SystemOperation::RemoveRecord {
                        system_schema: DEFINITION_SCHEMA.to_owned(),
                        system_table: SCHEMATA_TABLE.to_owned(),
                        record: Record::Schema {
                            catalog_name: DEFAULT_CATALOG.to_owned(),
                            schema_name: SCHEMA.to_owned()
                        }
                    },
                    SystemOperation::RemoveFolder {
                        name: SCHEMA.to_owned()
                    }
                ]
            ),
            Err(ExecutionError::SchemaDoesNotExist(SCHEMA.to_owned()))
        );
    }

    #[test]
    fn create_table_where_schema_not_found() {
        let executor = executor();

        assert_eq!(
            executor.execute(
                &SchemaChange::CreateTable(CreateTableQuery {
                    table_info: TableInfo::new(0, &SCHEMA, &TABLE),
                    column_defs: vec![],
                    if_not_exists: false,
                }),
                vec![
                    SystemOperation::CheckExistence {
                        system_object: SystemObject::Schema,
                        object_name: SCHEMA.to_owned()
                    },
                    SystemOperation::CheckExistence {
                        system_object: SystemObject::Table,
                        object_name: TABLE.to_owned()
                    },
                    SystemOperation::CreateFile {
                        folder_name: SCHEMA.to_owned(),
                        name: TABLE.to_owned()
                    },
                    SystemOperation::CreateRecord {
                        system_schema: DEFINITION_SCHEMA.to_owned(),
                        system_table: TABLES_TABLE.to_owned(),
                        record: Record::Table {
                            catalog_name: DEFAULT_CATALOG.to_owned(),
                            schema_name: SCHEMA.to_owned(),
                            table_name: TABLE.to_owned(),
                        }
                    }
                ]
            ),
            Err(ExecutionError::SchemaDoesNotExist(SCHEMA.to_owned()))
        );
    }

    #[test]
    fn create_same_table() {
        let executor = executor();

        if executor
            .execute(
                &SchemaChange::CreateSchema(CreateSchemaQuery {
                    schema_name: SchemaName::from(&SCHEMA),
                    if_not_exists: false,
                }),
                vec![
                    SystemOperation::CheckExistence {
                        system_object: SystemObject::Schema,
                        object_name: SCHEMA.to_owned(),
                    },
                    SystemOperation::CreateFolder {
                        name: SCHEMA.to_owned(),
                    },
                    SystemOperation::CreateRecord {
                        system_schema: DEFINITION_SCHEMA.to_owned(),
                        system_table: SCHEMATA_TABLE.to_owned(),
                        record: Record::Schema {
                            catalog_name: DEFAULT_CATALOG.to_owned(),
                            schema_name: SCHEMA.to_owned(),
                        },
                    },
                ],
            )
            .is_ok()
        {}

        assert_eq!(
            executor.execute(
                &SchemaChange::CreateTable(CreateTableQuery {
                    table_info: TableInfo::new(0, &SCHEMA, &TABLE),
                    column_defs: vec![],
                    if_not_exists: false,
                }),
                vec![
                    SystemOperation::CheckExistence {
                        system_object: SystemObject::Schema,
                        object_name: SCHEMA.to_owned()
                    },
                    SystemOperation::CheckExistence {
                        system_object: SystemObject::Table,
                        object_name: TABLE.to_owned()
                    },
                    SystemOperation::CreateFile {
                        folder_name: SCHEMA.to_owned(),
                        name: TABLE.to_owned()
                    },
                    SystemOperation::CreateRecord {
                        system_schema: DEFINITION_SCHEMA.to_owned(),
                        system_table: TABLES_TABLE.to_owned(),
                        record: Record::Table {
                            catalog_name: DEFAULT_CATALOG.to_owned(),
                            schema_name: SCHEMA.to_owned(),
                            table_name: TABLE.to_owned(),
                        }
                    }
                ]
            ),
            Ok(ExecutionOutcome::TableCreated)
        );

        assert_eq!(
            executor.execute(
                &SchemaChange::CreateTable(CreateTableQuery {
                    table_info: TableInfo::new(0, &SCHEMA, &TABLE),
                    column_defs: vec![],
                    if_not_exists: false,
                }),
                vec![
                    SystemOperation::CheckExistence {
                        system_object: SystemObject::Schema,
                        object_name: SCHEMA.to_owned()
                    },
                    SystemOperation::CheckExistence {
                        system_object: SystemObject::Table,
                        object_name: TABLE.to_owned()
                    },
                    SystemOperation::CreateFile {
                        folder_name: SCHEMA.to_owned(),
                        name: TABLE.to_owned()
                    },
                    SystemOperation::CreateRecord {
                        system_schema: DEFINITION_SCHEMA.to_owned(),
                        system_table: TABLES_TABLE.to_owned(),
                        record: Record::Table {
                            catalog_name: DEFAULT_CATALOG.to_owned(),
                            schema_name: SCHEMA.to_owned(),
                            table_name: TABLE.to_owned(),
                        }
                    }
                ]
            ),
            Err(ExecutionError::TableAlreadyExists(SCHEMA.to_owned(), TABLE.to_owned()))
        );
    }

    #[test]
    fn drop_table_where_schema_not_found() {
        let executor = executor();

        assert_eq!(
            executor.execute(
                &SchemaChange::DropTables(DropTablesQuery {
                    table_infos: vec![TableInfo::new(0, &SCHEMA, &TABLE),],
                    cascade: false,
                    if_exists: false
                }),
                vec![
                    SystemOperation::CheckExistence {
                        system_object: SystemObject::Schema,
                        object_name: SCHEMA.to_owned()
                    },
                    SystemOperation::CheckExistence {
                        system_object: SystemObject::Table,
                        object_name: TABLE.to_owned()
                    },
                    SystemOperation::CheckDependants {
                        system_object: SystemObject::Table,
                        object_name: TABLE.to_owned()
                    },
                    SystemOperation::RemoveColumns {
                        schema_name: SCHEMA.to_owned(),
                        table_name: TABLE.to_owned()
                    },
                    SystemOperation::RemoveRecord {
                        system_schema: DEFINITION_SCHEMA.to_owned(),
                        system_table: TABLES_TABLE.to_owned(),
                        record: Record::Table {
                            catalog_name: DEFAULT_CATALOG.to_owned(),
                            schema_name: SCHEMA.to_owned(),
                            table_name: TABLE.to_owned(),
                        }
                    },
                    SystemOperation::RemoveFile {
                        folder_name: SCHEMA.to_owned(),
                        name: TABLE.to_owned()
                    },
                ]
            ),
            Err(ExecutionError::SchemaDoesNotExist(SCHEMA.to_owned()))
        );
    }

    #[test]
    fn drop_nonexistent_table() {
        let executor = executor();

        if executor
            .execute(
                &SchemaChange::CreateSchema(CreateSchemaQuery {
                    schema_name: SchemaName::from(&SCHEMA),
                    if_not_exists: false,
                }),
                vec![
                    SystemOperation::CheckExistence {
                        system_object: SystemObject::Schema,
                        object_name: SCHEMA.to_owned(),
                    },
                    SystemOperation::CreateFolder {
                        name: SCHEMA.to_owned(),
                    },
                    SystemOperation::CreateRecord {
                        system_schema: DEFINITION_SCHEMA.to_owned(),
                        system_table: SCHEMATA_TABLE.to_owned(),
                        record: Record::Schema {
                            catalog_name: DEFAULT_CATALOG.to_owned(),
                            schema_name: SCHEMA.to_owned(),
                        },
                    },
                ],
            )
            .is_ok()
        {}

        assert_eq!(
            executor.execute(
                &SchemaChange::DropTables(DropTablesQuery {
                    table_infos: vec![TableInfo::new(0, &SCHEMA, &TABLE),],
                    cascade: false,
                    if_exists: false
                }),
                vec![
                    SystemOperation::CheckExistence {
                        system_object: SystemObject::Schema,
                        object_name: SCHEMA.to_owned()
                    },
                    SystemOperation::CheckExistence {
                        system_object: SystemObject::Table,
                        object_name: TABLE.to_owned()
                    },
                    SystemOperation::CheckDependants {
                        system_object: SystemObject::Table,
                        object_name: TABLE.to_owned()
                    },
                    SystemOperation::RemoveColumns {
                        schema_name: SCHEMA.to_owned(),
                        table_name: TABLE.to_owned()
                    },
                    SystemOperation::RemoveRecord {
                        system_schema: DEFINITION_SCHEMA.to_owned(),
                        system_table: TABLES_TABLE.to_owned(),
                        record: Record::Table {
                            catalog_name: DEFAULT_CATALOG.to_owned(),
                            schema_name: SCHEMA.to_owned(),
                            table_name: TABLE.to_owned(),
                        }
                    },
                    SystemOperation::RemoveFile {
                        folder_name: SCHEMA.to_owned(),
                        name: TABLE.to_owned()
                    },
                ]
            ),
            Err(ExecutionError::TableDoesNotExists(SCHEMA.to_owned(), TABLE.to_owned()))
        );
    }
}
