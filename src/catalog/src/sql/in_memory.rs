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

use crate::{
    DataCatalog, DataTable, Database, InMemoryCatalogHandle, SchemaHandle, SqlSchema, SqlTable, COLUMNS_TABLE,
    DEFINITION_SCHEMA, SCHEMATA_TABLE, TABLES_TABLE,
};
use binary::Binary;
use definition_operations::{ExecutionError, ExecutionOutcome, Record, SystemObject, SystemOperation};
use repr::Datum;
use std::sync::Arc;

const CATALOG: Datum = Datum::from_str("IN_MEMORY");

pub struct InMemoryDatabase {
    catalog: InMemoryCatalogHandle,
}

impl InMemoryDatabase {
    pub fn new() -> Arc<InMemoryDatabase> {
        Arc::new(InMemoryDatabase::create().bootstrap())
    }

    fn create() -> InMemoryDatabase {
        InMemoryDatabase {
            catalog: InMemoryCatalogHandle::default(),
        }
    }

    fn bootstrap(self) -> InMemoryDatabase {
        self.catalog.create_schema(DEFINITION_SCHEMA);
        self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
            schema.create_table(SCHEMATA_TABLE);
            schema.create_table(TABLES_TABLE);
            schema.create_table(COLUMNS_TABLE);
        });
        self
    }
}

impl Database for InMemoryDatabase {
    type Schema = InMemorySchema;
    type Table = InMemoryTable;

    fn execute(&self, operations: &[SystemOperation]) -> Result<ExecutionOutcome, ExecutionError> {
        let end = operations.len();
        let mut index = 0;
        while index < end {
            let operation = &operations[index];
            index += 1;
            match operation {
                SystemOperation::CheckExistence {
                    system_object,
                    object_name,
                } => match system_object {
                    SystemObject::Schema => {
                        let result = self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
                            schema.work_with(SCHEMATA_TABLE, |table| {
                                table.scan().any(|(_key, value)| {
                                    println!("VALUE {:?}", value);
                                    value == Binary::pack(&[CATALOG, Datum::from_str(object_name)])
                                })
                            })
                        });
                        if let Some(Some(true)) = result {
                            return Err(ExecutionError::SchemaAlreadyExists(object_name.to_owned()));
                        }
                        if let Some(Some(false)) = result {
                            return Err(ExecutionError::SchemaDoesNotExist(object_name.to_owned()));
                        }
                    }
                    SystemObject::Table => unimplemented!(),
                },
                SystemOperation::CheckDependants { .. } => {}
                SystemOperation::RemoveDependants { .. } => {}
                SystemOperation::RemoveColumns { .. } => {}
                SystemOperation::SkipIf { .. } => {}
                SystemOperation::CreateFolder { name } => {
                    self.catalog.create_schema(name);
                }
                SystemOperation::RemoveFolder { .. } => {}
                SystemOperation::CreateFile { .. } => {}
                SystemOperation::RemoveFile { .. } => {}
                SystemOperation::RemoveRecord { .. } => {}
                SystemOperation::CreateRecord {
                    system_schema,
                    system_table,
                    record,
                } => match record {
                    Record::Schema {
                        catalog_name,
                        schema_name,
                    } => {
                        self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
                            schema.work_with(SCHEMATA_TABLE, |table| {
                                table.insert(vec![Binary::pack(&[CATALOG, Datum::from_str(schema_name)])])
                            })
                        });
                        return Ok(ExecutionOutcome::SchemaCreated);
                    }
                    Record::Table { .. } => {}
                    Record::Column { .. } => {}
                },
            }
        }
        unreachable!()
    }
}

pub struct InMemorySchema;

impl SqlSchema for InMemorySchema {}

pub struct InMemoryTable;

impl SqlTable for InMemoryTable {}

#[cfg(test)]
mod test {
    use super::*;

    const DEFAULT_CATALOG: &str = "public";
    const SCHEMA: &str = "schema_name";
    const TABLE: &str = "table_name";

    fn executor() -> Arc<InMemoryDatabase> {
        InMemoryDatabase::new()
    }

    fn create_schema_ops(schema_name: &str) -> Vec<SystemOperation> {
        vec![
            SystemOperation::CheckExistence {
                system_object: SystemObject::Schema,
                object_name: schema_name.to_owned(),
            },
            SystemOperation::CreateFolder {
                name: schema_name.to_owned(),
            },
            SystemOperation::CreateRecord {
                system_schema: DEFINITION_SCHEMA.to_owned(),
                system_table: SCHEMATA_TABLE.to_owned(),
                record: Record::Schema {
                    catalog_name: DEFAULT_CATALOG.to_owned(),
                    schema_name: schema_name.to_owned(),
                },
            },
        ]
    }

    #[cfg(test)]
    mod schema {
        use super::*;

        #[test]
        fn create_schema() {
            let executor = executor();
            assert_eq!(
                executor.execute(&create_schema_ops(SCHEMA)),
                Ok(ExecutionOutcome::SchemaCreated)
            );
        }

        #[test]
        fn create_schema_with_the_same_name() {
            let executor = executor();
            assert_eq!(
                executor.execute(&create_schema_ops(SCHEMA)),
                Ok(ExecutionOutcome::SchemaCreated)
            );

            assert_eq!(
                executor.execute(&create_schema_ops(SCHEMA)),
                Err(ExecutionError::SchemaAlreadyExists(SCHEMA.to_owned()))
            );
        }

        #[test]
        fn drop_nonexistent_schema() {
            let executor = executor();

            assert_eq!(
                executor.execute(&[
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
                ]),
                Err(ExecutionError::SchemaDoesNotExist(SCHEMA.to_owned()))
            );
        }
    }

    // #[cfg(test)]
    // mod table {
    //     use super::*;
    //
    //     #[test]
    //     fn create_table_where_schema_not_found() {
    //         let executor = executor();
    //
    //         assert_eq!(
    //             executor.execute(&[
    //                 SystemOperation::CheckExistence {
    //                     system_object: SystemObject::Schema,
    //                     object_name: SCHEMA.to_owned()
    //                 },
    //                 SystemOperation::CheckExistence {
    //                     system_object: SystemObject::Table,
    //                     object_name: TABLE.to_owned()
    //                 },
    //                 SystemOperation::CreateFile {
    //                     folder_name: SCHEMA.to_owned(),
    //                     name: TABLE.to_owned()
    //                 },
    //                 SystemOperation::CreateRecord {
    //                     system_schema: DEFINITION_SCHEMA.to_owned(),
    //                     system_table: TABLES_TABLE.to_owned(),
    //                     record: Record::Table {
    //                         catalog_name: DEFAULT_CATALOG.to_owned(),
    //                         schema_name: SCHEMA.to_owned(),
    //                         table_name: TABLE.to_owned(),
    //                     }
    //                 }
    //             ]),
    //             Err(ExecutionError::SchemaDoesNotExist(SCHEMA.to_owned()))
    //         );
    //     }
    //
    //     #[test]
    //     fn create_table_with_the_same_name() {
    //         let executor = executor();
    //
    //         if executor
    //             .execute(&[
    //                 SystemOperation::CheckExistence {
    //                     system_object: SystemObject::Schema,
    //                     object_name: SCHEMA.to_owned(),
    //                 },
    //                 SystemOperation::CreateFolder {
    //                     name: SCHEMA.to_owned(),
    //                 },
    //                 SystemOperation::CreateRecord {
    //                     system_schema: DEFINITION_SCHEMA.to_owned(),
    //                     system_table: SCHEMATA_TABLE.to_owned(),
    //                     record: Record::Schema {
    //                         catalog_name: DEFAULT_CATALOG.to_owned(),
    //                         schema_name: SCHEMA.to_owned(),
    //                     },
    //                 },
    //             ])
    //             .is_ok()
    //         {}
    //
    //         assert_eq!(
    //             executor.execute(
    //                 &SchemaChange::CreateTable(CreateTableQuery {
    //                     table_info: TableInfo::new(0, &SCHEMA, &TABLE),
    //                     column_defs: vec![],
    //                     if_not_exists: false,
    //                 }),
    //                 vec![
    //                     SystemOperation::CheckExistence {
    //                         system_object: SystemObject::Schema,
    //                         object_name: SCHEMA.to_owned()
    //                     },
    //                     SystemOperation::CheckExistence {
    //                         system_object: SystemObject::Table,
    //                         object_name: TABLE.to_owned()
    //                     },
    //                     SystemOperation::CreateFile {
    //                         folder_name: SCHEMA.to_owned(),
    //                         name: TABLE.to_owned()
    //                     },
    //                     SystemOperation::CreateRecord {
    //                         system_schema: DEFINITION_SCHEMA.to_owned(),
    //                         system_table: TABLES_TABLE.to_owned(),
    //                         record: Record::Table {
    //                             catalog_name: DEFAULT_CATALOG.to_owned(),
    //                             schema_name: SCHEMA.to_owned(),
    //                             table_name: TABLE.to_owned(),
    //                         }
    //                     }
    //                 ]
    //             ),
    //             Ok(ExecutionOutcome::TableCreated)
    //         );
    //
    //         assert_eq!(
    //             executor.execute(
    //                 &SchemaChange::CreateTable(CreateTableQuery {
    //                     table_info: TableInfo::new(0, &SCHEMA, &TABLE),
    //                     column_defs: vec![],
    //                     if_not_exists: false,
    //                 }),
    //                 vec![
    //                     SystemOperation::CheckExistence {
    //                         system_object: SystemObject::Schema,
    //                         object_name: SCHEMA.to_owned()
    //                     },
    //                     SystemOperation::CheckExistence {
    //                         system_object: SystemObject::Table,
    //                         object_name: TABLE.to_owned()
    //                     },
    //                     SystemOperation::CreateFile {
    //                         folder_name: SCHEMA.to_owned(),
    //                         name: TABLE.to_owned()
    //                     },
    //                     SystemOperation::CreateRecord {
    //                         system_schema: DEFINITION_SCHEMA.to_owned(),
    //                         system_table: TABLES_TABLE.to_owned(),
    //                         record: Record::Table {
    //                             catalog_name: DEFAULT_CATALOG.to_owned(),
    //                             schema_name: SCHEMA.to_owned(),
    //                             table_name: TABLE.to_owned(),
    //                         }
    //                     }
    //                 ]
    //             ),
    //             Err(ExecutionError::TableAlreadyExists(SCHEMA.to_owned(), TABLE.to_owned()))
    //         );
    //     }
    //
    //     #[test]
    //     fn drop_table_where_schema_not_found() {
    //         let executor = executor();
    //
    //         assert_eq!(
    //             executor.execute(
    //                 &SchemaChange::DropTables(DropTablesQuery {
    //                     table_infos: vec![TableInfo::new(0, &SCHEMA, &TABLE),],
    //                     cascade: false,
    //                     if_exists: false
    //                 }),
    //                 vec![
    //                     SystemOperation::CheckExistence {
    //                         system_object: SystemObject::Schema,
    //                         object_name: SCHEMA.to_owned()
    //                     },
    //                     SystemOperation::CheckExistence {
    //                         system_object: SystemObject::Table,
    //                         object_name: TABLE.to_owned()
    //                     },
    //                     SystemOperation::CheckDependants {
    //                         system_object: SystemObject::Table,
    //                         object_name: TABLE.to_owned()
    //                     },
    //                     SystemOperation::RemoveColumns {
    //                         schema_name: SCHEMA.to_owned(),
    //                         table_name: TABLE.to_owned()
    //                     },
    //                     SystemOperation::RemoveRecord {
    //                         system_schema: DEFINITION_SCHEMA.to_owned(),
    //                         system_table: TABLES_TABLE.to_owned(),
    //                         record: Record::Table {
    //                             catalog_name: DEFAULT_CATALOG.to_owned(),
    //                             schema_name: SCHEMA.to_owned(),
    //                             table_name: TABLE.to_owned(),
    //                         }
    //                     },
    //                     SystemOperation::RemoveFile {
    //                         folder_name: SCHEMA.to_owned(),
    //                         name: TABLE.to_owned()
    //                     },
    //                 ]
    //             ),
    //             Err(ExecutionError::SchemaDoesNotExist(SCHEMA.to_owned()))
    //         );
    //     }
    //
    //     #[test]
    //     fn drop_nonexistent_table() {
    //         let executor = executor();
    //
    //         if executor
    //             .execute(
    //                 &SchemaChange::CreateSchema(CreateSchemaQuery {
    //                     schema_name: SchemaName::from(&SCHEMA),
    //                     if_not_exists: false,
    //                 }),
    //                 vec![
    //                     SystemOperation::CheckExistence {
    //                         system_object: SystemObject::Schema,
    //                         object_name: SCHEMA.to_owned(),
    //                     },
    //                     SystemOperation::CreateFolder {
    //                         name: SCHEMA.to_owned(),
    //                     },
    //                     SystemOperation::CreateRecord {
    //                         system_schema: DEFINITION_SCHEMA.to_owned(),
    //                         system_table: SCHEMATA_TABLE.to_owned(),
    //                         record: Record::Schema {
    //                             catalog_name: DEFAULT_CATALOG.to_owned(),
    //                             schema_name: SCHEMA.to_owned(),
    //                         },
    //                     },
    //                 ],
    //             )
    //             .is_ok()
    //         {}
    //
    //         assert_eq!(
    //             executor.execute(
    //                 &SchemaChange::DropTables(DropTablesQuery {
    //                     table_infos: vec![TableInfo::new(0, &SCHEMA, &TABLE),],
    //                     cascade: false,
    //                     if_exists: false
    //                 }),
    //                 vec![
    //                     SystemOperation::CheckExistence {
    //                         system_object: SystemObject::Schema,
    //                         object_name: SCHEMA.to_owned()
    //                     },
    //                     SystemOperation::CheckExistence {
    //                         system_object: SystemObject::Table,
    //                         object_name: TABLE.to_owned()
    //                     },
    //                     SystemOperation::CheckDependants {
    //                         system_object: SystemObject::Table,
    //                         object_name: TABLE.to_owned()
    //                     },
    //                     SystemOperation::RemoveColumns {
    //                         schema_name: SCHEMA.to_owned(),
    //                         table_name: TABLE.to_owned()
    //                     },
    //                     SystemOperation::RemoveRecord {
    //                         system_schema: DEFINITION_SCHEMA.to_owned(),
    //                         system_table: TABLES_TABLE.to_owned(),
    //                         record: Record::Table {
    //                             catalog_name: DEFAULT_CATALOG.to_owned(),
    //                             schema_name: SCHEMA.to_owned(),
    //                             table_name: TABLE.to_owned(),
    //                         }
    //                     },
    //                     SystemOperation::RemoveFile {
    //                         folder_name: SCHEMA.to_owned(),
    //                         name: TABLE.to_owned()
    //                     },
    //                 ]
    //             ),
    //             Err(ExecutionError::TableDoesNotExists(SCHEMA.to_owned(), TABLE.to_owned()))
    //         );
    //     }
    // }
}
