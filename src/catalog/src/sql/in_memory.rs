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
use definition_operations::{
    ExecutionError, ExecutionOutcome, Kind, ObjectState, Record, Step, SystemObject, SystemOperation,
};
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

    fn execute(&self, operation: SystemOperation) -> Result<ExecutionOutcome, ExecutionError> {
        let SystemOperation {
            kind,
            skip_steps_if,
            steps,
        } = operation;
        let end = steps.len();
        let mut index = 0;
        while index < end {
            let operations = &steps[index];
            index += 1;
            for operation in operations {
                println!("{:?}", operation);
                match operation {
                    Step::CheckExistence {
                        system_object,
                        object_name,
                    } => match system_object {
                        SystemObject::Schema => {
                            let result = self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
                                schema.work_with(SCHEMATA_TABLE, |table| {
                                    table.read().any(|(_key, value)| {
                                        value == Binary::pack(&[CATALOG, Datum::from_str(&object_name)])
                                    })
                                })
                            });
                            match skip_steps_if {
                                None => {
                                    if let (&Kind::Create(SystemObject::Schema), Some(Some(true))) = (&kind, result) {
                                        return Err(ExecutionError::SchemaAlreadyExists(object_name.to_owned()));
                                    }
                                    if let (&Kind::Drop(SystemObject::Schema), Some(Some(false))) = (&kind, result) {
                                        return Err(ExecutionError::SchemaDoesNotExist(object_name.to_owned()));
                                    }
                                }
                                Some(ObjectState::NotExists) => break,
                                Some(ObjectState::Exists) => {}
                            }
                        }
                        SystemObject::Table => {}
                    },
                    Step::CheckDependants {
                        system_object,
                        object_name,
                    } => match system_object {
                        SystemObject::Schema => {
                            let result = self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
                                let schema_id = Binary::pack(&[CATALOG, Datum::from_str(object_name)]);
                                schema.work_with(TABLES_TABLE, |table| {
                                    table.read().any(|(_key, value)| value.start_with(&schema_id))
                                })
                            });

                            if let Some(Some(true)) = result {
                                return Err(ExecutionError::SchemaHasDependentObjects(object_name.to_owned()));
                            }
                        }
                        SystemObject::Table => {}
                    },
                    Step::RemoveDependants { .. } => {}
                    Step::RemoveColumns { .. } => {}
                    Step::CreateFolder { name } => {
                        self.catalog.create_schema(&name);
                    }
                    Step::RemoveFolder { name } => {
                        self.catalog.drop_schema(&name);
                        return Ok(ExecutionOutcome::SchemaDropped);
                    }
                    Step::CreateFile { .. } => {}
                    Step::RemoveFile { .. } => {}
                    Step::RemoveRecord {
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
                                    let schema_id = table
                                        .read()
                                        .find(|(_key, value)| {
                                            value == &Binary::pack(&[CATALOG, Datum::from_str(&schema_name)])
                                        })
                                        .map(|(key, _value)| key);
                                    debug_assert!(
                                        matches!(schema_id, Some(_)),
                                        "record for {:?} schema had to be found in {:?} system table",
                                        schema_name,
                                        SCHEMATA_TABLE
                                    );
                                    let schema_id = schema_id.unwrap();
                                    table.delete(vec![schema_id]);
                                });
                            });
                        }
                        Record::Table { .. } => unimplemented!(),
                        Record::Column { .. } => unimplemented!(),
                    },
                    Step::CreateRecord {
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
                                    table.insert(vec![Binary::pack(&[CATALOG, Datum::from_str(&schema_name)])])
                                })
                            });
                            return Ok(ExecutionOutcome::SchemaCreated);
                        }
                        Record::Table {
                            catalog_name,
                            schema_name,
                            table_name,
                        } => {
                            self.catalog.work_with(DEFINITION_SCHEMA, |schema| {
                                schema.work_with(TABLES_TABLE, |table| {
                                    table.insert(vec![Binary::pack(&[
                                        CATALOG,
                                        Datum::from_str(&schema_name),
                                        Datum::from_str(&table_name),
                                    ])])
                                })
                            });
                            return Ok(ExecutionOutcome::TableCreated);
                        }
                        Record::Column { .. } => {}
                    },
                }
            }
        }
        match kind {
            Kind::Create(SystemObject::Schema) => Ok(ExecutionOutcome::SchemaCreated),
            Kind::Drop(SystemObject::Schema) => Ok(ExecutionOutcome::SchemaDropped),
            Kind::Create(SystemObject::Table) => unimplemented!(),
            Kind::Drop(SystemObject::Table) => unimplemented!(),
        }
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
    const OTHER_SCHEMA: &str = "other_schema_name";
    const TABLE: &str = "table_name";

    fn executor() -> Arc<InMemoryDatabase> {
        InMemoryDatabase::new()
    }

    fn create_schema_ops(schema_name: &str) -> SystemOperation {
        create_schema_inner(schema_name, false)
    }

    fn create_schema_if_not_exists_ops(schema_name: &str) -> SystemOperation {
        create_schema_inner(schema_name, true)
    }

    fn create_schema_inner(schema_name: &str, if_exists: bool) -> SystemOperation {
        SystemOperation {
            kind: Kind::Create(SystemObject::Schema),
            skip_steps_if: if if_exists { Some(ObjectState::Exists) } else { None },
            steps: vec![vec![
                Step::CheckExistence {
                    system_object: SystemObject::Schema,
                    object_name: schema_name.to_owned(),
                },
                Step::CreateFolder {
                    name: schema_name.to_owned(),
                },
                Step::CreateRecord {
                    system_schema: DEFINITION_SCHEMA.to_owned(),
                    system_table: SCHEMATA_TABLE.to_owned(),
                    record: Record::Schema {
                        catalog_name: DEFAULT_CATALOG.to_owned(),
                        schema_name: schema_name.to_owned(),
                    },
                },
            ]],
        }
    }

    fn drop_schemas_ops(schema_names: Vec<&str>) -> SystemOperation {
        let steps = schema_names.into_iter().map(drop_schema_op).collect::<Vec<Vec<Step>>>();
        SystemOperation {
            kind: Kind::Drop(SystemObject::Schema),
            skip_steps_if: None,
            steps,
        }
    }

    fn drop_schema_op(schema_name: &str) -> Vec<Step> {
        vec![
            Step::CheckExistence {
                system_object: SystemObject::Schema,
                object_name: schema_name.to_owned(),
            },
            Step::CheckDependants {
                system_object: SystemObject::Schema,
                object_name: schema_name.to_owned(),
            },
            Step::RemoveRecord {
                system_schema: DEFINITION_SCHEMA.to_owned(),
                system_table: SCHEMATA_TABLE.to_owned(),
                record: Record::Schema {
                    catalog_name: DEFAULT_CATALOG.to_owned(),
                    schema_name: schema_name.to_owned(),
                },
            },
            Step::RemoveFolder {
                name: schema_name.to_owned(),
            },
        ]
    }

    fn create_table_ops(schema_name: &str, table_name: &str) -> SystemOperation {
        SystemOperation {
            kind: Kind::Create(SystemObject::Table),
            skip_steps_if: None,
            steps: vec![vec![
                Step::CheckExistence {
                    system_object: SystemObject::Schema,
                    object_name: schema_name.to_owned(),
                },
                Step::CheckExistence {
                    system_object: SystemObject::Table,
                    object_name: table_name.to_owned(),
                },
                Step::CreateFile {
                    folder_name: schema_name.to_owned(),
                    name: table_name.to_owned(),
                },
                Step::CreateRecord {
                    system_schema: DEFINITION_SCHEMA.to_owned(),
                    system_table: TABLES_TABLE.to_owned(),
                    record: Record::Table {
                        catalog_name: DEFAULT_CATALOG.to_owned(),
                        schema_name: schema_name.to_owned(),
                        table_name: table_name.to_owned(),
                    },
                },
            ]],
        }
    }

    #[cfg(test)]
    mod schema {
        use super::*;

        #[test]
        fn create_schema() {
            let executor = executor();
            assert_eq!(
                executor.execute(create_schema_ops(SCHEMA)),
                Ok(ExecutionOutcome::SchemaCreated)
            );
        }

        #[test]
        fn create_if_not_exists() {
            let executor = executor();

            assert_eq!(
                executor.execute(create_schema_if_not_exists_ops(SCHEMA)),
                Ok(ExecutionOutcome::SchemaCreated)
            );

            assert_eq!(
                executor.execute(create_schema_if_not_exists_ops(SCHEMA)),
                Ok(ExecutionOutcome::SchemaCreated)
            );
        }

        #[test]
        fn create_schema_with_the_same_name() {
            let executor = executor();
            assert_eq!(
                executor.execute(create_schema_ops(SCHEMA)),
                Ok(ExecutionOutcome::SchemaCreated)
            );

            assert_eq!(
                executor.execute(create_schema_ops(SCHEMA)),
                Err(ExecutionError::SchemaAlreadyExists(SCHEMA.to_owned()))
            );
        }

        #[test]
        fn drop_nonexistent_schema() {
            let executor = executor();

            assert_eq!(
                executor.execute(drop_schemas_ops(vec![SCHEMA])),
                Err(ExecutionError::SchemaDoesNotExist(SCHEMA.to_owned()))
            );
        }

        #[test]
        fn drop_single_schema() {
            let executor = executor();

            assert_eq!(
                executor.execute(create_schema_ops(SCHEMA)),
                Ok(ExecutionOutcome::SchemaCreated)
            );

            assert_eq!(
                executor.execute(drop_schemas_ops(vec![SCHEMA])),
                Ok(ExecutionOutcome::SchemaDropped)
            );
        }

        #[test]
        fn drop_many_schemas() {
            let executor = executor();

            assert_eq!(
                executor.execute(create_schema_ops(SCHEMA)),
                Ok(ExecutionOutcome::SchemaCreated)
            );
            assert_eq!(
                executor.execute(create_schema_ops(OTHER_SCHEMA)),
                Ok(ExecutionOutcome::SchemaCreated)
            );

            assert_eq!(
                executor.execute(drop_schemas_ops(vec![SCHEMA, OTHER_SCHEMA])),
                Ok(ExecutionOutcome::SchemaDropped)
            );
        }

        #[test]
        fn drop_schema_with_table() {
            let executor = executor();

            assert_eq!(
                executor.execute(create_schema_ops(SCHEMA)),
                Ok(ExecutionOutcome::SchemaCreated)
            );
            assert_eq!(
                executor.execute(create_table_ops(SCHEMA, TABLE)),
                Ok(ExecutionOutcome::TableCreated)
            );

            assert_eq!(
                executor.execute(drop_schemas_ops(vec![SCHEMA])),
                Err(ExecutionError::SchemaHasDependentObjects(SCHEMA.to_owned()))
            );
        }

        #[test]
        fn drop_many_cascade() {
            let executor = executor();

            assert_eq!(
                executor.execute(create_schema_ops(SCHEMA)),
                Ok(ExecutionOutcome::SchemaCreated)
            );
            assert_eq!(
                executor.execute(create_schema_ops(OTHER_SCHEMA)),
                Ok(ExecutionOutcome::SchemaCreated)
            );

            assert_eq!(
                executor.execute(drop_schemas_ops(vec![SCHEMA, OTHER_SCHEMA])),
                Ok(ExecutionOutcome::SchemaDropped)
            );
        }

        #[test]
        fn drop_many_if_exists_first() {
            let executor = executor();

            assert_eq!(
                executor.execute(create_schema_ops(SCHEMA)),
                Ok(ExecutionOutcome::SchemaCreated)
            );

            assert_eq!(
                executor.execute(SystemOperation {
                    kind: Kind::Drop(SystemObject::Schema),
                    skip_steps_if: Some(ObjectState::NotExists),
                    steps: vec![
                        vec![
                            Step::CheckExistence {
                                system_object: SystemObject::Schema,
                                object_name: SCHEMA.to_owned(),
                            },
                            Step::CheckDependants {
                                system_object: SystemObject::Schema,
                                object_name: SCHEMA.to_owned()
                            },
                            Step::RemoveRecord {
                                system_schema: DEFINITION_SCHEMA.to_owned(),
                                system_table: SCHEMATA_TABLE.to_owned(),
                                record: Record::Schema {
                                    catalog_name: DEFAULT_CATALOG.to_owned(),
                                    schema_name: SCHEMA.to_owned()
                                }
                            },
                            Step::RemoveFolder {
                                name: SCHEMA.to_owned()
                            }
                        ],
                        vec![
                            Step::CheckExistence {
                                system_object: SystemObject::Schema,
                                object_name: OTHER_SCHEMA.to_owned(),
                            },
                            Step::CheckDependants {
                                system_object: SystemObject::Schema,
                                object_name: OTHER_SCHEMA.to_owned()
                            },
                            Step::RemoveRecord {
                                system_schema: DEFINITION_SCHEMA.to_owned(),
                                system_table: SCHEMATA_TABLE.to_owned(),
                                record: Record::Schema {
                                    catalog_name: DEFAULT_CATALOG.to_owned(),
                                    schema_name: OTHER_SCHEMA.to_owned()
                                }
                            },
                            Step::RemoveFolder {
                                name: OTHER_SCHEMA.to_owned()
                            }
                        ]
                    ]
                }),
                Ok(ExecutionOutcome::SchemaDropped)
            );
        }

        #[test]
        fn drop_many_if_exists_last() {
            let executor = executor();

            assert_eq!(
                executor.execute(create_schema_ops(OTHER_SCHEMA)),
                Ok(ExecutionOutcome::SchemaCreated)
            );

            assert_eq!(
                executor.execute(SystemOperation {
                    kind: Kind::Drop(SystemObject::Schema),
                    skip_steps_if: Some(ObjectState::NotExists),
                    steps: vec![
                        vec![
                            Step::CheckExistence {
                                system_object: SystemObject::Schema,
                                object_name: SCHEMA.to_owned(),
                            },
                            Step::CheckDependants {
                                system_object: SystemObject::Schema,
                                object_name: SCHEMA.to_owned()
                            },
                            Step::RemoveRecord {
                                system_schema: DEFINITION_SCHEMA.to_owned(),
                                system_table: SCHEMATA_TABLE.to_owned(),
                                record: Record::Schema {
                                    catalog_name: DEFAULT_CATALOG.to_owned(),
                                    schema_name: SCHEMA.to_owned()
                                }
                            },
                            Step::RemoveFolder {
                                name: SCHEMA.to_owned()
                            }
                        ],
                        vec![
                            Step::CheckExistence {
                                system_object: SystemObject::Schema,
                                object_name: OTHER_SCHEMA.to_owned(),
                            },
                            Step::CheckDependants {
                                system_object: SystemObject::Schema,
                                object_name: OTHER_SCHEMA.to_owned()
                            },
                            Step::RemoveRecord {
                                system_schema: DEFINITION_SCHEMA.to_owned(),
                                system_table: SCHEMATA_TABLE.to_owned(),
                                record: Record::Schema {
                                    catalog_name: DEFAULT_CATALOG.to_owned(),
                                    schema_name: OTHER_SCHEMA.to_owned()
                                }
                            },
                            Step::RemoveFolder {
                                name: OTHER_SCHEMA.to_owned()
                            }
                        ]
                    ]
                }),
                Ok(ExecutionOutcome::SchemaDropped)
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
    //                 Step::CheckExistence {
    //                     system_object: SystemObject::Schema,
    //                     object_name: SCHEMA.to_owned()
    //                 },
    //                 Step::CheckExistence {
    //                     system_object: SystemObject::Table,
    //                     object_name: TABLE.to_owned()
    //                 },
    //                 Step::CreateFile {
    //                     folder_name: SCHEMA.to_owned(),
    //                     name: TABLE.to_owned()
    //                 },
    //                 Step::CreateRecord {
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
    //                 Step::CheckExistence {
    //                     system_object: SystemObject::Schema,
    //                     object_name: SCHEMA.to_owned(),
    //                 },
    //                 Step::CreateFolder {
    //                     name: SCHEMA.to_owned(),
    //                 },
    //                 Step::CreateRecord {
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
    //                     Step::CheckExistence {
    //                         system_object: SystemObject::Schema,
    //                         object_name: SCHEMA.to_owned()
    //                     },
    //                     Step::CheckExistence {
    //                         system_object: SystemObject::Table,
    //                         object_name: TABLE.to_owned()
    //                     },
    //                     Step::CreateFile {
    //                         folder_name: SCHEMA.to_owned(),
    //                         name: TABLE.to_owned()
    //                     },
    //                     Step::CreateRecord {
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
    //                     Step::CheckExistence {
    //                         system_object: SystemObject::Schema,
    //                         object_name: SCHEMA.to_owned()
    //                     },
    //                     Step::CheckExistence {
    //                         system_object: SystemObject::Table,
    //                         object_name: TABLE.to_owned()
    //                     },
    //                     Step::CreateFile {
    //                         folder_name: SCHEMA.to_owned(),
    //                         name: TABLE.to_owned()
    //                     },
    //                     Step::CreateRecord {
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
    //                     Step::CheckExistence {
    //                         system_object: SystemObject::Schema,
    //                         object_name: SCHEMA.to_owned()
    //                     },
    //                     Step::CheckExistence {
    //                         system_object: SystemObject::Table,
    //                         object_name: TABLE.to_owned()
    //                     },
    //                     Step::CheckDependants {
    //                         system_object: SystemObject::Table,
    //                         object_name: TABLE.to_owned()
    //                     },
    //                     Step::RemoveColumns {
    //                         schema_name: SCHEMA.to_owned(),
    //                         table_name: TABLE.to_owned()
    //                     },
    //                     Step::RemoveRecord {
    //                         system_schema: DEFINITION_SCHEMA.to_owned(),
    //                         system_table: TABLES_TABLE.to_owned(),
    //                         record: Record::Table {
    //                             catalog_name: DEFAULT_CATALOG.to_owned(),
    //                             schema_name: SCHEMA.to_owned(),
    //                             table_name: TABLE.to_owned(),
    //                         }
    //                     },
    //                     Step::RemoveFile {
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
    //                     Step::CheckExistence {
    //                         system_object: SystemObject::Schema,
    //                         object_name: SCHEMA.to_owned(),
    //                     },
    //                     Step::CreateFolder {
    //                         name: SCHEMA.to_owned(),
    //                     },
    //                     Step::CreateRecord {
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
    //                     Step::CheckExistence {
    //                         system_object: SystemObject::Schema,
    //                         object_name: SCHEMA.to_owned()
    //                     },
    //                     Step::CheckExistence {
    //                         system_object: SystemObject::Table,
    //                         object_name: TABLE.to_owned()
    //                     },
    //                     Step::CheckDependants {
    //                         system_object: SystemObject::Table,
    //                         object_name: TABLE.to_owned()
    //                     },
    //                     Step::RemoveColumns {
    //                         schema_name: SCHEMA.to_owned(),
    //                         table_name: TABLE.to_owned()
    //                     },
    //                     Step::RemoveRecord {
    //                         system_schema: DEFINITION_SCHEMA.to_owned(),
    //                         system_table: TABLES_TABLE.to_owned(),
    //                         record: Record::Table {
    //                             catalog_name: DEFAULT_CATALOG.to_owned(),
    //                             schema_name: SCHEMA.to_owned(),
    //                             table_name: TABLE.to_owned(),
    //                         }
    //                     },
    //                     Step::RemoveFile {
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
