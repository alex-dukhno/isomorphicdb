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

use analysis::{
    ColumnDesc, CreateSchemaQuery, CreateTableQuery, DropSchemasQuery, DropTablesQuery, SchemaChange, TableInfo,
};
use data_manager::{COLUMNS_TABLE, DEFAULT_CATALOG, DEFINITION_SCHEMA, SCHEMATA_TABLE, TABLES_TABLE};
use definition_operations::{Kind, ObjectState, Record, Step, SystemObject, SystemOperation};

pub struct SystemSchemaPlanner;

impl SystemSchemaPlanner {
    pub const fn new() -> SystemSchemaPlanner {
        SystemSchemaPlanner
    }

    pub fn schema_change_plan(&self, schema_change: &SchemaChange) -> SystemOperation {
        match schema_change {
            SchemaChange::CreateSchema(CreateSchemaQuery {
                schema_name,
                if_not_exists,
            }) => {
                let mut steps = vec![];
                steps.push(Step::CheckExistence {
                    system_object: SystemObject::Schema,
                    object_name: schema_name.as_ref().to_string(),
                    skip_if: if *if_not_exists {
                        Some(ObjectState::Exists)
                    } else {
                        None
                    },
                });
                steps.push(Step::CreateFolder {
                    name: schema_name.as_ref().to_string(),
                });
                steps.push(Step::CreateRecord {
                    system_schema: DEFINITION_SCHEMA.to_owned(),
                    system_table: SCHEMATA_TABLE.to_owned(),
                    record: Record::Schema {
                        catalog_name: DEFAULT_CATALOG.to_owned(),
                        schema_name: schema_name.as_ref().to_string(),
                    },
                });
                SystemOperation {
                    kind: Kind::Create(SystemObject::Schema),
                    steps: vec![steps],
                }
            }
            SchemaChange::DropSchemas(DropSchemasQuery {
                schema_names,
                cascade,
                if_exists,
            }) => {
                let mut steps = vec![];
                for schema_name in schema_names {
                    let mut for_schema = vec![];
                    for_schema.push(Step::CheckExistence {
                        system_object: SystemObject::Schema,
                        object_name: schema_name.as_ref().to_string(),
                        skip_if: if *if_exists { Some(ObjectState::NotExists) } else { None },
                    });
                    if *cascade {
                        for_schema.push(Step::RemoveDependants {
                            system_object: SystemObject::Schema,
                            object_name: schema_name.as_ref().to_string(),
                        });
                    } else {
                        for_schema.push(Step::CheckDependants {
                            system_object: SystemObject::Schema,
                            object_name: schema_name.as_ref().to_string(),
                        });
                    }
                    for_schema.push(Step::RemoveRecord {
                        system_schema: DEFINITION_SCHEMA.to_owned(),
                        system_table: SCHEMATA_TABLE.to_owned(),
                        record: Record::Schema {
                            catalog_name: DEFAULT_CATALOG.to_owned(),
                            schema_name: schema_name.as_ref().to_string(),
                        },
                    });
                    for_schema.push(Step::RemoveFolder {
                        name: schema_name.as_ref().to_string(),
                    });
                    steps.push(for_schema);
                }
                SystemOperation {
                    kind: Kind::Drop(SystemObject::Schema),
                    steps,
                }
            }
            SchemaChange::CreateTable(CreateTableQuery {
                table_info:
                    TableInfo {
                        schema_id: _schema_id,
                        schema_name,
                        table_name,
                    },
                column_defs,
                if_not_exists,
            }) => {
                let mut steps = vec![];
                steps.push(Step::CheckExistence {
                    system_object: SystemObject::Schema,
                    object_name: schema_name.clone(),
                    skip_if: None,
                });
                steps.push(Step::CheckExistence {
                    system_object: SystemObject::Table,
                    object_name: table_name.clone(),
                    skip_if: if *if_not_exists {
                        Some(ObjectState::Exists)
                    } else {
                        None
                    },
                });
                steps.push(Step::CreateFile {
                    folder_name: schema_name.clone(),
                    name: table_name.clone(),
                });
                steps.push(Step::CreateRecord {
                    system_schema: DEFINITION_SCHEMA.to_owned(),
                    system_table: TABLES_TABLE.to_owned(),
                    record: Record::Table {
                        catalog_name: DEFAULT_CATALOG.to_owned(),
                        schema_name: schema_name.clone(),
                        table_name: table_name.clone(),
                    },
                });
                for ColumnDesc { name, sql_type } in column_defs {
                    steps.push(Step::CreateRecord {
                        system_schema: DEFINITION_SCHEMA.to_owned(),
                        system_table: COLUMNS_TABLE.to_owned(),
                        record: Record::Column {
                            catalog_name: DEFAULT_CATALOG.to_owned(),
                            schema_name: schema_name.clone(),
                            table_name: table_name.clone(),
                            column_name: name.clone(),
                            sql_type: *sql_type,
                        },
                    })
                }
                SystemOperation {
                    kind: Kind::Create(SystemObject::Table),
                    steps: vec![steps],
                }
            }
            SchemaChange::DropTables(DropTablesQuery {
                table_infos, if_exists, ..
            }) => {
                let mut steps = vec![];
                for TableInfo {
                    schema_id: _schema_id,
                    schema_name,
                    table_name,
                } in table_infos
                {
                    let mut for_table = vec![];
                    for_table.push(Step::CheckExistence {
                        system_object: SystemObject::Schema,
                        object_name: schema_name.clone(),
                        skip_if: None,
                    });
                    for_table.push(Step::CheckExistence {
                        system_object: SystemObject::Table,
                        object_name: table_name.clone(),
                        skip_if: if *if_exists { Some(ObjectState::NotExists) } else { None },
                    });
                    for_table.push(Step::RemoveColumns {
                        schema_name: schema_name.to_owned(),
                        table_name: table_name.to_owned(),
                    });
                    for_table.push(Step::RemoveRecord {
                        system_schema: DEFINITION_SCHEMA.to_owned(),
                        system_table: TABLES_TABLE.to_owned(),
                        record: Record::Table {
                            catalog_name: DEFAULT_CATALOG.to_owned(),
                            schema_name: schema_name.clone(),
                            table_name: table_name.clone(),
                        },
                    });
                    for_table.push(Step::RemoveFile {
                        folder_name: schema_name.to_owned(),
                        name: table_name.clone(),
                    });
                    steps.push(for_table);
                }
                SystemOperation {
                    kind: Kind::Drop(SystemObject::Table),
                    steps,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use analysis::SchemaName;
    use types::SqlType;

    const SCHEMA: &str = "schema";
    const OTHER_SCHEMA: &str = "other_schema";
    const TABLE: &str = "table";
    const OTHER_TABLE: &str = "other_table";

    const QUERY_PLANNER: SystemSchemaPlanner = SystemSchemaPlanner::new();

    #[cfg(test)]
    mod schema {
        use super::*;

        #[test]
        fn create() {
            assert_eq!(
                QUERY_PLANNER.schema_change_plan(&SchemaChange::CreateSchema(CreateSchemaQuery {
                    schema_name: SchemaName::from(&SCHEMA),
                    if_not_exists: false,
                })),
                SystemOperation {
                    kind: Kind::Create(SystemObject::Schema),
                    steps: vec![vec![
                        Step::CheckExistence {
                            system_object: SystemObject::Schema,
                            object_name: SCHEMA.to_owned(),
                            skip_if: None,
                        },
                        Step::CreateFolder {
                            name: SCHEMA.to_owned()
                        },
                        Step::CreateRecord {
                            system_schema: DEFINITION_SCHEMA.to_owned(),
                            system_table: SCHEMATA_TABLE.to_owned(),
                            record: Record::Schema {
                                catalog_name: DEFAULT_CATALOG.to_owned(),
                                schema_name: SCHEMA.to_owned()
                            }
                        }
                    ]]
                }
            );
        }

        #[test]
        fn create_if_not_exists() {
            assert_eq!(
                QUERY_PLANNER.schema_change_plan(&SchemaChange::CreateSchema(CreateSchemaQuery {
                    schema_name: SchemaName::from(&SCHEMA),
                    if_not_exists: true,
                })),
                SystemOperation {
                    kind: Kind::Create(SystemObject::Schema),
                    steps: vec![vec![
                        Step::CheckExistence {
                            system_object: SystemObject::Schema,
                            object_name: SCHEMA.to_owned(),
                            skip_if: Some(ObjectState::Exists),
                        },
                        Step::CreateFolder {
                            name: SCHEMA.to_owned()
                        },
                        Step::CreateRecord {
                            system_schema: DEFINITION_SCHEMA.to_owned(),
                            system_table: SCHEMATA_TABLE.to_owned(),
                            record: Record::Schema {
                                catalog_name: DEFAULT_CATALOG.to_owned(),
                                schema_name: SCHEMA.to_owned()
                            }
                        }
                    ]]
                }
            );
        }

        #[test]
        fn drop_single_schema() {
            assert_eq!(
                QUERY_PLANNER.schema_change_plan(&SchemaChange::DropSchemas(DropSchemasQuery {
                    schema_names: vec![SchemaName::from(&SCHEMA)],
                    cascade: false,
                    if_exists: false
                })),
                SystemOperation {
                    kind: Kind::Drop(SystemObject::Schema),
                    steps: vec![vec![
                        Step::CheckExistence {
                            system_object: SystemObject::Schema,
                            object_name: SCHEMA.to_owned(),
                            skip_if: None,
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
                    ]]
                }
            );
        }

        #[test]
        fn drop_many() {
            assert_eq!(
                QUERY_PLANNER.schema_change_plan(&SchemaChange::DropSchemas(DropSchemasQuery {
                    schema_names: vec![SchemaName::from(&SCHEMA), SchemaName::from(&OTHER_SCHEMA)],
                    cascade: false,
                    if_exists: false
                })),
                SystemOperation {
                    kind: Kind::Drop(SystemObject::Schema),
                    steps: vec![
                        vec![
                            Step::CheckExistence {
                                system_object: SystemObject::Schema,
                                object_name: SCHEMA.to_owned(),
                                skip_if: None,
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
                                skip_if: None,
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
                }
            );
        }

        #[test]
        fn drop_many_cascade() {
            assert_eq!(
                QUERY_PLANNER.schema_change_plan(&SchemaChange::DropSchemas(DropSchemasQuery {
                    schema_names: vec![SchemaName::from(&SCHEMA), SchemaName::from(&OTHER_SCHEMA)],
                    cascade: true,
                    if_exists: false
                })),
                SystemOperation {
                    kind: Kind::Drop(SystemObject::Schema),
                    steps: vec![
                        vec![
                            Step::CheckExistence {
                                system_object: SystemObject::Schema,
                                object_name: SCHEMA.to_owned(),
                                skip_if: None,
                            },
                            Step::RemoveDependants {
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
                                skip_if: None,
                            },
                            Step::RemoveDependants {
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
                }
            );
        }

        #[test]
        fn drop_many_if_exists() {
            assert_eq!(
                QUERY_PLANNER.schema_change_plan(&SchemaChange::DropSchemas(DropSchemasQuery {
                    schema_names: vec![SchemaName::from(&SCHEMA), SchemaName::from(&OTHER_SCHEMA)],
                    cascade: false,
                    if_exists: true
                })),
                SystemOperation {
                    kind: Kind::Drop(SystemObject::Schema),
                    steps: vec![
                        vec![
                            Step::CheckExistence {
                                system_object: SystemObject::Schema,
                                object_name: SCHEMA.to_owned(),
                                skip_if: Some(ObjectState::NotExists),
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
                                skip_if: Some(ObjectState::NotExists),
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
                }
            );
        }
    }

    #[cfg(test)]
    mod table {
        use super::*;

        #[test]
        fn create_without_columns() {
            assert_eq!(
                QUERY_PLANNER.schema_change_plan(&SchemaChange::CreateTable(CreateTableQuery {
                    table_info: TableInfo::new(0, &SCHEMA, &TABLE),
                    column_defs: vec![],
                    if_not_exists: false,
                })),
                SystemOperation {
                    kind: Kind::Create(SystemObject::Table),
                    steps: vec![vec![
                        Step::CheckExistence {
                            system_object: SystemObject::Schema,
                            object_name: SCHEMA.to_owned(),
                            skip_if: None,
                        },
                        Step::CheckExistence {
                            system_object: SystemObject::Table,
                            object_name: TABLE.to_owned(),
                            skip_if: None,
                        },
                        Step::CreateFile {
                            folder_name: SCHEMA.to_owned(),
                            name: TABLE.to_owned()
                        },
                        Step::CreateRecord {
                            system_schema: DEFINITION_SCHEMA.to_owned(),
                            system_table: TABLES_TABLE.to_owned(),
                            record: Record::Table {
                                catalog_name: DEFAULT_CATALOG.to_owned(),
                                schema_name: SCHEMA.to_owned(),
                                table_name: TABLE.to_owned(),
                            }
                        }
                    ]]
                }
            );
        }

        #[test]
        fn create_if_not_exists() {
            assert_eq!(
                QUERY_PLANNER.schema_change_plan(&SchemaChange::CreateTable(CreateTableQuery {
                    table_info: TableInfo::new(0, &SCHEMA, &TABLE),
                    column_defs: vec![],
                    if_not_exists: true,
                })),
                SystemOperation {
                    kind: Kind::Create(SystemObject::Table),
                    steps: vec![vec![
                        Step::CheckExistence {
                            system_object: SystemObject::Schema,
                            object_name: SCHEMA.to_owned(),
                            skip_if: None,
                        },
                        Step::CheckExistence {
                            system_object: SystemObject::Table,
                            object_name: TABLE.to_owned(),
                            skip_if: Some(ObjectState::Exists),
                        },
                        Step::CreateFile {
                            folder_name: SCHEMA.to_owned(),
                            name: TABLE.to_owned()
                        },
                        Step::CreateRecord {
                            system_schema: DEFINITION_SCHEMA.to_owned(),
                            system_table: TABLES_TABLE.to_owned(),
                            record: Record::Table {
                                catalog_name: DEFAULT_CATALOG.to_owned(),
                                schema_name: SCHEMA.to_owned(),
                                table_name: TABLE.to_owned(),
                            }
                        }
                    ]]
                }
            );
        }

        #[test]
        fn create_with_columns() {
            assert_eq!(
                QUERY_PLANNER.schema_change_plan(&SchemaChange::CreateTable(CreateTableQuery {
                    table_info: TableInfo::new(0, &SCHEMA, &TABLE),
                    column_defs: vec![
                        ColumnDesc {
                            name: "col_1".to_owned(),
                            sql_type: SqlType::SmallInt
                        },
                        ColumnDesc {
                            name: "col_2".to_owned(),
                            sql_type: SqlType::BigInt
                        }
                    ],
                    if_not_exists: false,
                })),
                SystemOperation {
                    kind: Kind::Create(SystemObject::Table),
                    steps: vec![vec![
                        Step::CheckExistence {
                            system_object: SystemObject::Schema,
                            object_name: SCHEMA.to_owned(),
                            skip_if: None,
                        },
                        Step::CheckExistence {
                            system_object: SystemObject::Table,
                            object_name: TABLE.to_owned(),
                            skip_if: None,
                        },
                        Step::CreateFile {
                            folder_name: SCHEMA.to_owned(),
                            name: TABLE.to_owned()
                        },
                        Step::CreateRecord {
                            system_schema: DEFINITION_SCHEMA.to_owned(),
                            system_table: TABLES_TABLE.to_owned(),
                            record: Record::Table {
                                catalog_name: DEFAULT_CATALOG.to_owned(),
                                schema_name: SCHEMA.to_owned(),
                                table_name: TABLE.to_owned(),
                            }
                        },
                        Step::CreateRecord {
                            system_schema: DEFINITION_SCHEMA.to_owned(),
                            system_table: COLUMNS_TABLE.to_owned(),
                            record: Record::Column {
                                catalog_name: DEFAULT_CATALOG.to_owned(),
                                schema_name: SCHEMA.to_string(),
                                table_name: TABLE.to_string(),
                                column_name: "col_1".to_string(),
                                sql_type: SqlType::SmallInt
                            }
                        },
                        Step::CreateRecord {
                            system_schema: DEFINITION_SCHEMA.to_owned(),
                            system_table: COLUMNS_TABLE.to_owned(),
                            record: Record::Column {
                                catalog_name: DEFAULT_CATALOG.to_owned(),
                                schema_name: SCHEMA.to_string(),
                                table_name: TABLE.to_string(),
                                column_name: "col_2".to_string(),
                                sql_type: SqlType::BigInt
                            }
                        }
                    ]]
                }
            );
        }

        #[test]
        fn drop_many() {
            assert_eq!(
                QUERY_PLANNER.schema_change_plan(&SchemaChange::DropTables(DropTablesQuery {
                    table_infos: vec![
                        TableInfo::new(0, &SCHEMA, &TABLE),
                        TableInfo::new(0, &SCHEMA, &OTHER_TABLE)
                    ],
                    cascade: false,
                    if_exists: false
                })),
                SystemOperation {
                    kind: Kind::Drop(SystemObject::Table),
                    steps: vec![
                        vec![
                            Step::CheckExistence {
                                system_object: SystemObject::Schema,
                                object_name: SCHEMA.to_owned(),
                                skip_if: None,
                            },
                            Step::CheckExistence {
                                system_object: SystemObject::Table,
                                object_name: TABLE.to_owned(),
                                skip_if: None,
                            },
                            Step::RemoveColumns {
                                schema_name: SCHEMA.to_owned(),
                                table_name: TABLE.to_owned()
                            },
                            Step::RemoveRecord {
                                system_schema: DEFINITION_SCHEMA.to_owned(),
                                system_table: TABLES_TABLE.to_owned(),
                                record: Record::Table {
                                    catalog_name: DEFAULT_CATALOG.to_owned(),
                                    schema_name: SCHEMA.to_owned(),
                                    table_name: TABLE.to_owned(),
                                }
                            },
                            Step::RemoveFile {
                                folder_name: SCHEMA.to_owned(),
                                name: TABLE.to_owned()
                            }
                        ],
                        vec![
                            Step::CheckExistence {
                                system_object: SystemObject::Schema,
                                object_name: SCHEMA.to_owned(),
                                skip_if: None,
                            },
                            Step::CheckExistence {
                                system_object: SystemObject::Table,
                                object_name: OTHER_TABLE.to_owned(),
                                skip_if: None,
                            },
                            Step::RemoveColumns {
                                schema_name: SCHEMA.to_owned(),
                                table_name: OTHER_TABLE.to_owned()
                            },
                            Step::RemoveRecord {
                                system_schema: DEFINITION_SCHEMA.to_owned(),
                                system_table: TABLES_TABLE.to_owned(),
                                record: Record::Table {
                                    catalog_name: DEFAULT_CATALOG.to_owned(),
                                    schema_name: SCHEMA.to_owned(),
                                    table_name: OTHER_TABLE.to_owned(),
                                }
                            },
                            Step::RemoveFile {
                                folder_name: SCHEMA.to_owned(),
                                name: OTHER_TABLE.to_owned()
                            }
                        ]
                    ]
                }
            );
        }

        #[test]
        fn drop_many_cascade() {
            assert_eq!(
                QUERY_PLANNER.schema_change_plan(&SchemaChange::DropTables(DropTablesQuery {
                    table_infos: vec![
                        TableInfo::new(0, &SCHEMA, &TABLE),
                        TableInfo::new(0, &SCHEMA, &OTHER_TABLE)
                    ],
                    cascade: true,
                    if_exists: false
                })),
                SystemOperation {
                    kind: Kind::Drop(SystemObject::Table),
                    steps: vec![
                        vec![
                            Step::CheckExistence {
                                system_object: SystemObject::Schema,
                                object_name: SCHEMA.to_owned(),
                                skip_if: None,
                            },
                            Step::CheckExistence {
                                system_object: SystemObject::Table,
                                object_name: TABLE.to_owned(),
                                skip_if: None,
                            },
                            Step::RemoveColumns {
                                schema_name: SCHEMA.to_owned(),
                                table_name: TABLE.to_owned()
                            },
                            Step::RemoveRecord {
                                system_schema: DEFINITION_SCHEMA.to_owned(),
                                system_table: TABLES_TABLE.to_owned(),
                                record: Record::Table {
                                    catalog_name: DEFAULT_CATALOG.to_owned(),
                                    schema_name: SCHEMA.to_owned(),
                                    table_name: TABLE.to_owned(),
                                }
                            },
                            Step::RemoveFile {
                                folder_name: SCHEMA.to_owned(),
                                name: TABLE.to_owned()
                            }
                        ],
                        vec![
                            Step::CheckExistence {
                                system_object: SystemObject::Schema,
                                object_name: SCHEMA.to_owned(),
                                skip_if: None,
                            },
                            Step::CheckExistence {
                                system_object: SystemObject::Table,
                                object_name: OTHER_TABLE.to_owned(),
                                skip_if: None,
                            },
                            Step::RemoveColumns {
                                schema_name: SCHEMA.to_owned(),
                                table_name: OTHER_TABLE.to_owned()
                            },
                            Step::RemoveRecord {
                                system_schema: DEFINITION_SCHEMA.to_owned(),
                                system_table: TABLES_TABLE.to_owned(),
                                record: Record::Table {
                                    catalog_name: DEFAULT_CATALOG.to_owned(),
                                    schema_name: SCHEMA.to_owned(),
                                    table_name: OTHER_TABLE.to_owned(),
                                }
                            },
                            Step::RemoveFile {
                                folder_name: SCHEMA.to_owned(),
                                name: OTHER_TABLE.to_owned()
                            }
                        ]
                    ]
                }
            );
        }

        #[test]
        fn drop_many_if_exists() {
            assert_eq!(
                QUERY_PLANNER.schema_change_plan(&SchemaChange::DropTables(DropTablesQuery {
                    table_infos: vec![
                        TableInfo::new(0, &SCHEMA, &TABLE),
                        TableInfo::new(0, &SCHEMA, &OTHER_TABLE)
                    ],
                    cascade: false,
                    if_exists: true
                })),
                SystemOperation {
                    kind: Kind::Drop(SystemObject::Table),
                    steps: vec![
                        vec![
                            Step::CheckExistence {
                                system_object: SystemObject::Schema,
                                object_name: SCHEMA.to_owned(),
                                skip_if: None,
                            },
                            Step::CheckExistence {
                                system_object: SystemObject::Table,
                                object_name: TABLE.to_owned(),
                                skip_if: Some(ObjectState::NotExists),
                            },
                            Step::RemoveColumns {
                                schema_name: SCHEMA.to_owned(),
                                table_name: TABLE.to_owned(),
                            },
                            Step::RemoveRecord {
                                system_schema: DEFINITION_SCHEMA.to_owned(),
                                system_table: TABLES_TABLE.to_owned(),
                                record: Record::Table {
                                    catalog_name: DEFAULT_CATALOG.to_owned(),
                                    schema_name: SCHEMA.to_owned(),
                                    table_name: TABLE.to_owned(),
                                }
                            },
                            Step::RemoveFile {
                                folder_name: SCHEMA.to_owned(),
                                name: TABLE.to_owned()
                            }
                        ],
                        vec![
                            Step::CheckExistence {
                                system_object: SystemObject::Schema,
                                object_name: SCHEMA.to_owned(),
                                skip_if: None,
                            },
                            Step::CheckExistence {
                                system_object: SystemObject::Table,
                                object_name: OTHER_TABLE.to_owned(),
                                skip_if: Some(ObjectState::NotExists),
                            },
                            Step::RemoveColumns {
                                schema_name: SCHEMA.to_owned(),
                                table_name: OTHER_TABLE.to_owned(),
                            },
                            Step::RemoveRecord {
                                system_schema: DEFINITION_SCHEMA.to_owned(),
                                system_table: TABLES_TABLE.to_owned(),
                                record: Record::Table {
                                    catalog_name: DEFAULT_CATALOG.to_owned(),
                                    schema_name: SCHEMA.to_owned(),
                                    table_name: OTHER_TABLE.to_owned(),
                                }
                            },
                            Step::RemoveFile {
                                folder_name: SCHEMA.to_owned(),
                                name: OTHER_TABLE.to_owned()
                            }
                        ]
                    ]
                }
            );
        }
    }
}
