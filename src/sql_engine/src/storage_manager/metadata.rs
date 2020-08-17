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

use crate::definitions::{ColumnDefinition, TableDefinition};
use crate::storage_manager::{DatabaseResult, RecordId, SchemaId, SchemaName, StorageError, TableId, TableName};
use kernel::SystemResult;
use representation::{Binary, Datum};
use sql_types::SqlType;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::RwLock;
use storage::{KeyValueStorage, SledKeyValueStorage};

const DEFAULT_CATALOG: &'_ str = "public";
const DEFINITION_SCHEMA: &'_ str = "DEFINITION_SCHEMA";
// CREATE TABLE SCHEMATA (
//     CATALOG_NAME                    INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     SCHEMA_NAME                     INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     SCHEMA_OWNER                    INFORMATION_SCHEMA.SQL_IDENTIFIER
//                                     CONSTRAINT
//                                         SCHEMA_OWNER_NOT_NULL NOT NULL,
//     DEFAULT_CHARACTER_SET_CATALOG   INFORMATION_SCHEMA.SQL_IDENTIFIER
//                                     CONSTRAINT
//                                         DEFAULT_CHARACTER_SET_CATALOG_NOT_NULL NOT NULL,
//     DEFAULT_CHARACTER_SET_SCHEMA    INFORMATION_SCHEMA.SQL_IDENTIFIER
//                                     CONSTRAINT
//                                         DEFAULT_CHARACTER_SET_SCHEMA_NOT_NULL NOT NULL,
//     DEFAULT_CHARACTER_SET_NAME      INFORMATION_SCHEMA.SQL_IDENTIFIER
//                                     CONSTRAINT
//                                         DEFAULT_CHARACTER_SET_NAME_NOT_NULL NOT NULL,
//     SQL_PATH                        INFORMATION_SCHEMA.CHARACTER_DATA,
//
//     CONSTRAINT SCHEMATA_PRIMARY_KEY
//         PRIMARY KEY (CATALOG_NAME, SCHEMA_NAME),
//
//     CONSTRAINT SCHEMATA_FOREIGN_KEY_AUTHORIZATIONS
//         FOREIGN KEY (SCHEMA_OWNER)
//         REFERENCES AUTHORIZATIONS,
//
//     CONSTRAINT SCHEMATA_FOREIGN_KEY_CATALOG_NAMES
//         FOREIGN KEY (CATALOG_NAME)
//         REFERENCES CATALOG_NAMES
// )
const SCHEMATA_TABLE: &'_ str = "SCHEMATA";
// CREATE TABLE TABLES (
//     TABLE_CATALOG                               INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     TABLE_SCHEMA                                INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     TABLE_NAME                                  INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     TABLE_TYPE                                  INFORMATION_SCHEMA.CHARACTER_DATA
//                                                 CONSTRAINT
//                                                     TABLE_TYPE_NOT_NULL NOT NULL
//                                                 CONSTRAINT
//                                                     TABLE_TYPE_CHECK CHECK (
//                                                         TABLE_TYPE IN ('BASE TABLE', 'VIEW', 'GLOBAL TEMPORARY', 'LOCAL TEMPORARY')
//                                                     ),
//     SELF_REFERENCING_COLUMN_NAME                INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     REFERENCE_GENERATION                        INFORMATION_SCHEMA.CHARACTER_DATA
//                                                 CONSTRAINT
//                                                     REFERENCE_GENERATION_CHECK CHECK (
//                                                         REFERENCE_GENERATION IN ('SYSTEM GENERATED', 'USER GENERATED', 'DERIVED')
//                                                     ),
//     USER_DEFINED_TYPE_CATALOG                   INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     USER_DEFINED_TYPE_SCHEMA                    INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     USER_DEFINED_TYPE_NAME                      INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     IS_INSERTABLE_INTO                          INFORMATION_SCHEMA.CHARACTER_DATA
//                                                 CONSTRAINT
//                                                     IS_INSERTABLE_INTO_NOT_NULL NOT NULL
//                                                 CONSTRAINT
//                                                     IS_INSERTABLE_INTO_CHECK CHECK (
//                                                         IS_INSERTABLE_INTO IN ('YES', 'NO')
//                                                     )
//     IS_TYPED                                    INFORMATION_SCHEMA.CHARACTER_DATA
//                                                 CONSTRAINT
//                                                     IS_TYPED_NOT_NULL NOT NULL
//                                                 CONSTRAINT
//                                                     IS_TYPED_CHECK CHECK (IS_TYPED IN ('YES', 'NO'))
//     COMMIT_ACTION                               INFORMATION_SCHEMA.CHARACTER_DATA
//                                                 CONSTRAINT
//                                                     COMMIT_ACTIONCHECK CHECK (
//                                                         COMMIT_ACTION IN ('DELETE', 'PRESERVE')
//                                                     ),
//
//     CONSTRAINT TABLES_PRIMARY_KEY
//         PRIMARY KEY (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME),
//
//     CONSTRAINT TABLES_FOREIGN_KEY_SCHEMATA
//         FOREIGN KEY (TABLE_CATALOG, TABLE_SCHEMA)
//         REFERENCES SCHEMATA,
//
//     CONSTRAINT TABLES_CHECK_TABLE_IN_COLUMNS
//         CHECK (
//             (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME) IN (SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME FROM COLUMNS)
//         ),
//
//     CONSTRAINT TABLES_FOREIGN_KEY_USER_DEFINED_TYPES
//         FOREIGN KEY (USER_DEFINED_TYPE_CATALOG, USER_DEFINED_TYPE_SCHEMA, USER_DEFINED_TYPE_NAME)
//         REFERENCES USER_DEFINED_TYPES MATCH FULL,
//
//     CONSTRAINT TABLES_TYPED_TABLE_CHECK CHECK (
//         (
//             IS_TYPED = 'YES'
//             AND (
//                 (USER_DEFINED_TYPE_CATALOG,
//                     USER_DEFINED_TYPE_SCHEMA,
//                     USER_DEFINED_TYPE_NAME,
//                     SELF_REFERENCING_COLUMN_NAME,
//                     REFERENCE_GENERATION) IS NOT NULL
//                 AND
//                 (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, SELF_REFERENCING_COLUMN_NAME) IN
//                     (SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM COLUMNS WHERE IS_SELF_REFERENCING = 'YES')
//             )
//         ) OR (
//             IS_TYPED = 'NO'
//             AND (
//                 USER_DEFINED_TYPE_CATALOG,
//                 USER_DEFINED_TYPE_SCHEMA,
//                 USER_DEFINED_TYPE_NAME,
//                 SELF_REFERENCING_COLUMN_NAME,
//                 REFERENCE_GENERATION ) IS NULL
//         )
//     ),
//
//     CONSTRAINT TABLES_SELF_REFERENCING_COLUMN_CHECK
//         CHECK (
//             (SELF_REFERENCING_COLUMN_NAME, REFERENCE_GENERATION) IS NULL
//             OR (SELF_REFERENCING_COLUMN_NAME, REFERENCE_GENERATION) IS NOT NULL
//         ),
//
//     CONSTRAINT TABLES_TEMPORARY_TABLE_CHECK
//         CHECK (
//             (TABLE_TYPE IN ('GLOBAL TEMPORARY', 'LOCAL TEMPORARY')) = (COMMIT_ACTION IS NOT NULL)
//         ),
//
//     CONSTRAINT TABLES_CHECK_NOT_VIEW
//         CHECK (
//             NOT EXISTS (
//                 SELECT  TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
//                 FROM    TABLES
//                 WHERE   TABLE_TYPE = 'VIEW'
//                 EXCEPT
//                 SELECT  TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
//                 FROM    VIEWS
//             )
//         )
// )
const TABLES_TABLE: &'_ str = "TABLES";
// CREATE TABLE COLUMNS (
//     TABLE_CATALOG           INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     TABLE_SCHEMA            INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     TABLE_NAME              INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     COLUMN_NAME             INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     ORDINAL_POSITION        INFORMATION_SCHEMA.CARDINAL_NUMBER
//                             CONSTRAINT
//                                 COLUMNS_ORDINAL_POSITION_NOT_NULL NOT NULL
//                             CONSTRAINT
//                                 COLUMNS_ORDINAL_POSITION_GREATER_THAN_ZERO_CHECK
//                                 CHECK (ORDINAL_POSITION > 0)
//                             CONSTRAINT COLUMNS_ORDINAL_POSITION_CONTIGUOUS_CHECK
//                                 CHECK (0 = ALL(
//                                     SELECT      MAX(ORDINAL_POSITION) - COUNT(*)
//                                     FROM        COLUMNS
//                                     GROUP BY    TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
//                                     )),
//     DTD_IDENTIFIER          INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     DOMAIN_CATALOG          INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     DOMAIN_SCHEMA           INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     DOMAIN_NAME             INFORMATION_SCHEMA.SQL_IDENTIFIER,
//     COLUMN_DEFAULT          INFORMATION_SCHEMA.CHARACTER_DATA,
//     IS_NULLABLE             INFORMATION_SCHEMA.CHARACTER_DATA
//                             CONSTRAINT
//                                 COLUMNS_IS_NULLABLE_NOT_NULL NOT NULL
//                             CONSTRAINT
//                                 COLUMNS_IS_NULLABLE_CHECK
//                                 CHECK (IS_NULLABLE IN ('YES', 'NO')),
//     IS_SELF_REFERENCING     INFORMATION_SCHEMA.CHARACTER_DATA
//                             CONSTRAINT
//                                 COLUMNS_IS_SELF_REFERENCING_NOT_NULL NOT NULL
//                             CONSTRAINT
//                                 COLUMNS_IS_SELF_REFERENCING_CHECK
//                                 CHECK (IS_SELF_REFERENCING IN ('YES', 'NO')),
//     IS_IDENTITY             INFORMATION_SCHEMA.CHARACTER_DATA
//                             CONSTRAINT COLUMNS_IS_IDENTITY_NOT_NULL NOT NULL
//                             CONSTRAINT COLUMNS_IS_IDENTITY_CHECK
//                                 CHECK (IS_IDENTITY IN ('YES', 'NO')),
//     IDENTITY_GENERATION     INFORMATION_SCHEMA.CHARACTER_DATA,
//     IDENTITY_START          INFORMATION_SCHEMA.CHARACTER_DATA,
//     IDENTITY_INCREMENT      INFORMATION_SCHEMA.CHARACTER_DATA,
//     IDENTITY_MAXIMUM        INFORMATION_SCHEMA.CHARACTER_DATA,
//     IDENTITY_MINIMUM        INFORMATION_SCHEMA.CHARACTER_DATA,
//     IDENTITY_CYCLE          INFORMATION_SCHEMA.CHARACTER_DATA
//                             CONSTRAINT
//                                 COLUMNS_IDENTITY_CYCLE_CHECK
//                                 CHECK (IDENTITY_CYCLE IN ('YES', 'NO')),
//     IS_GENERATED            INFORMATION_SCHEMA.CHARACTER_DATA,
//     GENERATION_EXPRESSION   INFORMATION_SCHEMA.CHARACTER_DATA,
//     IS_UPDATABLE            INFORMATION_SCHEMA.CHARACTER_DATA
//                             CONSTRAINT COLUMNS_IS_UPDATABLE_NOT_NULL NOT NULL
//                             CONSTRAINT COLUMNS_IS_UPDATABLE_CHECK
//                                 CHECK (IS_UPDATABLE IN ('YES', 'NO')),
//
//     CONSTRAINT COLUMNS_PRIMARY_KEY
//         PRIMARY KEY (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME),
//
//     CONSTRAINT COLUMNS_UNIQUE
//         UNIQUE (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION),
//
//     CONSTRAINT COLUMNS_FOREIGN_KEY_TABLES
//         FOREIGN KEY (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME)
//         REFERENCES TABLES,
//
//     CONSTRAINT COLUMNS_CHECK_REFERENCES_DOMAIN
//         CHECK (
//             DOMAIN_CATALOG NOT IN (SELECT CATALOG_NAME FROM SCHEMATA)
//             OR (DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME)
//                 IN (SELECT DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME FROM DOMAINS)
//         ),
//
//     CONSTRAINT COLUMNS_CHECK_IDENTITY_COMBINATIONS
//         CHECK (
//             (IS_IDENTITY = 'NO') =
//             ((IDENTITY_GENERATION, IDENTITY_START, IDENTITY_INCREMENT, IDENTITY_MAXIMUM, IDENTITY_MINIMUM, IDENTITY_CYCLE) IS NULL)
//         ),
//
//     CONSTRAINT COLUMNS_CHECK_DATA_TYPE
//         CHECK (
//             DOMAIN_CATALOG NOT IN (SELECT CATALOG_NAME FROM SCHEMATA)
//             OR (
//                 (DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME) IS NOT NULL
//                 AND
//                 (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, 'TABLE', DTD_IDENTIFIER)
//                     NOT IN (SELECT OBJECT_CATALOG, OBJECT_SCHEMA, OBJECT_NAME, OBJECT_TYPE, DTD_IDENTIFIER FROM DATA_TYPE_DESCRIPTOR))
//             OR (
//                 (
//                     (DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME) IS NULL
//                     AND
//                     (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, 'TABLE', DTD_IDENTIFIER)
//                         IN (SELECT OBJECT_CATALOG, OBJECT_SCHEMA, OBJECT_NAME, OBJECT_TYPE, DTD_IDENTIFIER FROM DATA_TYPE_DESCRIPTOR)
//                 )
//             )
//         )
// )
const COLUMNS_TABLE: &'_ str = "COLUMNS";

/// **SCHEMATA_TABLE** sql types definition
/// SCHEMA_NAME     varchar(255)
const fn schemata_table_types() -> &[ColumnDefinition] {
    &[ColumnDefinition::new("SCHEMA_NAME", SqlType::VarChar(255))]
}

/// **TABLES_TABLE** sql types definition
/// SCHEMA_NAME     varchar(255)
/// TABLE_NAME      varchar(255)
const fn tables_table_types() -> &[ColumnDefinition] {
    &[
        ColumnDefinition::new("SCHEMA_NAME", SqlType::VarChar(255)),
        ColumnDefinition::new("TABLE_NAME", SqlType::VarChar(255)),
    ]
}

const DEFINITION_SCHEMA_ID: RecordId = 0;

pub(crate) struct DataDefinition {
    system_record_ids: RwLock<HashMap<TableName, RecordId>>,
    schema_ids: RwLock<HashMap<SchemaName, SchemaId>>,
    table_ids: RwLock<HashMap<SchemaId, HashMap<TableName, TableId>>>,
    table_descriptions: RwLock<HashMap<SchemaId, HashMap<TableId, TableDefinition>>>,
    definition_schema: Option<Box<dyn KeyValueStorage>>,
}

impl DataDefinition {
    pub(crate) fn persistent(root_path: PathBuf) -> SystemResult<(DataDefinition, bool)> {
        let (definition_schema, new_storage) = SledKeyValueStorage::init(root_path)?;

        let data_definition = DataDefinition {
            system_record_ids: RwLock::new(HashMap::new()),
            table_descriptions: RwLock::new(HashMap::new()),
            definition_schema: Some(Box::new(definition_schema)),
        };

        if newly_created {
            let values = Binary::pack(&[Datum::from_str(DEFINITION_SCHEMA)]);
            let record_id = data_definition.next_record_id_in(SCHEMATA_TABLE);
            match definition_schema.write_to_key_space(SCHEMATA_TABLE, &(record_id.to_be_bytes()), values.to_bytes()) {
                Ok(()) => {}
                Err(error) => return Err(error.into()),
            }
            for table in [SCHEMATA_TABLE, TABLES_TABLE, COLUMNS_TABLE] {
                let values = Binary::pack(&[Datum::from_str(DEFINITION_SCHEMA), Datum::from_str(table)]);
                let record_id = data_definition.next_record_id_in(TABLES_TABLE);
                match definition_schema.write_to_key_space(TABLES_TABLE, &(record_id.to_be_bytes()), values.to_bytes())
                {
                    Ok(()) => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }

        Ok((data_definition, new_storage))
    }

    pub fn create_schema(&self, schema_name: &str) -> DatabaseResult<()> {
        let schema_id = self.next_record_id_in(SCHEMATA_TABLE);
        if let Some(definition_schema) = self.definition_schema.as_ref() {
            definition_schema.write_to_key_space(SCHEMATA_TABLE, &schema_id.to_be_bytes(), schema_name.as_bytes())?
        }
        self.schema_ids
            .write()
            .expect("to acquire write lock")
            .insert(schema_name.to_owned(), schema_id);
        Ok(())
    }

    pub fn schema_exists(&self, schema_name: &str) -> Option<SchemaId> {
        self.schema_ids
            .read()
            .expect("to acquire read lock")
            .get(schema_name)
            .cloned()
    }

    pub fn drop_schema(&self, schema_name: &str) -> DatabaseResult<()> {
        match self
            .schema_ids
            .write()
            .expect("to acquire write lock")
            .remove(schema_name)
        {
            Some(schema_id) => {
                if let Some(definition_schema) = self.definition_schema.as_ref() {
                    definition_schema.delete_from_key_space(SCHEMATA_TABLE, &schema_id.to_be_bytes())?;
                }
            }
            None => return Err(StorageError::RuntimeCheckError),
        }
        Ok(())
    }

    pub fn table_exists(&self, schema_name: &str, table_name: &str) -> Option<(SchemaId, Option<TableId>)> {
        match self.schema_exists(schema_name) {
            Some(schema_id) => self
                .table_ids
                .read()
                .expect("to acquire read lock")
                .get(&schema_id)
                .and_then(|tables| tables.get(table_name))
                .cloned()
                .map(|table_id| (schema_id, Some(table_id)))
                .or_else(|| {
                    let table_id: Option<TableId> = None;
                    Some((schema_id, table_id))
                }),
            None => None,
        }
    }

    pub(crate) fn add_table_definition(
        &self,
        schema_id: SchemaName,
        table_id: TableName,
        column_definitions: &[ColumnDefinition],
    ) -> DatabaseResult<()> {
        let mut guard = self.table_descriptions.write().expect("to acquire write lock");
        match guard.get_mut(&schema_id) {
            Some(schema) => schema.insert(table_id, column_definitions),
            None => Err(StorageError::RuntimeCheckError),
        }
    }

    fn next_record_id_in(&self, table_name: &str) -> RecordId {
        let mut records = self.system_record_ids.write().expect("to acquire write lock");
        match records.get_mut(table_name) {
            Some(table_sequence) => {
                let current_id = *table_sequence;
                *table_sequence += 1;
                current_id
            }
            None => {
                records.insert(table_name.to_owned(), 1);
                0
            }
        }
    }
}
