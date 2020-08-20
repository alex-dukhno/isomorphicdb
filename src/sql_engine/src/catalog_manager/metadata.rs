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

use crate::ColumnDefinition;
use sql_types::SqlType;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        RwLock,
    },
};

#[allow(dead_code)]
const DEFAULT_CATALOG: &'_ str = "public";
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
const COLUMNS_TABLE: &'_ str = "COLUMNS";

/// **SCHEMATA_TABLE** sql types definition
/// CATALOG_NAME    varchar(255)
/// SCHEMA_NAME     varchar(255)
#[allow(dead_code)]
fn schemata_table_types() -> [ColumnDefinition; 2] {
    [
        ColumnDefinition::new("CATALOG_NAME", SqlType::VarChar(255)),
        ColumnDefinition::new("SCHEMA_NAME", SqlType::VarChar(255)),
    ]
}

/// **TABLES_TABLE** sql types definition
/// TABLE_CATALOG   varchar(255)
/// TABLE_SCHEMA    varchar(255)
/// TABLE_NAME      varchar(255)
#[allow(dead_code)]
fn tables_table_types() -> [ColumnDefinition; 3] {
    [
        ColumnDefinition::new("TABLE_CATALOG", SqlType::VarChar(255)),
        ColumnDefinition::new("TABLE_SCHEMA", SqlType::VarChar(255)),
        ColumnDefinition::new("TABLE_NAME", SqlType::VarChar(255)),
    ]
}

/// **COLUMNS_TABLE** sql type definition
/// TABLE_CATALOG       varchar(255)
/// TABLE_SCHEMA        varchar(255)
/// TABLE_NAME          varchar(255)
/// COLUMN_NAME         varchar(255)
/// ORDINAL_POSITION    integer > 0
#[allow(dead_code)]
fn columns_table_types() -> [ColumnDefinition; 5] {
    [
        ColumnDefinition::new("TABLE_CATALOG", SqlType::VarChar(255)),
        ColumnDefinition::new("TABLE_SCHEMA", SqlType::VarChar(255)),
        ColumnDefinition::new("TABLE_NAME", SqlType::VarChar(255)),
        ColumnDefinition::new("COLUMN_NAME", SqlType::VarChar(255)),
        ColumnDefinition::new("ORDINAL_POSITION", SqlType::Integer(1)),
    ]
}

#[allow(dead_code)]
pub(crate) struct DataDefinition {
    schemas_id: AtomicU64,
    schemas: RwLock<HashMap<String, u64>>,
}

#[allow(dead_code)]
impl DataDefinition {
    pub(crate) fn in_memory() -> DataDefinition {
        DataDefinition {
            schemas_id: AtomicU64::default(),
            schemas: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn create_schema(&self, catalog_name: &str, schema_name: &str) {
        let id = self.schemas_id.fetch_add(1, Ordering::SeqCst);
        self.schemas
            .write()
            .expect("to acquire write lock")
            .insert(catalog_name.to_owned() + "." + schema_name, id);
    }

    pub(crate) fn schema_exists(&self, catalog_name: &str, schema_name: &str) -> Option<u64> {
        self.schemas
            .read()
            .expect("to acquire read lock")
            .get(&(catalog_name.to_owned() + "." + schema_name))
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_created_schema_does_not_exist() {
        let data_definition = DataDefinition::in_memory();

        assert!(data_definition.schema_exists("catalog_name", "schema_name").is_none());
    }

    #[test]
    fn create_schema() {
        let data_definition = DataDefinition::in_memory();

        data_definition.create_schema("catalog_name", "schema_name");

        assert!(data_definition.schema_exists("catalog_name", "schema_name").is_some());
    }
}
