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
use kernel::{SystemError, SystemResult};
use representation::{Binary, Datum};
use sql_types::SqlType;
use std::iter::empty;
use std::{collections::HashMap, path::PathBuf, sync::RwLock};
use storage::{KeyValueStorage, NewReadCursor, SledKeyValueStorage, StorageResult, Values};

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

pub type DatabaseResult<T> = std::result::Result<T, StorageError>;

#[derive(Debug, PartialEq)]
pub enum StorageError {
    RuntimeCheckError,
    SystemError(SystemError),
}

pub(crate) type RecordId = u8;
pub(crate) type SchemaName = String;
pub(crate) type SchemaId = u8;
pub(crate) type TableName = String;
pub(crate) type TableId = u8;

struct Persistence {
    definition_schema: Box<dyn KeyValueStorage>,
    schemas: HashMap<SchemaId, Box<dyn KeyValueStorage>>,
}

impl Persistence {
    fn init(root_path: PathBuf) -> SystemResult<(Persistence, bool)> {
        match SledKeyValueStorage::init(root_path) {
            Ok((definition_schema, new_storage)) => Ok((
                Persistence {
                    definition_schema: Box::new(definition_schema),
                    schemas: HashMap::new(),
                },
                new_storage,
            )),
            Err(error) => Err(SystemError::bug_in_storage(format!(
                "could not initialize definition schema due to {:?}",
                error
            ))),
        }
    }
}

/// **SCHEMATA_TABLE** sql types definition
/// SCHEMA_NAME     varchar(255)
fn schemata_table_types() -> Vec<SqlType> {
    vec![SqlType::VarChar(255)]
}

/// **TABLES_TABLE** sql types definition
/// SCHEMA_NAME     varchar(255)
/// TABLE_NAME      varchar(255)
fn tables_table_types() -> Vec<SqlType> {
    vec![SqlType::VarChar(255), SqlType::VarChar(255)]
}

pub struct DatabaseManager {
    record_ids: RwLock<HashMap<SchemaName, HashMap<TableName, RecordId>>>,
    schema_ids: RwLock<HashMap<SchemaName, SchemaId>>,
    table_ids: RwLock<HashMap<SchemaName, HashMap<TableName, TableId>>>,
    persistence: Option<RwLock<Persistence>>,
}

impl DatabaseManager {
    pub fn persistent(root_path: PathBuf) -> SystemResult<DatabaseManager> {
        let (persistence, newly_created) = Persistence::init(root_path.clone())?;
        let database_manager = DatabaseManager {
            record_ids: RwLock::new(HashMap::new()),
            schema_ids: RwLock::new(HashMap::new()),
            table_ids: RwLock::new(HashMap::new()),
            persistence: Some(RwLock::new(persistence)),
        };

        if newly_created {
            let values = Binary::pack(&[Datum::from_str(DEFINITION_SCHEMA)]);
            let record_id = database_manager.next_record_id_in(DEFINITION_SCHEMA, SCHEMATA_TABLE);
            match persistence.definition_schema.write_to_key_space(
                SCHEMATA_TABLE,
                &(record_id.to_be_bytes()),
                values.to_bytes(),
            ) {
                Ok(()) => {}
                Err(error) => {
                    return Err(SystemError::bug_in_storage(format!(
                        "could not create {} in definition schema due to {:?}",
                        SCHEMATA_TABLE, error
                    )))
                }
            }
            for table in [SCHEMATA_TABLE, TABLES_TABLE, COLUMNS_TABLE].iter() {
                let values = Binary::pack(&[Datum::from_str(DEFINITION_SCHEMA), Datum::from_str(table)]);
                let record_id = database_manager.next_record_id_in(DEFINITION_SCHEMA, TABLES_TABLE);
                match persistence.definition_schema.write_to_key_space(
                    TABLES_TABLE,
                    &(record_id.to_be_bytes()),
                    values.to_bytes(),
                ) {
                    Ok(()) => {}
                    Err(error) => {
                        return Err(SystemError::bug_in_storage(format!(
                            "could not create {} in definition schema due to {:?}",
                            table, error
                        )))
                    }
                }
            }
        }

        Ok(database_manager)
    }

    pub fn in_memory() -> DatabaseManager {
        DatabaseManager {
            record_ids: RwLock::new(HashMap::new()),
            schema_ids: RwLock::new(HashMap::new()),
            table_ids: RwLock::new(HashMap::new()),
            persistence: None,
        }
    }

    fn next_record_id_in(&self, schema_name: &str, table_name: &str) -> RecordId {
        let mut records = self.record_ids.write().expect("to acquire write lock");
        match records
            .get_mut(schema_name)
            .and_then(|tables| tables.get_mut(table_name))
        {
            Some(schema_sequence) => {
                let current_id = *schema_sequence;
                *schema_sequence += 1;
                current_id
            }
            None => {
                let mut tables = HashMap::new();
                tables.insert(table_name.to_owned(), 1);
                records.insert(schema_name.to_owned(), tables);
                0
            }
        }
    }

    pub fn create_schema(&self, schema_name: &str) -> DatabaseResult<()> {
        match self.schema_exists(schema_name) {
            Some(_schema_id) => Err(StorageError::RuntimeCheckError),
            None => {
                let schema_id = self.next_record_id_in(DEFINITION_SCHEMA, SCHEMATA_TABLE);
                if let Some(on_disk) = self.persistence.as_ref() {
                    match on_disk
                        .read()
                        .expect("to acquire read lock")
                        .definition_schema
                        .write_to_key_space(SCHEMATA_TABLE, &schema_id.to_be_bytes(), schema_name.as_bytes())
                    {
                        Ok(()) => {}
                        Err(error) => {
                            return Err(StorageError::SystemError(SystemError::bug_in_storage(format!(
                                "could not create {} in definition schema due to {:?}",
                                schema_name, error
                            ))))
                        }
                    }
                }
                self.schema_ids
                    .write()
                    .expect("to acquire write lock")
                    .insert(schema_name.to_owned(), schema_id);
                Ok(())
            }
        }
    }

    pub fn schema_exists(&self, schema_name: &str) -> Option<u8> {
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
                if let Some(persistence) = self.persistence.as_ref() {
                    match persistence
                        .read()
                        .expect("to acquire read lock")
                        .definition_schema
                        .delete_from_key_space(SCHEMATA_TABLE, &schema_id.to_be_bytes())
                    {
                        Ok(()) => {}
                        Err(error) => Err(SystemError::bug_in_storage(format!(
                            "could not drop {} from definition schema due to {:?}",
                            schema_name, error
                        ))),
                    }
                }
            }
            None => return Err(StorageError::RuntimeCheckError),
        }
        Ok(())
    }

    pub fn create_table(
        &self,
        schema_name: &str,
        table_name: &str,
        column_definitions: &[ColumnDefinition],
    ) -> DatabaseResult<()> {
        match self.table_exists(schema_name, table_name) {
            None => Err(StorageError::RuntimeCheckError),
            Some((_schema_id, Some(_table_id))) => Err(StorageError::RuntimeCheckError),
            Some((schema_id, None)) => {
                let table_id = self.next_record_id_in(DEFINITION_SCHEMA, TABLES_TABLE);
                if let Some(persistence) = self.persistence.as_ref() {
                    let schema = persistence
                        .read()
                        .expect("to acquire read lock")
                        .schemas
                        .get(&schema_id);
                    match schema {
                        Some(schema) => match (*schema).create_key_space(table_name) {
                            Ok(()) => {
                                let guard = persistence.read().expect("to acquire read lock");
                                let values = Binary::pack(&[Datum::from_str(schema_name), Datum::from_str(table_name)]);
                                guard.definition_schema.write_to_key_space(
                                    TABLES_TABLE,
                                    &table_id.to_be_bytes(),
                                    values.to_bytes(),
                                );
                                for column_definition in column_definitions {
                                    let column_id = self.next_record_id_in(DEFINITION_SCHEMA, COLUMNS_TABLE);
                                    let values = Binary::pack(&[
                                        Datum::from_str(schema_name),
                                        Datum::from_str(table_name),
                                        Datum::from_str(column_definition.name().as_str()),
                                        Datum::from_sql_type(column_definition.sql_type()),
                                    ]);
                                    match guard.definition_schema.write_to_key_space(
                                        COLUMNS_TABLE,
                                        &(column_id.to_be_bytes()),
                                        values.to_bytes(),
                                    ) {
                                        Ok(()) => {}
                                        Err(error) => {
                                            return Err(StorageError::SystemError(SystemError::bug_in_storage(
                                                format!("{} couldn't be found on persistent device", schema_name),
                                            )))
                                        }
                                    }
                                }
                            }
                            Err(error) => return Err(StorageError::SystemError(error.into())),
                        },
                        None => {
                            return Err(StorageError::SystemError(SystemError::bug_in_storage(format!(
                                "could not create {} in definition schema due to {:?}",
                                schema_name
                            ))));
                        }
                    }
                }
                self.table_ids
                    .write()
                    .expect("to acquire write lock")
                    .get_mut(schema_name)
                    .map(|tables| tables.insert(table_name.to_owned(), table_id));
                Ok(())
            }
        }
    }

    pub fn table_exists(&self, schema_name: &str, table_name: &str) -> Option<(SchemaId, Option<TableId>)> {
        match self.schema_exists(schema_name) {
            Some(schema_id) => self
                .table_ids
                .read()
                .expect("to acquire read lock")
                .get(schema_name)
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

    pub fn drop_table(&self, schema_name: &str, table_name: &str) -> DatabaseResult<()> {
        match self
            .table_ids
            .write()
            .expect("to acquire write lock")
            .remove(&(schema_name.to_owned() + "." + table_name))
        {
            Some(table_id) => {
                if let Some(persistence) = self.persistence.as_ref() {
                    persistence.delete_from_key_space(TABLES_TABLE, &table_id.to_be_bytes())?;
                }
            }
            None => return Err(StorageError::RuntimeCheckError),
        }
        Ok(())
    }

    pub fn insert_into_table(&self, schema_name: &str, table_name: &str, rows: Vec<Binary>) -> DatabaseResult<usize> {
        match self.table_exists(schema_name, table_name) {
            None => Err(StorageError::RuntimeCheckError),
            Some((_schema_id, None)) => Err(StorageError::RuntimeCheckError),
            Some((schema_id, table_id)) => {
                let size = rows.len();
                match self.persistence.as_ref() {
                    None => {}
                    Some(persistence) => {
                        let guard = persistence.read().expect("to acquire read lock");
                        match guard.schemas.get(&schema_id) {
                            Some(schema) => {
                                for row in rows {
                                    let record_id = self.next_record_id_in(schema_name, table_name);
                                    match schema.write_to_key_space(
                                        table_name,
                                        &record_id.to_be_bytes(),
                                        row.to_bytes(),
                                    ) {
                                        Ok(()) => {}
                                        Err(error) => return Err(error.into()),
                                    }
                                }
                            }
                            None => {
                                return Err(StorageError::SystemError(SystemError::bug_in_storage(format!(
                                    "{} couldn't be found in persistence",
                                    schema_name
                                ))));
                            }
                        }
                    }
                }
                Ok(size)
            }
        }
    }

    pub fn table_descriptor(&self, schema_name: &str, table_name: &str) -> DatabaseResult<TableDefinition> {
        match self.table_exists(schema_name, table_name) {
            None => Err(StorageError::RuntimeCheckError),
            Some((_schema_id, None)) => Err(StorageError::RuntimeCheckError),
            Some((_schema_id, Some(_table_id))) => match self.persistence.as_ref() {
                None => {}
                Some(persistence) => {
                    let guard = persistence.read().expect("to acquire read lock");
                    match guard.definition_schema.scan_key_space(COLUMNS_TABLE) {
                        Err(error) => return Err(error.into()),
                        Ok(cursor) => {
                            let columns = cursor
                                .map(Result::unwrap)
                                .map(|(key, values)| Binary::with_data(values).unpack())
                                .filter(|data| {
                                    data[0] == Datum::from_str(schema_name) && data[1] == Datum::from_str(table_name)
                                })
                                .map(|data| ColumnDefinition::new(data[2].as_str(), data[3].as_sql_type()))
                                .collect();
                            Ok(TableDefinition::new(schema_name, table_name, columns))
                        }
                    }
                }
            },
        }
    }

    pub fn table_columns(&self, schema_name: &str, table_name: &str) -> DatabaseResult<Vec<ColumnDefinition>> {
        match self.table_exists(schema_name, table_name) {
            None => Err(StorageError::RuntimeCheckError),
            Some((_schema_id, None)) => Err(StorageError::RuntimeCheckError),
            Some((_schema_id, Some(_table_id))) => match self.persistence.as_ref() {
                None => {}
                Some(persistence) => {
                    let guard = persistence.read().expect("to acquire read lock");
                    match guard.definition_schema.scan_key_space(COLUMNS_TABLE) {
                        Err(error) => return Err(error.into()),
                        Ok(cursor) => Ok(cursor
                            .map(Result::unwrap)
                            .map(|(key, values)| Binary::with_data(values).unpack())
                            .filter(|data| {
                                data[0] == Datum::from_str(schema_name) && data[1] == Datum::from_str(table_name)
                            })
                            .map(|data| ColumnDefinition::new(data[2].as_str(), data[3].as_sql_type()))
                            .collect()),
                    }
                }
            },
        }
    }

    pub fn table_scan(&self, schema_name: &str, table_name: &str) -> DatabaseResult<NewReadCursor> {
        match self.table_exists(schema_name, table_name) {
            None => Err(StorageError::RuntimeCheckError),
            Some((_schema_id, None)) => Err(StorageError::RuntimeCheckError),
            Some((schema_id, Some(table_id))) => match self.persistence.as_ref() {
                Some(persistence) => {
                    match persistence
                        .read()
                        .expect("to acquire read lock")
                        .schemas
                        .get(&schema_id)
                    {
                        Some(schema) => schema.scan_key_space(table_name),
                        None => Err(StorageError::SystemError(SystemError::bug_in_storage(format!(
                            "{} schema could not be found in persistence layer",
                            schema_name
                        )))),
                    }
                }
                None => Ok(Box::new(empty())),
            },
        }
    }

    pub fn delete_all_from(&mut self, schema_name: &str, table_name: &str) -> DatabaseResult<usize> {
        match self.table_exists(schema_name, table_name) {
            None => Err(StorageError::RuntimeCheckError),
            Some((_schema_id, None)) => Err(StorageError::RuntimeCheckError),
            Some((schema_id, Some(table_id))) => match self.persistence.as_ref() {
                Some(persistence) => {
                    match persistence
                        .read()
                        .expect("to acquire read lock")
                        .schemas
                        .get(&schema_id)
                    {
                        Some(schema) => {
                            let mut size = 0;
                            for key in schema
                                .scan_key_space(table_name)
                                .map(Result::unwrap)
                                .map(|key, _values| key)
                            {
                                size += 1;
                                schema.delete_from_key_space(table_name, key)
                            }
                            Ok(size)
                        }
                        None => Err(StorageError::SystemError(SystemError::bug_in_storage(format!(
                            "{} schema could not be found in persistence layer",
                            schema_name
                        )))),
                    }
                }
                None => Ok(0),
            },
        }
    }
}

#[cfg(test)]
mod tests;
