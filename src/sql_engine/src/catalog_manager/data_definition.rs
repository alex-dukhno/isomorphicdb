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

use crate::{catalog_manager::DropStrategy, ColumnDefinition};
use kernel::{SystemError, SystemResult};
use representation::{Binary, Datum};
use sql_types::SqlType;
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
};
use storage::{Database, InitStatus, PersistentDatabase, StorageError};

const SYSTEM_CATALOG: &'_ str = "system";
// CREATE SCHEMA DEFINITION_SCHEMA
//      AUTHORIZATION DEFINITION_SCHEMA
const DEFINITION_SCHEMA: &'_ str = "DEFINITION_SCHEMA";
//CREATE TABLE CATALOG_NAMES (
//     CATALOG_NAME    INFORMATION_SCHEMA.SQL_IDENTIFIER,
//                     CONSTRAINT CATALOG_NAMES_PRIMARY_KEY
//                         PRIMARY KEY (CATALOG_NAME)
// )
const CATALOG_NAMES_TABLE: &'_ str = "CATALOG_NAMES";
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

#[allow(dead_code)]
fn catalog_names_types() -> [ColumnDefinition; 1] {
    [ColumnDefinition::new("CATALOG_NAME", SqlType::VarChar(255))]
}

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

pub(crate) type Id = u64;
#[allow(dead_code)]
pub(crate) type CatalogId = Option<Id>;
pub(crate) type SchemaId = Option<(Id, Option<Id>)>;
pub(crate) type TableId = Option<(Id, Option<(Id, Option<Id>)>)>;
type Name = String;

struct Catalog {
    id: Id,
    schemas: RwLock<HashMap<Name, Arc<Schema>>>,
    schema_id_generator: AtomicU64,
}

impl Catalog {
    fn new(id: Id) -> Catalog {
        Catalog {
            id,
            schemas: RwLock::default(),
            schema_id_generator: AtomicU64::default(),
        }
    }

    fn id(&self) -> Id {
        self.id
    }

    fn schema_exists(&self, schema_name: &str) -> Option<Id> {
        self.schemas
            .read()
            .expect("to acquire read lock")
            .get(schema_name)
            .map(|schema| schema.id())
    }

    fn create_schema(&self, schema_name: &str) -> Id {
        let schema_id = self.schema_id_generator.fetch_add(1, Ordering::SeqCst);
        self.schemas
            .write()
            .expect("to acquire write lock")
            .insert(schema_name.to_owned(), Arc::new(Schema::new(schema_id)));
        schema_id
    }

    fn add_schema(&self, schema_id: Id, schema_name: &str) -> Arc<Schema> {
        let schema = Arc::new(Schema::new(schema_id));
        self.schemas
            .write()
            .expect("to acquire write lock")
            .insert(schema_name.to_owned(), schema.clone());
        schema
    }

    fn remove_schema(&self, schema_name: &str) -> Option<Id> {
        self.schemas
            .write()
            .expect("to acquire write lock")
            .remove(schema_name)
            .map(|schema| schema.id())
    }

    fn schema(&self, schema_name: &str) -> Option<Arc<Schema>> {
        self.schemas
            .read()
            .expect("to acquire read lock")
            .get(schema_name)
            .cloned()
    }

    fn schemas(&self) -> Vec<String> {
        self.schemas
            .read()
            .expect("to acquire read lock")
            .keys()
            .cloned()
            .collect()
    }

    fn empty(&self) -> bool {
        self.schemas.read().expect("to acquire read lock").is_empty()
    }
}

struct Schema {
    id: Id,
    tables: RwLock<HashMap<Name, Arc<Table>>>,
    table_id_generator: AtomicU64,
}

impl Schema {
    fn new(id: Id) -> Schema {
        Schema {
            id,
            tables: RwLock::default(),
            table_id_generator: AtomicU64::default(),
        }
    }

    fn id(&self) -> Id {
        self.id
    }

    fn create_table(&self, table_name: &str, column_definitions: &[ColumnDefinition]) -> Arc<Table> {
        let table_id = self.table_id_generator.fetch_add(1, Ordering::SeqCst);
        let table = Arc::new(Table::new(table_id, column_definitions));
        self.tables
            .write()
            .expect("to acquire write lock")
            .insert(table_name.to_owned(), table.clone());
        table
    }

    fn add_table(
        &self,
        table_id: Id,
        table_name: &str,
        column_definitions: BTreeMap<Id, ColumnDefinition>,
        max_id: Id,
    ) {
        self.tables.write().expect("to acquire write lock").insert(
            table_name.to_owned(),
            Arc::new(Table::restore(table_id, column_definitions, max_id)),
        );
    }

    fn table_exists(&self, table_name: &str) -> Option<Id> {
        self.tables
            .read()
            .expect("to acquire read lock")
            .get(table_name)
            .map(|table| table.id())
    }

    fn table(&self, table_name: &str) -> Option<Arc<Table>> {
        self.tables
            .read()
            .expect("to acquire read lock")
            .get(table_name)
            .cloned()
    }

    fn remove_table(&self, table_name: &str) -> Option<Id> {
        self.tables
            .write()
            .expect("to acquire lock")
            .remove(table_name)
            .map(|table| table.id())
    }

    fn tables(&self) -> Vec<String> {
        self.tables
            .read()
            .expect("to acquire read lock")
            .keys()
            .cloned()
            .collect()
    }

    fn empty(&self) -> bool {
        self.tables.read().expect("to acquire read lock").is_empty()
    }
}

struct Table {
    id: Id,
    columns: RwLock<BTreeMap<Id, ColumnDefinition>>,
    column_id_generator: AtomicU64,
}

impl Table {
    fn new(id: Id, column_definitions: &[ColumnDefinition]) -> Table {
        let table = Table {
            id,
            columns: RwLock::default(),
            column_id_generator: AtomicU64::default(),
        };
        for column_definition in column_definitions.to_vec().into_iter() {
            table.add_column(column_definition)
        }
        table
    }

    fn restore(id: Id, column_definitions: BTreeMap<Id, ColumnDefinition>, max_id: Id) -> Table {
        Table {
            id,
            columns: RwLock::new(column_definitions),
            column_id_generator: AtomicU64::new(max_id),
        }
    }

    fn id(&self) -> Id {
        self.id
    }

    fn add_column(&self, column_definition: ColumnDefinition) {
        let column_id = self.column_id_generator.fetch_add(1, Ordering::SeqCst);
        self.columns
            .write()
            .expect("to acquire write lock")
            .insert(column_id, column_definition);
    }

    fn columns(&self) -> Vec<(Id, ColumnDefinition)> {
        self.columns
            .read()
            .expect("to acquire read lock")
            .iter()
            .map(|(id, definition)| (*id, definition.clone()))
            .collect()
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum DropCatalogError {
    DoesNotExist,
    HasDependentObjects,
}

#[derive(Debug, PartialEq)]
pub(crate) enum DropSchemaError {
    CatalogDoesNotExist,
    DoesNotExist,
    HasDependentObjects,
}

pub(crate) struct DataDefinition {
    catalog_ids: AtomicU64,
    catalogs: RwLock<HashMap<Name, Arc<Catalog>>>,
    system_catalog: Option<Box<dyn Database>>,
}

impl DataDefinition {
    pub(crate) fn in_memory() -> DataDefinition {
        DataDefinition {
            catalog_ids: AtomicU64::default(),
            catalogs: RwLock::default(),
            system_catalog: None,
        }
    }

    pub(crate) fn persistent(path: &PathBuf) -> SystemResult<DataDefinition> {
        let system_catalog = PersistentDatabase::new(path.join(SYSTEM_CATALOG));
        let (catalogs, catalog_ids) = match system_catalog.init(DEFINITION_SCHEMA) {
            Ok(InitStatus::Loaded) => {
                let mut max_id = 0;
                let catalogs = system_catalog
                    .read(DEFINITION_SCHEMA, CATALOG_NAMES_TABLE)
                    .expect("to have CATALOG_NAMES table")
                    .map(Result::unwrap)
                    .map(|(id, name)| {
                        let catalog_id = id.unpack()[0].as_u64();
                        max_id = max_id.max(catalog_id);
                        let catalog_name = name.unpack()[0].as_str().to_owned();
                        (catalog_name, Arc::new(Catalog::new(catalog_id)))
                    })
                    .collect::<HashMap<_, _>>();
                (catalogs, max_id)
            }
            Ok(InitStatus::Created) => {
                system_catalog
                    .create_object(DEFINITION_SCHEMA, CATALOG_NAMES_TABLE)
                    .expect("table CATALOG_NAMES is created");
                system_catalog
                    .create_object(DEFINITION_SCHEMA, SCHEMATA_TABLE)
                    .expect("table SCHEMATA is created");
                system_catalog
                    .create_object(DEFINITION_SCHEMA, TABLES_TABLE)
                    .expect("table TABLES is created");
                system_catalog
                    .create_object(DEFINITION_SCHEMA, COLUMNS_TABLE)
                    .expect("table COLUMNS is created");
                (HashMap::new(), 0)
            }
            Err(StorageError::RuntimeCheckError) => {
                return Err(SystemError::runtime_check_failure(
                    "No Path in SledDatabaseCatalog".to_owned(),
                ))
            }
            Err(StorageError::SystemError(error)) => return Err(error),
        };
        Ok(DataDefinition {
            catalog_ids: AtomicU64::new(catalog_ids),
            catalogs: RwLock::new(catalogs),
            system_catalog: Some(Box::new(system_catalog)),
        })
    }

    pub(crate) fn create_catalog(&self, catalog_name: &str) {
        let catalog_id = self.catalog_ids.fetch_add(1, Ordering::SeqCst);
        self.catalogs
            .write()
            .expect("to acquire write lock")
            .insert(catalog_name.to_owned(), Arc::new(Catalog::new(catalog_id)));
        if let Some(system_catalog) = self.system_catalog.as_ref() {
            system_catalog
                .write(
                    DEFINITION_SCHEMA,
                    CATALOG_NAMES_TABLE,
                    vec![(
                        Binary::pack(&[Datum::from_u64(catalog_id)]),
                        Binary::pack(&[Datum::from_str(catalog_name)]),
                    )],
                )
                .expect("to save catalog");
        }
    }

    #[allow(dead_code)]
    pub(crate) fn catalog_exists(&self, catalog_name: &str) -> CatalogId {
        self.catalogs
            .read()
            .expect("to acquire read lock")
            .get(catalog_name)
            .map(|catalog| catalog.id())
    }

    #[allow(dead_code)]
    pub(crate) fn drop_catalog(&self, catalog_name: &str, strategy: DropStrategy) -> Result<(), DropCatalogError> {
        if let Some(catalog) = self.catalog(catalog_name) {
            match strategy {
                DropStrategy::Restrict => {
                    if catalog.empty() {
                        if let Some(catalog) = self
                            .catalogs
                            .write()
                            .expect("to acquire write lock")
                            .remove(catalog_name)
                        {
                            if let Some(system_catalog) = self.system_catalog.as_ref() {
                                system_catalog
                                    .delete(
                                        DEFINITION_SCHEMA,
                                        CATALOG_NAMES_TABLE,
                                        vec![Binary::pack(&[Datum::from_u64(catalog.id())])],
                                    )
                                    .expect("to remove catalog");
                            }
                        }
                        Ok(())
                    } else {
                        Err(DropCatalogError::HasDependentObjects)
                    }
                }
                DropStrategy::Cascade => {
                    if let Some(catalog) = self
                        .catalogs
                        .write()
                        .expect("to acquire write lock")
                        .remove(catalog_name)
                    {
                        if let Some(system_catalog) = self.system_catalog.as_ref() {
                            system_catalog
                                .delete(
                                    DEFINITION_SCHEMA,
                                    CATALOG_NAMES_TABLE,
                                    vec![Binary::pack(&[Datum::from_u64(catalog.id())])],
                                )
                                .expect("to remove catalog");
                            let schema_record_ids = system_catalog
                                .read(DEFINITION_SCHEMA, SCHEMATA_TABLE)
                                .expect("to have SCHEMATA table")
                                .map(Result::unwrap)
                                .map(|(record_id, _columns)| {
                                    let catalog_id = record_id.unpack()[0].as_u64();
                                    (catalog_id, record_id)
                                })
                                .filter(|(catalog_id, _record_id)| *catalog_id == catalog.id())
                                .map(|(_catalog_id, record_id)| record_id)
                                .collect();
                            system_catalog
                                .delete(DEFINITION_SCHEMA, SCHEMATA_TABLE, schema_record_ids)
                                .expect("to remove schemas under catalog");
                            let table_record_ids = system_catalog
                                .read(DEFINITION_SCHEMA, TABLES_TABLE)
                                .expect("to have TABLES table")
                                .map(Result::unwrap)
                                .map(|(record_id, _columns)| {
                                    let catalog_id = record_id.unpack()[0].as_u64();
                                    (catalog_id, record_id)
                                })
                                .filter(|(catalog_id, _record_id)| *catalog_id == catalog.id())
                                .map(|(_catalog_id, record_id)| record_id)
                                .collect();
                            system_catalog
                                .delete(DEFINITION_SCHEMA, TABLES_TABLE, table_record_ids)
                                .expect("to remove tables under catalog");
                            let table_column_record_ids = system_catalog
                                .read(DEFINITION_SCHEMA, COLUMNS_TABLE)
                                .expect("to have COLUMNS table")
                                .map(Result::unwrap)
                                .map(|(record_id, _data)| {
                                    let record = record_id.unpack();
                                    let catalog = record[0].as_u64();
                                    (record_id, catalog)
                                })
                                .filter(|(_record_id, catalog_id)| *catalog_id == catalog.id())
                                .map(|(record_id, _catalog)| record_id)
                                .collect();
                            system_catalog
                                .delete(DEFINITION_SCHEMA, COLUMNS_TABLE, table_column_record_ids)
                                .expect("to have remove tables columns under catalog");
                        }
                    }
                    Ok(())
                }
            }
        } else {
            Err(DropCatalogError::DoesNotExist)
        }
    }

    pub(crate) fn create_schema(&self, catalog_name: &str, schema_name: &str) {
        let catalog = match self.catalog(catalog_name) {
            Some(catalog) => catalog,
            None => return,
        };
        let schema_id = catalog.create_schema(schema_name);
        if let Some(system_catalog) = self.system_catalog.as_ref() {
            system_catalog
                .write(
                    DEFINITION_SCHEMA,
                    SCHEMATA_TABLE,
                    vec![(
                        Binary::pack(&[Datum::from_u64(catalog.id()), Datum::from_u64(schema_id)]),
                        Binary::pack(&[Datum::from_str(catalog_name), Datum::from_str(schema_name)]),
                    )],
                )
                .expect("to save schema");
        }
    }

    pub(crate) fn schema_exists(&self, catalog_name: &str, schema_name: &str) -> SchemaId {
        log::debug!("checking schema existence {:?}.{:?}", catalog_name, schema_name);
        let catalog = match self.catalog(catalog_name) {
            Some(catalog) => catalog,
            None => return None,
        };
        let schema_id = match catalog.schema_exists(schema_name) {
            None => {
                if let Some(system_catalog) = self.system_catalog.as_ref() {
                    let schema_id = system_catalog
                        .read(DEFINITION_SCHEMA, SCHEMATA_TABLE)
                        .expect("to have SCHEMATA_TABLE table")
                        .map(Result::unwrap)
                        .map(|(record_id, columns)| {
                            let _catalog_id = record_id.unpack()[0].as_u64();
                            let id = record_id.unpack()[1].as_u64();
                            let columns = columns.unpack();
                            let catalog = columns[0].as_str().to_owned();
                            let schema = columns[1].as_str().to_owned();
                            (id, catalog, schema)
                        })
                        .filter(|(_id, catalog, schema)| catalog == catalog_name && schema == schema_name)
                        .map(|(id, _catalog, _schema)| id)
                        .next();
                    match schema_id {
                        Some(schema_id) => {
                            catalog.add_schema(schema_id, schema_name);
                            Some(schema_id)
                        }
                        None => None,
                    }
                } else {
                    None
                }
            }
            schema_id => schema_id,
        };
        let result = Some((catalog.id(), schema_id));
        log::debug!("{:?}", result);
        result
    }

    pub(crate) fn drop_schema(
        &self,
        catalog_name: &str,
        schema_name: &str,
        strategy: DropStrategy,
    ) -> Result<(), DropSchemaError> {
        let catalog = match self.catalog(catalog_name) {
            Some(catalog) => catalog,
            None => return Err(DropSchemaError::CatalogDoesNotExist),
        };
        let schema = match catalog.schema(schema_name) {
            Some(schema) => schema,
            None => return Err(DropSchemaError::DoesNotExist),
        };
        match strategy {
            DropStrategy::Restrict => {
                if schema.empty() {
                    let schema_id = catalog.remove_schema(schema_name);
                    match schema_id {
                        None => Err(DropSchemaError::DoesNotExist),
                        Some(schema_id) => {
                            if let Some(system_catalog) = self.system_catalog.as_ref() {
                                system_catalog
                                    .delete(
                                        DEFINITION_SCHEMA,
                                        SCHEMATA_TABLE,
                                        vec![Binary::pack(&[
                                            Datum::from_u64(catalog.id()),
                                            Datum::from_u64(schema_id),
                                        ])],
                                    )
                                    .expect("to remove schema");
                            }
                            Ok(())
                        }
                    }
                } else {
                    Err(DropSchemaError::HasDependentObjects)
                }
            }
            DropStrategy::Cascade => {
                let schema_id = catalog.remove_schema(schema_name);
                match schema_id {
                    None => Err(DropSchemaError::DoesNotExist),
                    Some(schema_id) => {
                        if let Some(system_catalog) = self.system_catalog.as_ref() {
                            system_catalog
                                .delete(
                                    DEFINITION_SCHEMA,
                                    SCHEMATA_TABLE,
                                    vec![Binary::pack(&[
                                        Datum::from_u64(catalog.id()),
                                        Datum::from_u64(schema_id),
                                    ])],
                                )
                                .expect("to remove schema");
                            let table_record_ids = system_catalog
                                .read(DEFINITION_SCHEMA, TABLES_TABLE)
                                .expect("to have TABLES table")
                                .map(Result::unwrap)
                                .map(|(record_id, _columns)| {
                                    let ids = record_id.unpack();
                                    let catalog_id = ids[0].as_u64();
                                    let schema_id = ids[1].as_u64();
                                    (catalog_id, schema_id, record_id)
                                })
                                .filter(|(catalog_id, schema, _record_id)| {
                                    *catalog_id == catalog.id() && *schema == schema_id
                                })
                                .map(|(_catalog_id, _schema, record_id)| record_id)
                                .collect();
                            system_catalog
                                .delete(DEFINITION_SCHEMA, TABLES_TABLE, table_record_ids)
                                .expect("to remove tables under catalog");
                            let table_column_record_ids = system_catalog
                                .read(DEFINITION_SCHEMA, COLUMNS_TABLE)
                                .expect("to have COLUMNS table")
                                .map(Result::unwrap)
                                .map(|(record_id, _data)| {
                                    let record = record_id.unpack();
                                    let catalog = record[0].as_u64();
                                    let schema = record[1].as_u64();
                                    (record_id, catalog, schema)
                                })
                                .filter(|(_record_id, catalog_id, schema)| {
                                    *catalog_id == catalog.id() && *schema == schema_id
                                })
                                .map(|(record_id, _catalog, _schema)| record_id)
                                .collect();
                            system_catalog
                                .delete(DEFINITION_SCHEMA, COLUMNS_TABLE, table_column_record_ids)
                                .expect("to have remove tables columns under catalog");
                        }
                        Ok(())
                    }
                }
            }
        }
    }

    pub(crate) fn schemas(&self, catalog_name: &str) -> Vec<String> {
        match self.catalog(catalog_name) {
            Some(catalog) => {
                if let Some(system_catalog) = self.system_catalog.as_ref() {
                    for (id, _catalog, schema) in system_catalog
                        .read(DEFINITION_SCHEMA, SCHEMATA_TABLE)
                        .expect("to have SCHEMATA_TABLE table")
                        .map(Result::unwrap)
                        .map(|(record_id, columns)| {
                            let id = record_id.unpack()[1].as_u64();
                            let columns = columns.unpack();
                            let catalog = columns[0].as_str().to_owned();
                            let schema = columns[1].as_str().to_owned();
                            (id, catalog, schema)
                        })
                        .filter(|(_id, catalog, _schema)| catalog == catalog_name)
                    {
                        catalog.add_schema(id, schema.as_str());
                    }
                }
                catalog.schemas()
            }
            None => vec![],
        }
    }

    pub(crate) fn table_exists(&self, catalog_name: &str, schema_name: &str, table_name: &str) -> TableId {
        let catalog = match self.catalog(catalog_name) {
            Some(catalog) => catalog,
            None => return None,
        };
        let schema = match catalog.schema(schema_name) {
            Some(schema) => schema,
            None => {
                if let Some(system_catalog) = self.system_catalog.as_ref() {
                    let schema_id = system_catalog
                        .read(DEFINITION_SCHEMA, SCHEMATA_TABLE)
                        .expect("to have SCHEMATA_TABLE table")
                        .map(Result::unwrap)
                        .map(|(record_id, columns)| {
                            let id = record_id.unpack()[0].as_u64();
                            let name = columns.unpack()[1].as_str().to_owned();
                            (id, name)
                        })
                        .filter(|(_id, name)| name == schema_name)
                        .map(|(id, _name)| id)
                        .next();
                    match schema_id {
                        Some(schema_id) => catalog.add_schema(schema_id, schema_name),
                        None => return Some((catalog.id(), None)),
                    }
                } else {
                    return Some((catalog.id(), None));
                }
            }
        };
        let table_id = match schema.table_exists(table_name) {
            None => {
                if let Some(system_catalog) = self.system_catalog.as_ref() {
                    let table_info = system_catalog
                        .read(DEFINITION_SCHEMA, TABLES_TABLE)
                        .expect("to have TABLES table")
                        .map(Result::unwrap)
                        .map(|(record_id, data)| {
                            let id = record_id.unpack()[0].as_u64();
                            let data = data.unpack();
                            let schema = data[1].as_str().to_owned();
                            let table = data[2].as_str().to_owned();
                            (id, schema, table)
                        })
                        .filter(|(_id, schema, table)| schema == schema_name && table == table_name)
                        .map(|(id, _schema, _table)| id)
                        .next();
                    match table_info {
                        Some(table_id) => {
                            let mut max_id = 0;
                            let table_columns = system_catalog
                                .read(DEFINITION_SCHEMA, COLUMNS_TABLE)
                                .expect("to have COLUMNS table")
                                .map(Result::unwrap)
                                .map(|(record_id, data)| {
                                    let id = record_id.unpack()[3].as_u64();
                                    let data = data.unpack();
                                    let schema = data[1].as_str().to_owned();
                                    let table = data[2].as_str().to_owned();
                                    let column = data[3].as_str().to_owned();
                                    let sql_type = data[4].as_sql_type();
                                    max_id = max_id.max(id);
                                    (id, schema, table, column, sql_type)
                                })
                                .filter(|(_id, schema, table, _column, _sql_type)| {
                                    schema == schema_name && table == table_name
                                })
                                .map(|(id, _schema, _table, column, sql_type)| {
                                    (id, ColumnDefinition::new(column.as_str(), sql_type))
                                })
                                .collect::<BTreeMap<_, _>>();
                            schema.add_table(table_id, table_name, table_columns, max_id);
                            Some(table_id)
                        }
                        None => None,
                    }
                } else {
                    None
                }
            }
            table_id => table_id,
        };
        let result = Some((catalog.id(), Some((schema.id(), table_id))));
        log::debug!("{:?}", result);
        result
    }

    pub(crate) fn create_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        column_definitions: &[ColumnDefinition],
    ) {
        let catalog = match self.catalog(catalog_name) {
            Some(catalog) => catalog,
            None => return,
        };
        let schema = match catalog.schema(schema_name) {
            Some(schema) => schema,
            None => return,
        };
        let created_table = schema.create_table(table_name, column_definitions);
        if let Some(system_catalog) = self.system_catalog.as_ref() {
            system_catalog
                .write(
                    DEFINITION_SCHEMA,
                    TABLES_TABLE,
                    vec![(
                        Binary::pack(&[
                            Datum::from_u64(catalog.id()),
                            Datum::from_u64(schema.id()),
                            Datum::from_u64(created_table.id()),
                        ]),
                        Binary::pack(&[
                            Datum::from_str(catalog_name),
                            Datum::from_str(schema_name),
                            Datum::from_str(table_name),
                        ]),
                    )],
                )
                .expect("to save table info");
            for (id, column) in created_table.columns() {
                system_catalog
                    .write(
                        DEFINITION_SCHEMA,
                        COLUMNS_TABLE,
                        vec![(
                            Binary::pack(&[
                                Datum::from_u64(catalog.id()),
                                Datum::from_u64(schema.id()),
                                Datum::from_u64(created_table.id()),
                                Datum::from_u64(id),
                            ]),
                            Binary::pack(&[
                                Datum::from_str(catalog_name),
                                Datum::from_str(schema_name),
                                Datum::from_str(table_name),
                                Datum::from_str(column.name().as_str()),
                                Datum::from_sql_type(column.sql_type()),
                                Datum::UInt64(id),
                            ]),
                        )],
                    )
                    .expect("to save column");
            }
        }
    }

    pub(crate) fn drop_table(&self, catalog_name: &str, schema_name: &str, table_name: &str) {
        let catalog = match self.catalog(catalog_name) {
            Some(catalog) => catalog,
            None => return,
        };
        let schema = match catalog.schema(schema_name) {
            Some(schema) => schema,
            None => return,
        };
        let table_id = schema.remove_table(table_name);
        if let Some(system_catalog) = self.system_catalog.as_ref() {
            if let Some(table_id) = table_id {
                system_catalog
                    .delete(
                        DEFINITION_SCHEMA,
                        TABLES_TABLE,
                        vec![Binary::pack(&[
                            Datum::from_u64(catalog.id()),
                            Datum::from_u64(schema.id()),
                            Datum::from_u64(table_id),
                        ])],
                    )
                    .expect("to remove table");
            }
        }
    }

    pub(crate) fn tables(&self, catalog_name: &str, schema_name: &str) -> Vec<String> {
        let catalog = match self.catalog(catalog_name) {
            Some(catalog) => catalog,
            None => return vec![],
        };
        let schema = match catalog.schema(schema_name) {
            Some(schema) => schema,
            None => return vec![],
        };
        if let Some(system_catalog) = self.system_catalog.as_ref() {
            for (table_id, _catalog, _schema, table) in system_catalog
                .read(DEFINITION_SCHEMA, TABLES_TABLE)
                .expect("to have SCHEMATA_TABLE table")
                .map(Result::unwrap)
                .map(|(record_id, columns)| {
                    let id = record_id.unpack()[1].as_u64();
                    let columns = columns.unpack();
                    let catalog = columns[0].as_str().to_owned();
                    let schema = columns[1].as_str().to_owned();
                    let table = columns[2].as_str().to_owned();
                    (id, catalog, schema, table)
                })
                .filter(|(_id, catalog, schema, _table)| catalog == catalog_name && schema == schema_name)
            {
                let mut max_id = 0;
                let table_columns = system_catalog
                    .read(DEFINITION_SCHEMA, COLUMNS_TABLE)
                    .expect("to have COLUMNS table")
                    .map(Result::unwrap)
                    .map(|(record_id, data)| {
                        let id = record_id.unpack()[3].as_u64();
                        let data = data.unpack();
                        let schema = data[1].as_str().to_owned();
                        let table = data[2].as_str().to_owned();
                        let column = data[3].as_str().to_owned();
                        let sql_type = data[4].as_sql_type();
                        max_id = max_id.max(id);
                        (id, schema, table, column, sql_type)
                    })
                    .filter(|(_id, schema, _table, _column, _sql_type)| schema == schema_name)
                    .map(|(id, _schema, _table, column, sql_type)| {
                        (id, ColumnDefinition::new(column.as_str(), sql_type))
                    })
                    .collect::<BTreeMap<_, _>>();
                schema.add_table(table_id, table.as_str(), table_columns, max_id);
            }
        }
        schema.tables()
    }

    pub(crate) fn table_columns(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Vec<ColumnDefinition> {
        match self.table_exists(catalog_name, schema_name, table_name) {
            Some((_, Some((_, Some(_))))) => {
                let catalog = match self.catalog(catalog_name) {
                    Some(catalog) => catalog,
                    None => return vec![],
                };
                let schema = match catalog.schema(schema_name) {
                    Some(schema) => schema,
                    None => return vec![],
                };
                let table = match schema.table(table_name) {
                    Some(table) => table,
                    None => return vec![],
                };
                table.columns().into_iter().map(|(_id, column)| column).collect()
            }
            _ => vec![],
        }
    }

    fn catalog(&self, catalog_name: &str) -> Option<Arc<Catalog>> {
        self.catalogs
            .read()
            .expect("to acquire read lock")
            .get(catalog_name)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_created_catalog_does_not_exist() {
        let data_definition = DataDefinition::in_memory();

        assert!(matches!(data_definition.catalog_exists("catalog_name"), None));
    }

    #[test]
    fn cant_drop_non_existent_catalog() {
        let data_definition = DataDefinition::in_memory();

        assert_eq!(
            data_definition.drop_catalog("catalog_name", DropStrategy::Restrict),
            Err(DropCatalogError::DoesNotExist)
        );
        assert_eq!(
            data_definition.drop_catalog("catalog_name", DropStrategy::Cascade),
            Err(DropCatalogError::DoesNotExist)
        );
    }

    #[test]
    fn create_catalog() {
        let data_definition = DataDefinition::in_memory();

        data_definition.create_catalog("catalog_name");

        assert!(matches!(data_definition.catalog_exists("catalog_name"), Some(_)));
    }

    #[test]
    fn drop_catalog() {
        let data_definition = DataDefinition::in_memory();

        data_definition.create_catalog("catalog_name");

        assert!(matches!(data_definition.catalog_exists("catalog_name"), Some(_)));

        assert_eq!(
            data_definition.drop_catalog("catalog_name", DropStrategy::Restrict),
            Ok(())
        );

        assert!(matches!(data_definition.catalog_exists("catalog_name"), None));
    }

    #[test]
    fn restrict_drop_strategy_cant_drop_non_empty_catalog() {
        let data_definition = DataDefinition::in_memory();

        data_definition.create_catalog("catalog_name");
        data_definition.create_schema("catalog_name", "schema_name");

        assert_eq!(
            data_definition.drop_catalog("catalog_name", DropStrategy::Restrict),
            Err(DropCatalogError::HasDependentObjects)
        );

        assert!(matches!(data_definition.catalog_exists("catalog_name"), Some(_)));
    }

    #[test]
    fn cascade_drop_of_non_empty_catalog() {
        let data_definition = DataDefinition::in_memory();

        data_definition.create_catalog("catalog_name");
        data_definition.create_schema("catalog_name", "schema_name");

        assert_eq!(
            data_definition.drop_catalog("catalog_name", DropStrategy::Cascade),
            Ok(())
        );

        assert!(matches!(data_definition.catalog_exists("catalog_name"), None));
        assert!(matches!(
            data_definition.schema_exists("catalog_name", "schema_name"),
            None
        ));
    }

    #[test]
    fn not_created_schema_does_not_exist() {
        let data_definition = DataDefinition::in_memory();

        data_definition.create_catalog("catalog_name");
        assert!(matches!(
            data_definition.schema_exists("catalog_name", "schema_name"),
            Some((_, None))
        ));
    }

    #[test]
    fn cant_drop_schema_from_nonexistent_catalog() {
        let data_definition = DataDefinition::in_memory();

        assert_eq!(
            data_definition.drop_schema("catalog_name", "schema_name", DropStrategy::Restrict),
            Err(DropSchemaError::CatalogDoesNotExist)
        );
        assert_eq!(
            data_definition.drop_schema("catalog_name", "schema_name", DropStrategy::Cascade),
            Err(DropSchemaError::CatalogDoesNotExist)
        );
    }

    #[test]
    fn cant_drop_non_existent_schema() {
        let data_definition = DataDefinition::in_memory();

        data_definition.create_catalog("catalog_name");
        assert_eq!(
            data_definition.drop_schema("catalog_name", "schema_name", DropStrategy::Restrict),
            Err(DropSchemaError::DoesNotExist)
        );
        assert_eq!(
            data_definition.drop_schema("catalog_name", "schema_name", DropStrategy::Cascade),
            Err(DropSchemaError::DoesNotExist)
        );
    }

    #[test]
    fn create_schema() {
        let data_definition = DataDefinition::in_memory();

        data_definition.create_catalog("catalog_name");
        data_definition.create_schema("catalog_name", "schema_name");

        assert!(data_definition.schema_exists("catalog_name", "schema_name").is_some());
    }

    #[test]
    fn drop_schema() {
        let data_definition = DataDefinition::in_memory();

        data_definition.create_catalog("catalog_name");
        data_definition.create_schema("catalog_name", "schema_name");

        assert!(data_definition.schema_exists("catalog_name", "schema_name").is_some());

        assert_eq!(
            data_definition.drop_schema("catalog_name", "schema_name", DropStrategy::Restrict),
            Ok(())
        );

        assert!(matches!(
            data_definition.schema_exists("catalog_name", "schema_name"),
            Some((_, None))
        ));
    }

    #[test]
    fn restrict_drop_strategy_cant_drop_non_empty_schema() {
        let data_definition = DataDefinition::in_memory();

        data_definition.create_catalog("catalog_name");
        data_definition.create_schema("catalog_name", "schema_name");
        data_definition.create_table("catalog_name", "schema_name", "table_name", &[]);

        assert_eq!(
            data_definition.drop_schema("catalog_name", "schema_name", DropStrategy::Restrict),
            Err(DropSchemaError::HasDependentObjects)
        );

        assert!(matches!(
            data_definition.schema_exists("catalog_name", "schema_name"),
            Some((_, Some(_)))
        ));
        assert!(matches!(
            data_definition.table_exists("catalog_name", "schema_name", "table_name"),
            Some((_, Some((_, Some(_)))))
        ))
    }

    #[test]
    fn cascade_drop_of_non_empty_schema() {
        let data_definition = DataDefinition::in_memory();

        data_definition.create_catalog("catalog_name");
        data_definition.create_schema("catalog_name", "schema_name");
        data_definition.create_table("catalog_name", "schema_name", "table_name", &[]);

        assert_eq!(
            data_definition.drop_schema("catalog_name", "schema_name", DropStrategy::Cascade),
            Ok(())
        );

        assert!(matches!(
            data_definition.schema_exists("catalog_name", "schema_name"),
            Some((_, None))
        ));
        assert!(matches!(
            data_definition.table_exists("catalog_name", "schema_name", "table_name"),
            Some((_, None))
        ));
    }

    #[test]
    fn not_created_table_does_not_exist() {
        let data_definition = DataDefinition::in_memory();

        data_definition.create_catalog("catalog_name");
        data_definition.create_schema("catalog_name", "schema_name");
        assert!(matches!(
            data_definition.table_exists("catalog_name", "schema_name", "table_name"),
            Some((_, Some((_, None))))
        ));
    }

    #[test]
    fn create_table() {
        let data_definition = DataDefinition::in_memory();

        data_definition.create_catalog("catalog_name");
        data_definition.create_schema("catalog_name", "schema_name");
        data_definition.create_table("catalog_name", "schema_name", "table_name", &[]);

        assert!(matches!(
            data_definition.table_exists("catalog_name", "schema_name", "table_name"),
            Some((_, Some((_, Some(_)))))
        ));
    }

    #[test]
    fn drop_table() {
        let data_definition = DataDefinition::in_memory();

        data_definition.create_catalog("catalog_name");
        data_definition.create_schema("catalog_name", "schema_name");
        data_definition.create_table("catalog_name", "schema_name", "table_name", &[]);

        assert!(matches!(
            data_definition.table_exists("catalog_name", "schema_name", "table_name"),
            Some((_, Some((_, Some(_)))))
        ));

        data_definition.drop_table("catalog_name", "schema_name", "table_name");

        assert!(matches!(
            data_definition.table_exists("catalog_name", "schema_name", "table_name"),
            Some((_, Some((_, None))))
        ));
    }

    #[cfg(test)]
    mod persistent {
        use super::*;

        #[rstest::fixture]
        fn storage_path() -> (DataDefinition, PathBuf) {
            let root_path = tempfile::tempdir().expect("to create temporary folder");
            let path = root_path.into_path();
            let data_definition = DataDefinition::persistent(&path).expect("create persistent data definition");
            (data_definition, path)
        }

        #[rstest::rstest]
        fn storage_preserve_created_catalog_after_restart(storage_path: (DataDefinition, PathBuf)) {
            let (data_definition, path) = storage_path;
            data_definition.create_catalog("catalog_name");
            drop(data_definition);

            let data_definition = DataDefinition::persistent(&path).expect("create persistent data definition");
            assert!(matches!(data_definition.catalog_exists("catalog_name"), Some(_)));
        }

        #[rstest::rstest]
        fn dropped_catalog_is_not_preserved_after_restart(storage_path: (DataDefinition, PathBuf)) {
            let (data_definition, path) = storage_path;
            data_definition.create_catalog("catalog_name");
            assert!(matches!(data_definition.catalog_exists("catalog_name"), Some(_)));
            data_definition
                .drop_catalog("catalog_name", DropStrategy::Restrict)
                .expect("to catalog dropped");
            assert!(matches!(data_definition.catalog_exists("catalog_name"), None));
            drop(data_definition);

            let data_definition = DataDefinition::persistent(&path).expect("create persistent data definition");
            assert!(matches!(data_definition.catalog_exists("catalog_name"), None));
        }

        #[rstest::rstest]
        fn storage_preserve_created_schema_after_restart(storage_path: (DataDefinition, PathBuf)) {
            let (data_definition, path) = storage_path;
            data_definition.create_catalog("catalog_name");
            data_definition.create_schema("catalog_name", "schema_name");
            drop(data_definition);

            let data_definition = DataDefinition::persistent(&path).expect("create persistent data definition");
            assert!(matches!(
                data_definition.schema_exists("catalog_name", "schema_name"),
                Some(_)
            ));
        }

        #[rstest::rstest]
        fn storage_preserve_created_multiple_schemas_in_different_catalogs_after_restart(
            storage_path: (DataDefinition, PathBuf),
        ) {
            let (data_definition, path) = storage_path;
            data_definition.create_catalog("catalog_name_1");
            data_definition.create_schema("catalog_name_1", "schema_name_1");
            data_definition.create_schema("catalog_name_1", "schema_name_2");
            data_definition.create_catalog("catalog_name_2");
            data_definition.create_schema("catalog_name_2", "schema_name_3");
            data_definition.create_schema("catalog_name_2", "schema_name_4");
            assert!(matches!(
                data_definition.schema_exists("catalog_name_1", "schema_name_1"),
                Some((_, Some(_)))
            ));
            assert!(matches!(
                data_definition.schema_exists("catalog_name_1", "schema_name_2"),
                Some((_, Some(_)))
            ));
            assert!(matches!(
                data_definition.schema_exists("catalog_name_2", "schema_name_3"),
                Some((_, Some(_)))
            ));
            assert!(matches!(
                data_definition.schema_exists("catalog_name_2", "schema_name_4"),
                Some((_, Some(_)))
            ));
            drop(data_definition);

            let data_definition = DataDefinition::persistent(&path).expect("create persistent data definition");
            assert!(matches!(
                data_definition.schema_exists("catalog_name_1", "schema_name_1"),
                Some((_, Some(_)))
            ));
            assert!(matches!(
                data_definition.schema_exists("catalog_name_1", "schema_name_2"),
                Some((_, Some(_)))
            ));
            assert!(matches!(
                data_definition.schema_exists("catalog_name_2", "schema_name_3"),
                Some((_, Some(_)))
            ));
            assert!(matches!(
                data_definition.schema_exists("catalog_name_2", "schema_name_4"),
                Some((_, Some(_)))
            ));
        }

        #[rstest::rstest]
        fn dropped_schema_is_not_preserved_after_restart(storage_path: (DataDefinition, PathBuf)) {
            let (data_definition, path) = storage_path;
            data_definition.create_catalog("catalog_name");
            data_definition.create_schema("catalog_name", "schema_name");
            assert!(matches!(
                data_definition.schema_exists("catalog_name", "schema_name"),
                Some((_, Some(_)))
            ));
            assert_eq!(
                data_definition.drop_schema("catalog_name", "schema_name", DropStrategy::Restrict),
                Ok(())
            );
            assert!(matches!(
                data_definition.schema_exists("catalog_name", "schema_name"),
                Some((_, None))
            ));
            drop(data_definition);

            let data_definition = DataDefinition::persistent(&path).expect("create persistent data definition");
            assert!(matches!(
                data_definition.schema_exists("catalog_name", "schema_name"),
                Some((_, None))
            ));
        }

        #[rstest::rstest]
        fn storage_preserve_created_table_after_restart(storage_path: (DataDefinition, PathBuf)) {
            let (data_definition, path) = storage_path;
            data_definition.create_catalog("catalog_name");
            data_definition.create_schema("catalog_name", "schema_name");
            data_definition.create_table(
                "catalog_name",
                "schema_name",
                "table_name",
                &[ColumnDefinition::new("col_1", SqlType::Integer(0))],
            );
            drop(data_definition);

            let data_definition = DataDefinition::persistent(&path).expect("create persistent data definition");
            assert!(data_definition
                .table_exists("catalog_name", "schema_name", "table_name")
                .expect("to have catalog")
                .1
                .expect("to have schema")
                .1
                .is_some());
        }

        #[rstest::rstest]
        fn storage_preserve_created_table_with_the_same_name_in_different_schemas_after_restart(
            storage_path: (DataDefinition, PathBuf),
        ) {
            let (data_definition, path) = storage_path;
            data_definition.create_catalog("catalog_name");
            data_definition.create_schema("catalog_name", "schema_name_1");
            data_definition.create_table(
                "catalog_name",
                "schema_name_1",
                "table_name",
                &[ColumnDefinition::new("col_1", SqlType::Integer(0))],
            );
            data_definition.create_schema("catalog_name", "schema_name_2");
            data_definition.create_table(
                "catalog_name",
                "schema_name_2",
                "table_name",
                &[ColumnDefinition::new("col_1", SqlType::SmallInt(0))],
            );
            assert_eq!(
                data_definition.table_columns("catalog_name", "schema_name_1", "table_name"),
                vec![ColumnDefinition::new("col_1", SqlType::Integer(0))]
            );
            assert_eq!(
                data_definition.table_columns("catalog_name", "schema_name_2", "table_name"),
                vec![ColumnDefinition::new("col_1", SqlType::SmallInt(0))]
            );
            drop(data_definition);

            let data_definition = DataDefinition::persistent(&path).expect("create persistent data definition");

            assert_eq!(
                data_definition.table_columns("catalog_name", "schema_name_1", "table_name"),
                vec![ColumnDefinition::new("col_1", SqlType::Integer(0))]
            );
            assert_eq!(
                data_definition.table_columns("catalog_name", "schema_name_2", "table_name"),
                vec![ColumnDefinition::new("col_1", SqlType::SmallInt(0))]
            );
        }

        #[rstest::rstest]
        fn storage_preserve_created_multiple_tables_in_different_schemas_and_catalogs_after_restart(
            storage_path: (DataDefinition, PathBuf),
        ) {
            let (data_definition, path) = storage_path;
            data_definition.create_catalog("catalog_name_1");
            data_definition.create_schema("catalog_name_1", "schema_name_1");
            data_definition.create_schema("catalog_name_1", "schema_name_2");
            data_definition.create_table(
                "catalog_name_1",
                "schema_name_1",
                "table_name_1",
                &[ColumnDefinition::new("col_1", SqlType::Integer(0))],
            );
            data_definition.create_table(
                "catalog_name_1",
                "schema_name_1",
                "table_name_2",
                &[ColumnDefinition::new("col_1", SqlType::Integer(0))],
            );
            data_definition.create_table(
                "catalog_name_1",
                "schema_name_2",
                "table_name_3",
                &[ColumnDefinition::new("col_1", SqlType::Integer(0))],
            );
            data_definition.create_table(
                "catalog_name_1",
                "schema_name_2",
                "table_name_4",
                &[ColumnDefinition::new("col_1", SqlType::Integer(0))],
            );
            data_definition.create_catalog("catalog_name_2");
            data_definition.create_schema("catalog_name_2", "schema_name_3");
            data_definition.create_schema("catalog_name_2", "schema_name_4");
            data_definition.create_table(
                "catalog_name_2",
                "schema_name_3",
                "table_name_5",
                &[ColumnDefinition::new("col_1", SqlType::Integer(0))],
            );
            data_definition.create_table(
                "catalog_name_2",
                "schema_name_3",
                "table_name_6",
                &[ColumnDefinition::new("col_1", SqlType::Integer(0))],
            );
            data_definition.create_table(
                "catalog_name_2",
                "schema_name_4",
                "table_name_7",
                &[ColumnDefinition::new("col_1", SqlType::Integer(0))],
            );
            data_definition.create_table(
                "catalog_name_2",
                "schema_name_4",
                "table_name_8",
                &[ColumnDefinition::new("col_1", SqlType::Integer(0))],
            );
            drop(data_definition);

            let data_definition = DataDefinition::persistent(&path).expect("create persistent data definition");
            assert!(matches!(
                data_definition.table_exists("catalog_name_1", "schema_name_1", "table_name_1"),
                Some((_, Some((_, Some(_)))))
            ));
            assert!(matches!(
                data_definition.table_exists("catalog_name_1", "schema_name_1", "table_name_2"),
                Some((_, Some((_, Some(_)))))
            ));
            assert!(matches!(
                data_definition.table_exists("catalog_name_1", "schema_name_2", "table_name_3"),
                Some((_, Some((_, Some(_)))))
            ));
            assert!(matches!(
                data_definition.table_exists("catalog_name_1", "schema_name_2", "table_name_4"),
                Some((_, Some((_, Some(_)))))
            ));
            assert!(matches!(
                data_definition.table_exists("catalog_name_2", "schema_name_3", "table_name_5"),
                Some((_, Some((_, Some(_)))))
            ));
            assert!(matches!(
                data_definition.table_exists("catalog_name_2", "schema_name_3", "table_name_6"),
                Some((_, Some((_, Some(_)))))
            ));
            assert!(matches!(
                data_definition.table_exists("catalog_name_2", "schema_name_4", "table_name_7"),
                Some((_, Some((_, Some(_)))))
            ));
            assert!(matches!(
                data_definition.table_exists("catalog_name_2", "schema_name_4", "table_name_8"),
                Some((_, Some((_, Some(_)))))
            ));
        }

        #[rstest::rstest]
        fn dropped_table_is_not_preserved_after_restart(storage_path: (DataDefinition, PathBuf)) {
            let (data_definition, path) = storage_path;
            data_definition.create_catalog("catalog_name");
            data_definition.create_schema("catalog_name", "schema_name");
            data_definition.create_table(
                "catalog_name",
                "schema_name",
                "table_name",
                &[ColumnDefinition::new("col_1", SqlType::Integer(0))],
            );
            assert!(matches!(
                data_definition.table_exists("catalog_name", "schema_name", "table_name"),
                Some((_, Some((_, Some(_)))))
            ));
            data_definition.drop_table("catalog_name", "schema_name", "table_name");
            assert!(matches!(
                data_definition.table_exists("catalog_name", "schema_name", "table_name"),
                Some((_, Some((_, None))))
            ));
            drop(data_definition);

            let data_definition = DataDefinition::persistent(&path).expect("create persistent data definition");
            assert!(matches!(
                data_definition.table_exists("catalog_name", "schema_name", "table_name"),
                Some((_, Some((_, None))))
            ));
        }

        #[rstest::rstest]
        fn table_columns_data_preserved_after_restart(storage_path: (DataDefinition, PathBuf)) {
            let (data_definition, path) = storage_path;
            data_definition.create_catalog("catalog_name");
            data_definition.create_schema("catalog_name", "schema_name");
            data_definition.create_table(
                "catalog_name",
                "schema_name",
                "table_name",
                &[
                    ColumnDefinition::new("col_1", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_2", SqlType::Integer(0)),
                    ColumnDefinition::new("col_3", SqlType::BigInt(0)),
                ],
            );
            assert_eq!(
                data_definition.table_columns("catalog_name", "schema_name", "table_name"),
                vec![
                    ColumnDefinition::new("col_1", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_2", SqlType::Integer(0)),
                    ColumnDefinition::new("col_3", SqlType::BigInt(0))
                ]
            );
            drop(data_definition);

            let data_definition = DataDefinition::persistent(&path).expect("create persistent data definition");
            assert_eq!(
                data_definition.table_columns("catalog_name", "schema_name", "table_name"),
                vec![
                    ColumnDefinition::new("col_1", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_2", SqlType::Integer(0)),
                    ColumnDefinition::new("col_3", SqlType::BigInt(0))
                ]
            );
        }

        #[rstest::rstest]
        fn table_columns_data_preserved_for_multiple_tables_schemas_and_catalogs_after_restart(
            storage_path: (DataDefinition, PathBuf),
        ) {
            let (data_definition, path) = storage_path;
            data_definition.create_catalog("catalog_name_1");
            data_definition.create_schema("catalog_name_1", "schema_name_1");
            data_definition.create_table(
                "catalog_name_1",
                "schema_name_1",
                "table_name_1",
                &[
                    ColumnDefinition::new("col_1", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_2", SqlType::Integer(0)),
                    ColumnDefinition::new("col_3", SqlType::BigInt(0)),
                ],
            );
            data_definition.create_table(
                "catalog_name_1",
                "schema_name_1",
                "table_name_2",
                &[
                    ColumnDefinition::new("col_4", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_5", SqlType::Integer(0)),
                    ColumnDefinition::new("col_6", SqlType::BigInt(0)),
                ],
            );
            data_definition.create_schema("catalog_name_1", "schema_name_2");
            data_definition.create_table(
                "catalog_name_1",
                "schema_name_2",
                "table_name_3",
                &[
                    ColumnDefinition::new("col_1", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_2", SqlType::Integer(0)),
                    ColumnDefinition::new("col_3", SqlType::BigInt(0)),
                ],
            );
            data_definition.create_table(
                "catalog_name_1",
                "schema_name_2",
                "table_name_4",
                &[
                    ColumnDefinition::new("col_4", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_5", SqlType::Integer(0)),
                    ColumnDefinition::new("col_6", SqlType::BigInt(0)),
                ],
            );

            data_definition.create_catalog("catalog_name_2");
            data_definition.create_schema("catalog_name_2", "schema_name_3");
            data_definition.create_table(
                "catalog_name_2",
                "schema_name_3",
                "table_name_5",
                &[
                    ColumnDefinition::new("col_1", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_2", SqlType::Integer(0)),
                    ColumnDefinition::new("col_3", SqlType::BigInt(0)),
                ],
            );
            data_definition.create_table(
                "catalog_name_2",
                "schema_name_3",
                "table_name_6",
                &[
                    ColumnDefinition::new("col_4", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_5", SqlType::Integer(0)),
                    ColumnDefinition::new("col_6", SqlType::BigInt(0)),
                ],
            );
            data_definition.create_schema("catalog_name_2", "schema_name_4");
            data_definition.create_table(
                "catalog_name_2",
                "schema_name_4",
                "table_name_7",
                &[
                    ColumnDefinition::new("col_1", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_2", SqlType::Integer(0)),
                    ColumnDefinition::new("col_3", SqlType::BigInt(0)),
                ],
            );
            data_definition.create_table(
                "catalog_name_2",
                "schema_name_4",
                "table_name_8",
                &[
                    ColumnDefinition::new("col_4", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_5", SqlType::Integer(0)),
                    ColumnDefinition::new("col_6", SqlType::BigInt(0)),
                ],
            );

            assert_eq!(
                data_definition.table_columns("catalog_name_1", "schema_name_1", "table_name_1"),
                vec![
                    ColumnDefinition::new("col_1", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_2", SqlType::Integer(0)),
                    ColumnDefinition::new("col_3", SqlType::BigInt(0))
                ]
            );
            assert_eq!(
                data_definition.table_columns("catalog_name_1", "schema_name_1", "table_name_2"),
                vec![
                    ColumnDefinition::new("col_4", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_5", SqlType::Integer(0)),
                    ColumnDefinition::new("col_6", SqlType::BigInt(0))
                ]
            );

            assert_eq!(
                data_definition.table_columns("catalog_name_1", "schema_name_2", "table_name_3"),
                vec![
                    ColumnDefinition::new("col_1", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_2", SqlType::Integer(0)),
                    ColumnDefinition::new("col_3", SqlType::BigInt(0))
                ]
            );
            assert_eq!(
                data_definition.table_columns("catalog_name_1", "schema_name_2", "table_name_4"),
                vec![
                    ColumnDefinition::new("col_4", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_5", SqlType::Integer(0)),
                    ColumnDefinition::new("col_6", SqlType::BigInt(0))
                ]
            );

            assert_eq!(
                data_definition.table_columns("catalog_name_2", "schema_name_3", "table_name_5"),
                vec![
                    ColumnDefinition::new("col_1", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_2", SqlType::Integer(0)),
                    ColumnDefinition::new("col_3", SqlType::BigInt(0))
                ]
            );
            assert_eq!(
                data_definition.table_columns("catalog_name_2", "schema_name_3", "table_name_6"),
                vec![
                    ColumnDefinition::new("col_4", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_5", SqlType::Integer(0)),
                    ColumnDefinition::new("col_6", SqlType::BigInt(0))
                ]
            );

            assert_eq!(
                data_definition.table_columns("catalog_name_2", "schema_name_4", "table_name_7"),
                vec![
                    ColumnDefinition::new("col_1", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_2", SqlType::Integer(0)),
                    ColumnDefinition::new("col_3", SqlType::BigInt(0))
                ]
            );
            assert_eq!(
                data_definition.table_columns("catalog_name_2", "schema_name_4", "table_name_8"),
                vec![
                    ColumnDefinition::new("col_4", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_5", SqlType::Integer(0)),
                    ColumnDefinition::new("col_6", SqlType::BigInt(0))
                ]
            );

            drop(data_definition);

            let data_definition = DataDefinition::persistent(&path).expect("create persistent data definition");

            assert_eq!(
                data_definition.table_columns("catalog_name_1", "schema_name_1", "table_name_1"),
                vec![
                    ColumnDefinition::new("col_1", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_2", SqlType::Integer(0)),
                    ColumnDefinition::new("col_3", SqlType::BigInt(0))
                ]
            );
            assert_eq!(
                data_definition.table_columns("catalog_name_1", "schema_name_1", "table_name_2"),
                vec![
                    ColumnDefinition::new("col_4", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_5", SqlType::Integer(0)),
                    ColumnDefinition::new("col_6", SqlType::BigInt(0))
                ]
            );

            assert_eq!(
                data_definition.table_columns("catalog_name_1", "schema_name_2", "table_name_3"),
                vec![
                    ColumnDefinition::new("col_1", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_2", SqlType::Integer(0)),
                    ColumnDefinition::new("col_3", SqlType::BigInt(0))
                ]
            );
            assert_eq!(
                data_definition.table_columns("catalog_name_1", "schema_name_2", "table_name_4"),
                vec![
                    ColumnDefinition::new("col_4", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_5", SqlType::Integer(0)),
                    ColumnDefinition::new("col_6", SqlType::BigInt(0))
                ]
            );

            assert_eq!(
                data_definition.table_columns("catalog_name_2", "schema_name_3", "table_name_5"),
                vec![
                    ColumnDefinition::new("col_1", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_2", SqlType::Integer(0)),
                    ColumnDefinition::new("col_3", SqlType::BigInt(0))
                ]
            );
            assert_eq!(
                data_definition.table_columns("catalog_name_2", "schema_name_3", "table_name_6"),
                vec![
                    ColumnDefinition::new("col_4", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_5", SqlType::Integer(0)),
                    ColumnDefinition::new("col_6", SqlType::BigInt(0))
                ]
            );

            assert_eq!(
                data_definition.table_columns("catalog_name_2", "schema_name_4", "table_name_7"),
                vec![
                    ColumnDefinition::new("col_1", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_2", SqlType::Integer(0)),
                    ColumnDefinition::new("col_3", SqlType::BigInt(0))
                ]
            );
            assert_eq!(
                data_definition.table_columns("catalog_name_2", "schema_name_4", "table_name_8"),
                vec![
                    ColumnDefinition::new("col_4", SqlType::SmallInt(0)),
                    ColumnDefinition::new("col_5", SqlType::Integer(0)),
                    ColumnDefinition::new("col_6", SqlType::BigInt(0))
                ]
            );
        }

        #[rstest::rstest]
        fn tables_schemas_are_not_preserved_after_cascade_catalog_drop(storage_path: (DataDefinition, PathBuf)) {
            let (data_definition, path) = storage_path;
            data_definition.create_catalog("catalog_name");
            data_definition.create_schema("catalog_name", "schema_name");
            data_definition.create_table(
                "catalog_name",
                "schema_name",
                "table_name",
                &[ColumnDefinition::new("col_1", SqlType::SmallInt(0))],
            );

            assert_eq!(
                data_definition.drop_catalog("catalog_name", DropStrategy::Cascade),
                Ok(())
            );
            assert!(matches!(data_definition.catalog_exists("catalog_name"), None));
            assert!(matches!(
                data_definition.schema_exists("catalog_name", "schema_name"),
                None
            ));
            assert!(matches!(
                data_definition.table_exists("catalog_name", "schema_name", "table_name"),
                None
            ));
            assert_eq!(
                data_definition.table_columns("catalog_name", "schema_name", "table_name"),
                vec![]
            );

            drop(data_definition);

            let data_definition = DataDefinition::persistent(&path).expect("create persistent data definition");

            assert!(matches!(data_definition.catalog_exists("catalog_name"), None));
            assert!(matches!(
                data_definition.schema_exists("catalog_name", "schema_name"),
                None
            ));
            assert!(matches!(
                data_definition.table_exists("catalog_name", "schema_name", "table_name"),
                None
            ));
            assert_eq!(
                data_definition.table_columns("catalog_name", "schema_name", "table_name"),
                vec![]
            );

            data_definition.create_catalog("catalog_name");
            assert!(matches!(
                data_definition.schema_exists("catalog_name", "schema_name"),
                Some((_, None))
            ));
            data_definition.create_schema("catalog_name", "schema_name");
            assert!(matches!(
                data_definition.table_exists("catalog_name", "schema_name", "table_name"),
                Some((_, Some((_, None))))
            ));
        }

        #[rstest::rstest]
        fn tables_are_not_preserved_after_cascade_schema_drop(storage_path: (DataDefinition, PathBuf)) {
            let (data_definition, path) = storage_path;
            data_definition.create_catalog("catalog_name");
            data_definition.create_schema("catalog_name", "schema_name");
            data_definition.create_table(
                "catalog_name",
                "schema_name",
                "table_name",
                &[ColumnDefinition::new("col_1", SqlType::SmallInt(0))],
            );

            assert_eq!(
                data_definition.drop_schema("catalog_name", "schema_name", DropStrategy::Cascade),
                Ok(())
            );
            assert!(matches!(
                data_definition.schema_exists("catalog_name", "schema_name"),
                Some((_, None))
            ));
            assert!(matches!(
                data_definition.table_exists("catalog_name", "schema_name", "table_name"),
                Some((_, None))
            ));
            assert_eq!(
                data_definition.table_columns("catalog_name", "schema_name", "table_name"),
                vec![]
            );

            drop(data_definition);

            let data_definition = DataDefinition::persistent(&path).expect("create persistent data definition");

            assert!(matches!(
                data_definition.schema_exists("catalog_name", "schema_name"),
                Some((_, None))
            ));
            assert!(matches!(
                data_definition.table_exists("catalog_name", "schema_name", "table_name"),
                Some((_, None))
            ));
            assert_eq!(
                data_definition.table_columns("catalog_name", "schema_name", "table_name"),
                vec![]
            );

            // data_definition.create_schema("catalog_name", "schema_name");
            // assert!(matches!(
            //     data_definition.table_exists("catalog_name", "schema_name", "table_name"),
            //     Some((_, Some((_, None))))
            // ));
        }
    }
}
