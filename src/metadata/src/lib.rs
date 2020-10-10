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

use binary::{Binary, StorageError};
use chashmap::CHashMap;
use meta_def::ColumnDefinition;
use repr::Datum;
use sql_model::{sql_types::SqlType, DropSchemaError, DropStrategy, Id, DEFAULT_CATALOG};
use std::{
    collections::{BTreeMap, HashMap},
    io,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
};
use storage::{Database, FullSchemaId, FullTableId, InMemoryDatabase, InitStatus, PersistentDatabase};

pub trait MetadataView {
    fn schema_exists<S: AsRef<str>>(&self, schema_name: &S) -> FullSchemaId;

    fn table_exists<S: AsRef<str>, T: AsRef<str>>(&self, schema_name: &S, table_name: &T) -> FullTableId;

    fn table_columns<I: AsRef<(Id, Id)>>(&self, table_id: &I) -> Result<Vec<ColumnDefinition>, ()>;

    fn column_ids<I: AsRef<(Id, Id)>, N: AsRef<str> + PartialEq<N>>(
        &self,
        table_id: &I,
        names: &[N],
    ) -> Result<(Vec<Id>, Vec<String>), ()>;

    fn column_defs<I: AsRef<(Id, Id)>>(&self, table_id: &I, ids: &[Id]) -> Vec<ColumnDefinition>;
}

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

type InnerCatalogId = Option<Id>;
type InnerFullSchemaId = Option<(Id, Option<Id>)>;
type InnerFullTableId = Option<(Id, Option<(Id, Option<Id>)>)>;
type Name = String;

struct Catalog {
    id: Id,
    schemas: CHashMap<Name, Arc<Schema>>,
    schema_id_generator: AtomicU64,
}

impl Catalog {
    fn new(id: Id) -> Catalog {
        Catalog {
            id,
            schemas: CHashMap::default(),
            schema_id_generator: AtomicU64::default(),
        }
    }

    fn id(&self) -> Id {
        self.id
    }

    fn create_schema(&self, schema_name: &str) -> Id {
        let schema_id = self.schema_id_generator.fetch_add(1, Ordering::SeqCst);
        self.schemas
            .insert(schema_name.to_owned(), Arc::new(Schema::new(schema_id)));
        schema_id
    }

    fn add_schema(&self, schema_id: Id, schema_name: &str) -> Arc<Schema> {
        let schema = Arc::new(Schema::new(schema_id));
        self.schemas.insert(schema_name.to_owned(), schema.clone());
        schema
    }

    fn remove_schema(&self, schema_name: &str) -> Option<Id> {
        self.schemas.remove(schema_name).map(|schema| schema.id())
    }

    fn schema(&self, schema_name: &str) -> Option<Arc<Schema>> {
        self.schemas.get(schema_name).map(|schema| (*schema).clone())
    }

    fn schemas(&self) -> Vec<(Id, String)> {
        self.schemas
            .clone()
            .into_iter()
            .map(|(name, schema)| (schema.id(), name))
            .collect()
    }

    fn empty(&self) -> bool {
        self.schemas.is_empty()
    }
}

struct Schema {
    id: Id,
    tables: CHashMap<Name, Arc<Table>>,
    table_id_generator: AtomicU64,
}

impl Schema {
    fn new(id: Id) -> Schema {
        Schema {
            id,
            tables: CHashMap::default(),
            table_id_generator: AtomicU64::default(),
        }
    }

    fn id(&self) -> Id {
        self.id
    }

    fn create_table(&self, table_name: &str, column_definitions: &[ColumnDefinition]) -> Arc<Table> {
        let table_id = self.table_id_generator.fetch_add(1, Ordering::SeqCst);
        let table = Arc::new(Table::new(table_id, column_definitions));
        self.tables.insert(table_name.to_owned(), table.clone());
        table
    }

    fn add_table(
        &self,
        table_id: Id,
        table_name: &str,
        column_definitions: BTreeMap<Id, ColumnDefinition>,
        max_id: Id,
    ) {
        self.tables.insert(
            table_name.to_owned(),
            Arc::new(Table::restore(table_id, column_definitions, max_id)),
        );
    }

    fn remove_table(&self, table_name: &str) -> Option<Id> {
        self.tables.remove(table_name).map(|table| table.id())
    }

    fn tables(&self) -> Vec<(Id, String)> {
        self.tables
            .clone()
            .into_iter()
            .map(|(name, table)| (table.id(), name))
            .collect()
    }

    fn empty(&self) -> bool {
        self.tables.is_empty()
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

unsafe impl Send for DataDefinition {}

unsafe impl Sync for DataDefinition {}

pub struct DataDefinition {
    catalog_ids: AtomicU64,
    catalogs: CHashMap<Name, Arc<Catalog>>,
    system_catalog: Box<dyn Database>,
}

impl DataDefinition {
    pub fn in_memory() -> DataDefinition {
        let system_catalog = InMemoryDatabase::default();
        system_catalog
            .create_schema(DEFINITION_SCHEMA)
            .expect("no io error")
            .expect("no platform error")
            .expect("table CATALOG_NAMES is created");
        system_catalog
            .create_object(DEFINITION_SCHEMA, CATALOG_NAMES_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("table CATALOG_NAMES is created");
        system_catalog
            .create_object(DEFINITION_SCHEMA, SCHEMATA_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("table SCHEMATA is created");
        system_catalog
            .create_object(DEFINITION_SCHEMA, TABLES_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("table TABLES is created");
        system_catalog
            .create_object(DEFINITION_SCHEMA, COLUMNS_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("table COLUMNS is created");
        DataDefinition {
            catalog_ids: AtomicU64::default(),
            catalogs: CHashMap::default(),
            system_catalog: Box::new(system_catalog),
        }
    }

    pub fn persistent(path: &PathBuf) -> io::Result<Result<DataDefinition, StorageError>> {
        let system_catalog = PersistentDatabase::new(path.join(SYSTEM_CATALOG));
        let (catalogs, catalog_ids) = match system_catalog.init(DEFINITION_SCHEMA)? {
            Ok(InitStatus::Loaded) => {
                let mut max_id = 0;
                let catalogs = system_catalog
                    .read(DEFINITION_SCHEMA, CATALOG_NAMES_TABLE)
                    .expect("no io error")
                    .expect("no platform error")
                    .expect("to have CATALOG_NAMES table")
                    .map(Result::unwrap)
                    .map(Result::unwrap)
                    .map(|(id, name)| {
                        let catalog_id = id.unpack()[0].as_u64();
                        max_id = max_id.max(catalog_id);
                        let catalog_name = name.unpack()[0].as_str().to_owned();
                        (catalog_name, Arc::new(Catalog::new(catalog_id)))
                    })
                    .collect::<CHashMap<_, _>>();
                (catalogs, max_id)
            }
            Ok(InitStatus::Created) => {
                system_catalog
                    .create_object(DEFINITION_SCHEMA, CATALOG_NAMES_TABLE)
                    .expect("no io error")
                    .expect("no platform error")
                    .expect("table CATALOG_NAMES is created");
                system_catalog
                    .create_object(DEFINITION_SCHEMA, SCHEMATA_TABLE)
                    .expect("no io error")
                    .expect("no platform error")
                    .expect("table SCHEMATA is created");
                system_catalog
                    .create_object(DEFINITION_SCHEMA, TABLES_TABLE)
                    .expect("no io error")
                    .expect("no platform error")
                    .expect("table TABLES is created");
                system_catalog
                    .create_object(DEFINITION_SCHEMA, COLUMNS_TABLE)
                    .expect("no io error")
                    .expect("no platform error")
                    .expect("table COLUMNS is created");
                (CHashMap::new(), 0)
            }
            Err(storage_error) => return Ok(Err(storage_error)),
        };
        Ok(Ok(DataDefinition {
            catalog_ids: AtomicU64::new(catalog_ids),
            catalogs,
            system_catalog: Box::new(system_catalog),
        }))
    }

    pub fn create_catalog(&self, catalog_name: &str) {
        let catalog_id = self.catalog_ids.fetch_add(1, Ordering::SeqCst);
        self.catalogs
            .insert(catalog_name.to_owned(), Arc::new(Catalog::new(catalog_id)));
        self.system_catalog
            .write(
                DEFINITION_SCHEMA,
                CATALOG_NAMES_TABLE,
                vec![(
                    Binary::pack(&[Datum::from_u64(catalog_id)]),
                    Binary::pack(&[Datum::from_str(catalog_name)]),
                )],
            )
            .expect("no io error")
            .expect("no platform error")
            .expect("to save catalog");
    }

    pub fn catalog_exists(&self, catalog_name: &str) -> InnerCatalogId {
        self.catalogs.get(catalog_name).map(|catalog| catalog.id())
    }

    #[allow(dead_code)]
    pub(crate) fn drop_catalog(&self, catalog_name: &str, strategy: DropStrategy) -> Result<(), DropCatalogError> {
        if let Some(catalog) = self.catalog(catalog_name) {
            match strategy {
                DropStrategy::Restrict => {
                    if catalog.empty() {
                        if let Some(catalog) = self.catalogs.remove(catalog_name) {
                            self.system_catalog
                                .delete(
                                    DEFINITION_SCHEMA,
                                    CATALOG_NAMES_TABLE,
                                    vec![Binary::pack(&[Datum::from_u64(catalog.id())])],
                                )
                                .expect("no io error")
                                .expect("no platform error")
                                .expect("to remove catalog");
                        }
                        Ok(())
                    } else {
                        Err(DropCatalogError::HasDependentObjects)
                    }
                }
                DropStrategy::Cascade => {
                    if let Some(catalog) = self.catalogs.remove(catalog_name) {
                        self.system_catalog
                            .delete(
                                DEFINITION_SCHEMA,
                                CATALOG_NAMES_TABLE,
                                vec![Binary::pack(&[Datum::from_u64(catalog.id())])],
                            )
                            .expect("no io error")
                            .expect("no platform error")
                            .expect("to remove catalog");
                        let schema_record_ids = self
                            .system_catalog
                            .read(DEFINITION_SCHEMA, SCHEMATA_TABLE)
                            .expect("no io error")
                            .expect("no platform error")
                            .expect("to have SCHEMATA table")
                            .map(Result::unwrap)
                            .map(Result::unwrap)
                            .map(|(record_id, _columns)| {
                                let catalog_id = record_id.unpack()[0].as_u64();
                                (catalog_id, record_id)
                            })
                            .filter(|(catalog_id, _record_id)| *catalog_id == catalog.id())
                            .map(|(_catalog_id, record_id)| record_id)
                            .collect();
                        self.system_catalog
                            .delete(DEFINITION_SCHEMA, SCHEMATA_TABLE, schema_record_ids)
                            .expect("no io error")
                            .expect("no platform error")
                            .expect("to remove schemas under catalog");
                        let table_record_ids = self
                            .system_catalog
                            .read(DEFINITION_SCHEMA, TABLES_TABLE)
                            .expect("no io error")
                            .expect("no platform error")
                            .expect("to have TABLES table")
                            .map(Result::unwrap)
                            .map(Result::unwrap)
                            .map(|(record_id, _columns)| {
                                let catalog_id = record_id.unpack()[0].as_u64();
                                (catalog_id, record_id)
                            })
                            .filter(|(catalog_id, _record_id)| *catalog_id == catalog.id())
                            .map(|(_catalog_id, record_id)| record_id)
                            .collect();
                        self.system_catalog
                            .delete(DEFINITION_SCHEMA, TABLES_TABLE, table_record_ids)
                            .expect("no io error")
                            .expect("no platform error")
                            .expect("to remove tables under catalog");
                        let table_column_record_ids = self
                            .system_catalog
                            .read(DEFINITION_SCHEMA, COLUMNS_TABLE)
                            .expect("no io error")
                            .expect("no platform error")
                            .expect("to have COLUMNS table")
                            .map(Result::unwrap)
                            .map(Result::unwrap)
                            .map(|(record_id, _data)| {
                                let record = record_id.unpack();
                                let catalog = record[0].as_u64();
                                (record_id, catalog)
                            })
                            .filter(|(_record_id, catalog_id)| *catalog_id == catalog.id())
                            .map(|(record_id, _catalog)| record_id)
                            .collect();
                        self.system_catalog
                            .delete(DEFINITION_SCHEMA, COLUMNS_TABLE, table_column_record_ids)
                            .expect("no io error")
                            .expect("no platform error")
                            .expect("to have remove tables columns under catalog");
                    }
                    Ok(())
                }
            }
        } else {
            Err(DropCatalogError::DoesNotExist)
        }
    }

    pub fn create_schema(&self, catalog_name: &str, schema_name: &str) -> InnerFullSchemaId {
        let catalog = match self.catalog(catalog_name) {
            Some(catalog) => catalog,
            None => return None,
        };
        let schema_id = catalog.create_schema(schema_name);
        self.system_catalog
            .write(
                DEFINITION_SCHEMA,
                SCHEMATA_TABLE,
                vec![(
                    Binary::pack(&[Datum::from_u64(catalog.id()), Datum::from_u64(schema_id)]),
                    Binary::pack(&[Datum::from_str(catalog_name), Datum::from_str(schema_name)]),
                )],
            )
            .expect("no io error")
            .expect("no platform error")
            .expect("to save schema");
        Some((catalog.id(), Some(schema_id)))
    }

    pub fn drop_schema(
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
                            self.system_catalog
                                .delete(
                                    DEFINITION_SCHEMA,
                                    SCHEMATA_TABLE,
                                    vec![Binary::pack(&[
                                        Datum::from_u64(catalog.id()),
                                        Datum::from_u64(schema_id),
                                    ])],
                                )
                                .expect("no io error")
                                .expect("no platform error")
                                .expect("to remove schema");
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
                        self.system_catalog
                            .delete(
                                DEFINITION_SCHEMA,
                                SCHEMATA_TABLE,
                                vec![Binary::pack(&[
                                    Datum::from_u64(catalog.id()),
                                    Datum::from_u64(schema_id),
                                ])],
                            )
                            .expect("no io error")
                            .expect("no platform error")
                            .expect("to remove schema");
                        let table_record_ids = self
                            .system_catalog
                            .read(DEFINITION_SCHEMA, TABLES_TABLE)
                            .expect("no io error")
                            .expect("no platform error")
                            .expect("to have TABLES table")
                            .map(Result::unwrap)
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
                        self.system_catalog
                            .delete(DEFINITION_SCHEMA, TABLES_TABLE, table_record_ids)
                            .expect("no io error")
                            .expect("no platform error")
                            .expect("to remove tables under catalog");
                        let table_column_record_ids = self
                            .system_catalog
                            .read(DEFINITION_SCHEMA, COLUMNS_TABLE)
                            .expect("no io error")
                            .expect("no platform error")
                            .expect("to have COLUMNS table")
                            .map(Result::unwrap)
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
                        self.system_catalog
                            .delete(DEFINITION_SCHEMA, COLUMNS_TABLE, table_column_record_ids)
                            .expect("no io error")
                            .expect("no platform error")
                            .expect("to have remove tables columns under catalog");
                        Ok(())
                    }
                }
            }
        }
    }

    pub fn schemas(&self, catalog_name: &str) -> Vec<(Id, String)> {
        match self.catalog(catalog_name) {
            Some(catalog) => {
                for (id, _catalog, schema) in self
                    .system_catalog
                    .read(DEFINITION_SCHEMA, SCHEMATA_TABLE)
                    .expect("no io error")
                    .expect("no platform error")
                    .expect("to have SCHEMATA_TABLE table")
                    .map(Result::unwrap)
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
                catalog.schemas()
            }
            None => vec![],
        }
    }

    pub fn create_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        column_definitions: &[ColumnDefinition],
    ) -> InnerFullTableId {
        let catalog = match self.catalog(catalog_name) {
            Some(catalog) => catalog,
            None => return None,
        };
        let schema = match catalog.schema(schema_name) {
            Some(schema) => schema,
            None => return Some((catalog.id(), None)),
        };
        let created_table = schema.create_table(table_name, column_definitions);
        self.system_catalog
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
            .expect("no io error")
            .expect("no platform error")
            .expect("to save table info");
        for (id, column) in created_table.columns() {
            self.system_catalog
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
                .expect("no io error")
                .expect("no platform error")
                .expect("to save column");
        }
        Some((catalog.id(), Some((schema.id(), Some(created_table.id())))))
    }

    pub fn drop_table(&self, catalog_name: &str, schema_name: &str, table_name: &str) {
        let catalog = match self.catalog(catalog_name) {
            Some(catalog) => catalog,
            None => return,
        };
        let schema = match catalog.schema(schema_name) {
            Some(schema) => schema,
            None => return,
        };
        let table_id = schema.remove_table(table_name);
        if let Some(table_id) = table_id {
            self.system_catalog
                .delete(
                    DEFINITION_SCHEMA,
                    TABLES_TABLE,
                    vec![Binary::pack(&[
                        Datum::from_u64(catalog.id()),
                        Datum::from_u64(schema.id()),
                        Datum::from_u64(table_id),
                    ])],
                )
                .expect("no io error")
                .expect("no platform error")
                .expect("to remove table");
        }
    }

    pub fn tables(&self, catalog_name: &str, schema_name: &str) -> Vec<(Id, String)> {
        let catalog = match self.catalog(catalog_name) {
            Some(catalog) => catalog,
            None => return vec![],
        };
        let schema = match catalog.schema(schema_name) {
            Some(schema) => schema,
            None => return vec![],
        };
        for (table_id, _catalog, _schema, table) in self
            .system_catalog
            .read(DEFINITION_SCHEMA, TABLES_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have SCHEMATA_TABLE table")
            .map(Result::unwrap)
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
            let table_columns = self
                .system_catalog
                .read(DEFINITION_SCHEMA, COLUMNS_TABLE)
                .expect("no io error")
                .expect("no platform error")
                .expect("to have COLUMNS table")
                .map(Result::unwrap)
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
                .map(|(id, _schema, _table, column, sql_type)| (id, ColumnDefinition::new(column.as_str(), sql_type)))
                .collect::<BTreeMap<_, _>>();
            schema.add_table(table_id, table.as_str(), table_columns, max_id);
        }
        schema.tables()
    }

    fn catalog(&self, catalog_name: &str) -> Option<Arc<Catalog>> {
        self.catalogs.get(catalog_name).map(|catalog| (*catalog).clone())
    }
}

impl MetadataView for DataDefinition {
    fn schema_exists<S: AsRef<str>>(&self, schema_name: &S) -> FullSchemaId {
        self.system_catalog
            .read(DEFINITION_SCHEMA, SCHEMATA_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have SCHEMATA_TABLE table")
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(record_id, columns)| {
                let id = record_id.unpack()[1].as_u64();
                let columns = columns.unpack();
                let catalog = columns[0].as_str().to_owned();
                let schema = columns[1].as_str().to_owned();
                (id, catalog, schema)
            })
            .filter(|(_id, catalog, schema)| catalog == DEFAULT_CATALOG && schema == schema_name.as_ref())
            .map(|(id, _catalog, _schema)| id)
            .next()
    }

    fn table_exists<S: AsRef<str>, T: AsRef<str>>(&self, schema_name: &S, table_name: &T) -> FullTableId {
        match self.schema_exists(schema_name) {
            None => None,
            Some(schema_id) => Some((
                schema_id,
                self.system_catalog
                    .read(DEFINITION_SCHEMA, TABLES_TABLE)
                    .expect("no io error")
                    .expect("no platform error")
                    .expect("to have SCHEMATA_TABLE table")
                    .map(Result::unwrap)
                    .map(Result::unwrap)
                    .map(|(record_id, columns)| {
                        let record = record_id.unpack();
                        let schema_id = record[1].as_u64();
                        let table_id = record[2].as_u64();
                        let columns = columns.unpack();
                        let table = columns[2].as_str().to_owned();
                        (schema_id, table_id, table)
                    })
                    .filter(|(schema, _id, name)| schema == &schema_id && name == table_name.as_ref())
                    .map(|(_schema_id, table_id, _table)| table_id)
                    .next(),
            )),
        }
    }

    fn table_columns<I: AsRef<(Id, Id)>>(&self, table_id: &I) -> Result<Vec<ColumnDefinition>, ()> {
        match self
            .system_catalog
            .read(DEFINITION_SCHEMA, TABLES_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have SCHEMATA_TABLE table")
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(record_id, _columns)| {
                let record = record_id.unpack();
                let schema_id = record[1].as_u64();
                let table_id = record[2].as_u64();
                (schema_id, table_id)
            })
            .find(|full_table_id| full_table_id == table_id.as_ref())
        {
            Some(_) => {}
            None => return Err(()),
        }
        Ok(self
            .system_catalog
            .read(DEFINITION_SCHEMA, COLUMNS_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have SCHEMATA_TABLE table")
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(record_id, columns)| {
                let record = record_id.unpack();
                let schema_id = record[1].as_u64();
                let table_id = record[2].as_u64();
                let columns = columns.unpack();
                let name = columns[3].as_str().to_owned();
                let sql_type = columns[4].as_sql_type();
                ((schema_id, table_id), name, sql_type)
            })
            .filter(|(full_table_id, _name, _sql_type)| full_table_id == table_id.as_ref())
            .map(|(_full_table_id, name, sql_type)| ColumnDefinition::new(&name, sql_type))
            .collect())
    }

    fn column_ids<I: AsRef<(Id, Id)>, N: AsRef<str> + PartialEq<N>>(
        &self,
        table_id: &I,
        names: &[N],
    ) -> Result<(Vec<Id>, Vec<String>), ()> {
        match self
            .system_catalog
            .read(DEFINITION_SCHEMA, TABLES_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have SCHEMATA_TABLE table")
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(record_id, _columns)| {
                let record = record_id.unpack();
                let schema_id = record[1].as_u64();
                let table_id = record[2].as_u64();
                (schema_id, table_id)
            })
            .find(|full_table_id| full_table_id == table_id.as_ref())
        {
            Some(_) => {}
            None => return Err(()),
        }
        let mut idx = vec![];
        let mut not_found = vec![];
        let columns = self
            .system_catalog
            .read(DEFINITION_SCHEMA, COLUMNS_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have SCHEMATA_TABLE table")
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(record_id, columns)| {
                let record = record_id.unpack();
                let schema_id = record[1].as_u64();
                let table_id = record[2].as_u64();
                let columns = columns.unpack();
                let name = columns[3].as_str().to_owned();
                ((schema_id, table_id), name)
            })
            .filter(|(full_table_id, _name)| full_table_id == table_id.as_ref())
            .map(|(_full_table_id, name)| name)
            .enumerate()
            .map(|(index, name)| (name, index as u64))
            .collect::<HashMap<_, _>>();
        for name in names {
            match columns.get(name.as_ref()) {
                None => not_found.push(name.as_ref().to_owned()),
                Some(id) => idx.push(*id),
            }
        }
        Ok((idx, not_found))
    }

    fn column_defs<I: AsRef<(Id, Id)>>(&self, table_id: &I, ids: &[Id]) -> Vec<ColumnDefinition> {
        match self
            .system_catalog
            .read(DEFINITION_SCHEMA, TABLES_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have SCHEMATA_TABLE table")
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(record_id, _columns)| {
                let record = record_id.unpack();
                let schema_id = record[1].as_u64();
                let table_id = record[2].as_u64();
                (schema_id, table_id)
            })
            .find(|full_table_id| full_table_id == table_id.as_ref())
        {
            Some(_) => {}
            None => return vec![],
        }
        let mut defs = vec![];
        let columns = self
            .system_catalog
            .read(DEFINITION_SCHEMA, COLUMNS_TABLE)
            .expect("no io error")
            .expect("no platform error")
            .expect("to have SCHEMATA_TABLE table")
            .map(Result::unwrap)
            .map(Result::unwrap)
            .map(|(record_id, columns)| {
                let record = record_id.unpack();
                let schema_id = record[1].as_u64();
                let table_id = record[2].as_u64();
                let column_id = record[3].as_u64();
                let columns = columns.unpack();
                let name = columns[3].as_str().to_owned();
                let sql_type = columns[4].as_sql_type().to_owned();
                ((schema_id, table_id), column_id, name, sql_type)
            })
            .filter(|(full_table_id, _column_id, _name, _sql_type)| full_table_id == table_id.as_ref())
            .map(|(_full_table_id, column_id, name, sql_type)| (column_id, ColumnDefinition::new(&name, sql_type)))
            .collect::<HashMap<_, _>>();
        for id in ids {
            match columns.get(id) {
                None => {}
                Some(def) => defs.push(def.clone()),
            }
        }
        defs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    mod metadata_view {
        use super::*;

        #[test]
        fn non_created_schema_does_not_exist() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            assert!(matches!(data_definition.schema_exists(&"schema_name"), None));
        }

        #[test]
        fn created_schema_does_exist() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            data_definition.create_schema(DEFAULT_CATALOG, "schema_name");

            assert!(matches!(data_definition.schema_exists(&"schema_name"), Some(_)));
        }

        #[test]
        fn table_in_nonexistent_schema_does_not_exist() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            assert!(matches!(
                data_definition.table_exists(&"schema_name", &"table_name"),
                None
            ));
        }

        #[test]
        fn not_created_table_does_not_exist() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
            assert!(matches!(
                data_definition.table_exists(&"schema_name", &"table_name"),
                Some((_, None))
            ));
        }

        #[test]
        fn created_table_exists() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
            data_definition.create_table(DEFAULT_CATALOG, "schema_name", "table_name", &[]);

            assert!(matches!(
                data_definition.table_exists(&"schema_name", &"table_name"),
                Some((_, Some(_)))
            ));
        }

        #[test]
        fn columns_for_non_existing_table() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
            assert_eq!(data_definition.table_columns(&Box::new((1, 1))), Err(()));
        }

        #[test]
        fn columns_for_existing_table() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
            match data_definition.create_table(
                DEFAULT_CATALOG,
                "schema_name",
                "table_name",
                &[ColumnDefinition::new("col1", SqlType::Integer(0))],
            ) {
                Some((_, Some((schema_id, Some(table_id))))) => assert_eq!(
                    data_definition.table_columns(&Box::new((schema_id, table_id))),
                    Ok(vec![ColumnDefinition::new("col1", SqlType::Integer(0))])
                ),
                _ => panic!(),
            }
        }

        #[test]
        fn columns_for_table_without_columns() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
            match data_definition.create_table(DEFAULT_CATALOG, "schema_name", "table_name", &[]) {
                Some((_, Some((schema_id, Some(table_id))))) => assert_eq!(
                    data_definition.table_columns(&Box::new((schema_id, table_id))),
                    Ok(vec![])
                ),
                _ => panic!(),
            }
        }

        #[test]
        fn column_ids_for_non_existing_table() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
            assert_eq!(data_definition.column_ids(&Box::new((1, 1)), &["col1"]), Err(()));
        }

        #[test]
        fn column_ids_for_existing_table() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
            match data_definition.create_table(
                DEFAULT_CATALOG,
                "schema_name",
                "table_name",
                &[ColumnDefinition::new("col1", SqlType::Integer(0))],
            ) {
                Some((_, Some((schema_id, Some(table_id))))) => assert_eq!(
                    data_definition.column_ids(&Box::new((schema_id, table_id)), &["col1".to_owned()]),
                    Ok((vec![0], vec![]))
                ),
                _ => panic!(),
            }
        }

        #[test]
        fn column_ids_for_table_without_columns() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
            match data_definition.create_table(DEFAULT_CATALOG, "schema_name", "table_name", &[]) {
                Some((_, Some((schema_id, Some(table_id))))) => assert_eq!(
                    data_definition.column_ids(&Box::new((schema_id, table_id)), &["col1".to_owned()]),
                    Ok((vec![], vec!["col1".to_owned()]))
                ),
                _ => panic!(),
            }
        }

        #[test]
        fn column_defs_for_non_existing_table() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
            assert_eq!(data_definition.column_defs(&Box::new((1, 1)), &[0]), vec![]);
        }

        #[test]
        fn column_defs_for_existing_table() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
            match data_definition.create_table(
                DEFAULT_CATALOG,
                "schema_name",
                "table_name",
                &[ColumnDefinition::new("col1", SqlType::Integer(0))],
            ) {
                Some((_, Some((schema_id, Some(table_id))))) => assert_eq!(
                    data_definition.column_defs(&Box::new((schema_id, table_id)), &[0]),
                    vec![ColumnDefinition::new("col1", SqlType::Integer(0))]
                ),
                _ => panic!(),
            }
        }

        #[test]
        fn column_defs_for_non_existing_column() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
            match data_definition.create_table(
                DEFAULT_CATALOG,
                "schema_name",
                "table_name",
                &[ColumnDefinition::new("col1", SqlType::Integer(0))],
            ) {
                Some((_, Some((schema_id, Some(table_id))))) => assert_eq!(
                    data_definition.column_defs(&Box::new((schema_id, table_id)), &[1]),
                    vec![]
                ),
                _ => panic!(),
            }
        }

        #[test]
        fn column_defs_for_table_without_columns() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
            match data_definition.create_table(DEFAULT_CATALOG, "schema_name", "table_name", &[]) {
                Some((_, Some((schema_id, Some(table_id))))) => assert_eq!(
                    data_definition.column_defs(&Box::new((schema_id, table_id)), &[]),
                    vec![]
                ),
                _ => panic!(),
            }
        }
    }

    #[cfg(test)]
    mod general_cases {
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

            data_definition.create_catalog(DEFAULT_CATALOG);
            data_definition.create_schema(DEFAULT_CATALOG, "schema_name");

            assert_eq!(
                data_definition.drop_catalog(DEFAULT_CATALOG, DropStrategy::Cascade),
                Ok(())
            );

            assert!(matches!(data_definition.catalog_exists(DEFAULT_CATALOG), None));
            assert!(matches!(data_definition.schema_exists(&"schema_name"), None));
        }

        #[test]
        fn not_created_schema_does_not_exist() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            assert!(matches!(data_definition.schema_exists(&"schema_name"), None));
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

            data_definition.create_catalog(DEFAULT_CATALOG);
            data_definition.create_schema(DEFAULT_CATALOG, "schema_name");

            assert!(matches!(data_definition.schema_exists(&"schema_name"), Some(_)));
        }

        #[test]
        fn drop_schema() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            data_definition.create_schema(DEFAULT_CATALOG, "schema_name");

            assert!(matches!(data_definition.schema_exists(&"schema_name"), Some(_)));

            assert_eq!(
                data_definition.drop_schema(DEFAULT_CATALOG, "schema_name", DropStrategy::Restrict),
                Ok(())
            );

            assert!(matches!(data_definition.schema_exists(&"schema_name"), None));
        }

        #[test]
        fn restrict_drop_strategy_cant_drop_non_empty_schema() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
            data_definition.create_table(DEFAULT_CATALOG, "schema_name", "table_name", &[]);

            assert_eq!(
                data_definition.drop_schema(DEFAULT_CATALOG, "schema_name", DropStrategy::Restrict),
                Err(DropSchemaError::HasDependentObjects)
            );

            assert!(matches!(data_definition.schema_exists(&"schema_name"), Some(_)));
            assert!(matches!(
                data_definition.table_exists(&"schema_name", &"table_name"),
                Some((_, Some(_)))
            ))
        }

        #[test]
        fn cascade_drop_of_non_empty_schema() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
            data_definition.create_table(DEFAULT_CATALOG, "schema_name", "table_name", &[]);

            assert_eq!(
                data_definition.drop_schema(DEFAULT_CATALOG, "schema_name", DropStrategy::Cascade),
                Ok(())
            );

            assert!(matches!(data_definition.schema_exists(&"schema_name"), None));
            assert!(matches!(
                data_definition.table_exists(&"schema_name", &"table_name"),
                None
            ));
        }

        #[test]
        fn not_created_table_does_not_exist() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
            assert!(matches!(
                data_definition.table_exists(&"schema_name", &"table_name"),
                Some((_, None))
            ));
        }

        #[test]
        fn create_table() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
            data_definition.create_table(DEFAULT_CATALOG, "schema_name", "table_name", &[]);

            assert!(matches!(
                data_definition.table_exists(&"schema_name", &"table_name"),
                Some((_, Some(_)))
            ));
        }

        #[test]
        fn drop_table() {
            let data_definition = DataDefinition::in_memory();

            data_definition.create_catalog(DEFAULT_CATALOG);
            data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
            data_definition.create_table(DEFAULT_CATALOG, "schema_name", "table_name", &[]);

            assert!(matches!(
                data_definition.table_exists(&"schema_name", &"table_name"),
                Some((_, Some(_)))
            ));

            data_definition.drop_table(DEFAULT_CATALOG, "schema_name", "table_name");

            assert!(matches!(
                data_definition.table_exists(&"schema_name", &"table_name"),
                Some((_, None))
            ));
        }

        #[cfg(test)]
        mod persistent {
            use super::*;

            #[rstest::fixture]
            fn storage_path() -> (DataDefinition, PathBuf) {
                let root_path = tempfile::tempdir().expect("to create temporary folder");
                let path = root_path.into_path();
                let data_definition = DataDefinition::persistent(&path)
                    .expect("no io errors")
                    .expect("create persistent data definition");
                (data_definition, path)
            }

            #[rstest::rstest]
            fn storage_preserve_created_catalog_after_restart(storage_path: (DataDefinition, PathBuf)) {
                let (data_definition, path) = storage_path;
                data_definition.create_catalog("catalog_name");
                drop(data_definition);

                let data_definition = DataDefinition::persistent(&path)
                    .expect("no io errors")
                    .expect("create persistent data definition");
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

                let data_definition = DataDefinition::persistent(&path)
                    .expect("no io errors")
                    .expect("create persistent data definition");
                assert!(matches!(data_definition.catalog_exists("catalog_name"), None));
            }

            #[rstest::rstest]
            fn storage_preserve_created_schema_after_restart(storage_path: (DataDefinition, PathBuf)) {
                let (data_definition, path) = storage_path;
                data_definition.create_catalog(DEFAULT_CATALOG);
                data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
                drop(data_definition);

                let data_definition = DataDefinition::persistent(&path)
                    .expect("no io errors")
                    .expect("create persistent data definition");
                assert!(matches!(data_definition.schema_exists(&"schema_name"), Some(_)));
            }

            #[rstest::rstest]
            fn dropped_schema_is_not_preserved_after_restart(storage_path: (DataDefinition, PathBuf)) {
                let (data_definition, path) = storage_path;
                data_definition.create_catalog(DEFAULT_CATALOG);
                data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
                assert!(matches!(data_definition.schema_exists(&"schema_name"), Some(_)));
                assert_eq!(
                    data_definition.drop_schema(DEFAULT_CATALOG, "schema_name", DropStrategy::Restrict),
                    Ok(())
                );
                assert!(matches!(data_definition.schema_exists(&"schema_name"), None));
                drop(data_definition);

                let data_definition = DataDefinition::persistent(&path)
                    .expect("no io errors")
                    .expect("create persistent data definition");
                assert!(matches!(data_definition.schema_exists(&"schema_name"), None));
            }

            #[rstest::rstest]
            fn storage_preserve_created_table_after_restart(storage_path: (DataDefinition, PathBuf)) {
                let (data_definition, path) = storage_path;
                data_definition.create_catalog(DEFAULT_CATALOG);
                data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
                data_definition.create_table(
                    DEFAULT_CATALOG,
                    "schema_name",
                    "table_name",
                    &[ColumnDefinition::new("col_1", SqlType::Integer(0))],
                );
                drop(data_definition);

                let data_definition = DataDefinition::persistent(&path)
                    .expect("no io errors")
                    .expect("create persistent data definition");
                assert!(matches!(
                    data_definition.table_exists(&"schema_name", &"table_name"),
                    Some((_, Some(_)))
                ));
            }

            #[rstest::rstest]
            fn storage_preserve_created_table_with_the_same_name_in_different_schemas_after_restart(
                storage_path: (DataDefinition, PathBuf),
            ) {
                let (data_definition, path) = storage_path;
                data_definition.create_catalog(DEFAULT_CATALOG);
                data_definition.create_schema(DEFAULT_CATALOG, "schema_name_1");
                let full_table_1_id = match data_definition.create_table(
                    DEFAULT_CATALOG,
                    "schema_name_1",
                    "table_name",
                    &[ColumnDefinition::new("col_1", SqlType::Integer(0))],
                ) {
                    Some((_, Some((schema_id, Some(table_id))))) => Box::new((schema_id, table_id)),
                    _ => panic!(),
                };
                data_definition.create_schema(DEFAULT_CATALOG, "schema_name_2");
                let full_table_2_id = match data_definition.create_table(
                    DEFAULT_CATALOG,
                    "schema_name_2",
                    "table_name",
                    &[ColumnDefinition::new("col_1", SqlType::SmallInt(0))],
                ) {
                    Some((_, Some((schema_id, Some(table_id))))) => Box::new((schema_id, table_id)),
                    _ => panic!(),
                };
                assert_eq!(
                    data_definition.table_columns(&full_table_1_id),
                    Ok(vec![ColumnDefinition::new("col_1", SqlType::Integer(0))])
                );
                assert_eq!(
                    data_definition.table_columns(&full_table_2_id),
                    Ok(vec![ColumnDefinition::new("col_1", SqlType::SmallInt(0))])
                );
                drop(data_definition);

                let data_definition = DataDefinition::persistent(&path)
                    .expect("no io errors")
                    .expect("create persistent data definition");

                assert_eq!(
                    data_definition.table_columns(&full_table_1_id),
                    Ok(vec![ColumnDefinition::new("col_1", SqlType::Integer(0))])
                );
                assert_eq!(
                    data_definition.table_columns(&full_table_2_id),
                    Ok(vec![ColumnDefinition::new("col_1", SqlType::SmallInt(0))])
                );
            }

            #[rstest::rstest]
            fn dropped_table_is_not_preserved_after_restart(storage_path: (DataDefinition, PathBuf)) {
                let (data_definition, path) = storage_path;
                data_definition.create_catalog(DEFAULT_CATALOG);
                data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
                data_definition.create_table(
                    DEFAULT_CATALOG,
                    "schema_name",
                    "table_name",
                    &[ColumnDefinition::new("col_1", SqlType::Integer(0))],
                );
                assert!(matches!(
                    data_definition.table_exists(&"schema_name", &"table_name"),
                    Some((_, Some(_)))
                ));
                data_definition.drop_table(DEFAULT_CATALOG, "schema_name", "table_name");
                assert!(matches!(
                    data_definition.table_exists(&"schema_name", &"table_name"),
                    Some((_, None))
                ));
                drop(data_definition);

                let data_definition = DataDefinition::persistent(&path)
                    .expect("no io errors")
                    .expect("create persistent data definition");
                assert!(matches!(
                    data_definition.table_exists(&"schema_name", &"table_name"),
                    Some((_, None))
                ));
            }

            #[rstest::rstest]
            fn table_columns_data_preserved_after_restart(storage_path: (DataDefinition, PathBuf)) {
                let (data_definition, path) = storage_path;
                data_definition.create_catalog(DEFAULT_CATALOG);
                data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
                let full_table_id = match data_definition.create_table(
                    DEFAULT_CATALOG,
                    "schema_name",
                    "table_name",
                    &[
                        ColumnDefinition::new("col_1", SqlType::SmallInt(0)),
                        ColumnDefinition::new("col_2", SqlType::Integer(0)),
                        ColumnDefinition::new("col_3", SqlType::BigInt(0)),
                    ],
                ) {
                    Some((_, Some((schema_id, Some(table_id))))) => Box::new((schema_id, table_id)),
                    _ => panic!(),
                };
                assert_eq!(
                    data_definition.table_columns(&full_table_id),
                    Ok(vec![
                        ColumnDefinition::new("col_1", SqlType::SmallInt(0)),
                        ColumnDefinition::new("col_2", SqlType::Integer(0)),
                        ColumnDefinition::new("col_3", SqlType::BigInt(0))
                    ])
                );
                drop(data_definition);

                let data_definition = DataDefinition::persistent(&path)
                    .expect("no io errors")
                    .expect("create persistent data definition");
                assert_eq!(
                    data_definition.table_columns(&full_table_id),
                    Ok(vec![
                        ColumnDefinition::new("col_1", SqlType::SmallInt(0)),
                        ColumnDefinition::new("col_2", SqlType::Integer(0)),
                        ColumnDefinition::new("col_3", SqlType::BigInt(0))
                    ])
                );
            }

            #[rstest::rstest]
            fn tables_schemas_are_not_preserved_after_cascade_catalog_drop(storage_path: (DataDefinition, PathBuf)) {
                let (data_definition, path) = storage_path;
                data_definition.create_catalog(DEFAULT_CATALOG);
                data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
                let full_table_id = match data_definition.create_table(
                    DEFAULT_CATALOG,
                    "schema_name",
                    "table_name",
                    &[ColumnDefinition::new("col_1", SqlType::SmallInt(0))],
                ) {
                    Some((_, Some((schema_id, Some(table_id))))) => Box::new((schema_id, table_id)),
                    _ => panic!(),
                };

                assert_eq!(
                    data_definition.drop_catalog(DEFAULT_CATALOG, DropStrategy::Cascade),
                    Ok(())
                );
                assert!(matches!(data_definition.catalog_exists(DEFAULT_CATALOG), None));
                assert!(matches!(data_definition.schema_exists(&"schema_name"), None));
                assert!(matches!(
                    data_definition.table_exists(&"schema_name", &"table_name"),
                    None
                ));
                assert_eq!(data_definition.table_columns(&full_table_id), Err(()));

                drop(data_definition);

                let data_definition = DataDefinition::persistent(&path)
                    .expect("no io errors")
                    .expect("create persistent data definition");

                assert!(matches!(data_definition.catalog_exists(DEFAULT_CATALOG), None));
                assert!(matches!(data_definition.schema_exists(&"schema_name"), None));
                assert!(matches!(
                    data_definition.table_exists(&"schema_name", &"table_name"),
                    None
                ));
                assert_eq!(data_definition.table_columns(&full_table_id), Err(()));

                data_definition.create_catalog(DEFAULT_CATALOG);
                assert!(matches!(data_definition.schema_exists(&"schema_name"), None));
                data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
                assert!(matches!(
                    data_definition.table_exists(&"schema_name", &"table_name"),
                    Some((_, None))
                ));
            }

            #[rstest::rstest]
            fn tables_are_not_preserved_after_cascade_schema_drop(storage_path: (DataDefinition, PathBuf)) {
                let (data_definition, path) = storage_path;
                data_definition.create_catalog(DEFAULT_CATALOG);
                data_definition.create_schema(DEFAULT_CATALOG, "schema_name");
                let full_table_id = match data_definition.create_table(
                    DEFAULT_CATALOG,
                    "schema_name",
                    "table_name",
                    &[ColumnDefinition::new("col_1", SqlType::SmallInt(0))],
                ) {
                    Some((_, Some((schema_id, Some(table_id))))) => Box::new((schema_id, table_id)),
                    _ => panic!(),
                };

                assert_eq!(
                    data_definition.drop_schema(DEFAULT_CATALOG, "schema_name", DropStrategy::Cascade),
                    Ok(())
                );
                assert!(matches!(data_definition.schema_exists(&"schema_name"), None));
                assert!(matches!(
                    data_definition.table_exists(&"schema_name", &"table_name"),
                    None
                ));
                assert_eq!(data_definition.table_columns(&full_table_id), Err(()));

                drop(data_definition);

                let data_definition = DataDefinition::persistent(&path)
                    .expect("no io errors")
                    .expect("create persistent data definition");

                assert!(matches!(data_definition.schema_exists(&"schema_name"), None));
                assert!(matches!(
                    data_definition.table_exists(&"schema_name", &"table_name"),
                    None
                ));
                assert_eq!(data_definition.table_columns(&full_table_id), Err(()));
            }
        }
    }
}
