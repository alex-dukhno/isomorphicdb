// Copyright 2020 - present Alex Dukhno
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

use super::*;
use data_definition_execution_plan::{CreateSchemaQuery, DropSchemasQuery, CreateTableQuery};

#[test]
fn create_schema() {
    let database = database();
    assert_eq!(
        database.execute_new(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::SchemaCreated)
    );

    assert_eq!(database.schema_exists(SCHEMA), true);
}

#[test]
fn create_if_not_exists() {
    let database = database();

    assert_eq!(
        database.execute_new(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: true,
        })),
        Ok(ExecutionOutcome::SchemaCreated)
    );

    assert_eq!(
        database.execute_new(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: true,
        })),
        Ok(ExecutionOutcome::SchemaCreated)
    );
}

#[test]
fn create_schema_with_the_same_name() {
    let database = database();
    assert_eq!(
        database.execute_new(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::SchemaCreated)
    );

    assert_eq!(
        database.execute_new(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: false,
        })),
        Err(ExecutionError::SchemaAlreadyExists(SCHEMA.to_owned()))
    );
}

#[test]
fn drop_nonexistent_schema() {
    let database = database();

    assert_eq!(
        database.execute_new(SchemaChange::DropSchemas(DropSchemasQuery {
            schema_names: vec![SchemaName::from(&SCHEMA)],
            cascade: false,
            if_exists: false
        })),
        Err(ExecutionError::SchemaDoesNotExist(SCHEMA.to_owned()))
    );
}

#[test]
fn drop_single_schema() {
    let database = database();

    assert_eq!(
        database.execute_new(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::SchemaCreated)
    );

    assert_eq!(
        database.execute_new(SchemaChange::DropSchemas(DropSchemasQuery {
            schema_names: vec![SchemaName::from(&SCHEMA)],
            cascade: false,
            if_exists: false
        })),
        Ok(ExecutionOutcome::SchemaDropped)
    );

    assert_eq!(database.schema_exists(SCHEMA), false);
}

#[test]
fn drop_many_schemas() {
    let database = database();

    assert_eq!(
        database.execute_new(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::SchemaCreated)
    );
    assert_eq!(
        database.execute_new(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&OTHER_SCHEMA),
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::SchemaCreated)
    );

    assert_eq!(
        database.execute_new(SchemaChange::DropSchemas(DropSchemasQuery {
            schema_names: vec![SchemaName::from(&SCHEMA), SchemaName::from(&OTHER_SCHEMA)],
            cascade: false,
            if_exists: false
        })),
        Ok(ExecutionOutcome::SchemaDropped)
    );

    assert_eq!(database.schema_exists(SCHEMA), false);
    assert_eq!(database.schema_exists(OTHER_SCHEMA), false);
}

#[test]
fn drop_schema_with_table() {
    let database = database();

    assert_eq!(
        database.execute_new(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::SchemaCreated)
    );
    assert_eq!(
        database.execute_new(SchemaChange::CreateTable(CreateTableQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_defs: vec![],
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::TableCreated)
    );

    assert_eq!(
        database.execute_new(SchemaChange::DropSchemas(DropSchemasQuery {
            schema_names: vec![SchemaName::from(&SCHEMA)],
            cascade: false,
            if_exists: false
        })),
        Err(ExecutionError::SchemaHasDependentObjects(SCHEMA.to_owned()))
    );

    assert_eq!(database.schema_exists(SCHEMA), true);
    assert_eq!(database.table_exists(&FullTableName::from((&SCHEMA, &TABLE))), true);
}

#[test]
fn drop_many_cascade() {
    let database = database();

    assert_eq!(
        database.execute_new(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::SchemaCreated)
    );
    assert_eq!(
        database.execute_new(SchemaChange::CreateTable(CreateTableQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_defs: vec![ColumnInfo { name: "col_1".to_owned(), sql_type: SqlType::small_int() }, ColumnInfo { name: "col_2".to_owned(), sql_type: SqlType::big_int() }],
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::TableCreated)
    );

    assert_eq!(
        database.execute_new(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&OTHER_SCHEMA),
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::SchemaCreated)
    );
    assert_eq!(
        database.execute_new(SchemaChange::CreateTable(CreateTableQuery {
            full_table_name: FullTableName::from((&OTHER_SCHEMA, &TABLE)),
            column_defs: vec![ColumnInfo { name: "col_1".to_owned(), sql_type: SqlType::small_int() }, ColumnInfo { name: "col_2".to_owned(), sql_type: SqlType::big_int() }],
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::TableCreated)
    );

    assert_eq!(
        database.execute_new(SchemaChange::DropSchemas(DropSchemasQuery {
            schema_names: vec![SchemaName::from(&SCHEMA), SchemaName::from(&OTHER_SCHEMA)],
            cascade: true,
            if_exists: false
        })),
        Ok(ExecutionOutcome::SchemaDropped)
    );

    assert_eq!(database.schema_exists(SCHEMA), false);
    assert_eq!(database.schema_exists(OTHER_SCHEMA), false);
    assert_eq!(database.table_exists(&FullTableName::from((&SCHEMA, &TABLE))), false);
    assert_eq!(database.table_columns(&FullTableName::from((&SCHEMA, &TABLE))), vec![]);
    assert_eq!(database.table_exists(&FullTableName::from((&OTHER_SCHEMA, &TABLE))), false);
    assert_eq!(database.table_columns(&FullTableName::from((&OTHER_SCHEMA, &TABLE))), vec![]);
}

#[test]
fn drop_many_if_exists_first() {
    let database = database();

    assert_eq!(
        database.execute_new(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::SchemaCreated)
    );

    assert_eq!(
        database.execute_new(SchemaChange::DropSchemas(DropSchemasQuery {
            schema_names: vec![SchemaName::from(&SCHEMA), SchemaName::from(&OTHER_SCHEMA)],
            cascade: false,
            if_exists: true
        })),
        Ok(ExecutionOutcome::SchemaDropped)
    );

    assert_eq!(database.schema_exists(SCHEMA), false);
}

#[test]
fn drop_many_if_exists_last() {
    let database = database();

    assert_eq!(
        database.execute_new(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&OTHER_SCHEMA),
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::SchemaCreated)
    );

    assert_eq!(
        database.execute_new(SchemaChange::DropSchemas(DropSchemasQuery {
            schema_names: vec![SchemaName::from(&SCHEMA), SchemaName::from(&OTHER_SCHEMA)],
            cascade: false,
            if_exists: true
        })),
        Ok(ExecutionOutcome::SchemaDropped)
    );

    assert_eq!(database.schema_exists(OTHER_SCHEMA), false);
}
