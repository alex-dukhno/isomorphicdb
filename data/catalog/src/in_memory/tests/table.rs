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
use data_definition_execution_plan::DropTablesQuery;

#[test]
fn create_table_where_schema_not_found() {
    let database = database();

    assert_eq!(
        database.execute(SchemaChange::CreateTable(CreateTableQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_defs: vec![ColumnInfo { name: "col_1".to_owned(), sql_type: SqlType::small_int() }, ColumnInfo { name: "col_2".to_owned(), sql_type: SqlType::big_int() }],
            if_not_exists: false,
        })),
        Err(ExecutionError::SchemaDoesNotExist(SCHEMA.to_owned()))
    );
}

#[test]
fn create_table_with_the_same_name() {
    let database = database();

    assert_eq!(
        database.execute(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::SchemaCreated)
    );

    assert_eq!(
        database.execute(SchemaChange::CreateTable(CreateTableQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_defs: vec![ColumnInfo { name: "col_1".to_owned(), sql_type: SqlType::small_int() }, ColumnInfo { name: "col_2".to_owned(), sql_type: SqlType::big_int() }],
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::TableCreated)
    );
    assert_eq!(database.table_exists(&FullTableName::from((&SCHEMA, &TABLE))), true);
    assert_eq!(database.table_columns(&FullTableName::from((&SCHEMA, &TABLE))), vec![ColumnDef::new("col_1".to_owned(), SqlType::small_int(), 0), ColumnDef::new("col_2".to_owned(), SqlType::big_int(), 1)]);

    assert_eq!(
        database.execute(SchemaChange::CreateTable(CreateTableQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_defs: vec![ColumnInfo { name: "col_1".to_owned(), sql_type: SqlType::small_int() }, ColumnInfo { name: "col_2".to_owned(), sql_type: SqlType::big_int() }],
            if_not_exists: false,
        })),
        Err(ExecutionError::TableAlreadyExists(SCHEMA.to_owned(), TABLE.to_owned()))
    );
}

#[test]
fn create_if_not_exists() {
    let database = database();

    assert_eq!(
        database.execute(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::SchemaCreated)
    );

    assert_eq!(
        database.execute(SchemaChange::CreateTable(CreateTableQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_defs: vec![ColumnInfo { name: "col_1".to_owned(), sql_type: SqlType::small_int() }, ColumnInfo { name: "col_2".to_owned(), sql_type: SqlType::big_int() }],
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::TableCreated)
    );

    assert_eq!(
        database.execute(SchemaChange::CreateTable(CreateTableQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_defs: vec![ColumnInfo { name: "col_1".to_owned(), sql_type: SqlType::small_int() }, ColumnInfo { name: "col_2".to_owned(), sql_type: SqlType::big_int() }],
            if_not_exists: true,
        })),
        Ok(ExecutionOutcome::TableCreated)
    );
    assert_eq!(database.table_columns(&FullTableName::from((&SCHEMA, &TABLE))), vec![ColumnDef::new("col_1".to_owned(), SqlType::small_int(), 0), ColumnDef::new("col_2".to_owned(), SqlType::big_int(), 1)]);
}

#[test]
fn drop_table_where_schema_not_found() {
    let database = database();

    assert_eq!(
        database.execute(SchemaChange::DropTables(DropTablesQuery {
            full_table_names: vec![FullTableName::from((&SCHEMA, &TABLE))],
            cascade: false,
            if_exists: false
        })),
        Err(ExecutionError::SchemaDoesNotExist(SCHEMA.to_owned()))
    );
}

#[test]
fn drop_nonexistent_table() {
    let database = database();

    assert_eq!(
        database.execute(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::SchemaCreated)
    );

    assert_eq!(
        database.execute(SchemaChange::DropTables(DropTablesQuery {
            full_table_names: vec![FullTableName::from((&SCHEMA, &TABLE))],
            cascade: false,
            if_exists: false
        })),
        Err(ExecutionError::TableDoesNotExist(SCHEMA.to_owned(), TABLE.to_owned()))
    );
}

#[test]
fn drop_many() {
    let database = database();

    assert_eq!(
        database.execute(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::SchemaCreated)
    );

    assert_eq!(
        database.execute(SchemaChange::CreateTable(CreateTableQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_defs: vec![ColumnInfo { name: "col_1".to_owned(), sql_type: SqlType::small_int() }, ColumnInfo { name: "col_2".to_owned(), sql_type: SqlType::big_int() }],
            if_not_exists: true,
        })),
        Ok(ExecutionOutcome::TableCreated)
    );

    assert_eq!(
        database.execute(SchemaChange::CreateTable(CreateTableQuery {
            full_table_name: FullTableName::from((&SCHEMA, &OTHER_TABLE)),
            column_defs: vec![ColumnInfo { name: "col_1".to_owned(), sql_type: SqlType::small_int() }, ColumnInfo { name: "col_2".to_owned(), sql_type: SqlType::big_int() }],
            if_not_exists: true,
        })),
        Ok(ExecutionOutcome::TableCreated)
    );

    assert_eq!(
        database.execute(SchemaChange::DropTables(DropTablesQuery {
            full_table_names: vec![FullTableName::from((&SCHEMA, &TABLE)), FullTableName::from((&SCHEMA, &OTHER_TABLE))],
            cascade: false,
            if_exists: false
        })),
        Ok(ExecutionOutcome::TableDropped)
    );
    assert_eq!(database.table_exists(&FullTableName::from((&SCHEMA, &TABLE))), false);
    assert_eq!(database.table_columns(&FullTableName::from((&SCHEMA, &TABLE))), vec![]);

    assert_eq!(database.table_exists(&FullTableName::from((&SCHEMA, &OTHER_TABLE))), false);
    assert_eq!(database.table_columns(&FullTableName::from((&SCHEMA, &OTHER_TABLE))), vec![]);
}

#[test]
fn drop_if_exists_first() {
    let database = database();

    assert_eq!(
        database.execute(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::SchemaCreated)
    );

    assert_eq!(
        database.execute(SchemaChange::CreateTable(CreateTableQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_defs: vec![ColumnInfo { name: "col_1".to_owned(), sql_type: SqlType::small_int() }, ColumnInfo { name: "col_2".to_owned(), sql_type: SqlType::big_int() }],
            if_not_exists: true,
        })),
        Ok(ExecutionOutcome::TableCreated)
    );

    assert_eq!(
        database.execute(SchemaChange::DropTables(DropTablesQuery {
            full_table_names: vec![FullTableName::from((&SCHEMA, &TABLE)), FullTableName::from((&SCHEMA, &OTHER_TABLE))],
            cascade: false,
            if_exists: true
        })),
        Ok(ExecutionOutcome::TableDropped)
    );

    assert_eq!(database.table_exists(&FullTableName::from((&SCHEMA, &TABLE))), false);
    assert_eq!(database.table_exists(&FullTableName::from((&SCHEMA, &OTHER_TABLE))), false);
}

#[test]
fn drop_if_exists_last() {
    let database = database();

    assert_eq!(
        database.execute(SchemaChange::CreateSchema(CreateSchemaQuery {
            schema_name: SchemaName::from(&SCHEMA),
            if_not_exists: false,
        })),
        Ok(ExecutionOutcome::SchemaCreated)
    );

    assert_eq!(
        database.execute(SchemaChange::CreateTable(CreateTableQuery {
            full_table_name: FullTableName::from((&SCHEMA, &OTHER_TABLE)),
            column_defs: vec![ColumnInfo { name: "col_1".to_owned(), sql_type: SqlType::small_int() }, ColumnInfo { name: "col_2".to_owned(), sql_type: SqlType::big_int() }],
            if_not_exists: true,
        })),
        Ok(ExecutionOutcome::TableCreated)
    );

    assert_eq!(
        database.execute(SchemaChange::DropTables(DropTablesQuery {
            full_table_names: vec![FullTableName::from((&SCHEMA, &TABLE)), FullTableName::from((&SCHEMA, &OTHER_TABLE))],
            cascade: false,
            if_exists: true
        })),
        Ok(ExecutionOutcome::TableDropped)
    );

    assert_eq!(database.table_exists(&FullTableName::from((&SCHEMA, &TABLE))), false);
    assert_eq!(database.table_exists(&FullTableName::from((&SCHEMA, &OTHER_TABLE))), false);
}
