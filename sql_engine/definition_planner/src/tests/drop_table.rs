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

fn inner_drop(names: Vec<(&str, &str)>, if_exists: bool, cascade: bool) -> Definition {
    Definition::DropTables {
        if_exists,
        names: names
            .into_iter()
            .map(|(schema, table)| (schema.to_owned(), table.to_owned()))
            .collect(),
        cascade,
    }
}

fn drop_table_stmt(names: Vec<(&str, &str)>) -> Definition {
    inner_drop(names, false, false)
}

fn drop_if_exists(names: Vec<(&str, &str)>) -> Definition {
    inner_drop(names, true, false)
}

fn drop_cascade(names: Vec<(&str, &str)>) -> Definition {
    inner_drop(names, false, true)
}

#[test]
fn drop_table_from_nonexistent_schema() {
    let analyzer = DefinitionPlanner::new(InMemoryDatabase::new());
    assert_eq!(
        analyzer.plan(drop_table_stmt(vec![("non_existent_schema", TABLE)])),
        Err(SchemaPlanError::schema_does_not_exist(&"non_existent_schema"))
    );
}

#[test]
fn drop_table() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())]))
        .unwrap();
    let analyzer = DefinitionPlanner::new(database);
    assert_eq!(
        analyzer.plan(drop_table_stmt(vec![(SCHEMA, TABLE)])),
        Ok(QueryAnalysis::DDL(SchemaChange::DropTables(DropTablesQuery {
            full_table_names: vec![FullTableName::from((&SCHEMA, &TABLE))],
            cascade: false,
            if_exists: false
        })))
    );
}

#[test]
fn drop_nonexistent_table() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    let analyzer = DefinitionPlanner::new(database);
    assert_eq!(
        analyzer.plan(drop_table_stmt(vec![(SCHEMA, "non_existent_table")])),
        Ok(QueryAnalysis::DDL(SchemaChange::DropTables(DropTablesQuery {
            full_table_names: vec![FullTableName::from((&SCHEMA, &"non_existent_table"))],
            cascade: false,
            if_exists: false
        })))
    );
}

#[test]
fn drop_table_if_exists() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())]))
        .unwrap();
    database
        .execute(create_table_ops(SCHEMA, "table_1", vec![("col", SqlType::bool())]))
        .unwrap();
    let analyzer = DefinitionPlanner::new(database);
    assert_eq!(
        analyzer.plan(drop_if_exists(vec![(SCHEMA, TABLE), (SCHEMA, "table_1")],)),
        Ok(SchemaChange::DropTables(DropTablesQuery {
            full_table_names: vec![
                FullTableName::from((&SCHEMA, &TABLE)),
                FullTableName::from((&SCHEMA, &"table_1"))
            ],
            cascade: false,
            if_exists: true
        }))
    );
}

#[test]
fn drop_table_cascade() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())]))
        .unwrap();
    database
        .execute(create_table_ops(SCHEMA, "table_1", vec![("col", SqlType::bool())]))
        .unwrap();
    let analyzer = DefinitionPlanner::new(database);
    assert_eq!(
        analyzer.plan(drop_cascade(vec![(SCHEMA, TABLE), (SCHEMA, "table_1")],)),
        Ok(SchemaChange::DropTables(DropTablesQuery {
            full_table_names: vec![
                FullTableName::from((&SCHEMA, &TABLE)),
                FullTableName::from((&SCHEMA, &"table_1"))
            ],
            cascade: true,
            if_exists: false
        }))
    );
}
