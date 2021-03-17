// Copyright 2020 - 2021 Alex Dukhno
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

fn inner_drop(names: Vec<&str>, if_exists: bool, cascade: bool) -> Definition {
    Definition::DropSchemas {
        if_exists,
        names: names.into_iter().map(ToOwned::to_owned).collect(),
        cascade,
    }
}

fn drop_schema_stmt(names: Vec<&str>) -> Definition {
    inner_drop(names, false, false)
}

fn drop_if_exists(names: Vec<&str>) -> Definition {
    inner_drop(names, true, false)
}

fn drop_cascade(names: Vec<&str>) -> Definition {
    inner_drop(names, false, true)
}

#[test]
fn drop_non_existent_schema() {
    let analyzer = DefinitionPlanner::new(InMemoryDatabase::new());
    assert_eq!(
        analyzer.plan(drop_schema_stmt(vec!["non_existent"])),
        Ok(SchemaChange::DropSchemas(DropSchemasQuery {
            schema_names: vec![SchemaName::from(&"non_existent")],
            cascade: false,
            if_exists: false,
        }))
    );
}

#[test]
fn drop_schema() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    let analyzer = DefinitionPlanner::new(database);

    assert_eq!(
        analyzer.plan(drop_schema_stmt(vec![SCHEMA])),
        Ok(SchemaChange::DropSchemas(DropSchemasQuery {
            schema_names: vec![SchemaName::from(&SCHEMA)],
            cascade: false,
            if_exists: false,
        }))
    );
}

#[test]
fn drop_schema_cascade() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    let analyzer = DefinitionPlanner::new(database);

    assert_eq!(
        analyzer.plan(drop_cascade(vec![SCHEMA])),
        Ok(SchemaChange::DropSchemas(DropSchemasQuery {
            schema_names: vec![SchemaName::from(&SCHEMA)],
            cascade: true,
            if_exists: false,
        }))
    );
}

#[test]
fn drop_schema_if_exists() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    let analyzer = DefinitionPlanner::new(database);

    assert_eq!(
        analyzer.plan(drop_if_exists(vec![SCHEMA, "schema_1"])),
        Ok(SchemaChange::DropSchemas(DropSchemasQuery {
            schema_names: vec![SchemaName::from(&SCHEMA), SchemaName::from(&"schema_1")],
            cascade: false,
            if_exists: true,
        }))
    );
}
