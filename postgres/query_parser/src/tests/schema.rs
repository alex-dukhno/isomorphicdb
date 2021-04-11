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

#[test]
fn create_schema() {
    let statements = QUERY_PARSER.parse("create schema schema_name;");

    assert_eq!(
        statements,
        Ok(vec![Statement::Definition(Definition::CreateSchema {
            schema_name: "schema_name".to_owned(),
            if_not_exists: false
        })])
    );
}

#[test]
fn create_schema_if_not_exists() {
    let statements = QUERY_PARSER.parse("create schema if not exists schema_name;");

    assert_eq!(
        statements,
        Ok(vec![Statement::Definition(Definition::CreateSchema {
            schema_name: "schema_name".to_owned(),
            if_not_exists: true
        })])
    );
}

#[test]
fn drop_schema() {
    let statements = QUERY_PARSER.parse("drop schema schema_name;");

    assert_eq!(
        statements,
        Ok(vec![Statement::Definition(Definition::DropSchemas {
            names: vec!["schema_name".to_owned()],
            if_exists: false,
            cascade: false
        })])
    );
}

#[test]
fn drop_schemas() {
    let statements = QUERY_PARSER.parse("drop schema schema_name_1, schema_name_2;");

    assert_eq!(
        statements,
        Ok(vec![Statement::Definition(Definition::DropSchemas {
            names: vec!["schema_name_1".to_owned(), "schema_name_2".to_owned()],
            if_exists: false,
            cascade: false
        })])
    );
}

#[test]
fn drop_schemas_cascade() {
    let statements = QUERY_PARSER.parse("drop schema schema_name_1, schema_name_2 cascade;");

    assert_eq!(
        statements,
        Ok(vec![Statement::Definition(Definition::DropSchemas {
            names: vec!["schema_name_1".to_owned(), "schema_name_2".to_owned()],
            if_exists: false,
            cascade: true
        })])
    );
}

#[test]
fn drop_schema_if_exists() {
    let statements = QUERY_PARSER.parse("drop schema if exists schema_name;");

    assert_eq!(
        statements,
        Ok(vec![Statement::Definition(Definition::DropSchemas {
            names: vec!["schema_name".to_owned()],
            if_exists: true,
            cascade: false
        })])
    );
}
