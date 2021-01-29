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

fn create_schema(schema_name: Vec<&'static str>) -> sql_ast::Statement {
    create_schema_if_not_exists(schema_name, false)
}

fn create_schema_if_not_exists(schema_name: Vec<&'static str>, if_not_exists: bool) -> sql_ast::Statement {
    sql_ast::Statement::CreateSchema {
        schema_name: sql_ast::ObjectName(schema_name.into_iter().map(ident).collect()),
        if_not_exists,
    }
}

#[test]
fn create_new_schema() {
    let analyzer = Analyzer::new(InMemoryDatabase::new());
    assert_eq!(
        analyzer.analyze(create_schema(vec![SCHEMA])),
        Ok(QueryAnalysis::DataDefinition(SchemaChange::CreateSchema(
            CreateSchemaQuery {
                schema_name: SchemaName::from(&SCHEMA),
                if_not_exists: false,
            }
        )))
    );
}

#[test]
fn create_new_schema_if_not_exists() {
    let analyzer = Analyzer::new(InMemoryDatabase::new());
    assert_eq!(
        analyzer.analyze(create_schema_if_not_exists(vec![SCHEMA], true)),
        Ok(QueryAnalysis::DataDefinition(SchemaChange::CreateSchema(
            CreateSchemaQuery {
                schema_name: SchemaName::from(&SCHEMA),
                if_not_exists: true,
            }
        )))
    );
}

#[test]
fn create_schema_with_the_same_name() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    let analyzer = Analyzer::new(database);
    assert_eq!(
        analyzer.analyze(create_schema(vec![SCHEMA])),
        Ok(QueryAnalysis::DataDefinition(SchemaChange::CreateSchema(
            CreateSchemaQuery {
                schema_name: SchemaName::from(&SCHEMA),
                if_not_exists: false,
            }
        )))
    );
}

#[test]
fn create_schema_with_unqualified_name() {
    let analyzer = Analyzer::new(InMemoryDatabase::new());
    assert_eq!(
        analyzer.analyze(create_schema(vec![
            "first_part",
            "second_part",
            "third_part",
            "fourth_part",
        ])),
        Err(AnalysisError::schema_naming_error(
            &"Only unqualified schema names are supported, 'first_part.second_part.third_part.fourth_part'"
        ))
    );
}
