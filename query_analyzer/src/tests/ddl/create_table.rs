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

fn column(name: &str, data_type: sql_ast::DataType) -> sql_ast::ColumnDef {
    sql_ast::ColumnDef {
        name: ident(name),
        data_type,
        collation: None,
        options: vec![],
    }
}

fn create_table_if_not_exists(
    name: Vec<&str>,
    columns: Vec<sql_ast::ColumnDef>,
    if_not_exists: bool,
) -> sql_ast::Statement {
    sql_ast::Statement::CreateTable {
        or_replace: false,
        name: sql_ast::ObjectName(name.into_iter().map(ident).collect()),
        columns,
        constraints: vec![],
        with_options: vec![],
        if_not_exists,
        external: false,
        file_format: None,
        location: None,
        query: None,
        without_rowid: false,
    }
}

fn create_table(name: Vec<&str>, columns: Vec<sql_ast::ColumnDef>) -> sql_ast::Statement {
    create_table_if_not_exists(name, columns, false)
}

#[test]
fn create_table_with_nonexistent_schema() {
    let analyzer = Analyzer::new(InMemoryDatabase::new());

    assert_eq!(
        analyzer.analyze(create_table(vec!["non_existent_schema", "non_existent_table"], vec![])),
        Err(AnalysisError::schema_does_not_exist(&"non_existent_schema"))
    );
}

#[test]
fn create_table_with_unqualified_name() {
    let analyzer = Analyzer::new(InMemoryDatabase::new());

    assert_eq!(
        analyzer.analyze(create_table(vec!["only_table_in_the_name"], vec![])),
        Ok(QueryAnalysis::DataDefinition(SchemaChange::CreateTable(
            CreateTableQuery {
                full_table_name: FullTableName::from((&"public", &"only_table_in_the_name")),
                column_defs: vec![],
                if_not_exists: false
            }
        )))
    );
}

#[test]
fn create_table_with_unsupported_name() {
    let database = InMemoryDatabase::new();
    database.execute_new(create_schema_ops(SCHEMA)).unwrap();
    let analyzer = Analyzer::new(database);

    assert_eq!(
        analyzer.analyze(create_table(
            vec!["first_part", "second_part", "third_part", "fourth_part"],
            vec![],
        )),
        Err(AnalysisError::table_naming_error(
            &"Unable to process table name 'first_part.second_part.third_part.fourth_part'"
        ))
    );
}

#[test]
fn create_table_with_unsupported_column_type() {
    let database = InMemoryDatabase::new();
    database.execute_new(create_schema_ops(SCHEMA)).unwrap();
    let analyzer = Analyzer::new(database);

    assert_eq!(
        analyzer.analyze(create_table(
            vec![SCHEMA, TABLE],
            vec![column(
                "column_name",
                sql_ast::DataType::Custom(sql_ast::ObjectName(vec![ident("strange_type_name_whatever")])),
            )],
        )),
        Err(AnalysisError::type_is_not_supported(&"strange_type_name_whatever"))
    );
}

#[test]
fn create_table_with_the_same_name() {
    let database = InMemoryDatabase::new();
    database.execute_new(create_schema_ops(SCHEMA)).unwrap();
    database.execute_new(create_table_ops(SCHEMA, TABLE, vec![])).unwrap();
    let analyzer = Analyzer::new(database);

    assert_eq!(
        analyzer.analyze(create_table(vec![SCHEMA, TABLE], vec![])),
        Ok(QueryAnalysis::DataDefinition(SchemaChange::CreateTable(
            CreateTableQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                column_defs: vec![],
                if_not_exists: false,
            }
        )))
    );
}

#[test]
fn create_new_table_if_not_exist() {
    let database = InMemoryDatabase::new();
    database.execute_new(create_schema_ops(SCHEMA)).unwrap();
    let analyzer = Analyzer::new(database);
    assert_eq!(
        analyzer.analyze(create_table_if_not_exists(
            vec![SCHEMA, TABLE],
            vec![column("column_name", sql_ast::DataType::SmallInt)],
            true
        )),
        Ok(QueryAnalysis::DataDefinition(SchemaChange::CreateTable(
            CreateTableQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                column_defs: vec![ColumnInfo {
                    name: "column_name".to_owned(),
                    sql_type: SqlType::small_int()
                }],
                if_not_exists: true,
            }
        )))
    );
}

#[test]
fn successfully_create_table() {
    let database = InMemoryDatabase::new();
    database.execute_new(create_schema_ops(SCHEMA)).unwrap();
    let analyzer = Analyzer::new(database);
    assert_eq!(
        analyzer.analyze(create_table(
            vec![SCHEMA, TABLE],
            vec![column("column_name", sql_ast::DataType::SmallInt)],
        )),
        Ok(QueryAnalysis::DataDefinition(SchemaChange::CreateTable(
            CreateTableQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                column_defs: vec![ColumnInfo {
                    name: "column_name".to_owned(),
                    sql_type: SqlType::small_int()
                }],
                if_not_exists: false,
            }
        )))
    );
}
