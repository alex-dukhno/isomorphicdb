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
use data_definition_execution_plan::CreateIndexQuery;

fn create_index(index_name: &str, table_name: Vec<&str>, columns: Vec<&str>) -> sql_ast::Statement {
    sql_ast::Statement::CreateIndex {
        name: sql_ast::ObjectName(vec![ident(index_name)]),
        table_name: sql_ast::ObjectName(table_name.into_iter().map(ident).collect()),
        columns: columns
            .into_iter()
            .map(|name| sql_ast::OrderByExpr {
                expr: sql_ast::Expr::Identifier(ident(name)),
                asc: None,
                nulls_first: None,
            })
            .collect(),
        unique: true,
        if_not_exists: false,
    }
}

#[test]
fn create_index_for_not_existent_schema() {
    let analyzer = Analyzer::new(InMemoryDatabase::new());
    assert_eq!(
        analyzer.analyze(create_index("index_name", vec!["non_existent", TABLE], vec!["column"])),
        Err(AnalysisError::schema_does_not_exist(&"non_existent"))
    );
}

#[test]
fn create_index_for_not_existent_table() {
    let analyzer = Analyzer::new(InMemoryDatabase::new());
    assert_eq!(
        analyzer.analyze(create_index("index_name", vec!["non_existent"], vec!["column"])),
        Err(AnalysisError::table_does_not_exist(&"non_existent"))
    );
}

#[test]
fn create_index_for_table_with_unsupported_name() {
    let analyzer = Analyzer::new(InMemoryDatabase::new());
    assert_eq!(
        analyzer.analyze(create_index(
            "index_name",
            vec!["first_part", "second_part", "third_part", "fourth_part"],
            vec!["column"]
        )),
        Err(AnalysisError::table_naming_error(
            &"Unable to process table name 'first_part.second_part.third_part.fourth_part'"
        ))
    );
}

#[test]
fn create_index_over_column_that_does_not_exists_in_table() {
    let database = InMemoryDatabase::new();
    database
        .execute(create_table_ops(
            "public",
            TABLE,
            vec![("column", SqlType::small_int())],
        ))
        .unwrap();
    let analyzer = Analyzer::new(database);
    assert_eq!(
        analyzer.analyze(create_index("index_name", vec![TABLE], vec!["non_existent_column"])),
        Err(AnalysisError::column_not_found(&"non_existent_column"))
    );
}

#[test]
fn create_index_over_multiple_columns() {
    let database = InMemoryDatabase::new();
    database
        .execute(create_table_ops(
            "public",
            TABLE,
            vec![
                ("col_1", SqlType::small_int()),
                ("col_2", SqlType::small_int()),
                ("col_3", SqlType::small_int()),
            ],
        ))
        .unwrap();
    let analyzer = Analyzer::new(database);
    assert_eq!(
        analyzer.analyze(create_index("index_name", vec![TABLE], vec!["col_1", "col_2", "col_3"])),
        Ok(QueryAnalysis::DataDefinition(SchemaChange::CreateIndex(
            CreateIndexQuery {
                name: "index_name".to_owned(),
                full_table_name: FullTableName::from(TABLE),
                column_names: vec!["col_1".to_owned(), "col_2".to_owned(), "col_3".to_owned()]
            }
        )))
    );
}
