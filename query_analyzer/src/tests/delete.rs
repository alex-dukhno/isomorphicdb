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

fn delete_statement(table_name: Vec<&'static str>) -> sql_ast::Statement {
    sql_ast::Statement::Delete {
        table_name: sql_ast::ObjectName(table_name.into_iter().map(ident).collect()),
        selection: None,
    }
}

#[test]
fn delete_from_table_that_in_nonexistent_schema() {
    let analyzer = Analyzer::new(InMemoryDatabase::new());

    assert_eq!(
        analyzer.analyze(delete_statement(vec!["non_existent_schema", TABLE])),
        Err(AnalysisError::schema_does_not_exist(&"non_existent_schema"))
    );
}

#[test]
fn delete_from_nonexistent_table() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    let analyzer = Analyzer::new(database);

    assert_eq!(
        analyzer.analyze(delete_statement(vec![SCHEMA, "non_existent_table"])),
        Err(AnalysisError::table_does_not_exist(format!(
            "{}.{}",
            SCHEMA, "non_existent_table"
        )))
    );
}

#[test]
fn delete_from_table_with_unqualified_name() {
    let analyzer = Analyzer::new(InMemoryDatabase::new());
    assert_eq!(
        analyzer.analyze(delete_statement(vec!["only_schema_in_the_name"])),
        Err(AnalysisError::table_naming_error(
            &"Unsupported table name 'only_schema_in_the_name'. All table names must be qualified",
        ))
    );
}

#[test]
fn delete_from_table_with_unsupported_name() {
    let analyzer = Analyzer::new(InMemoryDatabase::new());
    assert_eq!(
        analyzer.analyze(delete_statement(vec![
            "first_part",
            "second_part",
            "third_part",
            "fourth_part"
        ])),
        Err(AnalysisError::table_naming_error(
            &"Unable to process table name 'first_part.second_part.third_part.fourth_part'",
        ))
    );
}

#[test]
fn delete_all_from_table() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_ops(SCHEMA, TABLE, vec![("col1", SqlType::integer())]))
        .unwrap();
    let analyzer = Analyzer::new(database);
    assert_eq!(
        analyzer.analyze(sql_ast::Statement::Delete {
            table_name: sql_ast::ObjectName(vec![ident(SCHEMA), ident(TABLE)]),
            selection: None
        }),
        Ok(QueryAnalysis::Write(UntypedWrite::Delete(DeleteQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
        })))
    );
}
