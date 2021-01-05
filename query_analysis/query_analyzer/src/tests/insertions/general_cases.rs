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

fn insert_statement(full_name: Vec<&'static str>) -> sql_ast::Statement {
    insert_with_values(full_name, vec![])
}

#[test]
fn schema_does_not_exist() {
    let analyzer = Analyzer::new(Arc::new(DatabaseHandle::in_memory()), InMemoryDatabase::new());

    assert_eq!(
        analyzer.analyze(insert_statement(vec![SCHEMA, TABLE])),
        Err(AnalysisError::schema_does_not_exist(&SCHEMA))
    );
}

#[test]
fn table_does_not_exist() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema(SCHEMA)).unwrap();
    let analyzer = Analyzer::new(Arc::new(DatabaseHandle::in_memory()), database);

    assert_eq!(
        analyzer.analyze(insert_statement(vec![SCHEMA, TABLE])),
        Err(AnalysisError::table_does_not_exist(format!("{}.{}", SCHEMA, TABLE)))
    );
}

#[test]
fn table_with_unqualified_name() {
    let analyzer = Analyzer::new(Arc::new(DatabaseHandle::in_memory()), InMemoryDatabase::new());
    assert_eq!(
        analyzer.analyze(insert_statement(vec!["only_schema_in_the_name"])),
        Err(AnalysisError::table_naming_error(
            &"Unsupported table name 'only_schema_in_the_name'. All table names must be qualified",
        ))
    );
}

#[test]
fn table_with_unsupported_name() {
    let analyzer = Analyzer::new(Arc::new(DatabaseHandle::in_memory()), InMemoryDatabase::new());
    assert_eq!(
        analyzer.analyze(insert_statement(vec![
            "first_part",
            "second_part",
            "third_part",
            "fourth_part",
        ])),
        Err(AnalysisError::table_naming_error(
            &"Unable to process table name 'first_part.second_part.third_part.fourth_part'",
        ))
    );
}

#[test]
fn with_column_names() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema(SCHEMA)).unwrap();
    database
        .execute(create_table(SCHEMA, TABLE, vec![("col", SqlType::SmallInt)]))
        .unwrap();
    let analyzer = Analyzer::new(Arc::new(DatabaseHandle::in_memory()), database);

    assert_eq!(
        analyzer.analyze(inner_insert(vec![SCHEMA, TABLE], vec![vec![small_int(1)]], vec!["col"])),
        Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_types: vec![SqlType::SmallInt],
            values: vec![vec![InsertTreeNode::Item(Operator::Const(ScalarValue::Number(
                BigDecimal::from(1)
            )))]],
        })))
    );
}

#[test]
fn column_not_found() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema(SCHEMA)).unwrap();
    database
        .execute(create_table(SCHEMA, TABLE, vec![("col", SqlType::SmallInt)]))
        .unwrap();
    let analyzer = Analyzer::new(Arc::new(DatabaseHandle::in_memory()), database);

    assert_eq!(
        analyzer.analyze(inner_insert(
            vec![SCHEMA, TABLE],
            vec![vec![small_int(1)]],
            vec!["not_found"]
        )),
        Err(AnalysisError::column_not_found(&"not_found"))
    );
}
