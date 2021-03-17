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

fn delete_statement(schema_name: &str, table_name: &str) -> Query {
    Query::Delete(DeleteStatement {
        schema_name: schema_name.to_owned(),
        table_name: table_name.to_owned(),
        where_clause: None,
    })
}

#[test]
fn delete_from_table_that_in_nonexistent_schema() {
    let analyzer = QueryAnalyzer::new(InMemoryDatabase::new());

    assert_eq!(
        analyzer.analyze(delete_statement("non_existent_schema", TABLE)),
        Err(AnalysisError::schema_does_not_exist(&"non_existent_schema"))
    );
}

#[test]
fn delete_from_nonexistent_table() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    let analyzer = QueryAnalyzer::new(database);

    assert_eq!(
        analyzer.analyze(delete_statement(SCHEMA, "non_existent_table")),
        Err(AnalysisError::table_does_not_exist(format!(
            "{}.{}",
            SCHEMA, "non_existent_table"
        )))
    );
}

#[test]
fn delete_all_from_table() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_ops(SCHEMA, TABLE, vec![("col1", SqlType::integer())]))
        .unwrap();
    let analyzer = QueryAnalyzer::new(database);
    assert_eq!(
        analyzer.analyze(delete_statement(SCHEMA, TABLE)),
        Ok(UntypedQuery::Delete(UntypedDeleteQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
        }))
    );
}
