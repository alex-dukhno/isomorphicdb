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
fn schema_does_not_exist() {
    let analyzer = QueryAnalyzer::new(InMemoryDatabase::new());

    assert_eq!(
        analyzer.analyze(update_statement("non_existent_schema", "non_existent_table", vec![])),
        Err(AnalysisError::schema_does_not_exist(&"non_existent_schema"))
    )
}

#[test]
fn table_does_not_exist() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    let analyzer = QueryAnalyzer::new(database);

    assert_eq!(
        analyzer.analyze(update_statement(SCHEMA, "non_existent", vec![])),
        Err(AnalysisError::table_does_not_exist(format!(
            "{}.{}",
            SCHEMA, "non_existent"
        )))
    );
}
