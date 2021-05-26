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

fn insert_statement(schema_name: &str, table_name: &str) -> Query {
    insert_with_values(schema_name, table_name, vec![])
}

#[test]
fn schema_does_not_exist() {
    let db = Database::new("");
    let transaction = db.transaction();

    let analyzer = QueryAnalyzer::from(transaction);

    assert_eq!(
        analyzer.analyze(insert_statement(SCHEMA, TABLE)),
        Err(AnalysisError::schema_does_not_exist(&SCHEMA))
    );
}

#[test]
fn table_does_not_exist() {
    let db = Database::new("");
    let transaction = db.transaction();
    let catalog = CatalogHandler::from(transaction.clone());
    catalog.apply(create_schema_ops(SCHEMA)).unwrap();

    let analyzer = QueryAnalyzer::from(transaction);

    assert_eq!(
        analyzer.analyze(insert_statement(SCHEMA, TABLE)),
        Err(AnalysisError::table_does_not_exist(format!("{}.{}", SCHEMA, TABLE)))
    );
}

#[test]
fn with_column_names() {
    let db = Database::new("");
    let transaction = db.transaction();
    let catalog = CatalogHandler::from(transaction.clone());
    catalog.apply(create_schema_ops(SCHEMA)).unwrap();
    catalog
        .apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::small_int())]))
        .unwrap();

    let analyzer = QueryAnalyzer::from(transaction);

    assert_eq!(
        analyzer.analyze(inner_insert(SCHEMA, TABLE, vec![vec![small_int(100)]], vec!["col"])),
        Ok(UntypedQuery::Insert(UntypedInsertQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            values: vec![vec![Some(UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(100))))]],
        }))
    );
}

#[test]
fn column_not_found() {
    let db = Database::new("");
    let transaction = db.transaction();
    let catalog = CatalogHandler::from(transaction.clone());
    catalog.apply(create_schema_ops(SCHEMA)).unwrap();
    catalog
        .apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::small_int())]))
        .unwrap();

    let analyzer = QueryAnalyzer::from(transaction);

    assert_eq!(
        analyzer.analyze(inner_insert(SCHEMA, TABLE, vec![vec![small_int(1)]], vec!["not_found"])),
        Err(AnalysisError::column_not_found(&"not_found"))
    );
}
