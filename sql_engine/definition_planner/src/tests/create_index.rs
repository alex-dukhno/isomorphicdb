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
use data_definition_execution_plan::CreateIndexQuery;

const DEFAULT_SCHEMA: &str = "public";

fn create_index(index_name: &str, schema_name: &str, table_name: &str, columns: Vec<&str>) -> Definition {
    Definition::CreateIndex {
        name: index_name.to_owned(),
        schema_name: schema_name.to_owned(),
        table_name: table_name.to_owned(),
        column_names: columns.into_iter().map(ToOwned::to_owned).collect(),
    }
}

#[test]
fn create_index_for_not_existent_schema() {
    let db = Database::new("");
    let planner = DefinitionPlanner::from(db.transaction());
    assert_eq!(
        planner.plan(create_index("index_name", "non_existent", TABLE, vec!["column"])),
        Err(SchemaPlanError::schema_does_not_exist(&"non_existent"))
    );
}

#[test]
fn create_index_for_not_existent_table() {
    let db = Database::new("");
    let planner = DefinitionPlanner::from(db.transaction());
    assert_eq!(
        planner.plan(create_index("index_name", DEFAULT_SCHEMA, "non_existent", vec!["column"])),
        Err(SchemaPlanError::table_does_not_exist(&format!("{}.{}", DEFAULT_SCHEMA, "non_existent")))
    );
}

#[test]
fn create_index_over_column_that_does_not_exists_in_table() {
    let db = Database::new("");
    let transaction = db.transaction();
    let catalog = CatalogHandler::from(transaction.clone());
    catalog
        .apply(create_table_ops("public", TABLE, vec![("column", SqlTypeOld::small_int())]))
        .unwrap();

    let planner = DefinitionPlanner::from(transaction);
    assert_eq!(
        planner.plan(create_index("index_name", DEFAULT_SCHEMA, TABLE, vec!["non_existent_column"])),
        Err(SchemaPlanError::column_not_found(&"non_existent_column"))
    );
}

#[test]
fn create_index_over_multiple_columns() {
    let db = Database::new("");
    let transaction = db.transaction();
    let catalog = CatalogHandler::from(transaction.clone());
    catalog
        .apply(create_table_ops(
            "public",
            TABLE,
            vec![
                ("col_1", SqlTypeOld::small_int()),
                ("col_2", SqlTypeOld::small_int()),
                ("col_3", SqlTypeOld::small_int()),
            ],
        ))
        .unwrap();

    let planner = DefinitionPlanner::from(transaction);
    assert_eq!(
        planner.plan(create_index("index_name", DEFAULT_SCHEMA, TABLE, vec!["col_1", "col_2", "col_3"])),
        Ok(SchemaChange::CreateIndex(CreateIndexQuery {
            name: "index_name".to_owned(),
            full_table_name: FullTableName::from((&DEFAULT_SCHEMA, &TABLE)),
            column_names: vec!["col_1".to_owned(), "col_2".to_owned(), "col_3".to_owned()]
        }))
    );
}
