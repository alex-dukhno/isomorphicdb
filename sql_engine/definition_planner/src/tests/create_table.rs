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
use query_ast::DataType;

fn column(name: &str, data_type: DataType) -> ColumnDef {
    ColumnDef {
        name: name.to_owned(),
        data_type,
    }
}

#[test]
fn create_table_with_nonexistent_schema() {
    let db = Database::new("");
    let transaction = db.transaction();

    let planner = DefinitionPlanner::from(transaction);
    assert_eq!(
        planner.plan(create_table("non_existent_schema", "non_existent_table", vec![])),
        Err(SchemaPlanError::schema_does_not_exist(&"non_existent_schema"))
    );
}

#[test]
fn create_table_with_the_same_name() {
    let db = Database::new("");
    let transaction = db.transaction();
    let catalog = CatalogHandler::from(transaction.clone());
    catalog.apply(create_schema_ops(SCHEMA)).unwrap();
    catalog.apply(create_table_ops(SCHEMA, TABLE, vec![])).unwrap();

    let planner = DefinitionPlanner::from(transaction);

    assert_eq!(
        planner.plan(create_table(SCHEMA, TABLE, vec![])),
        Ok(SchemaChange::CreateTable(CreateTableQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_defs: vec![],
            if_not_exists: false,
        }))
    );
}

#[test]
fn create_new_table_if_not_exist() {
    let db = Database::new("");
    let transaction = db.transaction();
    let catalog = CatalogHandler::from(transaction.clone());
    catalog.apply(create_schema_ops(SCHEMA)).unwrap();

    let planner = DefinitionPlanner::from(transaction);
    assert_eq!(
        planner.plan(create_table_if_not_exists(
            SCHEMA,
            TABLE,
            vec![column("column_name", DataType::SmallInt)],
            true
        )),
        Ok(SchemaChange::CreateTable(CreateTableQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_defs: vec![ColumnInfo {
                name: "column_name".to_owned(),
                sql_type: SqlTypeOld::small_int()
            }],
            if_not_exists: true,
        }))
    );
}

#[test]
fn successfully_create_table() {
    let db = Database::new("");
    let transaction = db.transaction();
    let catalog = CatalogHandler::from(transaction.clone());
    catalog.apply(create_schema_ops(SCHEMA)).unwrap();

    let planner = DefinitionPlanner::from(transaction);
    assert_eq!(
        planner.plan(create_table(SCHEMA, TABLE, vec![column("column_name", DataType::SmallInt)],)),
        Ok(SchemaChange::CreateTable(CreateTableQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_defs: vec![ColumnInfo {
                name: "column_name".to_owned(),
                sql_type: SqlTypeOld::small_int()
            }],
            if_not_exists: false,
        }))
    );
}
