// Copyright 2020 Alex Dukhno
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
use description::ParamIndex;

fn insert_stmt_with_values<S: ToString>(schema: S, table: S, values: Vec<&'static str>) -> Statement {
    Statement::Insert {
        table_name: ObjectName(vec![ident(schema), ident(table)]),
        columns: vec![],
        source: Box::new(Query {
            with: None,
            body: SetExpr::Values(Values(vec![values
                .into_iter()
                .map(|s| Expr::Value(Value::Number(s.parse().unwrap())))
                .collect()])),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        }),
    }
}

fn insert_statement<S: ToString>(schema: S, table: S) -> Statement {
    insert_stmt_with_values(schema, table, vec![])
}

fn insert_stmt_with_parameters<S: ToString>(schema: S, table: S, param_indexes: Vec<ParamIndex>) -> Statement {
    Statement::Insert {
        table_name: ObjectName(vec![ident(schema), ident(table)]),
        columns: vec![],
        source: Box::new(Query {
            with: None,
            body: SetExpr::Values(Values(vec![param_indexes
                .into_iter()
                .map(|i| {
                    Expr::Identifier(Ident {
                        value: format!("${}", i + 1),
                        quote_style: None,
                    })
                })
                .collect()])),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        }),
    }
}

#[test]
fn insert_into_table_under_non_existing_schema() {
    let metadata = Arc::new(DataManager::in_memory());
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&insert_statement("non_existent_schema", "non_existent_table"));

    assert_eq!(
        description,
        Err(DescriptionError::schema_does_not_exist(&"non_existent_schema"))
    )
}

#[test]
fn insert_into_non_existing_table() {
    let metadata = Arc::new(DataManager::in_memory());
    metadata.create_schema(SCHEMA).expect("schema created");
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&insert_statement(SCHEMA, "non_existent"));

    assert_eq!(
        description,
        Err(DescriptionError::table_does_not_exist(&format!(
            "{}.{}",
            SCHEMA, "non_existent"
        )))
    );
}

#[test]
fn insert_into_existing_table_without_columns() {
    let metadata = Arc::new(DataManager::in_memory());
    let schema_id = metadata.create_schema(SCHEMA).expect("schema created");
    let table_id = metadata.create_table(schema_id, TABLE, &[]).expect("table created");
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&insert_statement(SCHEMA, TABLE));

    assert_eq!(
        description,
        Ok(Description::Insert(InsertStatement {
            table_id: FullTableId::from((schema_id, table_id)),
            param_count: 0,
            param_types: ParamTypes::new(),
        }))
    );
}

#[test]
fn insert_into_existing_table_with_column() {
    let metadata = Arc::new(DataManager::in_memory());
    let schema_id = metadata.create_schema(SCHEMA).expect("schema created");
    let table_id = metadata
        .create_table(schema_id, TABLE, &[ColumnDefinition::new("col", SqlType::SmallInt)])
        .expect("table created");
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&insert_stmt_with_values(SCHEMA, TABLE, vec!["1"]));

    assert_eq!(
        description,
        Ok(Description::Insert(InsertStatement {
            table_id: FullTableId::from((schema_id, table_id)),
            param_count: 0,
            param_types: ParamTypes::new(),
        }))
    );
}

#[test]
fn insert_into_table_with_parameters() {
    let metadata = Arc::new(DataManager::in_memory());
    let schema_id = metadata.create_schema(SCHEMA).expect("schema created");
    let table_id = metadata
        .create_table(
            schema_id,
            TABLE,
            &[
                ColumnDefinition::new("col_1", SqlType::SmallInt),
                ColumnDefinition::new("col_2", SqlType::SmallInt),
            ],
        )
        .expect("table created");
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&insert_stmt_with_parameters(SCHEMA, TABLE, vec![0, 9]));
    let mut param_types = ParamTypes::new();
    param_types.insert(0, SqlType::SmallInt);
    param_types.insert(9, SqlType::SmallInt);

    assert_eq!(
        description,
        Ok(Description::Insert(InsertStatement {
            table_id: FullTableId::from((schema_id, table_id)),
            param_count: 10,
            param_types,
        }))
    );
}
