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
use bigdecimal::BigDecimal;
use sql_ast::BinaryOperator;

fn update_stmt<S: ToString>(schema: S, table: S) -> Statement {
    Statement::Update {
        table_name: ObjectName(vec![ident(schema), ident(table)]),
        assignments: vec![Assignment {
            id: ident("col_1"),
            value: Expr::Value(Value::Number(BigDecimal::from(1))),
        }],
        selection: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(ident("col_2"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(Value::Number(BigDecimal::from(2)))),
        }),
    }
}

fn update_stmt_with_parameters<S: ToString>(schema: S, table: S) -> Statement {
    Statement::Update {
        table_name: ObjectName(vec![ident(schema), ident(table)]),
        assignments: vec![Assignment {
            id: ident("col_1"),
            value: Expr::Identifier(ident("$1")),
        }],
        selection: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier(ident("col_2"))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Identifier(ident("$2"))),
        }),
    }
}

#[test]
fn update_table_under_non_existing_schema() {
    let metadata = Arc::new(DatabaseHandle::in_memory());
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&update_stmt("non_existent_schema", "non_existent_table"));

    assert_eq!(
        description,
        Err(DescriptionError::schema_does_not_exist(&"non_existent_schema"))
    )
}

#[test]
fn update_non_existing_table() {
    let metadata = Arc::new(DatabaseHandle::in_memory());
    metadata.create_schema(SCHEMA).expect("schema created");
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&update_stmt(SCHEMA, "non_existent"));

    assert_eq!(
        description,
        Err(DescriptionError::table_does_not_exist(&format!(
            "{}.{}",
            SCHEMA, "non_existent"
        )))
    );
}

#[test]
fn update_table_with_non_existing_columns() {
    let metadata = Arc::new(DatabaseHandle::in_memory());
    let schema_id = metadata.create_schema(SCHEMA).expect("schema created");
    let table_id = metadata.create_table(schema_id, TABLE, &[]).expect("table created");
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&update_stmt(SCHEMA, TABLE));

    assert_eq!(
        description,
        Ok(Description::Update(UpdateStatement {
            table_id: FullTableId::from((schema_id, table_id)),
            param_count: 0,
            param_types: ParamTypes::new(),
        }))
    );
}

#[test]
fn update_table_with_specified_columns() {
    let metadata = Arc::new(DatabaseHandle::in_memory());
    let schema_id = metadata.create_schema(SCHEMA).expect("schema created");
    let table_id = metadata
        .create_table(schema_id, TABLE, &[ColumnDefinition::new("col", SqlType::SmallInt)])
        .expect("table created");
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&update_stmt(SCHEMA, TABLE));

    assert_eq!(
        description,
        Ok(Description::Update(UpdateStatement {
            table_id: FullTableId::from((schema_id, table_id)),
            param_count: 0,
            param_types: ParamTypes::new(),
        }))
    );
}

#[test]
fn update_table_with_parameters() {
    let metadata = Arc::new(DatabaseHandle::in_memory());
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
    let description = analyzer.describe(&update_stmt_with_parameters(SCHEMA, TABLE));
    let mut param_types = ParamTypes::new();
    param_types.insert(0, SqlType::SmallInt);
    param_types.insert(1, SqlType::SmallInt);

    assert_eq!(
        description,
        Ok(Description::Update(UpdateStatement {
            table_id: FullTableId::from((schema_id, table_id)),
            param_count: 2,
            param_types,
        }))
    );
}
