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
use description::{ProjectionItem, SelectStatement};
use sqlparser::ast::{ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins};

fn select_with_columns(name: ObjectName, projection: Vec<SelectItem>) -> Statement {
    Statement::Query(Box::new(Query {
        with: None,
        body: SetExpr::Select(Box::new(Select {
            distinct: false,
            top: None,
            projection,
            from: vec![TableWithJoins {
                relation: TableFactor::Table {
                    name,
                    alias: None,
                    args: vec![],
                    with_hints: vec![],
                },
                joins: vec![],
            }],
            selection: None,
            group_by: vec![],
            having: None,
        })),
        order_by: vec![],
        limit: None,
        offset: None,
        fetch: None,
    }))
}

fn select(name: ObjectName) -> Statement {
    select_with_columns(name, vec![SelectItem::Wildcard])
}

#[test]
fn select_from_table_that_in_nonexistent_schema() {
    let metadata = Arc::new(DataDefinition::in_memory());
    metadata.create_catalog(DEFAULT_CATALOG);
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&select(ObjectName(vec![ident("non_existent_schema"), ident(TABLE)])));
    assert_eq!(
        description,
        Err(DescriptionError::schema_does_not_exist(&"non_existent_schema"))
    );
}

#[test]
fn select_from_nonexistent_table() {
    let metadata = Arc::new(DataDefinition::in_memory());
    metadata.create_catalog(DEFAULT_CATALOG);
    metadata.create_schema(DEFAULT_CATALOG, SCHEMA);
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&select(ObjectName(vec![ident(SCHEMA), ident("non_existent_table")])));
    assert_eq!(
        description,
        Err(DescriptionError::table_does_not_exist(&format!(
            "{}.{}",
            SCHEMA, "non_existent_table"
        )))
    );
}

#[test]
fn select_from_table_with_unqualified_name() {
    let metadata = Arc::new(DataDefinition::in_memory());
    metadata.create_catalog(DEFAULT_CATALOG);
    metadata.create_schema(DEFAULT_CATALOG, SCHEMA);
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&select(ObjectName(vec![ident("only_schema_in_the_name")])));
    assert_eq!(
        description,
        Err(DescriptionError::syntax_error(
            &"Unsupported table name 'only_schema_in_the_name'. All table names must be qualified",
        ))
    );
}

#[test]
fn select_from_table_with_unsupported_name() {
    let metadata = Arc::new(DataDefinition::in_memory());
    metadata.create_catalog(DEFAULT_CATALOG);
    metadata.create_schema(DEFAULT_CATALOG, SCHEMA);
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&select(ObjectName(vec![
        ident("first_part"),
        ident("second_part"),
        ident("third_part"),
        ident("fourth_part"),
    ])));
    assert_eq!(
        description,
        Err(DescriptionError::syntax_error(
            &"Unable to process table name 'first_part.second_part.third_part.fourth_part'",
        ))
    );
}

#[test]
fn select_from_table() {
    let metadata = Arc::new(DataDefinition::in_memory());
    metadata.create_catalog(DEFAULT_CATALOG);
    metadata.create_schema(DEFAULT_CATALOG, SCHEMA);
    metadata.create_table(DEFAULT_CATALOG, SCHEMA, TABLE, &[]);
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&select(ObjectName(vec![ident(SCHEMA), ident(TABLE)])));
    assert_eq!(
        description,
        Ok(Description::Select(SelectStatement {
            full_table_id: FullTableId::from((0, 0)),
            projection_items: vec![],
        }))
    );
}

#[test]
fn select_from_table_with_column() {
    let metadata = Arc::new(DataDefinition::in_memory());
    metadata.create_catalog(DEFAULT_CATALOG);
    metadata.create_schema(DEFAULT_CATALOG, SCHEMA);
    metadata.create_table(
        DEFAULT_CATALOG,
        SCHEMA,
        TABLE,
        &[ColumnDefinition::new("col1", SqlType::Integer)],
    );
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&select_with_columns(
        ObjectName(vec![ident(SCHEMA), ident(TABLE)]),
        vec![SelectItem::UnnamedExpr(Expr::Identifier(ident("col1")))],
    ));
    assert_eq!(
        description,
        Ok(Description::Select(SelectStatement {
            full_table_id: FullTableId::from((0, 0)),
            projection_items: vec![ProjectionItem::Column(0, SqlType::Integer)],
        }))
    );
}

#[test]
#[ignore]
fn select_from_table_with_constant() {
    let metadata = Arc::new(DataDefinition::in_memory());
    metadata.create_catalog(DEFAULT_CATALOG);
    metadata.create_schema(DEFAULT_CATALOG, SCHEMA);
    metadata.create_table(
        DEFAULT_CATALOG,
        SCHEMA,
        TABLE,
        &[ColumnDefinition::new("col1", SqlType::Integer)],
    );
    let analyzer = Analyzer::new(metadata);
    let description = analyzer.describe(&select_with_columns(
        ObjectName(vec![ident(SCHEMA), ident(TABLE)]),
        vec![SelectItem::UnnamedExpr(Expr::Value(Value::Number(BigDecimal::from(1))))],
    ));
    assert_eq!(
        description,
        Ok(Description::Select(SelectStatement {
            full_table_id: FullTableId::from((0, 0)),
            projection_items: vec![ProjectionItem::Const(1)],
        }))
    );
}
