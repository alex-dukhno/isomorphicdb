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

use data_manipulation_untyped_tree::{UntypedItem, UntypedTree, UntypedValue};

use super::*;

#[test]
fn update_number() {
    let db = Database::new("");
    let transaction = db.transaction();
    let catalog = CatalogHandler::from(transaction.clone());
    catalog.apply(create_schema_ops(SCHEMA)).unwrap();
    catalog
        .apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::small_int())]))
        .unwrap();

    let analyzer = QueryAnalyzer::from(transaction);

    assert_eq!(
        analyzer.analyze(update_statement(SCHEMA, TABLE, vec![("col", Expr::Value(number(1)))])),
        Ok(UntypedQuery::Update(UntypedUpdateQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            assignments: vec![Some(UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))))],
            filter: None
        }))
    );
}

#[test]
fn update_string() {
    let db = Database::new("");
    let transaction = db.transaction();
    let catalog = CatalogHandler::from(transaction.clone());
    catalog.apply(create_schema_ops(SCHEMA)).unwrap();
    catalog.apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::char(5))])).unwrap();

    let analyzer = QueryAnalyzer::from(transaction);

    assert_eq!(
        analyzer.analyze(update_statement(SCHEMA, TABLE, vec![("col", string("str"))])),
        Ok(UntypedQuery::Update(UntypedUpdateQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            assignments: vec![Some(UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("str".to_owned()))))],
            filter: None
        }))
    );
}

#[test]
fn update_boolean() {
    let db = Database::new("");
    let transaction = db.transaction();
    let catalog = CatalogHandler::from(transaction.clone());
    catalog.apply(create_schema_ops(SCHEMA)).unwrap();
    catalog.apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())])).unwrap();

    let analyzer = QueryAnalyzer::from(transaction);

    assert_eq!(
        analyzer.analyze(update_statement(SCHEMA, TABLE, vec![("col", boolean(true))])),
        Ok(UntypedQuery::Update(UntypedUpdateQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            assignments: vec![Some(UntypedTree::UnOp {
                op: UnOperator::Cast(SqlType::Bool),
                item: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("t".to_owned()))))
            })],
            filter: None
        }))
    );
}

#[test]
fn update_null() {
    let db = Database::new("");
    let transaction = db.transaction();
    let catalog = CatalogHandler::from(transaction.clone());
    catalog.apply(create_schema_ops(SCHEMA)).unwrap();
    catalog.apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())])).unwrap();

    let analyzer = QueryAnalyzer::from(transaction);

    assert_eq!(
        analyzer.analyze(update_statement(SCHEMA, TABLE, vec![("col", null())])),
        Ok(UntypedQuery::Update(UntypedUpdateQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            assignments: vec![Some(UntypedTree::Item(UntypedItem::Const(UntypedValue::Null)))],
            filter: None
        }))
    );
}

#[test]
fn update_with_column_value() {
    let db = Database::new("");
    let transaction = db.transaction();
    let catalog = CatalogHandler::from(transaction.clone());
    catalog.apply(create_schema_ops(SCHEMA)).unwrap();
    catalog
        .apply(create_table_ops(
            SCHEMA,
            TABLE,
            vec![("col_1", SqlType::small_int()), ("col_2", SqlType::small_int())],
        ))
        .unwrap();

    let analyzer = QueryAnalyzer::from(transaction);

    assert_eq!(
        analyzer.analyze(update_statement(SCHEMA, TABLE, vec![("col_1", Expr::Column("col_2".to_owned()))])),
        Ok(UntypedQuery::Update(UntypedUpdateQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            assignments: vec![
                Some(UntypedTree::Item(UntypedItem::Column {
                    name: "col_2".to_owned(),
                    sql_type: SqlType::small_int(),
                    index: 1
                })),
                None
            ],
            filter: None
        }))
    );
}

#[test]
fn update_with_column_value_that_does_not_exists() {
    let db = Database::new("");
    let transaction = db.transaction();
    let catalog = CatalogHandler::from(transaction.clone());
    catalog.apply(create_schema_ops(SCHEMA)).unwrap();
    catalog
        .apply(create_table_ops(
            SCHEMA,
            TABLE,
            vec![("col_1", SqlType::small_int()), ("col_2", SqlType::small_int())],
        ))
        .unwrap();

    let analyzer = QueryAnalyzer::from(transaction);

    assert_eq!(
        analyzer.analyze(update_statement(SCHEMA, TABLE, vec![("col_1", Expr::Column("col_3".to_owned()))])),
        Err(AnalysisError::column_not_found(&"col_3"))
    );
}

#[test]
fn update_table_with_parameters() {
    let db = Database::new("");
    let transaction = db.transaction();
    let catalog = CatalogHandler::from(transaction.clone());
    catalog.apply(create_schema_ops(SCHEMA)).unwrap();
    catalog
        .apply(create_table_ops(
            SCHEMA,
            TABLE,
            vec![("col_1", SqlType::small_int()), ("col_2", SqlType::integer())],
        ))
        .unwrap();

    let analyzer = QueryAnalyzer::from(transaction);

    assert_eq!(
        analyzer.analyze(update_stmt_with_parameters(SCHEMA, TABLE)),
        Ok(UntypedQuery::Update(UntypedUpdateQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            assignments: vec![None, Some(UntypedTree::Item(UntypedItem::Param(0)))],
            filter: None
        }))
    );
}

#[cfg(test)]
mod multiple_values {
    use data_manipulation_untyped_tree::{UntypedItem, UntypedTree, UntypedValue};

    use super::*;

    fn update_value_as_expression_with_operation(left: Expr, op: BinaryOperator, right: Expr) -> Query {
        update_statement(
            SCHEMA,
            TABLE,
            vec![(
                "col",
                Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                },
            )],
        )
    }

    #[test]
    fn arithmetic() {
        let db = Database::new("");
        let transaction = db.transaction();
        let catalog = CatalogHandler::from(transaction.clone());
        catalog.apply(create_schema_ops(SCHEMA)).unwrap();
        catalog
            .apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::small_int())]))
            .unwrap();

        let analyzer = QueryAnalyzer::from(transaction);

        assert_eq!(
            analyzer.analyze(update_value_as_expression_with_operation(
                Expr::Value(number(1)),
                BinaryOperator::Plus,
                Expr::Value(number(1))
            )),
            Ok(UntypedQuery::Update(UntypedUpdateQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                assignments: vec![Some(UntypedTree::BiOp {
                    left: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1)))),
                    op: BiOperator::Arithmetic(BiArithmetic::Add),
                    right: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))))
                })],
                filter: None
            }))
        );
    }

    #[test]
    fn string_operation() {
        let db = Database::new("");
        let transaction = db.transaction();
        let catalog = CatalogHandler::from(transaction.clone());
        catalog.apply(create_schema_ops(SCHEMA)).unwrap();
        catalog
            .apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::var_char(255))]))
            .unwrap();

        let analyzer = QueryAnalyzer::from(transaction);

        assert_eq!(
            analyzer.analyze(update_value_as_expression_with_operation(
                string("str"),
                BinaryOperator::StringConcat,
                string("str")
            )),
            Ok(UntypedQuery::Update(UntypedUpdateQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                assignments: vec![Some(UntypedTree::BiOp {
                    left: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("str".to_owned())))),
                    op: BiOperator::StringOp(Concat),
                    right: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("str".to_owned()))))
                })],
                filter: None
            }))
        );
    }

    #[test]
    fn comparison() {
        let db = Database::new("");
        let transaction = db.transaction();
        let catalog = CatalogHandler::from(transaction.clone());
        catalog.apply(create_schema_ops(SCHEMA)).unwrap();
        catalog.apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())])).unwrap();

        let analyzer = QueryAnalyzer::from(transaction);

        assert_eq!(
            analyzer.analyze(update_value_as_expression_with_operation(
                Expr::Value(number(1)),
                BinaryOperator::Gt,
                Expr::Value(number(1))
            )),
            Ok(UntypedQuery::Update(UntypedUpdateQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                assignments: vec![Some(UntypedTree::BiOp {
                    left: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1)))),
                    op: BiOperator::Comparison(Comparison::Gt),
                    right: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))))
                })],
                filter: None
            }))
        );
    }

    #[test]
    fn logical() {
        let db = Database::new("");
        let transaction = db.transaction();
        let catalog = CatalogHandler::from(transaction.clone());
        catalog.apply(create_schema_ops(SCHEMA)).unwrap();
        catalog.apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())])).unwrap();

        let analyzer = QueryAnalyzer::from(transaction);

        assert_eq!(
            analyzer.analyze(update_value_as_expression_with_operation(
                boolean(true),
                BinaryOperator::And,
                boolean(true),
            )),
            Ok(UntypedQuery::Update(UntypedUpdateQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                assignments: vec![Some(UntypedTree::BiOp {
                    left: Box::new(UntypedTree::UnOp {
                        op: UnOperator::Cast(SqlType::Bool),
                        item: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("t".to_owned()))))
                    }),
                    op: BiOperator::Logical(BiLogical::And),
                    right: Box::new(UntypedTree::UnOp {
                        op: UnOperator::Cast(SqlType::Bool),
                        item: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("t".to_owned()))))
                    }),
                })],
                filter: None
            }))
        );
    }

    #[test]
    fn bitwise() {
        let db = Database::new("");
        let transaction = db.transaction();
        let catalog = CatalogHandler::from(transaction.clone());
        catalog.apply(create_schema_ops(SCHEMA)).unwrap();
        catalog
            .apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::small_int())]))
            .unwrap();

        let analyzer = QueryAnalyzer::from(transaction);

        assert_eq!(
            analyzer.analyze(update_value_as_expression_with_operation(
                Expr::Value(number(1)),
                BinaryOperator::BitwiseOr,
                Expr::Value(number(1))
            )),
            Ok(UntypedQuery::Update(UntypedUpdateQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                assignments: vec![Some(UntypedTree::BiOp {
                    left: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1)))),
                    op: BiOperator::Bitwise(Bitwise::Or),
                    right: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))))
                })],
                filter: None
            }))
        );
    }

    #[test]
    fn pattern_matching() {
        let db = Database::new("");
        let transaction = db.transaction();
        let catalog = CatalogHandler::from(transaction.clone());
        catalog.apply(create_schema_ops(SCHEMA)).unwrap();
        catalog.apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())])).unwrap();

        let analyzer = QueryAnalyzer::from(transaction);

        assert_eq!(
            analyzer.analyze(update_value_as_expression_with_operation(
                string("s"),
                BinaryOperator::Like,
                string("str")
            )),
            Ok(UntypedQuery::Update(UntypedUpdateQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                assignments: vec![Some(UntypedTree::BiOp {
                    left: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("s".to_owned())))),
                    op: BiOperator::Matching(Matching::Like),
                    right: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("str".to_owned()))))
                })],
                filter: None
            }))
        );
    }
}
