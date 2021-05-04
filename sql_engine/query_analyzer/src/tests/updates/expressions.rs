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

use data_manipulation_untyped_tree::{DynamicUntypedItem, DynamicUntypedTree, UntypedValue};

use super::*;

#[test]
fn update_number() -> TransactionResult<()> {
    Database::new("").old_transaction(|db| {
        let catalog = CatalogHandlerOld::from(db.clone());
        catalog.apply(create_schema_ops(SCHEMA)).unwrap();
        catalog
            .apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::small_int())]))
            .unwrap();
        let analyzer = QueryAnalyzerOld::from(db);

        assert_eq!(
            analyzer.analyze(update_statement(SCHEMA, TABLE, vec![("col", Expr::Value(number(1)))])),
            Ok(UntypedQuery::Update(UntypedUpdateQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                assignments: vec![Some(DynamicUntypedTree::Item(DynamicUntypedItem::Const(
                    UntypedValue::Number(BigDecimal::from(1))
                )))],
                filter: None
            }))
        );
        Ok(())
    })
}

#[test]
fn update_string() -> TransactionResult<()> {
    Database::new("").old_transaction(|db| {
        let catalog = CatalogHandlerOld::from(db.clone());
        catalog.apply(create_schema_ops(SCHEMA)).unwrap();
        catalog
            .apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::char(5))]))
            .unwrap();
        let analyzer = QueryAnalyzerOld::from(db);

        assert_eq!(
            analyzer.analyze(update_statement(SCHEMA, TABLE, vec![("col", string("str"))])),
            Ok(UntypedQuery::Update(UntypedUpdateQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                assignments: vec![Some(DynamicUntypedTree::Item(DynamicUntypedItem::Const(
                    UntypedValue::String("str".to_owned())
                )))],
                filter: None
            }))
        );
        Ok(())
    })
}

#[test]
fn update_boolean() -> TransactionResult<()> {
    Database::new("").old_transaction(|db| {
        let catalog = CatalogHandlerOld::from(db.clone());
        catalog.apply(create_schema_ops(SCHEMA)).unwrap();
        catalog
            .apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())]))
            .unwrap();
        let analyzer = QueryAnalyzerOld::from(db);

        assert_eq!(
            analyzer.analyze(update_statement(SCHEMA, TABLE, vec![("col", boolean(true))])),
            Ok(UntypedQuery::Update(UntypedUpdateQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                assignments: vec![Some(DynamicUntypedTree::Item(DynamicUntypedItem::Const(
                    UntypedValue::Bool(Bool(true))
                )))],
                filter: None
            }))
        );
        Ok(())
    })
}

#[test]
fn update_null() -> TransactionResult<()> {
    Database::new("").old_transaction(|db| {
        let catalog = CatalogHandlerOld::from(db.clone());
        catalog.apply(create_schema_ops(SCHEMA)).unwrap();
        catalog
            .apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())]))
            .unwrap();
        let analyzer = QueryAnalyzerOld::from(db);

        assert_eq!(
            analyzer.analyze(update_statement(SCHEMA, TABLE, vec![("col", null())])),
            Ok(UntypedQuery::Update(UntypedUpdateQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                assignments: vec![Some(DynamicUntypedTree::Item(DynamicUntypedItem::Const(
                    UntypedValue::Null
                )))],
                filter: None
            }))
        );
        Ok(())
    })
}

#[test]
fn update_with_column_value() -> TransactionResult<()> {
    Database::new("").old_transaction(|db| {
        let catalog = CatalogHandlerOld::from(db.clone());
        catalog.apply(create_schema_ops(SCHEMA)).unwrap();
        catalog
            .apply(create_table_ops(
                SCHEMA,
                TABLE,
                vec![("col_1", SqlType::small_int()), ("col_2", SqlType::small_int())],
            ))
            .unwrap();
        let analyzer = QueryAnalyzerOld::from(db);

        assert_eq!(
            analyzer.analyze(update_statement(
                SCHEMA,
                TABLE,
                vec![("col_1", Expr::Column("col_2".to_owned()))]
            )),
            Ok(UntypedQuery::Update(UntypedUpdateQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                assignments: vec![
                    Some(DynamicUntypedTree::Item(DynamicUntypedItem::Column {
                        name: "col_2".to_owned(),
                        sql_type: SqlType::small_int(),
                        index: 1
                    })),
                    None
                ],
                filter: None
            }))
        );
        Ok(())
    })
}

#[test]
fn update_with_column_value_that_does_not_exists() -> TransactionResult<()> {
    Database::new("").old_transaction(|db| {
        let catalog = CatalogHandlerOld::from(db.clone());
        catalog.apply(create_schema_ops(SCHEMA)).unwrap();
        catalog
            .apply(create_table_ops(
                SCHEMA,
                TABLE,
                vec![("col_1", SqlType::small_int()), ("col_2", SqlType::small_int())],
            ))
            .unwrap();
        let analyzer = QueryAnalyzerOld::from(db);

        assert_eq!(
            analyzer.analyze(update_statement(
                SCHEMA,
                TABLE,
                vec![("col_1", Expr::Column("col_3".to_owned()))]
            )),
            Err(AnalysisError::column_not_found(&"col_3"))
        );
        Ok(())
    })
}

#[test]
fn update_table_with_parameters() -> TransactionResult<()> {
    Database::new("").old_transaction(|db| {
        let catalog = CatalogHandlerOld::from(db.clone());
        catalog.apply(create_schema_ops(SCHEMA)).unwrap();
        catalog
            .apply(create_table_ops(
                SCHEMA,
                TABLE,
                vec![("col_1", SqlType::small_int()), ("col_2", SqlType::integer())],
            ))
            .unwrap();
        let analyzer = QueryAnalyzerOld::from(db);

        assert_eq!(
            analyzer.analyze(update_stmt_with_parameters(SCHEMA, TABLE)),
            Ok(UntypedQuery::Update(UntypedUpdateQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                assignments: vec![None, Some(DynamicUntypedTree::Item(DynamicUntypedItem::Param(0)))],
                filter: None
            }))
        );
        Ok(())
    })
}

#[cfg(test)]
mod multiple_values {
    use data_manipulation_untyped_tree::{DynamicUntypedItem, DynamicUntypedTree, UntypedValue};

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
    fn arithmetic() -> TransactionResult<()> {
        Database::new("").old_transaction(|db| {
            let catalog = CatalogHandlerOld::from(db.clone());
            catalog.apply(create_schema_ops(SCHEMA)).unwrap();
            catalog
                .apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::small_int())]))
                .unwrap();
            let analyzer = QueryAnalyzerOld::from(db);

            assert_eq!(
                analyzer.analyze(update_value_as_expression_with_operation(
                    Expr::Value(number(1)),
                    BinaryOperator::Plus,
                    Expr::Value(number(1))
                )),
                Ok(UntypedQuery::Update(UntypedUpdateQuery {
                    full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                    assignments: vec![Some(DynamicUntypedTree::BiOp {
                        left: Box::new(DynamicUntypedTree::Item(DynamicUntypedItem::Const(
                            UntypedValue::Number(BigDecimal::from(1))
                        ))),
                        op: BiOperator::Arithmetic(BiArithmetic::Add),
                        right: Box::new(DynamicUntypedTree::Item(DynamicUntypedItem::Const(
                            UntypedValue::Number(BigDecimal::from(1))
                        )))
                    })],
                    filter: None
                }))
            );
            Ok(())
        })
    }

    #[test]
    fn string_operation() -> TransactionResult<()> {
        Database::new("").old_transaction(|db| {
            let catalog = CatalogHandlerOld::from(db.clone());
            catalog.apply(create_schema_ops(SCHEMA)).unwrap();
            catalog
                .apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::var_char(255))]))
                .unwrap();
            let analyzer = QueryAnalyzerOld::from(db);

            assert_eq!(
                analyzer.analyze(update_value_as_expression_with_operation(
                    string("str"),
                    BinaryOperator::StringConcat,
                    string("str")
                )),
                Ok(UntypedQuery::Update(UntypedUpdateQuery {
                    full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                    assignments: vec![Some(DynamicUntypedTree::BiOp {
                        left: Box::new(DynamicUntypedTree::Item(DynamicUntypedItem::Const(
                            UntypedValue::String("str".to_owned())
                        ))),
                        op: BiOperator::StringOp(Concat),
                        right: Box::new(DynamicUntypedTree::Item(DynamicUntypedItem::Const(
                            UntypedValue::String("str".to_owned())
                        )))
                    })],
                    filter: None
                }))
            );
            Ok(())
        })
    }

    #[test]
    fn comparison() -> TransactionResult<()> {
        Database::new("").old_transaction(|db| {
            let catalog = CatalogHandlerOld::from(db.clone());
            catalog.apply(create_schema_ops(SCHEMA)).unwrap();
            catalog
                .apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())]))
                .unwrap();
            let analyzer = QueryAnalyzerOld::from(db);

            assert_eq!(
                analyzer.analyze(update_value_as_expression_with_operation(
                    Expr::Value(number(1)),
                    BinaryOperator::Gt,
                    Expr::Value(number(1))
                )),
                Ok(UntypedQuery::Update(UntypedUpdateQuery {
                    full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                    assignments: vec![Some(DynamicUntypedTree::BiOp {
                        left: Box::new(DynamicUntypedTree::Item(DynamicUntypedItem::Const(
                            UntypedValue::Number(BigDecimal::from(1))
                        ))),
                        op: BiOperator::Comparison(Comparison::Gt),
                        right: Box::new(DynamicUntypedTree::Item(DynamicUntypedItem::Const(
                            UntypedValue::Number(BigDecimal::from(1))
                        )))
                    })],
                    filter: None
                }))
            );
            Ok(())
        })
    }

    #[test]
    fn logical() -> TransactionResult<()> {
        Database::new("").old_transaction(|db| {
            let catalog = CatalogHandlerOld::from(db.clone());
            catalog.apply(create_schema_ops(SCHEMA)).unwrap();
            catalog
                .apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())]))
                .unwrap();
            let analyzer = QueryAnalyzerOld::from(db);

            assert_eq!(
                analyzer.analyze(update_value_as_expression_with_operation(
                    Expr::Value(Value::Boolean(true)),
                    BinaryOperator::And,
                    Expr::Value(Value::Boolean(true)),
                )),
                Ok(UntypedQuery::Update(UntypedUpdateQuery {
                    full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                    assignments: vec![Some(DynamicUntypedTree::BiOp {
                        left: Box::new(DynamicUntypedTree::Item(DynamicUntypedItem::Const(UntypedValue::Bool(
                            Bool(true)
                        )))),
                        op: BiOperator::Logical(BiLogical::And),
                        right: Box::new(DynamicUntypedTree::Item(DynamicUntypedItem::Const(UntypedValue::Bool(
                            Bool(true)
                        )))),
                    })],
                    filter: None
                }))
            );
            Ok(())
        })
    }

    #[test]
    fn bitwise() -> TransactionResult<()> {
        Database::new("").old_transaction(|db| {
            let catalog = CatalogHandlerOld::from(db.clone());
            catalog.apply(create_schema_ops(SCHEMA)).unwrap();
            catalog
                .apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::small_int())]))
                .unwrap();
            let analyzer = QueryAnalyzerOld::from(db);

            assert_eq!(
                analyzer.analyze(update_value_as_expression_with_operation(
                    Expr::Value(number(1)),
                    BinaryOperator::BitwiseOr,
                    Expr::Value(number(1))
                )),
                Ok(UntypedQuery::Update(UntypedUpdateQuery {
                    full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                    assignments: vec![Some(DynamicUntypedTree::BiOp {
                        left: Box::new(DynamicUntypedTree::Item(DynamicUntypedItem::Const(
                            UntypedValue::Number(BigDecimal::from(1))
                        ))),
                        op: BiOperator::Bitwise(Bitwise::Or),
                        right: Box::new(DynamicUntypedTree::Item(DynamicUntypedItem::Const(
                            UntypedValue::Number(BigDecimal::from(1))
                        )))
                    })],
                    filter: None
                }))
            );
            Ok(())
        })
    }

    #[test]
    fn pattern_matching() -> TransactionResult<()> {
        Database::new("").old_transaction(|db| {
            let catalog = CatalogHandlerOld::from(db.clone());
            catalog.apply(create_schema_ops(SCHEMA)).unwrap();
            catalog
                .apply(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())]))
                .unwrap();
            let analyzer = QueryAnalyzerOld::from(db);

            assert_eq!(
                analyzer.analyze(update_value_as_expression_with_operation(
                    string("s"),
                    BinaryOperator::Like,
                    string("str")
                )),
                Ok(UntypedQuery::Update(UntypedUpdateQuery {
                    full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                    assignments: vec![Some(DynamicUntypedTree::BiOp {
                        left: Box::new(DynamicUntypedTree::Item(DynamicUntypedItem::Const(
                            UntypedValue::String("s".to_owned())
                        ))),
                        op: BiOperator::Matching(Matching::Like),
                        right: Box::new(DynamicUntypedTree::Item(DynamicUntypedItem::Const(
                            UntypedValue::String("str".to_owned())
                        )))
                    })],
                    filter: None
                }))
            );
            Ok(())
        })
    }
}
