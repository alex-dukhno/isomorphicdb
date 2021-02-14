// Copyright 2020 - present Alex Dukhno
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
fn update_number() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::small_int())]))
        .unwrap();
    let analyzer = Analyzer::new(database);

    assert_eq!(
        analyzer.analyze(update_statement(
            vec![SCHEMA, TABLE],
            vec![("col", sql_ast::Expr::Value(number(1)))]
        )),
        Ok(QueryAnalysis::DML(UntypedQuery::Update(UpdateQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            assignments: vec![Some(DynamicUntypedTree::Item(DynamicUntypedItem::Const(
                UntypedValue::Number(BigDecimal::from(1))
            )))]
        })))
    );
}

#[test]
fn update_string() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::char(5))]))
        .unwrap();
    let analyzer = Analyzer::new(database);

    assert_eq!(
        analyzer.analyze(update_statement(vec![SCHEMA, TABLE], vec![("col", string("str"))])),
        Ok(QueryAnalysis::DML(UntypedQuery::Update(UpdateQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            assignments: vec![Some(DynamicUntypedTree::Item(DynamicUntypedItem::Const(
                UntypedValue::String("str".to_owned())
            )))]
        })))
    );
}

#[test]
fn update_boolean() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())]))
        .unwrap();
    let analyzer = Analyzer::new(database);

    assert_eq!(
        analyzer.analyze(update_statement(vec![SCHEMA, TABLE], vec![("col", boolean(true))])),
        Ok(QueryAnalysis::DML(UntypedQuery::Update(UpdateQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            assignments: vec![Some(DynamicUntypedTree::Item(DynamicUntypedItem::Const(
                UntypedValue::Bool(Bool(true))
            )))],
        })))
    );
}

#[test]
fn update_null() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())]))
        .unwrap();
    let analyzer = Analyzer::new(database);

    assert_eq!(
        analyzer.analyze(update_statement(vec![SCHEMA, TABLE], vec![("col", null())])),
        Ok(QueryAnalysis::DML(UntypedQuery::Update(UpdateQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            assignments: vec![Some(DynamicUntypedTree::Item(DynamicUntypedItem::Const(
                UntypedValue::Null
            )))],
        })))
    );
}

#[test]
fn update_with_column_value() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_ops(
            SCHEMA,
            TABLE,
            vec![("col_1", SqlType::small_int()), ("col_2", SqlType::small_int())],
        ))
        .unwrap();
    let analyzer = Analyzer::new(database);

    assert_eq!(
        analyzer.analyze(update_statement(
            vec![SCHEMA, TABLE],
            vec![("col_1", sql_ast::Expr::Identifier(ident("col_2")))]
        )),
        Ok(QueryAnalysis::DML(UntypedQuery::Update(UpdateQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            assignments: vec![
                Some(DynamicUntypedTree::Item(DynamicUntypedItem::Column {
                    name: "col_2".to_owned(),
                    sql_type: SqlType::small_int(),
                    index: 1
                })),
                None
            ],
        })))
    );
}

#[test]
fn update_with_column_value_that_does_not_exists() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_ops(
            SCHEMA,
            TABLE,
            vec![("col_1", SqlType::small_int()), ("col_2", SqlType::small_int())],
        ))
        .unwrap();
    let analyzer = Analyzer::new(database);

    assert_eq!(
        analyzer.analyze(update_statement(
            vec![SCHEMA, TABLE],
            vec![("col_1", sql_ast::Expr::Identifier(ident("col_3")))]
        )),
        Err(AnalysisError::column_not_found(&"col_3"))
    );
}

#[test]
fn update_table_with_parameters() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_ops(
            SCHEMA,
            TABLE,
            vec![("col_1", SqlType::small_int()), ("col_2", SqlType::integer())],
        ))
        .unwrap();
    let analyzer = Analyzer::new(database);

    assert_eq!(
        analyzer.analyze(update_stmt_with_parameters(vec![SCHEMA, TABLE])),
        Ok(QueryAnalysis::DML(UntypedQuery::Update(UpdateQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            assignments: vec![None, Some(DynamicUntypedTree::Item(DynamicUntypedItem::Param(0)))]
        })))
    );
}

#[cfg(test)]
mod multiple_values {
    use data_manipulation_untyped_tree::{DynamicUntypedItem, DynamicUntypedTree, UntypedValue};

    use super::*;

    fn update_value_as_expression_with_operation(
        left: sql_ast::Expr,
        op: sql_ast::BinaryOperator,
        right: sql_ast::Expr,
    ) -> sql_ast::Statement {
        update_statement(
            vec![SCHEMA, TABLE],
            vec![(
                "col",
                sql_ast::Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                },
            )],
        )
    }

    #[test]
    fn arithmetic() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        database
            .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::small_int())]))
            .unwrap();
        let analyzer = Analyzer::new(database);

        assert_eq!(
            analyzer.analyze(update_value_as_expression_with_operation(
                sql_ast::Expr::Value(number(1)),
                sql_ast::BinaryOperator::Plus,
                sql_ast::Expr::Value(number(1))
            )),
            Ok(QueryAnalysis::DML(UntypedQuery::Update(UpdateQuery {
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
            })))
        );
    }

    #[test]
    fn string_operation() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        database
            .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::var_char(255))]))
            .unwrap();
        let analyzer = Analyzer::new(database);

        assert_eq!(
            analyzer.analyze(update_value_as_expression_with_operation(
                string("str"),
                sql_ast::BinaryOperator::StringConcat,
                string("str")
            )),
            Ok(QueryAnalysis::DML(UntypedQuery::Update(UpdateQuery {
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
            })))
        );
    }

    #[test]
    fn comparison() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        database
            .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())]))
            .unwrap();
        let analyzer = Analyzer::new(database);

        assert_eq!(
            analyzer.analyze(update_value_as_expression_with_operation(
                sql_ast::Expr::Value(number(1)),
                sql_ast::BinaryOperator::Gt,
                sql_ast::Expr::Value(number(1))
            )),
            Ok(QueryAnalysis::DML(UntypedQuery::Update(UpdateQuery {
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
            })))
        );
    }

    #[test]
    fn logical() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        database
            .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())]))
            .unwrap();
        let analyzer = Analyzer::new(database);

        assert_eq!(
            analyzer.analyze(update_value_as_expression_with_operation(
                sql_ast::Expr::Value(sql_ast::Value::Boolean(true)),
                sql_ast::BinaryOperator::And,
                sql_ast::Expr::Value(sql_ast::Value::Boolean(true)),
            )),
            Ok(QueryAnalysis::DML(UntypedQuery::Update(UpdateQuery {
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
            })))
        );
    }

    #[test]
    fn bitwise() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        database
            .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::small_int())]))
            .unwrap();
        let analyzer = Analyzer::new(database);

        assert_eq!(
            analyzer.analyze(update_value_as_expression_with_operation(
                sql_ast::Expr::Value(number(1)),
                sql_ast::BinaryOperator::BitwiseOr,
                sql_ast::Expr::Value(number(1))
            )),
            Ok(QueryAnalysis::DML(UntypedQuery::Update(UpdateQuery {
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
            })))
        );
    }

    #[test]
    fn pattern_matching() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        database
            .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())]))
            .unwrap();
        let analyzer = Analyzer::new(database);

        assert_eq!(
            analyzer.analyze(update_value_as_expression_with_operation(
                string("s"),
                sql_ast::BinaryOperator::Like,
                string("str")
            )),
            Ok(QueryAnalysis::DML(UntypedQuery::Update(UpdateQuery {
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
            })))
        );
    }
}

#[cfg(test)]
mod not_supported_values {
    use super::*;

    #[test]
    fn national_strings() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        database
            .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::small_int())]))
            .unwrap();
        let analyzer = Analyzer::new(database);

        assert_eq!(
            analyzer.analyze(update_statement(
                vec![SCHEMA, TABLE],
                vec![(
                    "col",
                    sql_ast::Expr::Value(sql_ast::Value::NationalStringLiteral("str".to_owned()))
                )]
            )),
            Err(AnalysisError::FeatureNotSupported(Feature::NationalStringLiteral))
        );
    }

    #[test]
    fn hex_strings() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        database
            .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::small_int())]))
            .unwrap();
        let analyzer = Analyzer::new(database);

        assert_eq!(
            analyzer.analyze(update_statement(
                vec![SCHEMA, TABLE],
                vec![(
                    "col",
                    sql_ast::Expr::Value(sql_ast::Value::HexStringLiteral("str".to_owned()))
                )]
            )),
            Err(AnalysisError::FeatureNotSupported(Feature::HexStringLiteral))
        );
    }

    #[test]
    fn time_intervals() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        database
            .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::small_int())]))
            .unwrap();
        let analyzer = Analyzer::new(database);

        assert_eq!(
            analyzer.analyze(update_statement(
                vec![SCHEMA, TABLE],
                vec![(
                    "col",
                    sql_ast::Expr::Value(sql_ast::Value::Interval {
                        value: "value".to_owned(),
                        leading_field: None,
                        leading_precision: None,
                        last_field: None,
                        fractional_seconds_precision: None
                    })
                )]
            )),
            Err(AnalysisError::FeatureNotSupported(Feature::TimeInterval))
        );
    }
}
