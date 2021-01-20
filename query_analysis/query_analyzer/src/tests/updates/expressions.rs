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

use super::*;

#[test]
fn update_number() {
    let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::small_int())]);
    let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());

    assert_eq!(
        analyzer.analyze(update_statement(
            vec![SCHEMA, TABLE],
            vec![("col", sql_ast::Expr::Value(number(1)))]
        )),
        Ok(QueryAnalysis::Write(Write::Update(UpdateQuery {
            full_table_id: FullTableId::from((schema_id, table_id)),
            sql_types: vec![SqlType::small_int()],
            assignments: vec![DynamicEvaluationTree::Item(DynamicItem::Const(ScalarValue::Number(
                BigDecimal::from(1)
            )))]
        })))
    );
}

#[test]
fn update_string() {
    let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::char(5))]);
    let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());

    assert_eq!(
        analyzer.analyze(update_statement(vec![SCHEMA, TABLE], vec![("col", string("str"))])),
        Ok(QueryAnalysis::Write(Write::Update(UpdateQuery {
            full_table_id: FullTableId::from((schema_id, table_id)),
            sql_types: vec![SqlType::char(5)],
            assignments: vec![DynamicEvaluationTree::Item(DynamicItem::Const(ScalarValue::String(
                "str".to_owned()
            )))]
        })))
    );
}

#[test]
fn update_boolean() {
    let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::bool())]);
    let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());

    assert_eq!(
        analyzer.analyze(update_statement(vec![SCHEMA, TABLE], vec![("col", boolean(true))])),
        Ok(QueryAnalysis::Write(Write::Update(UpdateQuery {
            full_table_id: FullTableId::from((schema_id, table_id)),
            sql_types: vec![SqlType::bool()],
            assignments: vec![DynamicEvaluationTree::Item(DynamicItem::Const(ScalarValue::Bool(Bool(true))))],
        })))
    );
}

#[test]
fn update_null() {
    let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::bool())]);
    let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());

    assert_eq!(
        analyzer.analyze(update_statement(vec![SCHEMA, TABLE], vec![("col", null())])),
        Ok(QueryAnalysis::Write(Write::Update(UpdateQuery {
            full_table_id: FullTableId::from((schema_id, table_id)),
            sql_types: vec![SqlType::bool()],
            assignments: vec![DynamicEvaluationTree::Item(DynamicItem::Const(ScalarValue::Null))],
        })))
    );
}

#[test]
fn update_with_column_value() {
    let (data_definition, schema_id, table_id) = with_table(&[
        ColumnDefinition::new("col_1", SqlType::small_int()),
        ColumnDefinition::new("col_2", SqlType::small_int()),
    ]);
    let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());

    assert_eq!(
        analyzer.analyze(update_statement(
            vec![SCHEMA, TABLE],
            vec![("col_1", sql_ast::Expr::Identifier(ident("col_2")))]
        )),
        Ok(QueryAnalysis::Write(Write::Update(UpdateQuery {
            full_table_id: FullTableId::from((schema_id, table_id)),
            sql_types: vec![SqlType::small_int()],
            assignments: vec![DynamicEvaluationTree::Item(DynamicItem::Column {
                sql_type: SqlType::small_int(),
                index: 1
            })],
        })))
    );
}

#[test]
fn update_with_column_value_that_does_not_exists() {
    let (data_definition, _schema_id, _table_id) = with_table(&[
        ColumnDefinition::new("col_1", SqlType::small_int()),
        ColumnDefinition::new("col_2", SqlType::small_int()),
    ]);
    let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());

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
    let (data_definition, schema_id, table_id) = with_table(&[
        ColumnDefinition::new("col_1", SqlType::small_int()),
        ColumnDefinition::new("col_2", SqlType::integer()),
    ]);
    let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());

    assert_eq!(
        analyzer.analyze(update_stmt_with_parameters(vec![SCHEMA, TABLE])),
        Ok(QueryAnalysis::Write(Write::Update(UpdateQuery {
            full_table_id: FullTableId::from((schema_id, table_id)),
            sql_types: vec![SqlType::integer()],
            assignments: vec![DynamicEvaluationTree::Item(DynamicItem::Param(0))]
        })))
    );
}

#[cfg(test)]
mod multiple_values {
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
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::small_int())]);
        let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());

        assert_eq!(
            analyzer.analyze(update_value_as_expression_with_operation(
                sql_ast::Expr::Value(number(1)),
                sql_ast::BinaryOperator::Plus,
                sql_ast::Expr::Value(number(1))
            )),
            Ok(QueryAnalysis::Write(Write::Update(UpdateQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                sql_types: vec![SqlType::small_int()],
                assignments: vec![DynamicEvaluationTree::Operation {
                    left: Box::new(DynamicEvaluationTree::Item(DynamicItem::Const(ScalarValue::Number(
                        BigDecimal::from(1)
                    )))),
                    op: Operation::Arithmetic(Arithmetic::Add),
                    right: Box::new(DynamicEvaluationTree::Item(DynamicItem::Const(ScalarValue::Number(
                        BigDecimal::from(1)
                    ))))
                }],
            })))
        );
    }

    #[test]
    fn string_operation() {
        let (data_definition, schema_id, table_id) =
            with_table(&[ColumnDefinition::new("col", SqlType::var_char(255))]);
        let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());

        assert_eq!(
            analyzer.analyze(update_value_as_expression_with_operation(
                string("str"),
                sql_ast::BinaryOperator::StringConcat,
                string("str")
            )),
            Ok(QueryAnalysis::Write(Write::Update(UpdateQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                sql_types: vec![SqlType::var_char(255)],
                assignments: vec![DynamicEvaluationTree::Operation {
                    left: Box::new(DynamicEvaluationTree::Item(DynamicItem::Const(ScalarValue::String(
                        "str".to_owned()
                    )))),
                    op: Operation::StringOp(StringOp::Concat),
                    right: Box::new(DynamicEvaluationTree::Item(DynamicItem::Const(ScalarValue::String(
                        "str".to_owned()
                    ))))
                }],
            })))
        );
    }

    #[test]
    fn comparison() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::bool())]);
        let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());

        assert_eq!(
            analyzer.analyze(update_value_as_expression_with_operation(
                sql_ast::Expr::Value(number(1)),
                sql_ast::BinaryOperator::Gt,
                sql_ast::Expr::Value(number(1))
            )),
            Ok(QueryAnalysis::Write(Write::Update(UpdateQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                sql_types: vec![SqlType::bool()],
                assignments: vec![DynamicEvaluationTree::Operation {
                    left: Box::new(DynamicEvaluationTree::Item(DynamicItem::Const(ScalarValue::Number(
                        BigDecimal::from(1)
                    )))),
                    op: Operation::Comparison(Comparison::Gt),
                    right: Box::new(DynamicEvaluationTree::Item(DynamicItem::Const(ScalarValue::Number(
                        BigDecimal::from(1)
                    ))))
                }],
            })))
        );
    }

    #[test]
    fn logical() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::bool())]);
        let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());

        assert_eq!(
            analyzer.analyze(update_value_as_expression_with_operation(
                sql_ast::Expr::Value(sql_ast::Value::Boolean(true)),
                sql_ast::BinaryOperator::And,
                sql_ast::Expr::Value(sql_ast::Value::Boolean(true)),
            )),
            Ok(QueryAnalysis::Write(Write::Update(UpdateQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                sql_types: vec![SqlType::bool()],
                assignments: vec![DynamicEvaluationTree::Operation {
                    left: Box::new(DynamicEvaluationTree::Item(DynamicItem::Const(ScalarValue::Bool(Bool(true))))),
                    op: Operation::Logical(Logical::And),
                    right: Box::new(DynamicEvaluationTree::Item(DynamicItem::Const(ScalarValue::Bool(Bool(true))))),
                }],
            })))
        );
    }

    #[test]
    fn bitwise() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::small_int())]);
        let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());

        assert_eq!(
            analyzer.analyze(update_value_as_expression_with_operation(
                sql_ast::Expr::Value(number(1)),
                sql_ast::BinaryOperator::BitwiseOr,
                sql_ast::Expr::Value(number(1))
            )),
            Ok(QueryAnalysis::Write(Write::Update(UpdateQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                sql_types: vec![SqlType::small_int()],
                assignments: vec![DynamicEvaluationTree::Operation {
                    left: Box::new(DynamicEvaluationTree::Item(DynamicItem::Const(ScalarValue::Number(
                        BigDecimal::from(1)
                    )))),
                    op: Operation::Bitwise(Bitwise::Or),
                    right: Box::new(DynamicEvaluationTree::Item(DynamicItem::Const(ScalarValue::Number(
                        BigDecimal::from(1)
                    ))))
                }],
            })))
        );
    }

    #[test]
    fn pattern_matching() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::bool())]);
        let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());

        assert_eq!(
            analyzer.analyze(update_value_as_expression_with_operation(
                string("s"),
                sql_ast::BinaryOperator::Like,
                string("str")
            )),
            Ok(QueryAnalysis::Write(Write::Update(UpdateQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                sql_types: vec![SqlType::bool()],
                assignments: vec![DynamicEvaluationTree::Operation {
                    left: Box::new(DynamicEvaluationTree::Item(DynamicItem::Const(ScalarValue::String(
                        "s".to_owned()
                    )))),
                    op: Operation::PatternMatching(PatternMatching::Like),
                    right: Box::new(DynamicEvaluationTree::Item(DynamicItem::Const(ScalarValue::String(
                        "str".to_owned()
                    ))))
                }],
            })))
        );
    }
}

#[cfg(test)]
mod not_supported_values {
    use super::*;

    #[test]
    fn national_strings() {
        let (data_definition, _schema_id, _table_id) =
            with_table(&[ColumnDefinition::new("col", SqlType::small_int())]);
        let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());

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
        let (data_definition, _schema_id, _table_id) =
            with_table(&[ColumnDefinition::new("col", SqlType::small_int())]);
        let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());

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
        let (data_definition, _schema_id, _table_id) =
            with_table(&[ColumnDefinition::new("col", SqlType::small_int())]);
        let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());

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
