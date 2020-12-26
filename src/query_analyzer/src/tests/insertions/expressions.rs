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

fn small_int(value: i16) -> ast::Expr {
    ast::Expr::Value(number(value))
}

#[test]
fn insert_number() {
    let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::SmallInt)]);
    let analyzer = Analyzer::new(data_definition);

    assert_eq!(
        analyzer.analyze(insert_with_values(vec![SCHEMA, TABLE], vec![vec![small_int(1)]])),
        Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
            full_table_id: FullTableId::from((schema_id, table_id)),
            column_types: vec![SqlType::SmallInt],
            values: vec![vec![InsertTreeNode::Item(Operator::Const(ScalarValue::Number(
                BigDecimal::from(1)
            )))]],
        })))
    );
}

#[test]
fn insert_string() {
    let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::Char(5))]);
    let analyzer = Analyzer::new(data_definition);

    assert_eq!(
        analyzer.analyze(insert_with_values(vec![SCHEMA, TABLE], vec![vec![string("str")]])),
        Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
            full_table_id: FullTableId::from((schema_id, table_id)),
            column_types: vec![SqlType::Char(5)],
            values: vec![vec![InsertTreeNode::Item(Operator::Const(ScalarValue::String(
                "str".to_owned()
            )))]],
        })))
    );
}

#[test]
fn insert_boolean() {
    let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::Bool)]);
    let analyzer = Analyzer::new(data_definition);

    assert_eq!(
        analyzer.analyze(insert_with_values(vec![SCHEMA, TABLE], vec![vec![boolean(true)]])),
        Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
            full_table_id: FullTableId::from((schema_id, table_id)),
            column_types: vec![SqlType::Bool],
            values: vec![vec![InsertTreeNode::Item(Operator::Const(ScalarValue::Bool(Bool(
                true
            ))))]],
        })))
    );
}

#[test]
fn insert_null() {
    let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::Bool)]);
    let analyzer = Analyzer::new(data_definition);

    assert_eq!(
        analyzer.analyze(insert_with_values(vec![SCHEMA, TABLE], vec![vec![null()]])),
        Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
            full_table_id: FullTableId::from((schema_id, table_id)),
            column_types: vec![SqlType::Bool],
            values: vec![vec![InsertTreeNode::Item(Operator::Const(ScalarValue::Null))]],
        })))
    );
}

#[cfg(test)]
mod implicit_cast {
    use super::*;

    #[test]
    fn string_to_boolean() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::Bool)]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(insert_with_values(
                vec![SCHEMA, TABLE],
                vec![vec![ast::Expr::Value(ast::Value::SingleQuotedString("t".to_owned()))]]
            )),
            Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                column_types: vec![SqlType::Bool],
                values: vec![vec![InsertTreeNode::Item(Operator::Const(ScalarValue::Bool(Bool(
                    true
                ))))]],
            })))
        );
    }

    #[test]
    fn boolean_to_string() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::VarChar(5))]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(insert_with_values(
                vec![SCHEMA, TABLE],
                vec![vec![ast::Expr::Value(ast::Value::Boolean(true))]]
            )),
            Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                column_types: vec![SqlType::VarChar(5)],
                values: vec![vec![InsertTreeNode::Item(Operator::Const(ScalarValue::String(
                    "true".to_string()
                )))]],
            })))
        );
    }

    #[test]
    fn boolean_to_string_not_enough_length() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::Char(1))]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(insert_with_values(
                vec![SCHEMA, TABLE],
                vec![vec![ast::Expr::Value(ast::Value::Boolean(true))]]
            )),
            Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                column_types: vec![SqlType::Char(1)],
                values: vec![vec![InsertTreeNode::Item(Operator::Const(ScalarValue::String(
                    "true".to_owned()
                )))]]
            })))
        );
    }

    #[test]
    fn string_to_number() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::SmallInt)]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(insert_with_values(
                vec![SCHEMA, TABLE],
                vec![vec![ast::Expr::Value(ast::Value::SingleQuotedString("100".to_owned()))]]
            )),
            Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                column_types: vec![SqlType::SmallInt],
                values: vec![vec![InsertTreeNode::Item(Operator::Const(ScalarValue::Number(
                    BigDecimal::from(100)
                )))]],
            })))
        );
    }

    #[test]
    fn number_to_string_not_enough_length() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::Char(1))]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(insert_with_values(
                vec![SCHEMA, TABLE],
                vec![vec![ast::Expr::Value(ast::Value::Number(BigDecimal::from(123)))]]
            )),
            Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                column_types: vec![SqlType::Char(1)],
                values: vec![vec![InsertTreeNode::Item(Operator::Const(ScalarValue::String(
                    "123".to_owned()
                )))]]
            })))
        );
    }

    #[test]
    fn number_to_boolean() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::Bool)]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(insert_with_values(
                vec![SCHEMA, TABLE],
                vec![vec![ast::Expr::Value(ast::Value::Number(BigDecimal::from(0)))]]
            )),
            Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                column_types: vec![SqlType::Bool],
                values: vec![vec![InsertTreeNode::Item(Operator::Const(ScalarValue::Bool(Bool(
                    false
                ))))]]
            })))
        );
    }
}

#[cfg(test)]
mod multiple_values {
    use super::*;

    fn insert_value_as_expression_with_operation(
        left: ast::Expr,
        op: ast::BinaryOperator,
        right: ast::Expr,
    ) -> ast::Statement {
        insert_with_values(
            vec![SCHEMA, TABLE],
            vec![vec![ast::Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }]],
        )
    }

    #[test]
    fn arithmetic() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::SmallInt)]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(insert_value_as_expression_with_operation(
                ast::Expr::Value(number(1)),
                ast::BinaryOperator::Plus,
                ast::Expr::Value(number(1))
            )),
            Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                column_types: vec![SqlType::SmallInt],
                values: vec![vec![InsertTreeNode::Operation {
                    left: Box::new(InsertTreeNode::Item(Operator::Const(ScalarValue::Number(
                        BigDecimal::from(1)
                    )))),
                    op: Operation::Arithmetic(Arithmetic::Add),
                    right: Box::new(InsertTreeNode::Item(Operator::Const(ScalarValue::Number(
                        BigDecimal::from(1)
                    ))))
                }]],
            })))
        );
    }

    #[test]
    fn string_operation() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::VarChar(255))]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(insert_value_as_expression_with_operation(
                string("str"),
                ast::BinaryOperator::StringConcat,
                string("str")
            )),
            Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                column_types: vec![SqlType::VarChar(255)],
                values: vec![vec![InsertTreeNode::Operation {
                    left: Box::new(InsertTreeNode::Item(Operator::Const(ScalarValue::String(
                        "str".to_owned()
                    )))),
                    op: Operation::StringOp(StringOp::Concat),
                    right: Box::new(InsertTreeNode::Item(Operator::Const(ScalarValue::String(
                        "str".to_owned()
                    ))))
                }]],
            })))
        );
    }

    #[test]
    fn comparison() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::Bool)]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(insert_value_as_expression_with_operation(
                ast::Expr::Value(number(1)),
                ast::BinaryOperator::Gt,
                ast::Expr::Value(number(1))
            )),
            Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                column_types: vec![SqlType::Bool],
                values: vec![vec![InsertTreeNode::Operation {
                    left: Box::new(InsertTreeNode::Item(Operator::Const(ScalarValue::Number(
                        BigDecimal::from(1)
                    )))),
                    op: Operation::Comparison(Comparison::Gt),
                    right: Box::new(InsertTreeNode::Item(Operator::Const(ScalarValue::Number(
                        BigDecimal::from(1)
                    ))))
                }]],
            })))
        );
    }

    #[test]
    fn logical() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::Bool)]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(insert_value_as_expression_with_operation(
                ast::Expr::Value(ast::Value::Boolean(true)),
                ast::BinaryOperator::And,
                ast::Expr::Value(ast::Value::Boolean(true)),
            )),
            Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                column_types: vec![SqlType::Bool],
                values: vec![vec![InsertTreeNode::Operation {
                    left: Box::new(InsertTreeNode::Item(Operator::Const(ScalarValue::Bool(Bool(true))))),
                    op: Operation::Logical(Logical::And),
                    right: Box::new(InsertTreeNode::Item(Operator::Const(ScalarValue::Bool(Bool(true))))),
                }]],
            })))
        );
    }

    #[test]
    fn bitwise() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::SmallInt)]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(insert_value_as_expression_with_operation(
                ast::Expr::Value(number(1)),
                ast::BinaryOperator::BitwiseOr,
                ast::Expr::Value(number(1))
            )),
            Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                column_types: vec![SqlType::SmallInt],
                values: vec![vec![InsertTreeNode::Operation {
                    left: Box::new(InsertTreeNode::Item(Operator::Const(ScalarValue::Number(
                        BigDecimal::from(1)
                    )))),
                    op: Operation::Bitwise(Bitwise::Or),
                    right: Box::new(InsertTreeNode::Item(Operator::Const(ScalarValue::Number(
                        BigDecimal::from(1)
                    ))))
                }]],
            })))
        );
    }

    #[test]
    fn pattern_matching() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::Bool)]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(insert_value_as_expression_with_operation(
                string("s"),
                ast::BinaryOperator::Like,
                string("str")
            )),
            Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                column_types: vec![SqlType::Bool],
                values: vec![vec![InsertTreeNode::Operation {
                    left: Box::new(InsertTreeNode::Item(Operator::Const(ScalarValue::String(
                        "s".to_owned()
                    )))),
                    op: Operation::PatternMatching(PatternMatching::Like),
                    right: Box::new(InsertTreeNode::Item(Operator::Const(ScalarValue::String(
                        "str".to_owned()
                    ))))
                }]],
            })))
        );
    }
}

#[cfg(test)]
mod not_supported_values {
    use super::*;

    #[test]
    fn national_strings() {
        let (data_definition, _schema_id, _table_id) = with_table(&[ColumnDefinition::new("col", SqlType::SmallInt)]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(insert_with_values(
                vec![SCHEMA, TABLE],
                vec![vec![ast::Expr::Value(ast::Value::NationalStringLiteral(
                    "str".to_owned()
                ))]]
            )),
            Err(AnalysisError::FeatureNotSupported(Feature::NationalStringLiteral))
        );
    }

    #[test]
    fn hex_strings() {
        let (data_definition, _schema_id, _table_id) = with_table(&[ColumnDefinition::new("col", SqlType::SmallInt)]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(insert_with_values(
                vec![SCHEMA, TABLE],
                vec![vec![ast::Expr::Value(ast::Value::HexStringLiteral("str".to_owned()))]]
            )),
            Err(AnalysisError::FeatureNotSupported(Feature::HexStringLiteral))
        );
    }

    #[test]
    fn time_intervals() {
        let (data_definition, _schema_id, _table_id) = with_table(&[ColumnDefinition::new("col", SqlType::SmallInt)]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(insert_with_values(
                vec![SCHEMA, TABLE],
                vec![vec![ast::Expr::Value(ast::Value::Interval {
                    value: "value".to_owned(),
                    leading_field: None,
                    leading_precision: None,
                    last_field: None,
                    fractional_seconds_precision: None
                })]]
            )),
            Err(AnalysisError::FeatureNotSupported(Feature::TimeInterval))
        );
    }
}
