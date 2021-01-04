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
fn select_all_columns_from_table() {
    let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col1", SqlType::Integer)]);
    let analyzer = Analyzer::new(data_definition);

    assert_eq!(
        analyzer.analyze(select(vec![SCHEMA, TABLE])),
        Ok(QueryAnalysis::Read(SelectQuery {
            full_table_id: FullTableId::from((schema_id, table_id)),
            projection_items: vec![ProjectionTreeNode::Item(Operator::Column {
                index: 0,
                sql_type: SqlType::Integer
            })],
        }))
    );
}

#[test]
fn select_specified_column_from_table() {
    let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col1", SqlType::Integer)]);
    let analyzer = Analyzer::new(data_definition);

    assert_eq!(
        analyzer.analyze(select_with_columns(
            vec![SCHEMA, TABLE],
            vec![sql_ast::SelectItem::UnnamedExpr(sql_ast::Expr::Identifier(ident(
                "col1"
            )))]
        )),
        Ok(QueryAnalysis::Read(SelectQuery {
            full_table_id: FullTableId::from((schema_id, table_id)),
            projection_items: vec![ProjectionTreeNode::Item(Operator::Column {
                index: 0,
                sql_type: SqlType::Integer
            })],
        }))
    );
}

#[test]
fn select_column_that_is_not_in_table() {
    let (data_definition, _schema_id, _table_id) = with_table(&[ColumnDefinition::new("col1", SqlType::Integer)]);
    let analyzer = Analyzer::new(data_definition);

    assert_eq!(
        analyzer.analyze(select_with_columns(
            vec![SCHEMA, TABLE],
            vec![sql_ast::SelectItem::UnnamedExpr(sql_ast::Expr::Identifier(ident(
                "col2"
            )))]
        )),
        Err(AnalysisError::column_not_found(&"col2"))
    );
}

#[test]
fn select_from_table_with_constant() {
    let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col1", SqlType::Integer)]);
    let analyzer = Analyzer::new(data_definition);

    assert_eq!(
        analyzer.analyze(select_with_columns(
            vec![SCHEMA, TABLE],
            vec![sql_ast::SelectItem::UnnamedExpr(sql_ast::Expr::Value(number(1)))],
        )),
        Ok(QueryAnalysis::Read(SelectQuery {
            full_table_id: FullTableId::from((schema_id, table_id)),
            projection_items: vec![ProjectionTreeNode::Item(Operator::Const(ScalarValue::Number(
                BigDecimal::from(1)
            )))],
        }))
    );
}

#[test]
fn select_parameters_from_a_table() {
    let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col1", SqlType::Integer)]);
    let analyzer = Analyzer::new(data_definition);

    assert_eq!(
        analyzer.analyze(select_with_columns(
            vec![SCHEMA, TABLE],
            vec![sql_ast::SelectItem::UnnamedExpr(sql_ast::Expr::Identifier(ident("$1")))],
        )),
        Ok(QueryAnalysis::Read(SelectQuery {
            full_table_id: FullTableId::from((schema_id, table_id)),
            projection_items: vec![ProjectionTreeNode::Item(Operator::Param(0))],
        }))
    );
}

#[cfg(test)]
mod multiple_values {
    use super::*;

    fn select_value_as_expression_with_operation(
        left: sql_ast::Expr,
        op: sql_ast::BinaryOperator,
        right: sql_ast::Expr,
    ) -> sql_ast::Statement {
        select_with_columns(
            vec![SCHEMA, TABLE],
            vec![sql_ast::SelectItem::UnnamedExpr(sql_ast::Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })],
        )
    }

    #[test]
    fn arithmetic() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::SmallInt)]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(select_value_as_expression_with_operation(
                string("1"),
                sql_ast::BinaryOperator::Plus,
                sql_ast::Expr::Value(number(1))
            )),
            Ok(QueryAnalysis::Read(SelectQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                projection_items: vec![ProjectionTreeNode::Operation {
                    left: Box::new(ProjectionTreeNode::Item(Operator::Const(ScalarValue::String(
                        "1".to_owned()
                    )))),
                    op: Operation::Arithmetic(Arithmetic::Add),
                    right: Box::new(ProjectionTreeNode::Item(Operator::Const(ScalarValue::Number(
                        BigDecimal::from(1)
                    ))))
                }],
            }))
        );
    }

    #[test]
    fn string_operation() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::VarChar(255))]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(select_value_as_expression_with_operation(
                string("str"),
                sql_ast::BinaryOperator::StringConcat,
                string("str")
            )),
            Ok(QueryAnalysis::Read(SelectQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                projection_items: vec![ProjectionTreeNode::Operation {
                    left: Box::new(ProjectionTreeNode::Item(Operator::Const(ScalarValue::String(
                        "str".to_owned()
                    )))),
                    op: Operation::StringOp(StringOp::Concat),
                    right: Box::new(ProjectionTreeNode::Item(Operator::Const(ScalarValue::String(
                        "str".to_owned()
                    ))))
                }],
            }))
        );
    }

    #[test]
    fn comparison() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::Bool)]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(select_value_as_expression_with_operation(
                string("1"),
                sql_ast::BinaryOperator::Gt,
                sql_ast::Expr::Value(number(1))
            )),
            Ok(QueryAnalysis::Read(SelectQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                projection_items: vec![ProjectionTreeNode::Operation {
                    left: Box::new(ProjectionTreeNode::Item(Operator::Const(ScalarValue::String(
                        "1".to_owned()
                    )))),
                    op: Operation::Comparison(Comparison::Gt),
                    right: Box::new(ProjectionTreeNode::Item(Operator::Const(ScalarValue::Number(
                        BigDecimal::from(1)
                    ))))
                }],
            }))
        );
    }

    #[test]
    fn logical() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::Bool)]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(select_value_as_expression_with_operation(
                sql_ast::Expr::Value(sql_ast::Value::Boolean(true)),
                sql_ast::BinaryOperator::And,
                sql_ast::Expr::Value(sql_ast::Value::Boolean(true)),
            )),
            Ok(QueryAnalysis::Read(SelectQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                projection_items: vec![ProjectionTreeNode::Operation {
                    left: Box::new(ProjectionTreeNode::Item(Operator::Const(ScalarValue::Bool(Bool(true))))),
                    op: Operation::Logical(Logical::And),
                    right: Box::new(ProjectionTreeNode::Item(Operator::Const(ScalarValue::Bool(Bool(true))))),
                }],
            }))
        );
    }

    #[test]
    fn bitwise() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::SmallInt)]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(select_value_as_expression_with_operation(
                sql_ast::Expr::Value(number(1)),
                sql_ast::BinaryOperator::BitwiseOr,
                sql_ast::Expr::Value(number(1))
            )),
            Ok(QueryAnalysis::Read(SelectQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                projection_items: vec![ProjectionTreeNode::Operation {
                    left: Box::new(ProjectionTreeNode::Item(Operator::Const(ScalarValue::Number(
                        BigDecimal::from(1)
                    )))),
                    op: Operation::Bitwise(Bitwise::Or),
                    right: Box::new(ProjectionTreeNode::Item(Operator::Const(ScalarValue::Number(
                        BigDecimal::from(1)
                    ))))
                }],
            }))
        );
    }

    #[test]
    fn pattern_matching() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::Bool)]);
        let analyzer = Analyzer::new(data_definition);

        assert_eq!(
            analyzer.analyze(select_value_as_expression_with_operation(
                string("s"),
                sql_ast::BinaryOperator::Like,
                string("str")
            )),
            Ok(QueryAnalysis::Read(SelectQuery {
                full_table_id: FullTableId::from((schema_id, table_id)),
                projection_items: vec![ProjectionTreeNode::Operation {
                    left: Box::new(ProjectionTreeNode::Item(Operator::Const(ScalarValue::String(
                        "s".to_owned()
                    )))),
                    op: Operation::PatternMatching(PatternMatching::Like),
                    right: Box::new(ProjectionTreeNode::Item(Operator::Const(ScalarValue::String(
                        "str".to_owned()
                    ))))
                }],
            }))
        );
    }
}
