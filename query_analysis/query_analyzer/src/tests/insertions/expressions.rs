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

fn insert_with_parameters(full_name: Vec<&'static str>, parameters: Vec<&'static str>) -> sql_ast::Statement {
    insert_with_values(
        full_name,
        vec![parameters
            .into_iter()
            .map(ident)
            .map(sql_ast::Expr::Identifier)
            .collect()],
    )
}

#[test]
fn insert_number() {
    let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::SmallInt)]);
    let database = InMemoryDatabase::new();
    database.execute(create_schema(SCHEMA));
    database.execute(create_table(SCHEMA, TABLE, vec![("col", SqlType::SmallInt)]));
    let analyzer = Analyzer::new(data_definition, database);

    assert_eq!(
        analyzer.analyze(insert_with_values(vec![SCHEMA, TABLE], vec![vec![small_int(1)]])),
        Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
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
    let database = InMemoryDatabase::new();
    database.execute(create_schema(SCHEMA));
    database.execute(create_table(SCHEMA, TABLE, vec![("col", SqlType::Char(5))]));
    let analyzer = Analyzer::new(data_definition, database);

    assert_eq!(
        analyzer.analyze(insert_with_values(vec![SCHEMA, TABLE], vec![vec![string("str")]])),
        Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
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
    let database = InMemoryDatabase::new();
    database.execute(create_schema(SCHEMA));
    database.execute(create_table(SCHEMA, TABLE, vec![("col", SqlType::Bool)]));
    let analyzer = Analyzer::new(data_definition, database);

    assert_eq!(
        analyzer.analyze(insert_with_values(vec![SCHEMA, TABLE], vec![vec![boolean(true)]])),
        Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
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
    let database = InMemoryDatabase::new();
    database.execute(create_schema(SCHEMA));
    database.execute(create_table(SCHEMA, TABLE, vec![("col", SqlType::Bool)]));
    let analyzer = Analyzer::new(data_definition, database);

    assert_eq!(
        analyzer.analyze(insert_with_values(vec![SCHEMA, TABLE], vec![vec![null()]])),
        Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_types: vec![SqlType::Bool],
            values: vec![vec![InsertTreeNode::Item(Operator::Const(ScalarValue::Null))]],
        })))
    );
}

#[test]
fn insert_identifier() {
    let (data_definition, _schema_id, _table_id) = with_table(&[ColumnDefinition::new("col", SqlType::SmallInt)]);
    let database = InMemoryDatabase::new();
    database.execute(create_schema(SCHEMA));
    database.execute(create_table(SCHEMA, TABLE, vec![("col", SqlType::SmallInt)]));
    let analyzer = Analyzer::new(data_definition, database);

    assert_eq!(
        analyzer.analyze(insert_with_values(
            vec![SCHEMA, TABLE],
            vec![vec![sql_ast::Expr::Identifier(ident("col"))]]
        )),
        Err(AnalysisError::column_cant_be_referenced(&"col"))
    );
}

#[test]
fn insert_into_table_with_parameters() {
    let (data_definition, schema_id, table_id) = with_table(&[
        ColumnDefinition::new("col_1", SqlType::SmallInt),
        ColumnDefinition::new("col_2", SqlType::SmallInt),
    ]);
    let database = InMemoryDatabase::new();
    database.execute(create_schema(SCHEMA));
    database.execute(create_table(
        SCHEMA,
        TABLE,
        vec![("col_1", SqlType::SmallInt), ("col_2", SqlType::SmallInt)],
    ));
    let analyzer = Analyzer::new(data_definition, database);

    assert_eq!(
        analyzer.analyze(insert_with_parameters(vec![SCHEMA, TABLE], vec!["$1", "$2"])),
        Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_types: vec![SqlType::SmallInt, SqlType::SmallInt],
            values: vec![vec![
                InsertTreeNode::Item(Operator::Param(0)),
                InsertTreeNode::Item(Operator::Param(1))
            ]],
        })))
    );
}

#[test]
fn insert_into_table_with_parameters_and_values() {
    let (data_definition, schema_id, table_id) = with_table(&[
        ColumnDefinition::new("col_1", SqlType::SmallInt),
        ColumnDefinition::new("col_2", SqlType::SmallInt),
    ]);
    let database = InMemoryDatabase::new();
    database.execute(create_schema(SCHEMA));
    database.execute(create_table(
        SCHEMA,
        TABLE,
        vec![("col_1", SqlType::SmallInt), ("col_2", SqlType::SmallInt)],
    ));
    let analyzer = Analyzer::new(data_definition, database);

    assert_eq!(
        analyzer.analyze(insert_with_values(
            vec![SCHEMA, TABLE],
            vec![vec![
                sql_ast::Expr::Identifier(ident("$1")),
                sql_ast::Expr::Value(number(1))
            ]]
        )),
        Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            column_types: vec![SqlType::SmallInt, SqlType::SmallInt],
            values: vec![vec![
                InsertTreeNode::Item(Operator::Param(0)),
                InsertTreeNode::Item(Operator::Const(ScalarValue::Number(BigDecimal::from(1))))
            ]],
        })))
    );
}

// #[cfg(test)]
// mod implicit_cast {
//     use super::*;
//
//     #[test]
//     fn string_to_boolean() {
//         let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::Bool)]);
//         let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());
//
//         assert_eq!(
//             analyzer.analyze(insert_with_values(
//                 vec![SCHEMA, TABLE],
//                 vec![vec![sql_ast::Expr::Value(sql_ast::Value::SingleQuotedString(
//                     "t".to_owned()
//                 ))]]
//             )),
//             Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
//                 full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
//                 column_types: vec![SqlType::Bool],
//                 values: vec![vec![InsertTreeNode::Item(Operator::Const(ScalarValue::Bool(Bool(
//                     true
//                 ))))]],
//             })))
//         );
//     }
//
//     #[test]
//     fn boolean_to_string() {
//         let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::VarChar(5))]);
//         let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());
//
//         assert_eq!(
//             analyzer.analyze(insert_with_values(
//                 vec![SCHEMA, TABLE],
//                 vec![vec![sql_ast::Expr::Value(sql_ast::Value::Boolean(true))]]
//             )),
//             Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
//                 full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
//                 column_types: vec![SqlType::VarChar(5)],
//                 values: vec![vec![InsertTreeNode::Item(Operator::Const(ScalarValue::String(
//                     "true".to_string()
//                 )))]],
//             })))
//         );
//     }
//
//     #[test]
//     fn boolean_to_string_not_enough_length() {
//         let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::Char(1))]);
//         let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());
//
//         assert_eq!(
//             analyzer.analyze(insert_with_values(
//                 vec![SCHEMA, TABLE],
//                 vec![vec![sql_ast::Expr::Value(sql_ast::Value::Boolean(true))]]
//             )),
//             Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
//                 full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
//                 column_types: vec![SqlType::Char(1)],
//                 values: vec![vec![InsertTreeNode::Item(Operator::Const(ScalarValue::String(
//                     "true".to_owned()
//                 )))]]
//             })))
//         );
//     }
//
//     #[test]
//     fn string_to_number() {
//         let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::SmallInt)]);
//         let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());
//
//         assert_eq!(
//             analyzer.analyze(insert_with_values(
//                 vec![SCHEMA, TABLE],
//                 vec![vec![sql_ast::Expr::Value(sql_ast::Value::SingleQuotedString(
//                     "100".to_owned()
//                 ))]]
//             )),
//             Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
//                 full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
//                 column_types: vec![SqlType::SmallInt],
//                 values: vec![vec![InsertTreeNode::Item(Operator::Const(ScalarValue::Number(
//                     BigDecimal::from(100)
//                 )))]],
//             })))
//         );
//     }
//
//     #[test]
//     fn number_to_string_not_enough_length() {
//         let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::Char(1))]);
//         let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());
//
//         assert_eq!(
//             analyzer.analyze(insert_with_values(
//                 vec![SCHEMA, TABLE],
//                 vec![vec![sql_ast::Expr::Value(sql_ast::Value::Number(BigDecimal::from(
//                     123
//                 )))]]
//             )),
//             Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
//                 full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
//                 column_types: vec![SqlType::Char(1)],
//                 values: vec![vec![InsertTreeNode::Item(Operator::Const(ScalarValue::String(
//                     "123".to_owned()
//                 )))]]
//             })))
//         );
//     }
//
//     #[test]
//     fn number_to_boolean() {
//         let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::Bool)]);
//         let analyzer = Analyzer::new(data_definition, InMemoryDatabase::new());
//
//         assert_eq!(
//             analyzer.analyze(insert_with_values(
//                 vec![SCHEMA, TABLE],
//                 vec![vec![sql_ast::Expr::Value(sql_ast::Value::Number(BigDecimal::from(0)))]]
//             )),
//             Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
//                 full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
//                 column_types: vec![SqlType::Bool],
//                 values: vec![vec![InsertTreeNode::Item(Operator::Const(ScalarValue::Bool(Bool(
//                     false
//                 ))))]]
//             })))
//         );
//     }
// }

#[cfg(test)]
mod multiple_values {
    use super::*;

    fn insert_value_as_expression_with_operation(
        left: sql_ast::Expr,
        op: sql_ast::BinaryOperator,
        right: sql_ast::Expr,
    ) -> sql_ast::Statement {
        insert_with_values(
            vec![SCHEMA, TABLE],
            vec![vec![sql_ast::Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }]],
        )
    }

    #[test]
    fn arithmetic() {
        let (data_definition, schema_id, table_id) = with_table(&[ColumnDefinition::new("col", SqlType::SmallInt)]);
        let database = InMemoryDatabase::new();
        database.execute(create_schema(SCHEMA));
        database.execute(create_table(SCHEMA, TABLE, vec![("col", SqlType::SmallInt)]));
        let analyzer = Analyzer::new(data_definition, database);

        assert_eq!(
            analyzer.analyze(insert_value_as_expression_with_operation(
                sql_ast::Expr::Value(number(1)),
                sql_ast::BinaryOperator::Plus,
                sql_ast::Expr::Value(number(1))
            )),
            Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
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
        let database = InMemoryDatabase::new();
        database.execute(create_schema(SCHEMA));
        database.execute(create_table(SCHEMA, TABLE, vec![("col", SqlType::VarChar(255))]));
        let analyzer = Analyzer::new(data_definition, database);

        assert_eq!(
            analyzer.analyze(insert_value_as_expression_with_operation(
                string("str"),
                sql_ast::BinaryOperator::StringConcat,
                string("str")
            )),
            Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
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
        let database = InMemoryDatabase::new();
        database.execute(create_schema(SCHEMA));
        database.execute(create_table(SCHEMA, TABLE, vec![("col", SqlType::Bool)]));
        let analyzer = Analyzer::new(data_definition, database);

        assert_eq!(
            analyzer.analyze(insert_value_as_expression_with_operation(
                sql_ast::Expr::Value(number(1)),
                sql_ast::BinaryOperator::Gt,
                sql_ast::Expr::Value(number(1))
            )),
            Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
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
        let database = InMemoryDatabase::new();
        database.execute(create_schema(SCHEMA));
        database.execute(create_table(SCHEMA, TABLE, vec![("col", SqlType::Bool)]));
        let analyzer = Analyzer::new(data_definition, database);

        assert_eq!(
            analyzer.analyze(insert_value_as_expression_with_operation(
                sql_ast::Expr::Value(sql_ast::Value::Boolean(true)),
                sql_ast::BinaryOperator::And,
                sql_ast::Expr::Value(sql_ast::Value::Boolean(true)),
            )),
            Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
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
        let database = InMemoryDatabase::new();
        database.execute(create_schema(SCHEMA));
        database.execute(create_table(SCHEMA, TABLE, vec![("col", SqlType::SmallInt)]));
        let analyzer = Analyzer::new(data_definition, database);

        assert_eq!(
            analyzer.analyze(insert_value_as_expression_with_operation(
                sql_ast::Expr::Value(number(1)),
                sql_ast::BinaryOperator::BitwiseOr,
                sql_ast::Expr::Value(number(1))
            )),
            Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
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
        let database = InMemoryDatabase::new();
        database.execute(create_schema(SCHEMA));
        database.execute(create_table(SCHEMA, TABLE, vec![("col", SqlType::Bool)]));
        let analyzer = Analyzer::new(data_definition, database);

        assert_eq!(
            analyzer.analyze(insert_value_as_expression_with_operation(
                string("s"),
                sql_ast::BinaryOperator::Like,
                string("str")
            )),
            Ok(QueryAnalysis::Write(Write::Insert(InsertQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
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
        let database = InMemoryDatabase::new();
        database.execute(create_schema(SCHEMA));
        database.execute(create_table(SCHEMA, TABLE, vec![("col", SqlType::SmallInt)]));
        let analyzer = Analyzer::new(data_definition, database);

        assert_eq!(
            analyzer.analyze(insert_with_values(
                vec![SCHEMA, TABLE],
                vec![vec![sql_ast::Expr::Value(sql_ast::Value::NationalStringLiteral(
                    "str".to_owned()
                ))]]
            )),
            Err(AnalysisError::FeatureNotSupported(Feature::NationalStringLiteral))
        );
    }

    #[test]
    fn hex_strings() {
        let (data_definition, _schema_id, _table_id) = with_table(&[ColumnDefinition::new("col", SqlType::SmallInt)]);
        let database = InMemoryDatabase::new();
        database.execute(create_schema(SCHEMA));
        database.execute(create_table(SCHEMA, TABLE, vec![("col", SqlType::SmallInt)]));
        let analyzer = Analyzer::new(data_definition, database);

        assert_eq!(
            analyzer.analyze(insert_with_values(
                vec![SCHEMA, TABLE],
                vec![vec![sql_ast::Expr::Value(sql_ast::Value::HexStringLiteral(
                    "str".to_owned()
                ))]]
            )),
            Err(AnalysisError::FeatureNotSupported(Feature::HexStringLiteral))
        );
    }

    #[test]
    fn time_intervals() {
        let (data_definition, _schema_id, _table_id) = with_table(&[ColumnDefinition::new("col", SqlType::SmallInt)]);
        let database = InMemoryDatabase::new();
        database.execute(create_schema(SCHEMA));
        database.execute(create_table(SCHEMA, TABLE, vec![("col", SqlType::SmallInt)]));
        let analyzer = Analyzer::new(data_definition, database);

        assert_eq!(
            analyzer.analyze(insert_with_values(
                vec![SCHEMA, TABLE],
                vec![vec![sql_ast::Expr::Value(sql_ast::Value::Interval {
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
