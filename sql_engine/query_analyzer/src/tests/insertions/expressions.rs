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

use super::*;

fn insert_with_parameters(schema_name: &str, table_name: &str, parameters: Vec<u32>) -> Query {
    insert_with_values(
        schema_name,
        table_name,
        vec![parameters.into_iter().map(Expr::Param).collect()],
    )
}

#[test]
fn insert_number() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::small_int())]))
        .unwrap();
    let analyzer = QueryAnalyzer::new(database);

    assert_eq!(
        analyzer.analyze(insert_with_values(SCHEMA, TABLE, vec![vec![small_int(1)]])),
        Ok(UntypedQuery::Insert(UntypedInsertQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            values: vec![vec![Some(StaticUntypedTree::Item(StaticUntypedItem::Const(
                UntypedValue::Number(BigDecimal::from(1))
            )))]],
        }))
    );
}

#[test]
fn insert_string() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::char(5))]))
        .unwrap();
    let analyzer = QueryAnalyzer::new(database);

    assert_eq!(
        analyzer.analyze(insert_with_values(SCHEMA, TABLE, vec![vec![string("str")]])),
        Ok(UntypedQuery::Insert(UntypedInsertQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            values: vec![vec![Some(StaticUntypedTree::Item(StaticUntypedItem::Const(
                UntypedValue::String("str".to_owned())
            )))]],
        }))
    );
}

#[test]
fn insert_boolean() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())]))
        .unwrap();
    let analyzer = QueryAnalyzer::new(database);

    assert_eq!(
        analyzer.analyze(insert_with_values(SCHEMA, TABLE, vec![vec![boolean(true)]])),
        Ok(UntypedQuery::Insert(UntypedInsertQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            values: vec![vec![Some(StaticUntypedTree::Item(StaticUntypedItem::Const(
                UntypedValue::Bool(Bool(true))
            )))]],
        }))
    );
}

#[test]
fn insert_null() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())]))
        .unwrap();
    let analyzer = QueryAnalyzer::new(database);

    assert_eq!(
        analyzer.analyze(insert_with_values(SCHEMA, TABLE, vec![vec![null()]])),
        Ok(UntypedQuery::Insert(UntypedInsertQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            values: vec![vec![Some(StaticUntypedTree::Item(StaticUntypedItem::Const(
                UntypedValue::Null
            )))]],
        }))
    );
}

#[test]
fn insert_identifier() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::small_int())]))
        .unwrap();
    let analyzer = QueryAnalyzer::new(database);

    assert_eq!(
        analyzer.analyze(insert_with_values(
            SCHEMA,
            TABLE,
            vec![vec![Expr::Column("col".to_owned())]]
        )),
        Err(AnalysisError::column_cant_be_referenced(&"col"))
    );
}

#[test]
fn insert_into_table_with_parameters() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_ops(
            SCHEMA,
            TABLE,
            vec![("col_1", SqlType::small_int()), ("col_2", SqlType::small_int())],
        ))
        .unwrap();
    let analyzer = QueryAnalyzer::new(database);

    assert_eq!(
        analyzer.analyze(insert_with_parameters(SCHEMA, TABLE, vec![1, 2])),
        Ok(UntypedQuery::Insert(UntypedInsertQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            values: vec![vec![
                Some(StaticUntypedTree::Item(StaticUntypedItem::Param(0))),
                Some(StaticUntypedTree::Item(StaticUntypedItem::Param(1)))
            ]],
        }))
    );
}

#[test]
fn insert_into_table_with_parameters_and_values() {
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_ops(
            SCHEMA,
            TABLE,
            vec![("col_1", SqlType::small_int()), ("col_2", SqlType::small_int())],
        ))
        .unwrap();
    let analyzer = QueryAnalyzer::new(database);

    assert_eq!(
        analyzer.analyze(insert_with_values(
            SCHEMA,
            TABLE,
            vec![vec![Expr::Param(1), Expr::Value(number(1))]]
        )),
        Ok(UntypedQuery::Insert(UntypedInsertQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            values: vec![vec![
                Some(StaticUntypedTree::Item(StaticUntypedItem::Param(0))),
                Some(StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::Number(
                    BigDecimal::from(1)
                ))))
            ]],
        }))
    );
}

fn setup_logger() {
    if let Ok(()) = simple_logger::SimpleLogger::new().init() {};
}

#[test]
fn insert_into_table_negative_number() {
    setup_logger();
    let database = InMemoryDatabase::new();
    database.execute(create_schema_ops(SCHEMA)).unwrap();
    database
        .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::small_int())]))
        .unwrap();
    let analyzer = QueryAnalyzer::new(database);

    assert_eq!(
        analyzer.analyze(insert_with_values(SCHEMA, TABLE, vec![vec![small_int(-32768)]])),
        Ok(UntypedQuery::Insert(UntypedInsertQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            values: vec![vec![Some(StaticUntypedTree::Item(StaticUntypedItem::Const(
                UntypedValue::Number(BigDecimal::from(-32768))
            )))]],
        }))
    );
}

#[cfg(test)]
mod multiple_values {
    use data_manipulation_untyped_tree::{StaticUntypedItem, StaticUntypedTree, UntypedValue};

    use super::*;

    fn insert_value_as_expression_with_operation(left: Expr, op: BinaryOperator, right: Expr) -> Query {
        insert_with_values(
            SCHEMA,
            TABLE,
            vec![vec![Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }]],
        )
    }

    #[test]
    fn arithmetic() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        database
            .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::small_int())]))
            .unwrap();
        let analyzer = QueryAnalyzer::new(database);

        assert_eq!(
            analyzer.analyze(insert_value_as_expression_with_operation(
                Expr::Value(number(1)),
                BinaryOperator::Plus,
                Expr::Value(number(1))
            )),
            Ok(UntypedQuery::Insert(UntypedInsertQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                values: vec![vec![Some(StaticUntypedTree::BiOp {
                    left: Box::new(StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::Number(
                        BigDecimal::from(1)
                    )))),
                    op: BiOperator::Arithmetic(BiArithmetic::Add),
                    right: Box::new(StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::Number(
                        BigDecimal::from(1)
                    ))))
                })]],
            }))
        );
    }

    #[test]
    fn string_operation() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        database
            .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::var_char(255))]))
            .unwrap();
        let analyzer = QueryAnalyzer::new(database);

        assert_eq!(
            analyzer.analyze(insert_value_as_expression_with_operation(
                string("str"),
                BinaryOperator::StringConcat,
                string("str")
            )),
            Ok(UntypedQuery::Insert(UntypedInsertQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                values: vec![vec![Some(StaticUntypedTree::BiOp {
                    left: Box::new(StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::String(
                        "str".to_owned()
                    )))),
                    op: BiOperator::StringOp(Concat),
                    right: Box::new(StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::String(
                        "str".to_owned()
                    ))))
                })]],
            }))
        );
    }

    #[test]
    fn comparison() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        database
            .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())]))
            .unwrap();
        let analyzer = QueryAnalyzer::new(database);

        assert_eq!(
            analyzer.analyze(insert_value_as_expression_with_operation(
                Expr::Value(number(1)),
                BinaryOperator::Gt,
                Expr::Value(number(1))
            )),
            Ok(UntypedQuery::Insert(UntypedInsertQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                values: vec![vec![Some(StaticUntypedTree::BiOp {
                    left: Box::new(StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::Number(
                        BigDecimal::from(1)
                    )))),
                    op: BiOperator::Comparison(Comparison::Gt),
                    right: Box::new(StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::Number(
                        BigDecimal::from(1)
                    ))))
                })]],
            }))
        );
    }

    #[test]
    fn logical() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        database
            .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())]))
            .unwrap();
        let analyzer = QueryAnalyzer::new(database);

        assert_eq!(
            analyzer.analyze(insert_value_as_expression_with_operation(
                Expr::Value(Value::Boolean(true)),
                BinaryOperator::And,
                Expr::Value(Value::Boolean(true)),
            )),
            Ok(UntypedQuery::Insert(UntypedInsertQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                values: vec![vec![Some(StaticUntypedTree::BiOp {
                    left: Box::new(StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::Bool(
                        Bool(true)
                    )))),
                    op: BiOperator::Logical(BiLogical::And),
                    right: Box::new(StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::Bool(
                        Bool(true)
                    )))),
                })]],
            }))
        );
    }

    #[test]
    fn bitwise() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        database
            .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::small_int())]))
            .unwrap();
        let analyzer = QueryAnalyzer::new(database);

        assert_eq!(
            analyzer.analyze(insert_value_as_expression_with_operation(
                Expr::Value(number(1)),
                BinaryOperator::BitwiseOr,
                Expr::Value(number(1))
            )),
            Ok(UntypedQuery::Insert(UntypedInsertQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                values: vec![vec![Some(StaticUntypedTree::BiOp {
                    left: Box::new(StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::Number(
                        BigDecimal::from(1)
                    )))),
                    op: BiOperator::Bitwise(Bitwise::Or),
                    right: Box::new(StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::Number(
                        BigDecimal::from(1)
                    ))))
                })]],
            }))
        );
    }

    #[test]
    fn pattern_matching() {
        let database = InMemoryDatabase::new();
        database.execute(create_schema_ops(SCHEMA)).unwrap();
        database
            .execute(create_table_ops(SCHEMA, TABLE, vec![("col", SqlType::bool())]))
            .unwrap();
        let analyzer = QueryAnalyzer::new(database);

        assert_eq!(
            analyzer.analyze(insert_value_as_expression_with_operation(
                string("s"),
                BinaryOperator::Like,
                string("str")
            )),
            Ok(UntypedQuery::Insert(UntypedInsertQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                values: vec![vec![Some(StaticUntypedTree::BiOp {
                    left: Box::new(StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::String(
                        "s".to_owned()
                    )))),
                    op: BiOperator::Matching(Matching::Like),
                    right: Box::new(StaticUntypedTree::Item(StaticUntypedItem::Const(UntypedValue::String(
                        "str".to_owned()
                    ))))
                })]],
            }))
        );
    }
}
