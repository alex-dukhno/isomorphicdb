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

#[test]
fn select_all_columns_from_table() {
    let db = Database::new("");
    let transaction = db.transaction();
    let catalog = CatalogHandler::from(transaction.clone());
    catalog.apply(create_schema_ops(SCHEMA)).unwrap();
    catalog
        .apply(create_table_ops(SCHEMA, TABLE, vec![("col1", SqlType::integer())]))
        .unwrap();

    let analyzer = QueryAnalyzer::from(transaction);

    assert_eq!(
        analyzer.analyze(select(SCHEMA, TABLE)),
        Ok(UntypedQuery::Select(UntypedSelectQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            projection_items: vec![UntypedTree::Item(UntypedItem::Column {
                name: "col1".to_owned(),
                index: 0,
                sql_type: SqlType::integer()
            })],
            filter: None
        }))
    );
}

#[test]
fn select_specified_column_from_table() {
    let db = Database::new("");
    let transaction = db.transaction();
    let catalog = CatalogHandler::from(transaction.clone());
    catalog.apply(create_schema_ops(SCHEMA)).unwrap();
    catalog
        .apply(create_table_ops(SCHEMA, TABLE, vec![("col1", SqlType::integer())]))
        .unwrap();

    let analyzer = QueryAnalyzer::from(transaction);

    assert_eq!(
        analyzer.analyze(select_with_columns(
            SCHEMA,
            TABLE,
            vec![SelectItem::UnnamedExpr(Expr::Column("col1".to_owned()))]
        )),
        Ok(UntypedQuery::Select(UntypedSelectQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            projection_items: vec![UntypedTree::Item(UntypedItem::Column {
                name: "col1".to_owned(),
                index: 0,
                sql_type: SqlType::integer()
            })],
            filter: None
        }))
    );
}

#[test]
fn select_column_that_is_not_in_table() {
    let db = Database::new("");
    let transaction = db.transaction();
    let catalog = CatalogHandler::from(transaction.clone());
    catalog.apply(create_schema_ops(SCHEMA)).unwrap();
    catalog
        .apply(create_table_ops(SCHEMA, TABLE, vec![("col1", SqlType::integer())]))
        .unwrap();

    let analyzer = QueryAnalyzer::from(transaction);

    assert_eq!(
        analyzer.analyze(select_with_columns(
            SCHEMA,
            TABLE,
            vec![SelectItem::UnnamedExpr(Expr::Column("col2".to_owned()))]
        )),
        Err(AnalysisError::column_not_found(&"col2"))
    );
}

#[test]
fn select_from_table_with_constant() {
    let db = Database::new("");
    let transaction = db.transaction();
    let catalog = CatalogHandler::from(transaction.clone());
    catalog.apply(create_schema_ops(SCHEMA)).unwrap();
    catalog
        .apply(create_table_ops(SCHEMA, TABLE, vec![("col1", SqlType::integer())]))
        .unwrap();

    let analyzer = QueryAnalyzer::from(transaction);

    assert_eq!(
        analyzer.analyze(select_with_columns(SCHEMA, TABLE, vec![SelectItem::UnnamedExpr(Expr::Value(number(1)))],)),
        Ok(UntypedQuery::Select(UntypedSelectQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            projection_items: vec![UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1)))],
            filter: None
        }))
    );
}

#[test]
fn select_parameters_from_a_table() {
    let db = Database::new("");
    let transaction = db.transaction();
    let catalog = CatalogHandler::from(transaction.clone());
    catalog.apply(create_schema_ops(SCHEMA)).unwrap();
    catalog
        .apply(create_table_ops(SCHEMA, TABLE, vec![("col1", SqlType::integer())]))
        .unwrap();

    let analyzer = QueryAnalyzer::from(transaction);

    assert_eq!(
        analyzer.analyze(select_with_columns(SCHEMA, TABLE, vec![SelectItem::UnnamedExpr(Expr::Param(1))],)),
        Ok(UntypedQuery::Select(UntypedSelectQuery {
            full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
            projection_items: vec![UntypedTree::Item(UntypedItem::Param(0))],
            filter: None
        }))
    );
}

#[cfg(test)]
mod multiple_values {
    use super::*;

    fn select_value_as_expression_with_operation(left: Expr, op: BinaryOperator, right: Expr) -> Query {
        select_with_columns(
            SCHEMA,
            TABLE,
            vec![SelectItem::UnnamedExpr(Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })],
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
            analyzer.analyze(select_value_as_expression_with_operation(
                string("1"),
                BinaryOperator::Plus,
                Expr::Value(number(1))
            )),
            Ok(UntypedQuery::Select(UntypedSelectQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                projection_items: vec![UntypedTree::BiOp {
                    left: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("1".to_owned())))),
                    op: BiOperator::Arithmetic(BiArithmetic::Add),
                    right: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))))
                }],
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
            analyzer.analyze(select_value_as_expression_with_operation(
                string("str"),
                BinaryOperator::StringConcat,
                string("str")
            )),
            Ok(UntypedQuery::Select(UntypedSelectQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                projection_items: vec![UntypedTree::BiOp {
                    left: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("str".to_owned())))),
                    op: BiOperator::StringOp(Concat),
                    right: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("str".to_owned()))))
                }],
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
            analyzer.analyze(select_value_as_expression_with_operation(
                string("1"),
                BinaryOperator::Gt,
                Expr::Value(number(1))
            )),
            Ok(UntypedQuery::Select(UntypedSelectQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                projection_items: vec![UntypedTree::BiOp {
                    left: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("1".to_owned())))),
                    op: BiOperator::Comparison(Comparison::Gt),
                    right: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))))
                }],
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
            analyzer.analyze(select_value_as_expression_with_operation(
                boolean(true),
                BinaryOperator::And,
                boolean(true),
            )),
            Ok(UntypedQuery::Select(UntypedSelectQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                projection_items: vec![UntypedTree::BiOp {
                    left: Box::new(UntypedTree::UnOp {
                        op: UnOperator::Cast(SqlType::Bool),
                        item: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("t".to_owned()))))
                    }),
                    op: BiOperator::Logical(BiLogical::And),
                    right: Box::new(UntypedTree::UnOp {
                        op: UnOperator::Cast(SqlType::Bool),
                        item: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("t".to_owned()))))
                    }),
                }],
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
            analyzer.analyze(select_value_as_expression_with_operation(
                Expr::Value(number(1)),
                BinaryOperator::BitwiseOr,
                Expr::Value(number(1))
            )),
            Ok(UntypedQuery::Select(UntypedSelectQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                projection_items: vec![UntypedTree::BiOp {
                    left: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1)))),
                    op: BiOperator::Bitwise(Bitwise::Or),
                    right: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Int(1))))
                }],
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
            analyzer.analyze(select_value_as_expression_with_operation(
                string("s"),
                BinaryOperator::Like,
                string("str")
            )),
            Ok(UntypedQuery::Select(UntypedSelectQuery {
                full_table_name: FullTableName::from((&SCHEMA, &TABLE)),
                projection_items: vec![UntypedTree::BiOp {
                    left: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("s".to_owned())))),
                    op: BiOperator::Matching(Matching::Like),
                    right: Box::new(UntypedTree::Item(UntypedItem::Const(UntypedValue::Literal("str".to_owned()))))
                }],
                filter: None
            }))
        );
    }
}
