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
fn update_int() {
    let statements = QUERY_PARSER.parse("update schema_name.table_name set col1 = 123;");

    assert_eq!(
        statements,
        Ok(Request::Statement(Statement::Query(Query::Update(UpdateQuery {
            schema_name: "schema_name".to_owned(),
            table_name: "table_name".to_owned(),
            assignments: vec![Assignment {
                column: "col1".to_owned(),
                value: Expr::Value(Value::Int(123))
            }],
            where_clause: None
        }))))
    );
}

#[test]
fn update_string() {
    let statements = QUERY_PARSER.parse("update schema_name.table_name set col1 = 'abc';");

    assert_eq!(
        statements,
        Ok(Request::Statement(Statement::Query(Query::Update(UpdateQuery {
            schema_name: "schema_name".to_owned(),
            table_name: "table_name".to_owned(),
            assignments: vec![Assignment {
                column: "col1".to_owned(),
                value: Expr::Value(Value::String("abc".to_owned()))
            }],
            where_clause: None
        }))))
    );
}

#[cfg(test)]
mod bi_ops {
    use super::*;

    #[rstest::rstest(
        expected,
        tested,
        case::plus(BinaryOperator::Plus, "+"),
        case::minus(BinaryOperator::Minus, "-"),
        case::multiply(BinaryOperator::Multiply, "*"),
        case::divide(BinaryOperator::Divide, "/"),
        case::modulus(BinaryOperator::Modulus, "%"),
        case::exp(BinaryOperator::Exp, "^"),
        case::string_concat(BinaryOperator::StringConcat, "||"),
        case::gt(BinaryOperator::Gt, ">"),
        case::lt(BinaryOperator::Lt, "<"),
        case::gt_eq(BinaryOperator::GtEq, ">="),
        case::lt_eq(BinaryOperator::LtEq, "<="),
        case::eq(BinaryOperator::Eq, "="),
        case::not_eq(BinaryOperator::NotEq, "<>"),
        case::and(BinaryOperator::And, "AND"),
        case::or(BinaryOperator::Or, "OR"),
        case::like(BinaryOperator::Like, "LIKE"),
        case::not_like(BinaryOperator::NotLike, "NOT LIKE"),
        case::bitwise_or(BinaryOperator::BitwiseOr, "|"),
        case::bitwise_and(BinaryOperator::BitwiseAnd, "&"),
        case::bitwise_xor(BinaryOperator::BitwiseXor, "#"),
        case::bitwise_shift_left(BinaryOperator::BitwiseShiftLeft, "<<"),
        case::bitwise_shift_right(BinaryOperator::BitwiseShiftRight, ">>")
    )]
    fn update_with_op(expected: BinaryOperator, tested: &str) {
        let statements = QUERY_PARSER.parse(format!("update schema_name.table_name set col1 = 123 {} 456;", tested).as_str());

        assert_eq!(
            statements,
            Ok(Request::Statement(Statement::Query(Query::Update(UpdateQuery {
                schema_name: "schema_name".to_owned(),
                table_name: "table_name".to_owned(),
                assignments: vec![Assignment {
                    column: "col1".to_owned(),
                    value: Expr::BinaryOp {
                        left: Box::new(Expr::Value(Value::Int(123))),
                        op: expected,
                        right: Box::new(Expr::Value(Value::Int(456)))
                    }
                }],
                where_clause: None
            }))))
        );
    }
}

#[test]
fn update_params() {
    let statements = QUERY_PARSER.parse("update schema_name.table_name set col1 = $1;");

    assert_eq!(
        statements,
        Ok(Request::Statement(Statement::Query(Query::Update(UpdateQuery {
            schema_name: "schema_name".to_owned(),
            table_name: "table_name".to_owned(),
            assignments: vec![Assignment {
                column: "col1".to_owned(),
                value: Expr::Param(1)
            }],
            where_clause: None,
        }))))
    );
}

#[test]
fn update_columns() {
    let statements = QUERY_PARSER.parse("update schema_name.table_name set col1 = col1 + 1;");

    assert_eq!(
        statements,
        Ok(Request::Statement(Statement::Query(Query::Update(UpdateQuery {
            schema_name: "schema_name".to_owned(),
            table_name: "table_name".to_owned(),
            assignments: vec![Assignment {
                column: "col1".to_owned(),
                value: Expr::BinaryOp {
                    left: Box::new(Expr::Column("col1".to_owned())),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expr::Value(Value::Int(1)))
                }
            }],
            where_clause: None,
        }))))
    );
}
