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
fn insert_int() {
    let statements = QUERY_PARSER.parse("insert into schema_name.table_name values (123);");

    assert_eq!(
        statements,
        Ok(vec![Some(Statement::Query(Query::Insert(InsertStatement {
            schema_name: "schema_name".to_owned(),
            table_name: "table_name".to_owned(),
            columns: vec![],
            source: InsertSource::Values(Values(vec![vec![Expr::Value(Value::Int(123))]]))
        })))])
    );
}

#[test]
fn insert_string() {
    let statements = QUERY_PARSER.parse("insert into schema_name.table_name values ('abc');");

    assert_eq!(
        statements,
        Ok(vec![Some(Statement::Query(Query::Insert(InsertStatement {
            schema_name: "schema_name".to_owned(),
            table_name: "table_name".to_owned(),
            columns: vec![],
            source: InsertSource::Values(Values(vec![vec![Expr::Value(Value::String("abc".to_owned()))]]))
        })))])
    );
}

#[cfg(test)]
mod operators {
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
    fn binary(expected: BinaryOperator, tested: &str) {
        let statements =
            QUERY_PARSER.parse(format!("insert into schema_name.table_name values (123 {} 456);", tested).as_str());

        assert_eq!(
            statements,
            Ok(vec![Some(Statement::Query(Query::Insert(InsertStatement {
                schema_name: "schema_name".to_owned(),
                table_name: "table_name".to_owned(),
                columns: vec![],
                source: InsertSource::Values(Values(vec![vec![Expr::BinaryOp {
                    left: Box::new(Expr::Value(Value::Int(123))),
                    op: expected,
                    right: Box::new(Expr::Value(Value::Int(456)))
                }]]))
            })))])
        );
    }

    #[rstest::rstest(
        expected,
        tested,
        case::plus(UnaryOperator::Plus, "+"),
        case::minus(UnaryOperator::Minus, "-"),
        case::not(UnaryOperator::Not, "!"),
        case::bitwise_not(UnaryOperator::BitwiseNot, "~"),
        case::square_root(UnaryOperator::SquareRoot, "|/"),
        case::cube_root(UnaryOperator::CubeRoot, "||/"),
        case::prefix_factorial(UnaryOperator::PrefixFactorial, "!!"),
        case::abs(UnaryOperator::Abs, "@")
    )]
    fn prefix_unary(expected: UnaryOperator, tested: &str) {
        let statements =
            QUERY_PARSER.parse(format!("insert into schema_name.table_name values ({}(123 + 456));", tested).as_str());

        assert_eq!(
            statements,
            Ok(vec![Some(Statement::Query(Query::Insert(InsertStatement {
                schema_name: "schema_name".to_owned(),
                table_name: "table_name".to_owned(),
                columns: vec![],
                source: InsertSource::Values(Values(vec![vec![Expr::UnaryOp {
                    op: expected,
                    expr: Box::new(Expr::BinaryOp {
                        left: Box::new(Expr::Value(Value::Int(123))),
                        op: BinaryOperator::Plus,
                        right: Box::new(Expr::Value(Value::Int(456)))
                    })
                }]]))
            })))])
        );
    }

    #[test]
    fn postfix_factorial_unary() {
        let statements = QUERY_PARSER.parse("insert into schema_name.table_name values (456!);");

        assert_eq!(
            statements,
            Ok(vec![Some(Statement::Query(Query::Insert(InsertStatement {
                schema_name: "schema_name".to_owned(),
                table_name: "table_name".to_owned(),
                columns: vec![],
                source: InsertSource::Values(Values(vec![vec![Expr::UnaryOp {
                    op: UnaryOperator::PostfixFactorial,
                    expr: Box::new(Expr::Value(Value::Int(456)))
                }]]))
            })))])
        );
    }
}

#[test]
fn insert_with_columns() {
    let statements = QUERY_PARSER.parse("insert into schema_name.table_name (col1) values (123);");

    assert_eq!(
        statements,
        Ok(vec![Some(Statement::Query(Query::Insert(InsertStatement {
            schema_name: "schema_name".to_owned(),
            table_name: "table_name".to_owned(),
            columns: vec!["col1".to_owned()],
            source: InsertSource::Values(Values(vec![vec![Expr::Value(Value::Int(123))]]))
        })))])
    );
}

#[test]
fn insert_params() {
    let statements = QUERY_PARSER.parse("insert into schema_name.table_name (col1) values ($1);");

    assert_eq!(
        statements,
        Ok(vec![Some(Statement::Query(Query::Insert(InsertStatement {
            schema_name: "schema_name".to_owned(),
            table_name: "table_name".to_owned(),
            columns: vec!["col1".to_owned()],
            source: InsertSource::Values(Values(vec![vec![Expr::Param(1)]]))
        })))])
    );
}

#[test]
fn insert_column() {
    let statements = QUERY_PARSER.parse("insert into schema_name.table_name (col1) values (col2);");

    assert_eq!(
        statements,
        Ok(vec![Some(Statement::Query(Query::Insert(InsertStatement {
            schema_name: "schema_name".to_owned(),
            table_name: "table_name".to_owned(),
            columns: vec!["col1".to_owned()],
            source: InsertSource::Values(Values(vec![vec![Expr::Column("col2".to_owned())]]))
        })))])
    );
}

#[test]
fn insert_int_max() {
    let statements = QUERY_PARSER.parse(
        "insert into schema_name.table_name \
    values (32767, -32768, 2147483647, -2147483648, 9223372036854775807, -9223372036854775808);",
    );

    assert_eq!(
        statements,
        Ok(vec![Some(Statement::Query(Query::Insert(InsertStatement {
            schema_name: "schema_name".to_owned(),
            table_name: "table_name".to_owned(),
            columns: vec![],
            source: InsertSource::Values(Values(vec![vec![
                Expr::Value(Value::Int(32767)),
                Expr::Value(Value::Int(-32768)),
                Expr::Value(Value::Int(2147483647)),
                Expr::Value(Value::Number("-2147483648".to_owned())),
                Expr::Value(Value::Number("9223372036854775807".to_owned())),
                Expr::Value(Value::Number("-9223372036854775808".to_owned())),
            ]]))
        })))])
    );
}

#[test]
fn cast() {
    let statements = QUERY_PARSER.parse("insert into schema_name.table_name values('true'::boolean);");

    assert_eq!(
        statements,
        Ok(vec![Some(Statement::Query(Query::Insert(InsertStatement {
            schema_name: "schema_name".to_owned(),
            table_name: "table_name".to_owned(),
            columns: vec![],
            source: InsertSource::Values(Values(vec![vec![Expr::Cast {
                expr: Box::new(Expr::Value(Value::String("true".to_owned()))),
                data_type: DataType::Bool
            }]]))
        })))])
    );
}
