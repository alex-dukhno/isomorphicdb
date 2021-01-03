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

use sqlparser::ast::{Assignment, Expr, Ident, ObjectType, Query, Select, SetExpr, Statement};

pub struct ParamBinder;

impl ParamBinder {
    /// Replaces the parameters of prepared statement with values.
    #[allow(clippy::result_unit_err)]
    pub fn bind(&self, stmt: &mut Statement, params: &[Expr]) -> Result<(), ()> {
        match stmt {
            Statement::Insert { .. } => bind_insert(stmt, params),
            Statement::Update { .. } => bind_update(stmt, params),
            Statement::Query(_) => bind_select(stmt, params),
            Statement::SetVariable { .. } => Ok(()),
            Statement::CreateSchema { .. } => Ok(()),
            Statement::CreateTable { .. } => Ok(()),
            Statement::Drop { object_type, .. } if *object_type == ObjectType::Schema => Ok(()),
            Statement::Drop { object_type, .. } if *object_type == ObjectType::Table => Ok(()),
            _ => Err(()),
        }
    }
}

fn bind_insert(stmt: &mut Statement, params: &[Expr]) -> Result<(), ()> {
    let mut body = match stmt {
        Statement::Insert { source, .. } => {
            let source: &mut Query = source;
            let Query { body, .. } = source;
            body
        }
        _ => return Err(()),
    };

    if let SetExpr::Values(values) = &mut body {
        let values = &mut values.0;
        for line in values {
            for col in line {
                replace_expr_with_params(col, params);
            }
        }
    }

    log::debug!("bound insert SQL: {}", stmt);
    Ok(())
}

fn bind_select(stmt: &mut Statement, params: &[Expr]) -> Result<(), ()> {
    let mut body = match stmt {
        Statement::Query(query) => {
            let query: &mut Query = query;
            let Query { body, .. } = query;
            body
        }
        _ => return Err(()),
    };

    let selection = match &mut body {
        SetExpr::Select(select) => {
            let select: &mut Select = select;
            let Select { selection, .. } = select;
            selection
        }
        _ => return Ok(()),
    };

    if let Some(Expr::BinaryOp { left, right, .. }) = selection {
        let left: &mut Expr = left;
        replace_expr_with_params(left, params);
        let right: &mut Expr = right;
        replace_expr_with_params(right, params);
    }

    log::debug!("bound select SQL: {}", stmt);
    Ok(())
}

fn bind_update(stmt: &mut Statement, params: &[Expr]) -> Result<(), ()> {
    let assignments = match stmt {
        Statement::Update { assignments, .. } => assignments,
        _ => return Err(()),
    };

    for assignment in assignments {
        let Assignment { value, .. } = assignment;
        replace_expr_with_params(value, params);
    }

    log::debug!("bound update SQL: {}", stmt);
    Ok(())
}

fn parse_param_index(value: &str) -> Option<usize> {
    let mut chars = value.chars();
    if chars.next() != Some('$') || !chars.all(|c| c.is_digit(10)) {
        return None;
    }

    let index: usize = (&value[1..]).parse().unwrap();
    if index == 0 {
        return None;
    }

    Some(index - 1)
}

fn replace_expr_with_params(expr: &mut Expr, params: &[Expr]) {
    let value = match expr {
        Expr::Identifier(Ident { value, .. }) => value,
        _ => return,
    };

    let index = match parse_param_index(value) {
        Some(index) => index,
        _ => return,
    };

    if index < params.len() {
        *expr = params[index].clone();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigdecimal::BigDecimal;
    use sqlparser::ast::{BinaryOperator, ObjectName, SelectItem, TableFactor, TableWithJoins, Value, Values};

    fn ident<S: ToString>(name: S) -> Ident {
        Ident {
            value: name.to_string(),
            quote_style: None,
        }
    }

    #[test]
    fn bind_insert_statement() {
        let mut statement = Statement::Insert {
            table_name: ObjectName(vec![ident("schema_name"), ident("table_name")]),
            columns: vec![],
            source: Box::new(Query {
                with: None,
                body: SetExpr::Values(Values(vec![vec![
                    Expr::Identifier(ident("$1")),
                    Expr::Identifier(ident("$2")),
                ]])),
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None,
            }),
        };

        ParamBinder
            .bind(
                &mut statement,
                &[
                    Expr::Value(Value::Number(BigDecimal::from(1))),
                    Expr::Value(Value::SingleQuotedString("abc".into())),
                ],
            )
            .unwrap();

        assert_eq!(
            statement.to_string(),
            "INSERT INTO schema_name.table_name VALUES (1, 'abc')"
        );
    }

    #[test]
    fn bind_select_statement_with_both_parameters_of_binary_op() {
        let mut statement = Statement::Query(Box::new(Query {
            with: None,
            body: SetExpr::Select(Box::new(Select {
                distinct: false,
                top: None,
                projection: vec![SelectItem::Wildcard],
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![
                            Ident {
                                value: "schema_name".to_owned(),
                                quote_style: None,
                            },
                            Ident {
                                value: "table_name".to_owned(),
                                quote_style: None,
                            },
                        ]),
                        alias: None,
                        args: vec![],
                        with_hints: vec![],
                    },
                    joins: vec![],
                }],
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident {
                        value: "$2".to_owned(),
                        quote_style: None,
                    })),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::Identifier(Ident {
                        value: "$1".to_owned(),
                        quote_style: None,
                    })),
                }),
                group_by: vec![],
                having: None,
            })),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        }));

        ParamBinder
            .bind(
                &mut statement,
                &[
                    Expr::Value(Value::Number(BigDecimal::from(123))),
                    Expr::Value(Value::Number(BigDecimal::from(456))),
                ],
            )
            .unwrap();

        assert_eq!(
            statement.to_string(),
            "SELECT * FROM schema_name.table_name WHERE 456 > 123"
        );
    }

    #[test]
    fn bind_select_statement_with_left_parameter_of_binary_op() {
        let mut statement = Statement::Query(Box::new(Query {
            with: None,
            body: SetExpr::Select(Box::new(Select {
                distinct: false,
                top: None,
                projection: vec![SelectItem::Wildcard],
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![
                            Ident {
                                value: "schema_name".to_owned(),
                                quote_style: None,
                            },
                            Ident {
                                value: "table_name".to_owned(),
                                quote_style: None,
                            },
                        ]),
                        alias: None,
                        args: vec![],
                        with_hints: vec![],
                    },
                    joins: vec![],
                }],
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident {
                        value: "$1".to_owned(),
                        quote_style: None,
                    })),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::Identifier(Ident {
                        value: "column_1".to_owned(),
                        quote_style: None,
                    })),
                }),
                group_by: vec![],
                having: None,
            })),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        }));

        ParamBinder
            .bind(&mut statement, &[Expr::Value(Value::Number(BigDecimal::from(123)))])
            .unwrap();

        assert_eq!(
            statement.to_string(),
            "SELECT * FROM schema_name.table_name WHERE 123 > column_1"
        );
    }

    #[test]
    fn bind_select_statement_with_right_parameter_of_binary_op() {
        let mut statement = Statement::Query(Box::new(Query {
            with: None,
            body: SetExpr::Select(Box::new(Select {
                distinct: false,
                top: None,
                projection: vec![SelectItem::Wildcard],
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: ObjectName(vec![
                            Ident {
                                value: "schema_name".to_owned(),
                                quote_style: None,
                            },
                            Ident {
                                value: "table_name".to_owned(),
                                quote_style: None,
                            },
                        ]),
                        alias: None,
                        args: vec![],
                        with_hints: vec![],
                    },
                    joins: vec![],
                }],
                selection: Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident {
                        value: "column_1".to_owned(),
                        quote_style: None,
                    })),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::Identifier(Ident {
                        value: "$1".to_owned(),
                        quote_style: None,
                    })),
                }),
                group_by: vec![],
                having: None,
            })),
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
        }));

        ParamBinder
            .bind(&mut statement, &[Expr::Value(Value::Number(BigDecimal::from(123)))])
            .unwrap();

        assert_eq!(
            statement.to_string(),
            "SELECT * FROM schema_name.table_name WHERE column_1 > 123"
        );
    }

    #[test]
    fn bind_update_statement() {
        let mut statement = Statement::Update {
            table_name: ObjectName(vec![ident("schema_name"), ident("table_name")]),
            assignments: vec![
                Assignment {
                    id: ident("column_1"),
                    value: Expr::Identifier(ident("$1")),
                },
                Assignment {
                    id: ident("column_2"),
                    value: Expr::Identifier(ident("$2")),
                },
            ],
            selection: None,
        };

        ParamBinder
            .bind(
                &mut statement,
                &[
                    Expr::Value(Value::Number(BigDecimal::from(1))),
                    Expr::Value(Value::SingleQuotedString("abc".into())),
                ],
            )
            .unwrap();

        assert_eq!(
            statement.to_string(),
            "UPDATE schema_name.table_name SET column_1 = 1, column_2 = 'abc'"
        );
    }
}
