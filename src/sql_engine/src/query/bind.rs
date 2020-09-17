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

use std::sync::Arc;

use bigdecimal::BigDecimal;
use sqlparser::ast::{Assignment, Expr, Ident, ObjectType, Query, SetExpr, Statement, Value};

use protocol::{pgsql_types::PostgreSqlValue, results::QueryError, Sender};

type Result = std::result::Result<(), ()>;

pub(crate) struct ParamBinder {
    sender: Arc<dyn Sender>,
}

impl ParamBinder {
    pub fn new(sender: Arc<dyn Sender>) -> Self {
        Self { sender }
    }

    /// Replaces the parameters of prepared statement with values.
    ///
    /// TODO:
    /// Only two SQL formats has been supported to bind parameters as below.
    ///     `insert into schema_name.table_name values ($1, 1), ($2, 2)`
    ///     `update schema_name.table_name set col1 = $1, col2 = $2`
    /// Needs to support other statements (as `select` and `delete`) and other
    /// expressions in SQL (as `BinaryOp` and `UnaryOp` in `where` statement).
    pub fn bind(&self, stmt: &mut Statement, params: &[PostgreSqlValue]) -> Result {
        match stmt {
            Statement::Insert { .. } => bind_insert(stmt, params),
            Statement::Update { .. } => bind_update(stmt, params),
            Statement::SetVariable { .. } => Ok(()),
            Statement::CreateSchema { .. } => Ok(()),
            Statement::CreateTable { .. } => Ok(()),
            Statement::Drop { object_type, .. } if *object_type == ObjectType::Schema => Ok(()),
            Statement::Drop { object_type, .. } if *object_type == ObjectType::Table => Ok(()),
            _ => {
                self.sender
                    .send(Err(QueryError::feature_not_supported(format!(
                        "Bind parameters is not supported on SQL `{}`",
                        stmt
                    ))))
                    .expect("To Send Bind Error");
                Err(())
            }
        }
    }
}

fn bind_insert(stmt: &mut Statement, params: &[PostgreSqlValue]) -> Result {
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

fn bind_update(stmt: &mut Statement, params: &[PostgreSqlValue]) -> Result {
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

fn pg_value_to_expr(value: &PostgreSqlValue) -> Expr {
    match value {
        PostgreSqlValue::Null => Expr::Value(Value::Null),
        PostgreSqlValue::True => Expr::Value(Value::Boolean(true)),
        PostgreSqlValue::False => Expr::Value(Value::Boolean(false)),
        PostgreSqlValue::Int16(i) => Expr::Value(Value::Number(BigDecimal::from(*i))),
        PostgreSqlValue::Int32(i) => Expr::Value(Value::Number(BigDecimal::from(*i))),
        PostgreSqlValue::Int64(i) => Expr::Value(Value::Number(BigDecimal::from(*i))),
        PostgreSqlValue::String(s) => Expr::Value(Value::SingleQuotedString(s.into())),
    }
}

fn replace_expr_with_params(expr: &mut Expr, params: &[PostgreSqlValue]) {
    let value = match expr {
        Expr::Identifier(Ident { value, .. }) => value,
        _ => return,
    };

    let index = match parse_param_index(value) {
        Some(index) => index,
        _ => return,
    };

    if index < params.len() {
        *expr = pg_value_to_expr(&params[index]);
    }
}
