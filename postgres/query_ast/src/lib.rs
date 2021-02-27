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

use std::fmt::Debug;

#[derive(Debug, PartialEq)]
pub enum Statement {
    DDL(Definition),
    DML(Manipulation),
}

#[derive(Debug, PartialEq)]
pub enum Definition {
    CreateSchema {
        schema_name: String,
        if_not_exists: bool,
    },
    CreateTable {
        schema_name: String,
        table_name: String,
        columns: Vec<ColumnDef>,
        if_not_exists: bool,
    },
    CreateIndex {
        name: String,
        table_name: (String, String),
        column_names: Vec<String>,
    },
    DropSchemas {
        names: Vec<String>,
        if_exists: bool,
        cascade: bool,
    },
    DropTables {
        names: Vec<(String, String)>,
        if_exists: bool,
        cascade: bool,
    },
}

#[derive(Debug, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, PartialEq)]
pub enum DataType {
    SmallInt,
    Int,
    BigInt,
    Char(u32),
    VarChar(Option<u32>),
    Real,
    Double,
    Bool,
}

#[derive(Debug, PartialEq)]
pub enum ObjectType {
    Schema,
    Table,
}

#[derive(Debug, PartialEq)]
pub enum Manipulation {
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Select(SelectStatement),
}

#[derive(Debug, PartialEq)]
pub struct InsertStatement {
    pub schema_name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub source: Box<Query>,
}

#[derive(Debug, PartialEq)]
pub struct UpdateStatement {
    pub schema_name: String,
    pub table_name: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

#[derive(Debug, PartialEq)]
pub struct DeleteStatement {
    pub schema_name: String,
    pub table_name: String,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, PartialEq)]
pub struct SelectStatement {
    pub select_items: Vec<SelectItem>,
    pub schema_name: String,
    pub table_name: String,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, PartialEq)]
pub enum SelectItem {
    Wildcard,
}

#[derive(Debug, PartialEq)]
pub struct Query {
    //// WITH (common table expressions, or CTEs)
    // pub ctes: Vec<Cte>,
    //// SELECT or UNION / EXCEPT / INTECEPT
    pub body: SetExpr,
    //// ORDER BY
    // pub order_by: Vec<OrderByExpr>,
    //// `LIMIT { <N> | ALL }`
    // pub limit: Option<Expr>,
    //// `OFFSET <N> [ { ROW | ROWS } ]`
    // pub offset: Option<Offset>,
    //// `FETCH { FIRST | NEXT } <N> [ PERCENT ] { ROW | ROWS } | { ONLY | WITH TIES }`
    // pub fetch: Option<Fetch>,
}

/// A node in a tree, representing a "query body" expression, roughly:
/// `SELECT ... [ {UNION|EXCEPT|INTERSECT} SELECT ...]`
#[derive(Debug, PartialEq)]
pub enum SetExpr {
    // /// Restricted SELECT .. FROM .. HAVING (no ORDER BY or set operations)
    // Select(Box<Select>),
    // /// Parenthesized SELECT subquery, which may include more set operations
    // /// in its body and an optional ORDER BY / LIMIT.
    // Query(Box<Query>),
    // /// UNION/EXCEPT/INTERSECT of two queries
    // SetOperation {
    //     op: SetOperator,
    //     all: bool,
    //     left: Box<SetExpr>,
    //     right: Box<SetExpr>,
    // },
    Values(Values),
}

#[derive(Debug, PartialEq)]
pub struct Values(pub Vec<Vec<Expr>>);

#[derive(Debug, PartialEq)]
pub enum Expr {
    /// A literal value, such as string, number, date or NULL
    Value(Value),
    /// Binary operation e.g. `1 + 1` or `foo > bar`
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
}

#[derive(Debug, PartialEq)]
pub enum BinaryOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulus,
    Exp,
    StringConcat,
    Gt,
    Lt,
    GtEq,
    LtEq,
    Eq,
    NotEq,
    And,
    Or,
    Like,
    NotLike,
    BitwiseOr,
    BitwiseAnd,
    BitwiseXor,
    BitwiseShiftLeft,
    BitwiseShiftRight,
}

#[derive(Debug, PartialEq)]
pub enum Value {
    Number(i32),
    SingleQuotedString(String),
    Param(u32),
}
