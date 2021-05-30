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

use std::fmt::{self, Display, Formatter};

#[derive(Debug, PartialEq)]
pub enum Request {
    Transaction(Transaction),
    Config(Set),
    Statement(Statement),
}

#[derive(Debug, PartialEq)]
pub enum Statement {
    Definition(Definition),
    Query(Query),
    Extended(Extended),
}

impl Display for Statement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
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
        schema_name: String,
        table_name: String,
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

#[derive(Debug, PartialEq, Clone, Copy)]
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

#[derive(Debug, PartialEq, Clone)]
pub enum Query {
    Insert(InsertQuery),
    Update(UpdateQuery),
    Delete(DeleteQuery),
    Select(SelectQuery),
}

#[derive(Debug, PartialEq, Clone)]
pub struct InsertQuery {
    pub schema_name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub source: InsertSource,
}

#[derive(Debug, PartialEq, Clone)]
pub struct UpdateQuery {
    pub schema_name: String,
    pub table_name: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

#[derive(Debug, PartialEq, Clone)]
pub struct DeleteQuery {
    pub schema_name: String,
    pub table_name: String,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SelectQuery {
    pub select_items: Vec<SelectItem>,
    pub schema_name: String,
    pub table_name: String,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum SelectItem {
    Wildcard,
    UnnamedExpr(Expr),
}

#[derive(Debug, PartialEq, Clone)]
pub enum InsertSource {
    Values(Values),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Values(pub Vec<Vec<Expr>>);

#[derive(Debug, PartialEq, Clone)]
pub enum Expr {
    Value(Value),
    Param(u32),
    BinaryOp { left: Box<Expr>, op: BinaryOperator, right: Box<Expr> },
    UnaryOp { op: UnaryOperator, expr: Box<Expr> },
    Column(String),
    Cast { expr: Box<Expr>, data_type: DataType },
}

#[derive(Debug, PartialEq, Clone, Copy)]
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

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum UnaryOperator {
    Minus,
    Plus,
    Not,
    BitwiseNot,
    SquareRoot,
    CubeRoot,
    PostfixFactorial,
    PrefixFactorial,
    Abs,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    Int(i32),
    Number(String),
    String(String),
    Null,
}

#[derive(Debug, PartialEq)]
pub struct Set {
    pub variable: String,
    pub value: String,
}

#[derive(Debug, PartialEq)]
pub enum Extended {
    Prepare {
        query: Query,
        name: String,
        param_types: Vec<DataType>,
    },
    Execute {
        name: String,
        param_values: Vec<Value>,
    },
    Deallocate {
        name: String,
    },
}

#[derive(Debug, PartialEq)]
pub enum Transaction {
    Begin,
    Commit,
}
