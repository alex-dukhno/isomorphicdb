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

#[derive(Debug, PartialEq, Clone)]
pub enum QueryTemplate {
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
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    Column(String),
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
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
    Boolean(bool),
    Null,
}
