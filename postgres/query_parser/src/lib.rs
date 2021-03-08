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

use postgres_parser::{nodes, sys, Node, PgParserError, SqlStatementScanner};
use query_ast::{
    Assignment, BinaryOperator, ColumnDef, DataType, Definition, DeleteStatement, Expr, Extended, InsertSource,
    InsertStatement, Query, SelectItem, SelectStatement, Set, Statement, UnaryOperator, UpdateStatement, Value, Values,
};
use query_response::QueryError;
use std::fmt::{self, Display, Formatter};

pub struct QueryParser;

impl QueryParser {
    pub const fn new() -> QueryParser {
        QueryParser
    }

    pub fn parse(&self, sql: &str) -> Result<Vec<Statement>, ParserError> {
        let mut statements = vec![];
        for scanned_query in SqlStatementScanner::new(sql).into_iter() {
            match scanned_query.parsetree {
                Err(error) => return Err(ParserError::from(error)),
                Ok(None) => unimplemented!(),
                Ok(Some(Node::CreateSchemaStmt(nodes::CreateSchemaStmt {
                    schemaname: schema_name,
                    authrole: _auth_role,
                    schemaElts: _schema_elements,
                    if_not_exists,
                }))) => statements.push(Statement::DDL(Definition::CreateSchema {
                    schema_name: schema_name.unwrap(),
                    if_not_exists,
                })),
                Ok(Some(Node::CreateStmt(nodes::CreateStmt {
                    relation: table_name,
                    tableElts: table_elements,
                    inhRelations: _inheritance_tables,
                    partbound: _partition_bound,
                    partspec: _partition_spec,
                    ofTypename: _type_name,
                    constraints: _constraints,
                    options: _options,
                    oncommit: _on_commit,
                    tablespacename: _table_space_name,
                    accessMethod: _access_method,
                    if_not_exists,
                }))) => {
                    let mut columns = vec![];
                    for table_element in table_elements.unwrap_or_else(Vec::new) {
                        columns.push(self.process_column(table_element));
                    }
                    let table_name = table_name.unwrap();
                    statements.push(Statement::DDL(Definition::CreateTable {
                        if_not_exists,
                        schema_name: table_name.schemaname.unwrap_or_else(|| "public".to_owned()),
                        table_name: table_name.relname.unwrap(),
                        columns,
                    }));
                }
                Ok(Some(Node::DropStmt(nodes::DropStmt {
                    objects,
                    removeType: remove_type,
                    behavior,
                    missing_ok,
                    concurrent: _concurrent,
                }))) => {
                    match remove_type {
                        sys::ObjectType::OBJECT_SCHEMA => {
                            let mut names = vec![];
                            for object in objects.unwrap() {
                                println!("OBJECT - {:?}", object);
                                match object {
                                    Node::Value(nodes::Value { string: Some(name), .. }) => names.push(name),
                                    _ => unimplemented!(),
                                }
                            }
                            statements.push(Statement::DDL(Definition::DropSchemas {
                                names,
                                if_exists: missing_ok,
                                cascade: behavior == sys::DropBehavior::DROP_CASCADE,
                            }));
                        }
                        sys::ObjectType::OBJECT_TABLE => {
                            let mut names = vec![];
                            for object in objects.unwrap() {
                                println!("OBJECT - {:?}", object);
                                match object {
                                    Node::List(mut values) => {
                                        if values.len() == 1 {
                                            match values.pop() {
                                                Some(Node::Value(nodes::Value { string: Some(name), .. })) => {
                                                    names.push(("public".to_owned(), name))
                                                }
                                                _ => unimplemented!(),
                                            }
                                        } else if values.len() == 2 {
                                            match (values.pop(), values.pop()) {
                                                (
                                                    Some(Node::Value(nodes::Value { string: Some(name), .. })),
                                                    Some(Node::Value(nodes::Value {
                                                        string: Some(schema), ..
                                                    })),
                                                ) => names.push((schema, name)),
                                                _ => unimplemented!(),
                                            }
                                        } else {
                                            unimplemented!()
                                        }
                                    }
                                    _ => unimplemented!(),
                                }
                            }
                            statements.push(Statement::DDL(Definition::DropTables {
                                names,
                                if_exists: missing_ok,
                                cascade: behavior == sys::DropBehavior::DROP_CASCADE,
                            }));
                        }
                        _ => unimplemented!(),
                    };
                }
                Ok(Some(Node::IndexStmt(nodes::IndexStmt {
                    idxname: index_name,
                    relation: table_name,
                    accessMethod: _access_method,
                    tableSpace: _table_space,
                    indexParams: index_params,
                    indexIncludingParams: _index_including_params,
                    options: _options,
                    whereClause: _where_clause,
                    excludeOpNames: _exclude_op_names,
                    idxcomment: _index_comment,
                    indexOid: _index_oid,
                    oldNode: _old_node,
                    oldCreateSubid: _old_create_sub_id,
                    oldFirstRelfilenodeSubid: _old_first_rel_file_node_sub_id,
                    unique: _unique,
                    primary: _primary,
                    isconstraint: _is_constraint,
                    deferrable: _deferrable,
                    initdeferred: _init_deferred,
                    transformed: _transformed,
                    concurrent: _concurrent,
                    if_not_exists: _if_not_exists,
                    reset_default_tblspc: _reset_default_table_space,
                }))) => {
                    let mut column_names = vec![];
                    for index_param in index_params.unwrap() {
                        println!("INDEX PARAM - {:?}", index_param);
                        match index_param {
                            Node::IndexElem(nodes::IndexElem { name: Some(name), .. }) => {
                                column_names.push(name.to_lowercase())
                            }
                            _ => unimplemented!(),
                        }
                    }
                    let table_name = table_name.unwrap();
                    statements.push(Statement::DDL(Definition::CreateIndex {
                        name: index_name.unwrap(),
                        schema_name: table_name.schemaname.unwrap_or_else(|| "public".to_owned()),
                        table_name: table_name.relname.unwrap(),
                        column_names,
                    }));
                }
                Ok(Some(insert @ Node::InsertStmt(_))) => {
                    statements.push(Statement::DML(self.process_query(insert)));
                }
                Ok(Some(select @ Node::SelectStmt(_))) => {
                    statements.push(Statement::DML(self.process_query(select)));
                }
                Ok(Some(update @ Node::UpdateStmt(_))) => {
                    statements.push(Statement::DML(self.process_query(update)));
                }
                Ok(Some(delete @ Node::DeleteStmt(_))) => {
                    statements.push(Statement::DML(self.process_query(delete)));
                }
                Ok(Some(Node::VariableSetStmt(nodes::VariableSetStmt { name, .. }))) => {
                    statements.push(Statement::Config(Set {
                        variable: name.unwrap(),
                        value: "value".to_owned(),
                    }))
                }
                Ok(Some(Node::PrepareStmt(nodes::PrepareStmt {
                    name: Some(name),
                    argtypes: Some(arg_types),
                    query: Some(query),
                }))) => {
                    let mut param_types = vec![];
                    for arg_type in arg_types {
                        match arg_type {
                            Node::TypeName(type_name) => param_types.push(self.process_type(type_name)),
                            _ => unimplemented!(),
                        }
                    }
                    statements.push(Statement::Extended(Extended::Prepare {
                        name,
                        param_types,
                        query: self.process_query(*query),
                    }))
                }
                Ok(Some(Node::ExecuteStmt(nodes::ExecuteStmt {
                    name: Some(name),
                    params: Some(params),
                }))) => {
                    let mut param_values = vec![];
                    for param in params {
                        match self.parse_expr(param) {
                            Expr::Value(value) => param_values.push(value),
                            other => unreachable!("{:?} could not be used as parameter", other),
                        }
                    }
                    statements.push(Statement::Extended(Extended::Execute { name, param_values }))
                }
                Ok(Some(Node::DeallocateStmt(nodes::DeallocateStmt { name: Some(name) }))) => {
                    statements.push(Statement::Extended(Extended::Deallocate { name }))
                }
                Ok(Some(node)) => unimplemented!("NODE is not processed {:?}", node),
            }
        }
        Ok(statements)
    }

    fn process_query(&self, node: Node) -> Query {
        match node {
            Node::InsertStmt(nodes::InsertStmt {
                relation,
                cols,
                selectStmt: select_statement,
                onConflictClause: _on_conflict_clause,
                returningList: _return_list,
                withClause: _with_clause,
                override_: _override,
            }) => {
                let relation = relation.unwrap();
                let schema_name = relation.schemaname.unwrap_or_else(|| "public".to_owned());
                let table_name = relation.relname.unwrap();
                let mut columns = vec![];
                for col in cols.unwrap_or_else(Vec::new) {
                    println!("COL {:?}", col);
                    match col {
                        Node::ResTarget(nodes::ResTarget {
                            name: Some(col_name), ..
                        }) => {
                            columns.push(col_name);
                        }
                        _ => unimplemented!(),
                    }
                }
                println!("SELECT STMT - {:?}", select_statement);
                let mut values = vec![];
                if let Some(stmt) = select_statement {
                    if let Node::SelectStmt(nodes::SelectStmt {
                        valuesLists: Some(lists),
                        ..
                    }) = *stmt
                    {
                        for list in lists {
                            if let Node::List(list) = list {
                                let mut row = vec![];
                                for raw_value in list {
                                    row.push(self.parse_expr(raw_value));
                                }
                                values.push(row);
                            }
                        }
                    } else {
                        unimplemented!()
                    }
                }
                Query::Insert(InsertStatement {
                    schema_name,
                    table_name,
                    columns,
                    source: InsertSource::Values(Values(values)),
                })
            }
            Node::SelectStmt(nodes::SelectStmt {
                distinctClause: None,
                intoClause: None,
                targetList: target_list,
                fromClause: from_clause,
                whereClause: where_clause,
                groupClause: None,
                havingClause: None,
                windowClause: None,
                valuesLists: None,
                sortClause: None,
                limitOffset: None,
                limitCount: None,
                limitOption: sys::LimitOption::LIMIT_OPTION_COUNT,
                lockingClause: None,
                withClause: None,
                op: sys::SetOperation::SETOP_NONE,
                all: false,
                larg: None,
                rarg: None,
            }) => {
                println!("TARGET LIST {:?}", target_list);
                let mut select_items = vec![];
                for target in target_list.unwrap() {
                    match target {
                        Node::ResTarget(nodes::ResTarget { val: Some(ident), .. }) => match *ident {
                            Node::ColumnRef(nodes::ColumnRef {
                                fields: Some(fields), ..
                            }) => {
                                for field in fields {
                                    match field {
                                        Node::A_Star(_) => select_items.push(SelectItem::Wildcard),
                                        Node::Value(nodes::Value {
                                            string: Some(col_name), ..
                                        }) => select_items.push(SelectItem::UnnamedExpr(Expr::Column(col_name))),
                                        _ => unimplemented!(),
                                    }
                                }
                            }
                            _ => unimplemented!(),
                        },
                        _ => unimplemented!(),
                    }
                }
                let (schema_name, table_name) = match from_clause.unwrap().pop() {
                    Some(Node::RangeVar(nodes::RangeVar {
                        schemaname: schema_name,
                        relname: table_name,
                        ..
                    })) => (schema_name.unwrap(), table_name.unwrap()),
                    _ => unimplemented!(),
                };
                Query::Select(SelectStatement {
                    select_items,
                    schema_name,
                    table_name,
                    where_clause: where_clause.map(|expr| self.parse_expr(*expr)),
                })
            }
            Node::UpdateStmt(nodes::UpdateStmt {
                relation,
                targetList: target_list,
                whereClause: where_clause,
                fromClause: None,
                returningList: None,
                withClause: None,
            }) => {
                let relation = relation.unwrap();
                let schema_name = relation.schemaname.unwrap_or_else(|| "public".to_owned());
                let table_name = relation.relname.unwrap();
                let mut assignments = vec![];
                for target in target_list.unwrap() {
                    println!("{:?}", target);
                    match target {
                        Node::ResTarget(nodes::ResTarget { name, val, .. }) => assignments.push(Assignment {
                            column: name.unwrap().to_lowercase(),
                            value: self.parse_expr(*val.unwrap()),
                        }),
                        _ => unimplemented!(),
                    }
                }
                Query::Update(UpdateStatement {
                    schema_name,
                    table_name,
                    assignments,
                    where_clause: where_clause.map(|expr| self.parse_expr(*expr)),
                })
            }
            Node::DeleteStmt(nodes::DeleteStmt {
                relation,
                usingClause: None,
                whereClause: where_clause,
                returningList: None,
                withClause: None,
            }) => {
                let relation = relation.unwrap();
                let schema_name = relation.schemaname.unwrap_or_else(|| "public".to_owned());
                let table_name = relation.relname.unwrap();
                Query::Delete(DeleteStatement {
                    schema_name,
                    table_name,
                    where_clause: where_clause.map(|expr| self.parse_expr(*expr)),
                })
            }
            other => unimplemented!("NOT IMPL: {:?}", other),
        }
    }

    fn process_column(&self, node: Node) -> ColumnDef {
        if let Node::ColumnDef(column_def) = node {
            let data_type = self.process_type(*column_def.typeName.unwrap());
            ColumnDef {
                name: column_def.colname.unwrap().to_lowercase(),
                data_type,
            }
        } else {
            unimplemented!()
        }
    }

    fn process_type(&self, type_name: nodes::TypeName) -> DataType {
        println!("TYPE NAME {:#?}", type_name);
        let name = type_name.names.unwrap();
        let mode = type_name.typmods;
        match &name[1] {
            Node::Value(nodes::Value { string, .. }) if string.as_deref() == Some("int2") => DataType::SmallInt,
            Node::Value(nodes::Value { string, .. }) if string.as_deref() == Some("int4") => DataType::Int,
            Node::Value(nodes::Value { string, .. }) if string.as_deref() == Some("int8") => DataType::BigInt,
            Node::Value(nodes::Value { string, .. }) if string.as_deref() == Some("float4") => DataType::Real,
            Node::Value(nodes::Value { string, .. }) if string.as_deref() == Some("float8") => DataType::Double,
            Node::Value(nodes::Value { string, .. }) if string.as_deref() == Some("bool") => DataType::Bool,
            Node::Value(nodes::Value { string, .. }) if string.as_deref() == Some("bpchar") => {
                match mode.as_ref().map(|inner| &inner[0]) {
                    Some(&Node::A_Const(nodes::A_Const {
                        val: nodes::Value { int: None, .. },
                    })) => DataType::Char(1),
                    Some(&Node::A_Const(nodes::A_Const {
                        val: nodes::Value { int: Some(len), .. },
                    })) => DataType::Char(len as u32),
                    _ => unimplemented!(),
                }
            }
            Node::Value(nodes::Value { string, .. }) if string.as_deref() == Some("varchar") => {
                match mode.as_ref().map(|inner| &inner[0]) {
                    None => DataType::VarChar(None),
                    Some(&Node::A_Const(nodes::A_Const {
                        val: nodes::Value { int: Some(len), .. },
                    })) => DataType::VarChar(Some(len as u32)),
                    _ => unimplemented!(),
                }
            }
            _ => unimplemented!(),
        }
    }

    fn parse_expr(&self, node: Node) -> Expr {
        println!("NODE {:?}", node);
        match node {
            Node::BoolExpr(nodes::BoolExpr { boolop: bool_op, args }) => {
                let op = match bool_op {
                    sys::BoolExprType::AND_EXPR => BinaryOperator::And,
                    sys::BoolExprType::OR_EXPR => BinaryOperator::Or,
                    sys::BoolExprType::NOT_EXPR => unimplemented!(),
                };
                if let Some(mut values) = args {
                    let right = match values.pop() {
                        Some(Node::A_Const(constant)) => self.parse_const(constant),
                        _ => unimplemented!(),
                    };
                    let left = match values.pop() {
                        Some(Node::A_Const(constant)) => self.parse_const(constant),
                        _ => unimplemented!(),
                    };
                    Expr::BinaryOp {
                        left: Box::new(left),
                        op,
                        right: Box::new(right),
                    }
                } else {
                    unimplemented!()
                }
            }
            Node::A_Expr(nodes::A_Expr {
                kind: sys::A_Expr_Kind::AEXPR_LIKE,
                name,
                lexpr: left_expr,
                rexpr: right_expr,
            }) => {
                if let Some(values) = name {
                    let op = if let Node::Value(nodes::Value { string: Some(op), .. }) = &values[0] {
                        match op.as_str() {
                            "~~" => BinaryOperator::Like,
                            "!~~" => BinaryOperator::NotLike,
                            _ => unimplemented!(),
                        }
                    } else {
                        unimplemented!()
                    };
                    Expr::BinaryOp {
                        left: Box::new(self.parse_expr(*left_expr.unwrap())),
                        op,
                        right: Box::new(self.parse_expr(*right_expr.unwrap())),
                    }
                } else {
                    unimplemented!()
                }
            }
            Node::A_Expr(nodes::A_Expr {
                kind: sys::A_Expr_Kind::AEXPR_OP,
                name: Some(values),
                lexpr: None,
                rexpr: Some(right_expr),
            }) => {
                let op = if let Node::Value(nodes::Value { string: Some(op), .. }) = &values[0] {
                    match op.as_str() {
                        "+" => UnaryOperator::Plus,
                        "-" => UnaryOperator::Minus,
                        "!" => UnaryOperator::Not,
                        "~" => UnaryOperator::BitwiseNot,
                        "|/" => UnaryOperator::SquareRoot,
                        "||/" => UnaryOperator::CubeRoot,
                        "!!" => UnaryOperator::PrefixFactorial,
                        "@" => UnaryOperator::Abs,
                        _ => unimplemented!(),
                    }
                } else {
                    unimplemented!()
                };
                Expr::UnaryOp {
                    op,
                    expr: Box::new(self.parse_expr(*right_expr)),
                }
            }
            Node::A_Expr(nodes::A_Expr {
                kind: sys::A_Expr_Kind::AEXPR_OP,
                name: Some(values),
                lexpr: Some(left_expr),
                rexpr: None,
            }) => {
                let op = if let Node::Value(nodes::Value { string: Some(op), .. }) = &values[0] {
                    match op.as_str() {
                        "!" => UnaryOperator::PostfixFactorial,
                        _ => unimplemented!(),
                    }
                } else {
                    unimplemented!()
                };
                Expr::UnaryOp {
                    op,
                    expr: Box::new(self.parse_expr(*left_expr)),
                }
            }
            Node::A_Expr(nodes::A_Expr {
                kind: sys::A_Expr_Kind::AEXPR_OP,
                name: Some(values),
                lexpr: Some(left_expr),
                rexpr: Some(right_expr),
            }) => {
                let op = if let Node::Value(nodes::Value { string: Some(op), .. }) = &values[0] {
                    match op.as_str() {
                        "+" => BinaryOperator::Plus,
                        "-" => BinaryOperator::Minus,
                        "*" => BinaryOperator::Multiply,
                        "/" => BinaryOperator::Divide,
                        "%" => BinaryOperator::Modulus,
                        "^" => BinaryOperator::Exp,
                        "||" => BinaryOperator::StringConcat,
                        ">" => BinaryOperator::Gt,
                        "<" => BinaryOperator::Lt,
                        ">=" => BinaryOperator::GtEq,
                        "<=" => BinaryOperator::LtEq,
                        "=" => BinaryOperator::Eq,
                        "<>" => BinaryOperator::NotEq,
                        "|" => BinaryOperator::BitwiseOr,
                        "&" => BinaryOperator::BitwiseAnd,
                        "#" => BinaryOperator::BitwiseXor,
                        "<<" => BinaryOperator::BitwiseShiftLeft,
                        ">>" => BinaryOperator::BitwiseShiftRight,
                        _ => unimplemented!(),
                    }
                } else {
                    unimplemented!()
                };
                Expr::BinaryOp {
                    left: Box::new(self.parse_expr(*left_expr)),
                    op,
                    right: Box::new(self.parse_expr(*right_expr)),
                }
            }
            Node::A_Const(nodes::A_Const {
                val: nodes::Value { int: Some(int), .. },
            }) => Expr::Value(Value::Int(int)),
            Node::A_Const(nodes::A_Const {
                val: nodes::Value { float: Some(num), .. },
            }) => Expr::Value(Value::Number(num)),
            Node::A_Const(nodes::A_Const {
                val: nodes::Value {
                    string: Some(value), ..
                },
            }) => Expr::Value(Value::String(value)),
            Node::ParamRef(nodes::ParamRef { number }) => Expr::Value(Value::Param(number as u32)),
            Node::ColumnRef(nodes::ColumnRef {
                fields: Some(mut values),
            }) => match values.pop() {
                Some(Node::Value(nodes::Value { string: Some(name), .. })) => Expr::Column(name.to_lowercase()),
                _ => unimplemented!(),
            },
            Node::TypeCast(nodes::TypeCast {
                arg: Some(expr),
                typeName: Some(type_name),
            }) => Expr::Cast {
                expr: Box::new(self.parse_expr(*expr)),
                data_type: self.process_type(*type_name),
            },
            _ => unimplemented!(),
        }
    }

    fn parse_const(&self, node: nodes::A_Const) -> Expr {
        match node {
            nodes::A_Const {
                val: nodes::Value {
                    string: Some(value), ..
                },
            } => Expr::Value(Value::String(value)),
            nodes::A_Const {
                val: nodes::Value { int: Some(value), .. },
            } => Expr::Value(Value::Int(value)),
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ParserError {
    error: PgParserError,
}

impl From<PgParserError> for ParserError {
    fn from(error: PgParserError) -> ParserError {
        ParserError { error }
    }
}

impl From<ParserError> for QueryError {
    fn from(error: ParserError) -> QueryError {
        QueryError::syntax_error(error)
    }
}

impl Display for ParserError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.error)
    }
}

#[cfg(test)]
mod tests;
