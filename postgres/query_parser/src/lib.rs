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

use postgres_parser::{
    nodes, sys,
    sys::{LimitOption, SetOperation},
    Node, SqlStatementScanner,
};
use query_ast::{
    Assignment, BinaryOperator, ColumnDef, DataType, Definition, DeleteStatement, Expr, InsertStatement, Manipulation,
    Query, SelectItem, SelectStatement, SetExpr, Statement, UpdateStatement, Value, Values,
};

pub struct QueryParser;

impl QueryParser {
    pub const fn new() -> QueryParser {
        QueryParser
    }

    #[allow(clippy::result_unit_err)]
    pub fn parse(&self, sql: &str) -> Result<Statement, ()> {
        for scanned_query in SqlStatementScanner::new(sql).into_iter() {
            match scanned_query.parsetree.unwrap().unwrap() {
                Node::CreateSchemaStmt(nodes::CreateSchemaStmt {
                    schemaname: schema_name,
                    authrole: _auth_role,
                    schemaElts: _schema_elements,
                    if_not_exists,
                }) => {
                    return Ok(Statement::DDL(Definition::CreateSchema {
                        schema_name: schema_name.unwrap(),
                        if_not_exists,
                    }))
                }
                Node::CreateSeqStmt(_) => {}
                Node::CreateStatsStmt(_) => {}
                Node::CreateStmt(nodes::CreateStmt {
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
                }) => {
                    let mut columns = vec![];
                    for table_element in table_elements.unwrap() {
                        columns.push(self.process_column(table_element)?);
                    }
                    let table_name = table_name.unwrap();
                    return Ok(Statement::DDL(Definition::CreateTable {
                        if_not_exists,
                        schema_name: table_name.schemaname.unwrap_or_else(|| "public".to_owned()),
                        table_name: table_name.relname.unwrap(),
                        columns,
                    }));
                }
                Node::DropStmt(nodes::DropStmt {
                    objects,
                    removeType: remove_type,
                    behavior,
                    missing_ok,
                    concurrent: _concurrent,
                }) => {
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
                            return Ok(Statement::DDL(Definition::DropSchemas {
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
                            return Ok(Statement::DDL(Definition::DropTables {
                                names,
                                if_exists: missing_ok,
                                cascade: behavior == sys::DropBehavior::DROP_CASCADE,
                            }));
                        }
                        _ => unimplemented!(),
                    };
                }
                Node::IndexStmt(nodes::IndexStmt {
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
                }) => {
                    let mut column_names = vec![];
                    for index_param in index_params.unwrap() {
                        println!("INDEX PARAM - {:?}", index_param);
                        match index_param {
                            Node::IndexElem(nodes::IndexElem { name: Some(name), .. }) => column_names.push(name),
                            _ => unimplemented!(),
                        }
                    }
                    let table_name = table_name.unwrap();
                    return Ok(Statement::DDL(Definition::CreateIndex {
                        name: index_name.unwrap(),
                        table_name: (
                            table_name.schemaname.unwrap_or_else(|| "public".to_owned()),
                            table_name.relname.unwrap(),
                        ),
                        column_names,
                    }));
                }
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
                    return Ok(Statement::DML(Manipulation::Insert(InsertStatement {
                        schema_name,
                        table_name,
                        columns,
                        source: Box::new(Query {
                            body: SetExpr::Values(Values(values)),
                        }),
                    })));
                }
                Node::SelectStmt(nodes::SelectStmt {
                    distinctClause: None,
                    intoClause: None,
                    targetList: target_list,
                    fromClause: from_clause,
                    whereClause: None,
                    groupClause: None,
                    havingClause: None,
                    windowClause: None,
                    valuesLists: None,
                    sortClause: None,
                    limitOffset: None,
                    limitCount: None,
                    limitOption: LimitOption::LIMIT_OPTION_COUNT,
                    lockingClause: None,
                    withClause: None,
                    op: SetOperation::SETOP_NONE,
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
                    return Ok(Statement::DML(Manipulation::Select(SelectStatement {
                        select_items,
                        schema_name,
                        table_name,
                        where_clause: None,
                    })));
                }
                Node::UpdateStmt(nodes::UpdateStmt {
                    relation,
                    targetList: target_list,
                    whereClause: None,
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
                                column: name.unwrap(),
                                value: self.parse_expr(*val.unwrap()),
                            }),
                            _ => unimplemented!(),
                        }
                    }
                    return Ok(Statement::DML(Manipulation::Update(UpdateStatement {
                        schema_name,
                        table_name,
                        assignments,
                        where_clause: None,
                    })));
                }
                Node::DeleteStmt(nodes::DeleteStmt {
                    relation,
                    usingClause: None,
                    whereClause: None,
                    returningList: None,
                    withClause: None,
                }) => {
                    let relation = relation.unwrap();
                    let schema_name = relation.schemaname.unwrap_or_else(|| "public".to_owned());
                    let table_name = relation.relname.unwrap();
                    return Ok(Statement::DML(Manipulation::Delete(DeleteStatement {
                        schema_name,
                        table_name,
                        where_clause: None,
                    })));
                }
                node => unimplemented!("NODE is not processed {:?}", node),
            }
        }
        Err(())
    }

    fn process_column(&self, node: Node) -> Result<ColumnDef, ()> {
        if let Node::ColumnDef(column_def) = node {
            let data_type = self.process_type(*column_def.typeName.unwrap())?;
            Ok(ColumnDef {
                name: column_def.colname.unwrap(),
                data_type,
            })
        } else {
            Err(())
        }
    }

    fn process_type(&self, type_name: nodes::TypeName) -> Result<DataType, ()> {
        println!("TYPE NAME {:#?}", type_name);
        let name = type_name.names.unwrap();
        let mode = type_name.typmods;
        match &name[1] {
            Node::Value(nodes::Value { string, .. }) if string.as_deref() == Some("int2") => Ok(DataType::SmallInt),
            Node::Value(nodes::Value { string, .. }) if string.as_deref() == Some("int4") => Ok(DataType::Int),
            Node::Value(nodes::Value { string, .. }) if string.as_deref() == Some("int8") => Ok(DataType::BigInt),
            Node::Value(nodes::Value { string, .. }) if string.as_deref() == Some("float4") => Ok(DataType::Real),
            Node::Value(nodes::Value { string, .. }) if string.as_deref() == Some("float8") => Ok(DataType::Double),
            Node::Value(nodes::Value { string, .. }) if string.as_deref() == Some("bool") => Ok(DataType::Bool),
            Node::Value(nodes::Value { string, .. }) if string.as_deref() == Some("bpchar") => {
                match mode.as_ref().map(|inner| &inner[0]) {
                    Some(&Node::A_Const(nodes::A_Const {
                        val: nodes::Value { int: None, .. },
                    })) => Ok(DataType::Char(1)),
                    Some(&Node::A_Const(nodes::A_Const {
                        val: nodes::Value { int: Some(len), .. },
                    })) => Ok(DataType::Char(len as u32)),
                    _ => unimplemented!(),
                }
            }
            Node::Value(nodes::Value { string, .. }) if string.as_deref() == Some("varchar") => {
                match mode.as_ref().map(|inner| &inner[0]) {
                    None => Ok(DataType::VarChar(None)),
                    Some(&Node::A_Const(nodes::A_Const {
                        val: nodes::Value { int: Some(len), .. },
                    })) => Ok(DataType::VarChar(Some(len as u32))),
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
                name,
                lexpr: left_expr,
                rexpr: right_expr,
            }) => {
                if let Some(values) = name {
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
                        left: Box::new(self.parse_expr(*left_expr.unwrap())),
                        op,
                        right: Box::new(self.parse_expr(*right_expr.unwrap())),
                    }
                } else {
                    unimplemented!()
                }
            }
            Node::A_Const(nodes::A_Const {
                val: nodes::Value { int: Some(num), .. },
            }) => Expr::Value(Value::Number(num)),
            Node::A_Const(nodes::A_Const {
                val: nodes::Value {
                    string: Some(value), ..
                },
            }) => Expr::Value(Value::SingleQuotedString(value)),
            Node::ParamRef(nodes::ParamRef { number }) => Expr::Value(Value::Param(number as u32)),
            _ => unimplemented!(),
        }
    }

    fn parse_const(&self, node: nodes::A_Const) -> Expr {
        match node {
            nodes::A_Const {
                val: nodes::Value {
                    string: Some(value), ..
                },
            } => Expr::Value(Value::SingleQuotedString(value)),
            nodes::A_Const {
                val: nodes::Value { int: Some(value), .. },
            } => Expr::Value(Value::Number(value)),
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests;
